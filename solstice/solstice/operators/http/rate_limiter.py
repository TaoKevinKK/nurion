# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Rate limiting with local token bucket and global coordination.

Architecture:
- GlobalRateLimiter: Ray actor that distributes tokens to workers
- LocalRateLimiter: Per-worker token bucket that refills from global limiter

This avoids per-request Ray calls by:
1. Workers request tokens in batches from global limiter
2. Local token bucket handles most acquire/release without remote calls
3. Background refill task periodically requests more tokens
"""

import asyncio
import logging
import time
from typing import Optional

import ray


class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded and request cannot proceed."""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: float = 0):
        super().__init__(message)
        self.retry_after = retry_after


@ray.remote(num_cpus=0)
class GlobalRateLimiter:
    """Global rate limiter that distributes tokens to workers.

    Workers request tokens in batches, reducing Ray call overhead.
    Uses a token bucket algorithm for RPS limiting.

    Usage:
        limiter = GlobalRateLimiter.options(
            name="my_limiter",
            get_if_exists=True,
        ).remote(max_concurrent=100, requests_per_second=50.0)

        # Workers request tokens in batches
        tokens = await limiter.request_tokens.remote(batch_size=10)
    """

    def __init__(
        self,
        max_concurrent: int = 100,
        requests_per_second: float = 0,
    ):
        """Initialize global rate limiter.

        Args:
            max_concurrent: Maximum concurrent requests across all workers
            requests_per_second: Maximum RPS (0 = unlimited)
        """
        self._max_concurrent = max_concurrent
        self._rps = requests_per_second
        self._current_concurrent = 0

        # Token bucket for RPS limiting
        self._tokens = float(max_concurrent) if max_concurrent > 0 else 100.0
        self._last_refill = time.time()

        # Stats
        self._total_granted = 0
        self._total_returned = 0

    def _refill_tokens(self) -> None:
        """Refill tokens based on time elapsed."""
        if self._rps <= 0:
            return

        now = time.time()
        elapsed = now - self._last_refill
        self._last_refill = now

        new_tokens = elapsed * self._rps
        max_tokens = float(self._max_concurrent) if self._max_concurrent > 0 else 1000.0
        self._tokens = min(max_tokens, self._tokens + new_tokens)

    def request_tokens(self, count: int) -> int:
        """Request tokens from the global pool.

        Args:
            count: Number of tokens requested

        Returns:
            Number of tokens actually granted (may be less than requested)
        """
        self._refill_tokens()

        # Calculate available tokens
        available = int(self._tokens)
        if self._max_concurrent > 0:
            concurrent_available = self._max_concurrent - self._current_concurrent
            available = min(available, concurrent_available)

        # Grant up to requested amount
        granted = min(count, max(0, available))
        if granted > 0:
            self._tokens -= granted
            self._current_concurrent += granted
            self._total_granted += granted

        return granted

    def return_tokens(self, count: int) -> None:
        """Return unused tokens to the global pool.

        Args:
            count: Number of tokens to return
        """
        if count > 0:
            self._current_concurrent = max(0, self._current_concurrent - count)
            self._total_returned += count
            # Add back to token bucket (capped at max)
            if self._max_concurrent > 0:
                self._tokens = min(float(self._max_concurrent), self._tokens + count)

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        self._refill_tokens()
        return {
            "max_concurrent": self._max_concurrent,
            "current_concurrent": self._current_concurrent,
            "available_tokens": self._tokens,
            "rps": self._rps,
            "total_granted": self._total_granted,
            "total_returned": self._total_returned,
        }


class LocalRateLimiter:
    """Per-worker local rate limiter with background refill.

    Maintains a local token bucket that refills from the global limiter.
    Most operations are local, only refill calls the global limiter.

    Usage:
        local_limiter = LocalRateLimiter(global_limiter, batch_size=20)
        await local_limiter.start()

        # Fast local acquire (no Ray call)
        if local_limiter.acquire():
            try:
                await do_request()
            finally:
                local_limiter.release()

        await local_limiter.stop()
    """

    def __init__(
        self,
        global_limiter: ray.actor.ActorHandle,
        batch_size: int = 20,
        refill_interval: float = 1.0,
        low_watermark: int = 5,
    ):
        """Initialize local rate limiter.

        Args:
            global_limiter: Handle to GlobalRateLimiter actor
            batch_size: Number of tokens to request at once
            refill_interval: Seconds between refill attempts
            low_watermark: Trigger refill when tokens drop below this
        """
        self._global = global_limiter
        self._batch_size = batch_size
        self._refill_interval = refill_interval
        self._low_watermark = low_watermark

        # Local state
        self._tokens = 0
        self._in_flight = 0  # Requests currently using tokens
        self._lock = asyncio.Lock()

        # Background task
        self._refill_task: Optional[asyncio.Task] = None
        self._running = False

        self._logger = logging.getLogger("LocalRateLimiter")

    async def start(self) -> None:
        """Start the background refill task."""
        if self._running:
            return

        self._running = True

        # Initial token acquisition
        await self._refill()

        # Start background refill task
        self._refill_task = asyncio.create_task(self._refill_loop())

    async def stop(self) -> None:
        """Stop the limiter and return unused tokens."""
        self._running = False

        if self._refill_task:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            self._refill_task = None

        # Return unused tokens
        async with self._lock:
            if self._tokens > 0:
                try:
                    self._global.return_tokens.remote(self._tokens)
                except Exception as e:
                    self._logger.warning(f"Failed to return tokens: {e}")
                self._tokens = 0

    async def _refill_loop(self) -> None:
        """Background loop that refills tokens when low."""
        while self._running:
            try:
                await asyncio.sleep(self._refill_interval)

                # Check if we need more tokens
                if self._tokens < self._low_watermark:
                    await self._refill()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.warning(f"Refill error: {e}")

    async def _refill(self) -> None:
        """Request more tokens from global limiter."""
        try:
            granted = await self._global.request_tokens.remote(self._batch_size)
            async with self._lock:
                self._tokens += granted
        except Exception as e:
            self._logger.warning(f"Failed to refill tokens: {e}")

    def acquire(self) -> bool:
        """Try to acquire a token (non-blocking, no Ray call).

        Returns:
            True if token acquired, False if no tokens available
        """
        if self._tokens > 0:
            self._tokens -= 1
            self._in_flight += 1
            return True
        return False

    async def acquire_async(self, timeout: float = 5.0) -> bool:
        """Try to acquire a token, waiting if necessary.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if acquired, False if timed out
        """
        start = time.time()

        while (time.time() - start) < timeout:
            if self.acquire():
                return True

            # Try to get more tokens immediately
            if self._tokens < self._low_watermark:
                await self._refill()

            if self.acquire():
                return True

            # Wait a bit before retrying
            await asyncio.sleep(0.05)

        return False

    def release(self) -> None:
        """Release a token back to local pool."""
        if self._in_flight > 0:
            self._in_flight -= 1
            self._tokens += 1

    @property
    def available(self) -> int:
        """Number of locally available tokens."""
        return self._tokens

    @property
    def in_flight(self) -> int:
        """Number of tokens currently in use."""
        return self._in_flight


def get_or_create_rate_limiter(
    name: str,
    max_concurrent: int = 100,
    requests_per_second: float = 0,
) -> ray.actor.ActorHandle:
    """Get existing global rate limiter or create a new one.

    The actor is NOT detached, so it will be garbage collected when all
    references are released. LocalRateLimiter holds a reference, keeping
    it alive during the job.

    Args:
        name: Unique name for the rate limiter (Ray named actor)
        max_concurrent: Maximum concurrent requests
        requests_per_second: Maximum RPS

    Returns:
        Ray actor handle to the global rate limiter
    """
    return GlobalRateLimiter.options(
        name=name,
        get_if_exists=True,
        # No lifetime="detached" - actor will be GC'd when no references exist
    ).remote(
        max_concurrent=max_concurrent,
        requests_per_second=requests_per_second,
    )


def cleanup_rate_limiter(name: str) -> bool:
    """Explicitly kill a rate limiter actor by name.

    Call this at job completion to ensure cleanup. Not strictly necessary
    since actors are GC'd when references are released, but useful for
    immediate cleanup.

    Args:
        name: The rate limiter name (same as passed to get_or_create_rate_limiter)

    Returns:
        True if actor was found and killed, False if not found
    """
    try:
        actor = ray.get_actor(name)
        ray.kill(actor)
        return True
    except ValueError:
        # Actor not found
        return False
