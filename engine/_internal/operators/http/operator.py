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

"""HTTP Operator base class with built-in fault tolerance.

Provides:
- Rate limiting (local token bucket with global coordination)
- Circuit breaker pattern
- Automatic retries with exponential backoff
- Timeout handling
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, ClassVar, Optional, Type

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryCallState,
)

from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime
from _internal.core.models import Split, SplitPayload
from _internal.operators.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
)
from _internal.operators.http.rate_limiter import (
    LocalRateLimiter,
    RateLimitExceededError,
    get_or_create_rate_limiter,
)


class RetryableError(Exception):
    """Error that should trigger a retry."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class HttpOperatorConfig(OperatorConfig):
    """Configuration for HTTP operators.

    Attributes:
        base_url: Base URL for the service (can be set dynamically by stage master)
        connect_timeout: Connection timeout in seconds
        read_timeout: Read timeout in seconds (should be long for LLM inference)
        max_retries: Maximum retry attempts
        retry_backoff: Initial backoff time between retries (seconds)
        retry_on_status: HTTP status codes that trigger retry
        max_concurrent_requests: Maximum concurrent requests (0 = unlimited)
        requests_per_second: Maximum requests per second (0 = unlimited)
        circuit_breaker: Circuit breaker configuration
        rate_limiter_name: Custom name for rate limiter (default: auto-generated)
        rate_limit_batch_size: Tokens to request per batch from global limiter
    """

    operator_class: ClassVar[Type["HttpOperator"]]

    # Endpoint configuration
    base_url: str = ""

    # Timeout configuration
    connect_timeout: float = 10.0
    read_timeout: float = 120.0  # LLM inference can be slow

    # Retry configuration
    max_retries: int = 3
    retry_backoff: float = 1.0
    retry_on_status: list[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])

    # Rate limiting
    max_concurrent_requests: int = 100
    requests_per_second: float = 0  # 0 = unlimited

    # Circuit breaker
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    # Rate limiter settings
    rate_limiter_name: str = ""
    rate_limit_batch_size: int = 20  # Tokens per batch from global limiter


class HttpOperator(Operator):
    """Base HTTP operator with fault tolerance.

    Provides built-in:
    - Rate limiting with local token bucket (minimal Ray overhead)
    - Circuit breaker for fast failure
    - Retries with exponential backoff
    - Timeout handling

    Subclasses should implement process_split() and use _request() for HTTP calls.

    Example:
        class MyHttpOperator(HttpOperator):
            async def _call_api(self, data: dict) -> dict:
                return await self._request(
                    "POST",
                    f"{self.config.base_url}/api/endpoint",
                    json=data,
                )

            def process_split(self, split, payload):
                result = asyncio.run(self._call_api({"data": "value"}))
                return payload.with_new_data(result)
    """

    def __init__(self, config: HttpOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._http_config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._local_limiter: Optional[LocalRateLimiter] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None
        self._bound_loop: Optional[asyncio.AbstractEventLoop] = None

    async def _init_http(self) -> None:
        """Initialize HTTP client and fault tolerance components.

        Handles event loop changes (e.g., when using asyncio.run() multiple times)
        by reinitializing resources bound to the current loop.
        """
        current_loop = asyncio.get_running_loop()

        # Check if we need to reinitialize due to event loop change
        if self._bound_loop is not None and self._bound_loop is not current_loop:
            await self._cleanup_async_resources()

        if self._session is not None:
            return  # Already initialized for this loop

        # Create aiohttp session with timeouts
        timeout = aiohttp.ClientTimeout(
            connect=self._http_config.connect_timeout,
            total=self._http_config.read_timeout,
        )
        self._session = aiohttp.ClientSession(timeout=timeout)

        # Initialize rate limiter (local + global coordination)
        if (
            self._http_config.max_concurrent_requests > 0
            or self._http_config.requests_per_second > 0
        ):
            limiter_name = self._http_config.rate_limiter_name or (
                f"http_limiter_{self.job_id}_{self.stage_id}"
            )
            global_limiter = get_or_create_rate_limiter(
                name=limiter_name,
                max_concurrent=self._http_config.max_concurrent_requests,
                requests_per_second=self._http_config.requests_per_second,
            )
            self._local_limiter = LocalRateLimiter(
                global_limiter,
                batch_size=self._http_config.rate_limit_batch_size,
            )
            await self._local_limiter.start()

        # Initialize circuit breaker (per-worker, not loop-bound)
        if self._circuit_breaker is None:
            self._circuit_breaker = CircuitBreaker(self._http_config.circuit_breaker)

        self._bound_loop = current_loop

    async def _cleanup_async_resources(self) -> None:
        """Clean up async resources (session, limiter) without touching circuit breaker."""
        if self._local_limiter:
            try:
                await self._local_limiter.stop()
            except Exception:
                pass
            self._local_limiter = None

        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure HTTP session is initialized."""
        if self._session is None:
            await self._init_http()
        assert self._session is not None
        return self._session

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict:
        """Make an HTTP request with rate limiting, circuit breaker, and retries.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments passed to aiohttp

        Returns:
            Parsed JSON response

        Raises:
            CircuitBreakerOpenError: If circuit breaker is open
            RateLimitExceededError: If rate limit exceeded
            RetryableError: If all retries exhausted
            aiohttp.ClientError: For non-retryable errors
        """
        await self._init_http()
        assert self._circuit_breaker is not None

        # Check circuit breaker
        if not self._circuit_breaker.can_proceed():
            time_until_retry = self._circuit_breaker.get_time_until_retry()
            raise CircuitBreakerOpenError(
                f"Circuit breaker open for {url}",
                time_until_retry=time_until_retry,
            )

        # Acquire rate limit (local, fast - no Ray call for most requests)
        rate_limit_acquired = False
        if self._local_limiter:
            acquired = await self._local_limiter.acquire_async(timeout=30.0)
            if not acquired:
                raise RateLimitExceededError(
                    f"Rate limit exceeded for {url}",
                    retry_after=1.0,
                )
            rate_limit_acquired = True

        session = await self._ensure_session()

        try:
            return await self._request_with_retry(session, method, url, **kwargs)
        except (RetryableError, asyncio.TimeoutError):
            self._circuit_breaker.record_failure()
            raise
        except aiohttp.ClientError:
            self._circuit_breaker.record_failure()
            raise
        finally:
            # Release rate limit (local, fast)
            if rate_limit_acquired and self._local_limiter:
                self._local_limiter.release()

    def _create_retry_decorator(self) -> Any:
        """Create a tenacity retry decorator with current config."""

        def before_sleep_callback(retry_state: RetryCallState) -> None:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            self.logger.warning(
                f"Retry {retry_state.attempt_number}/{self._http_config.max_retries} failed: {exc}"
            )

        return retry(
            stop=stop_after_attempt(self._http_config.max_retries + 1),
            wait=wait_exponential(
                multiplier=self._http_config.retry_backoff,
                min=self._http_config.retry_backoff,
                max=self._http_config.retry_backoff * 8,
            ),
            retry=retry_if_exception_type((RetryableError, asyncio.TimeoutError)),
            before_sleep=before_sleep_callback,
            reraise=True,
        )

    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict:
        """Make HTTP request with tenacity retry logic."""
        # Create and apply retry decorator dynamically
        retry_decorator = self._create_retry_decorator()

        @retry_decorator
        async def _do_request() -> dict:
            async with session.request(method, url, **kwargs) as response:
                # Check if status code should trigger retry
                if response.status in self._http_config.retry_on_status:
                    body = await response.text()
                    raise RetryableError(
                        f"HTTP {response.status}: {body[:200]}",
                        status_code=response.status,
                    )

                # Check for other error status codes
                if response.status >= 400:
                    body = await response.text()
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=f"HTTP {response.status}: {body[:200]}",
                    )

                # Success
                assert self._circuit_breaker is not None
                self._circuit_breaker.record_success()
                return await response.json()

        try:
            return await _do_request()
        except asyncio.TimeoutError:
            raise RetryableError(f"Request timed out after retries: {url}")

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        """Process a split. Subclasses should override this."""
        raise NotImplementedError("Subclasses must implement process_split")

    def close(self) -> None:
        """Clean up HTTP resources."""
        if self._session or self._local_limiter:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._cleanup_async_resources())
                else:
                    loop.run_until_complete(self._cleanup_async_resources())
            except Exception:
                # Event loop may be closed, force cleanup
                self._session = None
                self._local_limiter = None
        self._bound_loop = None
        super().close()


# Set operator_class after definition
HttpOperatorConfig.operator_class = HttpOperator
