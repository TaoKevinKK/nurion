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

"""Circuit breaker pattern for fault tolerance.

The circuit breaker prevents cascading failures by failing fast when
a downstream service is unhealthy.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Service is down, requests fail immediately
- HALF_OPEN: Testing if service recovered, limited requests allowed

Transitions:
- CLOSED -> OPEN: After `failure_threshold` consecutive failures
- OPEN -> HALF_OPEN: After `recovery_timeout` seconds
- HALF_OPEN -> CLOSED: After `half_open_requests` successes
- HALF_OPEN -> OPEN: On any failure
"""

from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Optional


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and requests are rejected."""

    def __init__(self, message: str = "Circuit breaker is open", time_until_retry: float = 0):
        super().__init__(message)
        self.time_until_retry = time_until_retry


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker.

    Attributes:
        enabled: Whether circuit breaker is enabled
        failure_threshold: Number of consecutive failures before opening
        recovery_timeout: Seconds to wait before trying half-open
        half_open_requests: Number of successful requests to close circuit
        failure_window_seconds: Time window for counting failures (sliding window)
    """

    enabled: bool = True
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_requests: int = 3
    failure_window_seconds: float = 60.0


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance.

    Thread-safe implementation using a lock for state transitions.

    Usage:
        cb = CircuitBreaker(CircuitBreakerConfig())

        if cb.can_proceed():
            try:
                result = call_service()
                cb.record_success()
            except Exception:
                cb.record_failure()
                raise
        else:
            raise CircuitBreakerOpenError()
    """

    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    def __post_init__(self):
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at: Optional[float] = None
        self._failure_times: list[float] = []
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._failure_count

    def can_proceed(self) -> bool:
        """Check if a request can proceed through the circuit breaker.

        Returns:
            True if request can proceed, False if circuit is open
        """
        if not self.config.enabled:
            return True

        with self._lock:
            now = time.time()

            if self._state == CircuitState.CLOSED:
                return True

            elif self._state == CircuitState.OPEN:
                # Check if we should transition to half-open
                if self._opened_at and (now - self._opened_at) >= self.config.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    return True
                return False

            else:  # HALF_OPEN
                # Allow limited requests in half-open state
                return True

    def record_success(self) -> None:
        """Record a successful request."""
        if not self.config.enabled:
            return

        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.half_open_requests:
                    # Enough successes, close the circuit
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._failure_times.clear()
                    self._opened_at = None
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
                self._failure_times.clear()

    def record_failure(self) -> None:
        """Record a failed request."""
        if not self.config.enabled:
            return

        with self._lock:
            now = time.time()
            self._last_failure_time = now

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open state opens the circuit
                self._state = CircuitState.OPEN
                self._opened_at = now
                return

            # Clean up old failures outside the window
            window_start = now - self.config.failure_window_seconds
            self._failure_times = [t for t in self._failure_times if t > window_start]

            # Record this failure
            self._failure_times.append(now)
            self._failure_count = len(self._failure_times)

            if self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._opened_at = now

    def _get_time_until_retry_unlocked(self) -> float:
        """Calculate time until retry (caller must hold lock)."""
        if self._state != CircuitState.OPEN:
            return 0.0
        if self._opened_at is None:
            return 0.0
        elapsed = time.time() - self._opened_at
        remaining = self.config.recovery_timeout - elapsed
        return max(0.0, remaining)

    def get_time_until_retry(self) -> float:
        """Get seconds until circuit breaker might allow requests again.

        Returns:
            Seconds until retry is possible, 0 if requests are allowed
        """
        with self._lock:
            return self._get_time_until_retry_unlocked()

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._opened_at = None
            self._failure_times.clear()

    def get_stats(self) -> dict:
        """Get circuit breaker statistics.

        Returns:
            Dictionary with state, failure count, and timing info
        """
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "opened_at": self._opened_at,
                "time_until_retry": self._get_time_until_retry_unlocked(),
            }
