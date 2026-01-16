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

"""HTTP operator infrastructure for calling external services."""

from solstice.operators.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
)
from solstice.operators.http.rate_limiter import (
    GlobalRateLimiter,
    LocalRateLimiter,
    RateLimitExceededError,
    cleanup_rate_limiter,
)
from solstice.operators.http.operator import (
    HttpOperator,
    HttpOperatorConfig,
    RetryableError,
)

__all__ = [
    # Circuit breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    # Rate limiter
    "GlobalRateLimiter",
    "LocalRateLimiter",
    "RateLimitExceededError",
    "cleanup_rate_limiter",
    # HTTP operator
    "HttpOperator",
    "HttpOperatorConfig",
    "RetryableError",
]
