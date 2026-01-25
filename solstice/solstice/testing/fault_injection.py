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

"""Fault injection framework for testing exactly-once semantics.

This module provides environment-variable-based fault injection that works
across Ray worker processes. All workers read the same env vars, ensuring
consistent fault injection behavior.

Design principles:
1. Zero overhead in production (disabled by default via env var)
2. Consistent across all Ray workers (env vars are inherited)
3. Reproducible failures via deterministic triggers

Environment Variables:
    SOLSTICE_FAULT_INJECTION=1          # Enable fault injection (default: 0)
    SOLSTICE_FAULT_<POINT>_AFTER=N      # Fail after N calls at <POINT>
    SOLSTICE_FAULT_<POINT>_PROB=0.1     # Fail with 10% probability at <POINT>

    Where <POINT> is one of:
    - QUEUE_PRODUCE, QUEUE_FETCH, QUEUE_COMMIT
    - BEFORE_PROCESS, AFTER_PROCESS
    - BEFORE_MARK_PROCESSED, AFTER_MARK_PROCESSED
    - STATE_STORE_PUT, STATE_STORE_GET

Usage in tests:
    import os
    os.environ["SOLSTICE_FAULT_INJECTION"] = "1"
    os.environ["SOLSTICE_FAULT_QUEUE_PRODUCE_AFTER"] = "3"  # Fail on 4th call

    # Then run the pipeline - all workers will have the same fault config
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Set
import os
import random


@dataclass
class FaultConfig:
    """Configuration for a single fault injection point."""

    # Trigger conditions
    fail_after_count: int = 0  # Fail after N successful calls (0 = never)
    fail_probability: float = 0.0  # Random failure probability (0-1)
    fail_once: bool = True  # Only fail once, then stop

    # Failure behavior
    exception_class: type = RuntimeError
    exception_message: str = "Injected fault"

    # State
    call_count: int = field(default=0, init=False)
    has_failed: bool = field(default=False, init=False)


class FaultInjector:
    """Fault injection controller for testing.

    Register fault points and check them at critical locations.
    Disabled by default (no-op in production).

    Example:
        injector = FaultInjector(enabled=True)

        # Fail state_store.put_batch after 3 successful calls
        injector.register(
            "state_store.put_batch",
            FaultConfig(fail_after_count=3)
        )

        # In code:
        injector.check("state_store.put_batch")  # Raises on 4th call
    """

    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self._faults: Dict[str, FaultConfig] = {}
        self._triggered: Set[str] = set()

    def register(self, point: str, config: FaultConfig) -> "FaultInjector":
        """Register a fault injection point."""
        self._faults[point] = config
        return self

    def fail_after(
        self,
        point: str,
        count: int,
        exception: type = RuntimeError,
        message: str = "Injected fault",
    ) -> "FaultInjector":
        """Convenience: fail after N successful calls."""
        return self.register(
            point,
            FaultConfig(
                fail_after_count=count,
                exception_class=exception,
                exception_message=message,
            ),
        )

    def fail_randomly(
        self,
        point: str,
        probability: float,
        exception: type = RuntimeError,
        message: str = "Random injected fault",
    ) -> "FaultInjector":
        """Convenience: fail with given probability."""
        return self.register(
            point,
            FaultConfig(
                fail_probability=probability,
                fail_once=False,
                exception_class=exception,
                exception_message=message,
            ),
        )

    def check(self, point: str) -> None:
        """Check if fault should be triggered at this point.

        Call this at critical points in the code. No-op if disabled.
        """
        if not self.enabled:
            return

        config = self._faults.get(point)
        if config is None:
            return

        config.call_count += 1

        # Check if we should fail
        should_fail = False

        # Count-based trigger
        if config.fail_after_count > 0:
            if config.call_count > config.fail_after_count:
                if not config.fail_once or not config.has_failed:
                    should_fail = True

        # Probability-based trigger
        if config.fail_probability > 0:
            if random.random() < config.fail_probability:
                if not config.fail_once or not config.has_failed:
                    should_fail = True

        if should_fail:
            config.has_failed = True
            self._triggered.add(point)
            raise config.exception_class(config.exception_message)

    def was_triggered(self, point: str) -> bool:
        """Check if a fault point was triggered."""
        return point in self._triggered

    def reset(self) -> None:
        """Reset all fault states."""
        self._triggered.clear()
        for config in self._faults.values():
            config.call_count = 0
            config.has_failed = False

    def clear(self) -> None:
        """Remove all registered faults."""
        self._faults.clear()
        self._triggered.clear()


# Global injector - lazy initialized from environment variables
_global_injector: Optional[FaultInjector] = None
_injector_initialized: bool = False

# Mapping from env var suffix to fault point
_FAULT_POINT_MAP: Dict[str, str] = {
    "QUEUE_PRODUCE": "queue.produce",
    "QUEUE_FETCH": "queue.fetch",
    "QUEUE_COMMIT": "queue.commit",
    "BEFORE_PROCESS": "operator.before_process",
    "AFTER_PROCESS": "operator.after_process",
    "BEFORE_MARK_PROCESSED": "operator.before_mark_processed",
    "AFTER_MARK_PROCESSED": "operator.after_mark_processed",
    "STATE_STORE_PUT": "state_store.put_batch",
    "STATE_STORE_GET": "state_store.get",
}


def _init_global_injector() -> FaultInjector:
    """Initialize global injector from environment variables.

    Called lazily on first check_fault() call.
    """
    global _global_injector, _injector_initialized

    enabled = os.environ.get("SOLSTICE_FAULT_INJECTION", "0") == "1"
    injector = FaultInjector(enabled=enabled)

    if enabled:
        # Parse fault configs from environment
        for env_suffix, fault_point in _FAULT_POINT_MAP.items():
            # Check for _AFTER config (fail after N calls)
            after_key = f"SOLSTICE_FAULT_{env_suffix}_AFTER"
            after_val = os.environ.get(after_key)
            if after_val:
                try:
                    count = int(after_val)
                    injector.fail_after(fault_point, count)
                except ValueError:
                    pass

            # Check for _PROB config (fail with probability)
            prob_key = f"SOLSTICE_FAULT_{env_suffix}_PROB"
            prob_val = os.environ.get(prob_key)
            if prob_val:
                try:
                    prob = float(prob_val)
                    injector.fail_randomly(fault_point, prob)
                except ValueError:
                    pass

    _global_injector = injector
    _injector_initialized = True
    return injector


def _get_injector() -> FaultInjector:
    """Get the global injector, initializing if needed."""
    global _global_injector, _injector_initialized
    if not _injector_initialized:
        return _init_global_injector()
    return _global_injector  # type: ignore


def check_fault(point: str) -> None:
    """Check fault at point using global injector.

    No-op if SOLSTICE_FAULT_INJECTION env var is not "1".
    This is the function to call in production code.
    """
    injector = _get_injector()
    injector.check(point)


def reset_fault_injector() -> None:
    """Reset the global injector state (for tests).

    Re-reads environment variables and reinitializes.
    """
    global _global_injector, _injector_initialized
    _global_injector = None
    _injector_initialized = False


def is_fault_injection_enabled() -> bool:
    """Check if fault injection is enabled."""
    return os.environ.get("SOLSTICE_FAULT_INJECTION", "0") == "1"


# =============================================================================
# Fault Points (documented constants)
# =============================================================================

# State store faults
FAULT_STATE_STORE_PUT = "state_store.put_batch"
FAULT_STATE_STORE_GET = "state_store.get"

# Queue faults
FAULT_QUEUE_PRODUCE = "queue.produce"
FAULT_QUEUE_FETCH = "queue.fetch"
FAULT_QUEUE_COMMIT = "queue.commit"

# Operator faults
FAULT_BEFORE_PROCESS = "operator.before_process"
FAULT_AFTER_PROCESS = "operator.after_process"
FAULT_BEFORE_MARK_PROCESSED = "operator.before_mark_processed"
FAULT_AFTER_MARK_PROCESSED = "operator.after_mark_processed"
