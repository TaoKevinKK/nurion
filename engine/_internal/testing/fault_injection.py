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

This module provides Ray-actor-based fault injection that works correctly
across multiple worker processes. A shared Ray actor maintains fault state,
ensuring consistent behavior regardless of which worker triggers the fault.

Design principles:
1. Zero overhead in production (disabled by default via env var)
2. Consistent across all Ray workers (shared actor state)
3. Reproducible failures via deterministic triggers

Environment Variables:
    NURION_FAULT_INJECTION=1          # Enable fault injection (default: 0)
    NURION_FAULT_<POINT>_AFTER=N      # Fail after N calls at <POINT>
    NURION_FAULT_<POINT>_PROB=0.1     # Fail with 10% probability at <POINT>

    Where <POINT> is one of:
    - QUEUE_PRODUCE, QUEUE_FETCH, QUEUE_COMMIT
    - BEFORE_PROCESS, AFTER_PROCESS
    - BEFORE_MARK_PROCESSED, AFTER_MARK_PROCESSED

Usage in tests:
    import os
    os.environ["NURION_FAULT_INJECTION"] = "1"
    os.environ["NURION_FAULT_QUEUE_PRODUCE_AFTER"] = "3"  # Fail on 4th call

    # Reset to pick up new env vars
    reset_fault_injector()

    # Then run the pipeline - all workers share the same fault state
"""

import os
import random
from typing import Any, Optional, cast

import ray


class InjectedFaultError(Exception):
    """Exception raised by fault injection for testing.

    This is a distinct exception type so retry logic can specifically
    catch injected faults without catching real programming errors.
    """

    pass


# Actor name for the shared fault state
_FAULT_ACTOR_NAME = "nurion_fault_injector"


@ray.remote
class _FaultStateActor:
    """Ray actor that maintains shared fault injection state.

    All workers call this actor to check/update fault counters,
    ensuring consistent behavior across processes.
    """

    def __init__(self):
        # fault_point -> {after_count, probability, call_count, has_failed, fail_once}
        self._faults: dict[str, dict] = {}

    def register(
        self,
        point: str,
        after_count: int = 0,
        probability: float = 0.0,
        fail_once: bool = True,
    ) -> None:
        """Register a fault injection point."""
        self._faults[point] = {
            "after_count": after_count,
            "probability": probability,
            "call_count": 0,
            "has_failed": False,
            "fail_once": fail_once,
        }

    def check(self, point: str) -> bool:
        """Check if fault should trigger. Returns True if should fail."""
        config = self._faults.get(point)
        if config is None:
            return False

        config["call_count"] += 1

        should_fail = False

        # Count-based trigger
        if config["after_count"] > 0:
            if config["call_count"] > config["after_count"]:
                if not config["fail_once"] or not config["has_failed"]:
                    should_fail = True

        # Probability-based trigger
        if config["probability"] > 0:
            if random.random() < config["probability"]:
                if not config["fail_once"] or not config["has_failed"]:
                    should_fail = True

        if should_fail:
            config["has_failed"] = True

        return should_fail

    def reset(self) -> None:
        """Reset all fault states."""
        for config in self._faults.values():
            config["call_count"] = 0
            config["has_failed"] = False

    def clear(self) -> None:
        """Remove all registered faults."""
        self._faults.clear()


# Mapping from env var suffix to fault point
_FAULT_POINT_MAP: dict[str, str] = {
    "QUEUE_PRODUCE": "queue.produce",
    "QUEUE_FETCH": "queue.fetch",
    "QUEUE_COMMIT": "queue.commit",
    "BEFORE_PROCESS": "operator.before_process",
    "AFTER_PROCESS": "operator.after_process",
}

# Cache for the actor handle
_fault_actor: Optional[ray.actor.ActorHandle] = None
_initialized: bool = False


def _get_or_create_actor() -> Optional[ray.actor.ActorHandle]:
    """Get or create the fault state actor."""
    global _fault_actor, _initialized

    if not is_fault_injection_enabled():
        return None

    if _initialized and _fault_actor is not None:
        return _fault_actor

    try:
        # Try to get existing actor
        _fault_actor = ray.get_actor(_FAULT_ACTOR_NAME)
    except ValueError:
        # Create new actor
        actor_class = cast(Any, _FaultStateActor)
        _fault_actor = actor_class.options(
            name=_FAULT_ACTOR_NAME,
            lifetime="detached",
            get_if_exists=True,
        ).remote()

        # Register faults from environment variables
        _register_faults_from_env(_fault_actor)

    _initialized = True
    return _fault_actor


def _register_faults_from_env(actor: ray.actor.ActorHandle) -> None:
    """Register fault configurations from environment variables."""
    for env_suffix, fault_point in _FAULT_POINT_MAP.items():
        after_count = 0
        probability = 0.0

        # Check for _AFTER config
        after_key = f"NURION_FAULT_{env_suffix}_AFTER"
        after_val = os.environ.get(after_key)
        if after_val:
            try:
                after_count = int(after_val)
            except ValueError:
                pass

        # Check for _PROB config
        prob_key = f"NURION_FAULT_{env_suffix}_PROB"
        prob_val = os.environ.get(prob_key)
        if prob_val:
            try:
                probability = float(prob_val)
            except ValueError:
                pass

        # Register if any config is set
        if after_count > 0 or probability > 0:
            ray.get(
                actor.register.remote(
                    fault_point,
                    after_count=after_count,
                    probability=probability,
                    fail_once=(probability == 0),  # prob-based can fire multiple times
                )
            )


def check_fault(point: str) -> None:
    """Check fault at point using shared Ray actor.

    No-op if NURION_FAULT_INJECTION env var is not "1".
    This is the function to call in production code.
    """
    actor = _get_or_create_actor()
    if actor is None:
        return

    try:
        should_fail = ray.get(actor.check.remote(point))
        if should_fail:
            raise InjectedFaultError(f"Injected fault at {point}")
    except ray.exceptions.RayActorError:
        # Actor died, reset and retry
        reset_fault_injector()


def reset_fault_injector() -> None:
    """Reset the fault injector state.

    Kills the existing actor and clears cached references.
    Call this between tests to ensure clean state.
    """
    global _fault_actor, _initialized

    if _fault_actor is not None:
        try:
            ray.kill(_fault_actor)
        except Exception:
            pass

    _fault_actor = None
    _initialized = False


def is_fault_injection_enabled() -> bool:
    """Check if fault injection is enabled."""
    return os.environ.get("NURION_FAULT_INJECTION", "0") == "1"


# =============================================================================
# Fault Points (documented constants)
# =============================================================================

# Queue faults
FAULT_QUEUE_PRODUCE = "queue.produce"
FAULT_QUEUE_FETCH = "queue.fetch"
FAULT_QUEUE_COMMIT = "queue.commit"

# Operator faults
FAULT_BEFORE_PROCESS = "operator.before_process"
FAULT_AFTER_PROCESS = "operator.after_process"
