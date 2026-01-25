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

"""Testing utilities for Solstice."""

from solstice.testing.fault_injection import (
    FaultInjector,
    FaultConfig,
    check_fault,
    reset_fault_injector,
    is_fault_injection_enabled,
    # Fault points
    FAULT_STATE_STORE_PUT,
    FAULT_STATE_STORE_GET,
    FAULT_QUEUE_PRODUCE,
    FAULT_QUEUE_FETCH,
    FAULT_QUEUE_COMMIT,
    FAULT_BEFORE_PROCESS,
    FAULT_AFTER_PROCESS,
    FAULT_BEFORE_MARK_PROCESSED,
    FAULT_AFTER_MARK_PROCESSED,
)

__all__ = [
    "FaultInjector",
    "FaultConfig",
    "check_fault",
    "reset_fault_injector",
    "is_fault_injection_enabled",
    # Fault points
    "FAULT_STATE_STORE_PUT",
    "FAULT_STATE_STORE_GET",
    "FAULT_QUEUE_PRODUCE",
    "FAULT_QUEUE_FETCH",
    "FAULT_QUEUE_COMMIT",
    "FAULT_BEFORE_PROCESS",
    "FAULT_AFTER_PROCESS",
    "FAULT_BEFORE_MARK_PROCESSED",
    "FAULT_AFTER_MARK_PROCESSED",
]
