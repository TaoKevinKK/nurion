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

"""Typed error hierarchy for the Anvil queue subsystem.

Rust errors flow through gRPC as Status codes with structured messages
(key=value pairs). This module converts them into typed Python exceptions
with parsed fields for programmatic handling.

Error chain:
    Rust storage → gRPC Status::internal("Storage error: {details}")
      → Rust client → RuntimeError("{details}")
        → _raise_typed() → QueueFullError / ClaimTokenError

Usage:
    from _internal.queue.errors import QueueFullError, ClaimTokenError

    try:
        client.ack_and_scatter(...)
    except QueueFullError as e:
        logger.info(f"Queue {e.queue} full ({e.in_flight}/{e.max_pending})")
        await asyncio.sleep(1.0)  # wait for downstream to drain
    except ClaimTokenError as e:
        logger.warning(f"Stale token for {e.msg_id} on {e.queue} ({e.kind})")
"""

from __future__ import annotations


# =============================================================================
# Error classes
# =============================================================================


class AnvilError(RuntimeError):
    """Base class for Anvil queue errors.

    All subclasses carry the raw error message from Rust and expose
    structured fields parsed from it (queue, msg_id, etc.).
    """


class QueueFullError(AnvilError):
    """Raised when a bounded queue reaches its max_pending limit.

    Attributes:
        queue: The queue that is full.
        in_flight: Current number of messages in flight (pushed - acked).
        max_pending: The configured capacity limit.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.queue = _extract_field(message, "queue")
        self.in_flight = _extract_int_field(message, "in_flight")
        self.max_pending = _extract_int_field(message, "max_pending")


class ClaimTokenError(AnvilError):
    """Raised when a claim token, lease, or worker identity is invalid.

    This typically means the message was reclaimed by another worker
    (lease expired) or the token is stale after a nack+reclaim cycle.

    Attributes:
        queue: The queue where the mismatch occurred.
        msg_id: The message ID that failed validation.
        kind: Type of mismatch ('claim_token', 'lease_id', 'worker_id', 'not_claimed').
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.queue = _extract_field(message, "queue")
        self.msg_id = _extract_field(message, "msg_id")
        if "claim_token mismatch" in message:
            self.kind = "claim_token"
        elif "lease_id mismatch" in message:
            self.kind = "lease_id"
        elif "worker_id mismatch" in message:
            self.kind = "worker_id"
        elif "message_not_claimed" in message:
            self.kind = "not_claimed"
        else:
            self.kind = "unknown"


# =============================================================================
# Helpers
# =============================================================================


def _extract_field(msg: str, field: str) -> str:
    """Extract 'field=value' from structured error message."""
    for part in msg.replace(",", " ").split():
        if part.startswith(f"{field}="):
            return part[len(field) + 1 :]
    return ""


def _extract_int_field(msg: str, field: str) -> int:
    """Extract 'field=123' as int from structured error message."""
    val = _extract_field(msg, field)
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def raise_typed(e: RuntimeError) -> None:
    """Convert generic RuntimeError from Rust/gRPC into typed Anvil errors.

    Call this in except blocks around Rust client calls. If the error
    matches a known pattern, raises the typed exception. Otherwise
    returns without raising (caller should re-raise the original).
    """
    msg = str(e)
    if "QueueFull" in msg:
        raise QueueFullError(msg) from e
    if any(
        k in msg
        for k in (
            "claim_token mismatch",
            "lease_id mismatch",
            "worker_id mismatch",
            "message_not_claimed",
        )
    ):
        raise ClaimTokenError(msg) from e
