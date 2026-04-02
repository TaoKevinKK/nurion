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

"""
Anvil Implementation - Single-queue Multi-consumer Model.

Exceptions:
    QueueFullError: Raised when a bounded queue reaches its max_pending limit.
                    Callers should retry after a short delay.

Components:
- AnvilBrokerManager: Manages embedded Rust broker lifecycle
- AnvilQueueClient: Client for claim/ack operations

Unlike Kafka's partition model, Anvil uses:
- claim: Atomically grab messages (with timeout-based lease)
- ack: Confirm message processing
- nack: Return message to queue for retry

Example:
    # On Master
    broker = AnvilBrokerManager(db_path="file:///tmp/wq")
    broker.start()

    client = AnvilQueueClient(broker.get_broker_url(), worker_id="master")
    client.start()
    client.create_queue("my-queue")
    client.push("my-queue", b"hello")

    # On Worker
    client = AnvilQueueClient("master-host:50051", worker_id="worker-1")
    client.start()
    messages, drained = client.claim("my-queue", batch_size=10)
    client.ack(
        "my-queue",
        [m.msg_id for m in messages],
        claim_tokens=[m.claim_token for m in messages],
    )
    client.stop()
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

from anvil_py import BrokerConfig, BrokerError, AnvilBroker, AnvilRustClient

from _internal.config import get_config
from _internal.utils.logging import create_ray_logger
from _internal.queue.anvil_storage import AnvilStorageReader
from _internal.queue.errors import raise_typed as _raise_typed


# =============================================================================
# AnvilBrokerManager
# =============================================================================


class AnvilBrokerManager:
    """Manages the embedded Anvil broker lifecycle."""

    def __init__(
        self,
        db_path: str = "file:///tmp/anvil",
        port: int = 0,
        host: str = "0.0.0.0",
        startup_timeout: float | None = None,
        claim_timeout_secs: float | None = None,
        recovery_interval_secs: float | None = None,
        acked_retention_secs: float | None = None,
        gc_interval_secs: float | None = None,
    ):
        cfg = get_config()
        self.db_path = db_path
        self.port = port
        self.host = host
        self.startup_timeout = (
            startup_timeout if startup_timeout is not None else cfg.broker_startup_timeout_s
        )
        self.claim_timeout_secs = (
            claim_timeout_secs if claim_timeout_secs is not None else cfg.broker_claim_timeout_s
        )
        self.recovery_interval_secs = (
            recovery_interval_secs
            if recovery_interval_secs is not None
            else cfg.broker_recovery_interval_s
        )
        self.acked_retention_secs = (
            acked_retention_secs
            if acked_retention_secs is not None
            else cfg.broker_acked_retention_s
        )
        self.gc_interval_secs = (
            gc_interval_secs if gc_interval_secs is not None else cfg.broker_gc_interval_s
        )

        self._broker: Optional[AnvilBroker] = None
        self._running = False
        self._actual_port: Optional[int] = None
        self.logger = create_ray_logger(f"AnvilBroker:{port}")

    def start(self) -> None:
        if self._running:
            return

        config = BrokerConfig(
            db_path=self.db_path,
            host=self.host,
            port=self.port,
            claim_timeout_secs=self.claim_timeout_secs,
            recovery_interval_secs=self.recovery_interval_secs,
            acked_retention_secs=self.acked_retention_secs,
            gc_interval_secs=self.gc_interval_secs,
        )

        ready_event = threading.Event()
        handler = _BrokerEventHandler(self, ready_event)
        self._broker = AnvilBroker(config, event_handler=handler)
        self._broker.start()

        if not ready_event.wait(timeout=self.startup_timeout):
            raise RuntimeError(f"Broker failed to start within {self.startup_timeout}s")

        if not self._running:
            raise RuntimeError("Broker failed to start (fatal error)")

        self.logger.info(f"Broker ready at {self.get_broker_url()}")

    def stop(self) -> None:
        if self._broker:
            try:
                self._broker.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping broker: {e}")
            self._broker = None
        self._running = False

    def get_broker_url(self) -> str:
        port = self._actual_port or self.port
        host = "127.0.0.1" if self.host == "0.0.0.0" else self.host
        return f"{host}:{port}"

    def is_running(self) -> bool:
        return self._running

    def get_storage_reader(self) -> Optional[AnvilStorageReader]:
        """Get a storage reader backed by the broker's live storage."""
        if not self._broker:
            return None
        try:
            reader = self._broker.get_storage_reader()
            return AnvilStorageReader(reader=reader)
        except Exception as e:
            self.logger.warning(f"Failed to get storage reader: {e}")
            return None


class _BrokerEventHandler:
    """Internal event handler for broker lifecycle."""

    def __init__(self, manager: AnvilBrokerManager, ready_event: threading.Event):
        self.manager = manager
        self._ready_event = ready_event

    def on_started(self, port: int) -> None:
        self.manager.logger.info(f"Anvil broker started on port {port}")
        self.manager._actual_port = port
        self.manager._running = True
        self._ready_event.set()

    def on_stopped(self) -> None:
        self.manager.logger.info("Anvil broker stopped")
        self.manager._running = False

    def on_fatal(self, error: BrokerError) -> None:
        self.manager.logger.error(f"Anvil broker fatal error: {error.message}")
        self.manager._running = False
        self._ready_event.set()


# =============================================================================
# AnvilQueueClient
# =============================================================================


@dataclass
class AnvilRecord:
    """A record from Anvil."""

    msg_id: str
    value: bytes
    queue: str
    created_at: float
    metadata: Dict[str, str]
    claim_token: Optional[str] = None

    @classmethod
    def from_message(cls, msg) -> "AnvilRecord":
        """Create from Python Message or Rust RustMessage (duck typed)."""
        return cls(
            msg_id=msg.msg_id,
            value=msg.payload,
            queue=msg.queue,
            created_at=msg.created_at,
            metadata=dict(msg.metadata) if msg.metadata else {},
            claim_token=getattr(msg, "claim_token", None),
        )


def _compute_heartbeat_interval(claim_timeout_secs: Optional[float]) -> Optional[float]:
    """Compute a safe heartbeat interval from claim timeout."""
    if claim_timeout_secs is None:
        return None
    cfg = get_config()
    if claim_timeout_secs <= 0:
        return cfg.heartbeat_min_interval_s
    return max(
        cfg.heartbeat_min_interval_s, min(cfg.heartbeat_max_interval_s, claim_timeout_secs / 2)
    )


class AnvilQueueClient:
    """Anvil client for claim/ack operations.

    Uses the high-performance Rust gRPC client (via PyO3) which provides 10-20x
    throughput over the old Python grpcio client by eliminating Python protobuf
    serialization overhead and releasing the GIL during gRPC calls.
    """

    def __init__(
        self,
        broker_url: str,
        worker_id: str = "default",
        heartbeat_interval_secs: Optional[float] = None,
    ):
        self.broker_url = broker_url
        self.worker_id = worker_id
        self.heartbeat_interval_secs = heartbeat_interval_secs
        self._client: Optional[AnvilRustClient] = None
        self._running = False
        self.logger = create_ray_logger(f"AnvilClient:{worker_id}")

    # Lifecycle
    def start(self) -> None:
        if self._running:
            return
        if self.heartbeat_interval_secs is None:
            self._client = AnvilRustClient(self.broker_url, self.worker_id)
        else:
            self._client = AnvilRustClient(
                self.broker_url,
                self.worker_id,
                heartbeat_interval_secs=self.heartbeat_interval_secs,
            )
        self._client.start()
        self._running = True
        self.logger.info(f"Connected to {self.broker_url}")

    def stop(self) -> None:
        self._running = False
        if self._client:
            try:
                self._client.stop()
            except Exception:
                pass
            self._client = None

    def health_check(self) -> bool:
        return self._running and self._client is not None

    # Admin
    def create_queue(self, queue: str, max_pending: int = 0) -> None:
        client = self._check()
        client.create_queue(queue, max_depth=max_pending)

    def delete_queue(self, queue: str) -> None:
        client = self._check()
        client.delete_queue(queue)

    # Producer
    def push(self, queue: str, value: bytes, metadata: Optional[Dict[str, str]] = None) -> str:
        client = self._check()
        try:
            return client.push(queue, value, metadata or {})
        except RuntimeError as e:
            _raise_typed(e)
            raise

    def push_batch(self, queue: str, values: List[bytes]) -> List[str]:
        client = self._check()
        try:
            return client.push_batch(queue, values)
        except RuntimeError as e:
            _raise_typed(e)
            raise

    # Consumer
    def claim(
        self, queue: str, batch_size: int = 1, timeout_ms: int = 5000
    ) -> tuple[list[AnvilRecord], bool]:
        """Claim messages from a queue.

        Returns (records, upstream_drained). upstream_drained is True when
        records is empty AND the queue is finished + fully drained.
        """
        client = self._check()
        messages, drained = client.claim(queue, batch_size, timeout_ms)
        return [AnvilRecord.from_message(m) for m in messages], drained

    def ack(
        self,
        queue: str,
        msg_ids: List[str],
        claim_tokens: Optional[List[str]] = None,
        state_namespace: Optional[str] = None,
        state_puts: Optional[Dict[str, bytes]] = None,
        state_deletes: Optional[List[str]] = None,
    ) -> int:
        client = self._check()
        try:
            return client.ack(
                queue,
                msg_ids,
                claim_tokens=claim_tokens,
                state_namespace=state_namespace,
                state_puts=state_puts,
                state_deletes=state_deletes,
            )
        except RuntimeError as e:
            _raise_typed(e)
            raise

    def nack(
        self,
        queue: str,
        msg_ids: List[str],
        claim_tokens: Optional[List[str]] = None,
        reason: str = "processing_failed",
        delay_ms: int = 0,
        state_namespace: Optional[str] = None,
        state_puts: Optional[Dict[str, bytes]] = None,
        state_deletes: Optional[List[str]] = None,
    ) -> int:
        client = self._check()
        return client.nack(
            queue,
            msg_ids,
            claim_tokens=claim_tokens,
            reason=reason,
            delay_ms=delay_ms,
            state_namespace=state_namespace,
            state_puts=state_puts,
            state_deletes=state_deletes,
        )

    def ack_and_forward(
        self,
        upstream_queue: str,
        upstream_msg_ids: List[str],
        upstream_claim_tokens: Optional[List[str]],
        downstream_queue: str,
        downstream_payloads: List[bytes],
        state_namespace: Optional[str] = None,
        state_puts: Optional[Dict[str, bytes]] = None,
        state_deletes: Optional[List[str]] = None,
    ) -> List[str]:
        client = self._check()
        try:
            return client.ack_and_forward(
                upstream_queue,
                upstream_msg_ids,
                upstream_claim_tokens,
                downstream_queue,
                downstream_payloads,
                state_namespace=state_namespace,
                state_puts=state_puts,
                state_deletes=state_deletes,
            )
        except RuntimeError as e:
            _raise_typed(e)
            raise

    # State
    def state_get(self, namespace: str, keys: List[str]) -> Dict[str, bytes]:
        client = self._check()
        return client.state_get(namespace, keys)

    def state_put(
        self,
        namespace: str,
        puts: Optional[Dict[str, bytes]] = None,
        deletes: Optional[List[str]] = None,
    ) -> tuple:
        client = self._check()
        return client.state_put(namespace, puts, deletes)

    # Stats
    def get_stats(self, queue: str) -> Dict[str, int]:
        client = self._check()
        result = client.get_stats(queue)
        stats = result.get("queues", {}).get(queue, {})
        return {
            "pending_count": stats.get("pending_count", 0),
            "claimed_count": stats.get("claimed_count", 0),
            "total_pushed": stats.get("total_pushed", 0),
            "total_acked": stats.get("total_acked", 0),
        }

    def get_pending_count(self, queue: str) -> int:
        return self.get_stats(queue).get("pending_count", 0)

    # QueueGroup API
    def get_group_stats(self, group_name: str) -> Dict:
        """Get aggregate stats for all partitions in a group."""
        client = self._check()
        return client.get_group_stats(group_name)

    def create_queue_group(
        self, group_name: str, num_partitions: int, max_pending_per_partition: int = 0
    ) -> Dict:
        """Create a group of partition queues atomically.

        Args:
            group_name: Name for the queue group.
            num_partitions: Number of partition queues to create.
            max_pending_per_partition: Maximum pending messages per partition
                queue. 0 means unlimited (default).
        """
        client = self._check()
        return client.create_queue_group(group_name, num_partitions, max_pending_per_partition)

    def ack_and_scatter(
        self,
        upstream_queue: str,
        upstream_msg_ids: List[str],
        upstream_claim_tokens: Optional[List[str]],
        group_name: str,
        partition_payloads: Dict[int, List[bytes]],
        state_namespace: Optional[str] = None,
        state_puts: Optional[Dict[str, bytes]] = None,
        state_deletes: Optional[List[str]] = None,
    ) -> List[str]:
        """Atomically ack upstream + push to multiple partition queues."""
        client = self._check()
        try:
            return client.ack_and_scatter(
                upstream_queue,
                upstream_msg_ids,
                upstream_claim_tokens,
                group_name,
                partition_payloads,
                state_namespace=state_namespace,
                state_puts=state_puts,
                state_deletes=state_deletes,
            )
        except RuntimeError as e:
            _raise_typed(e)
            raise

    def claim_from_group(
        self,
        group_name: str,
        batch_size: int = 1,
        timeout_ms: int = 5000,
        assigned_partitions: Optional[List[int]] = None,
        allow_steal: bool = False,
        steal_pending_threshold: int = 0,
    ) -> "tuple[list[AnvilRecord], str, int, bool]":
        """Claim from a partition group (broker picks partition).

        Returns (records, source_queue, source_partition, upstream_drained).
        upstream_drained is True when records is empty AND the group is
        finished + fully drained.
        """
        client = self._check()
        messages, source_queue, source_partition, drained = client.claim_from_group(
            group_name,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            assigned_partitions=assigned_partitions,
            allow_steal=allow_steal,
            steal_pending_threshold=steal_pending_threshold,
        )
        return (
            [AnvilRecord.from_message(m) for m in messages],
            source_queue,
            source_partition,
            drained,
        )

    def is_group_finished(self, group_name: str) -> Dict:
        """Check if all queues in a group are finished and drained."""
        client = self._check()
        return client.is_group_finished(group_name)

    def mark_group_finished(self, group_name: str) -> Dict:
        """Mark all queues in a group as finished."""
        client = self._check()
        return client.mark_group_finished(group_name)

    # Queue Completion API
    def mark_queue_finished(self, queue: str) -> bool:
        """Mark queue as finished (no more messages will be pushed)."""
        client = self._check()
        return client.mark_queue_finished(queue)

    def is_queue_finished(self, queue: str) -> Dict[str, int]:
        """Check if queue is finished and safe to exit.

        Returns dict with: finished, drained, safe_to_exit, pending_count, claimed_count
        """
        client = self._check()
        return client.is_queue_finished(queue)

    def _check(self):
        if self._client is None:
            raise RuntimeError("Client not started")
        return self._client
