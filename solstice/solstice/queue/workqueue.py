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
WorkQueue Implementation - Single-queue Multi-consumer Model.

Components:
- WorkQueueBrokerManager: Manages embedded Rust broker lifecycle
- WorkQueueQueueClient: Client for claim/ack operations

Unlike Kafka's partition model, WorkQueue uses:
- claim: Atomically grab messages (with timeout-based lease)
- ack: Confirm message processing
- nack: Return message to queue for retry

Example:
    # On Master
    broker = WorkQueueBrokerManager(db_path="file:///tmp/wq")
    broker.start()

    client = WorkQueueQueueClient(broker.get_broker_url(), worker_id="master")
    client.start()
    client.create_queue("my-queue")
    client.push("my-queue", b"hello")

    # On Worker
    client = WorkQueueQueueClient("master-host:50051", worker_id="worker-1")
    client.start()
    messages = client.claim("my-queue", batch_size=10)
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

from workqueue_py import BrokerConfig, BrokerError, WorkQueueBroker
from workqueue_py.client import WorkQueueClient, Message

from solstice.utils.logging import create_ray_logger
from solstice.queue.workqueue_storage import WorkQueueStorageReader


# =============================================================================
# WorkQueueBrokerManager
# =============================================================================


class WorkQueueBrokerManager:
    """Manages the embedded WorkQueue broker lifecycle."""

    def __init__(
        self,
        db_path: str = "file:///tmp/workqueue",
        port: int = 0,
        host: str = "0.0.0.0",
        startup_timeout: float = 30.0,
        claim_timeout_secs: float = 60.0,
        recovery_interval_secs: float = 10.0,
        acked_retention_secs: float = 3600.0,
        gc_interval_secs: float = 60.0,
    ):
        self.db_path = db_path
        self.port = port
        self.host = host
        self.startup_timeout = startup_timeout
        self.claim_timeout_secs = claim_timeout_secs
        self.recovery_interval_secs = recovery_interval_secs
        self.acked_retention_secs = acked_retention_secs
        self.gc_interval_secs = gc_interval_secs

        self._broker: Optional[WorkQueueBroker] = None
        self._running = False
        self._actual_port: Optional[int] = None
        self.logger = create_ray_logger(f"WorkQueueBroker:{port}")

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
        self._broker = WorkQueueBroker(config, event_handler=handler)
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

    def get_storage_reader(self) -> Optional[WorkQueueStorageReader]:
        """Get a storage reader backed by the broker's live storage."""
        if not self._broker:
            return None
        try:
            reader = self._broker.get_storage_reader()
            return WorkQueueStorageReader(reader=reader)
        except Exception as e:
            self.logger.warning(f"Failed to get storage reader: {e}")
            return None


class _BrokerEventHandler:
    """Internal event handler for broker lifecycle."""

    def __init__(self, manager: WorkQueueBrokerManager, ready_event: threading.Event):
        self.manager = manager
        self._ready_event = ready_event

    def on_started(self, port: int) -> None:
        self.manager.logger.info(f"WorkQueue broker started on port {port}")
        self.manager._actual_port = port
        self.manager._running = True
        self._ready_event.set()

    def on_stopped(self) -> None:
        self.manager.logger.info("WorkQueue broker stopped")
        self.manager._running = False

    def on_fatal(self, error: BrokerError) -> None:
        self.manager.logger.error(f"WorkQueue broker fatal error: {error.message}")
        self.manager._running = False
        self._ready_event.set()


# =============================================================================
# WorkQueueQueueClient
# =============================================================================


@dataclass
class WorkQueueRecord:
    """A record from WorkQueue."""

    msg_id: str
    value: bytes
    queue: str
    created_at: float
    metadata: Dict[str, str]
    claim_token: Optional[str] = None

    @classmethod
    def from_message(cls, msg: Message) -> "WorkQueueRecord":
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
    if claim_timeout_secs <= 0:
        return 0.1
    return max(0.1, min(5.0, claim_timeout_secs / 2))


class WorkQueueQueueClient:
    """WorkQueue client for claim/ack operations."""

    def __init__(
        self,
        broker_url: str,
        worker_id: str = "default",
        heartbeat_interval_secs: Optional[float] = None,
    ):
        self.broker_url = broker_url
        self.worker_id = worker_id
        self.heartbeat_interval_secs = heartbeat_interval_secs
        self._client: Optional[WorkQueueClient] = None
        self._running = False
        self.logger = create_ray_logger(f"WorkQueueClient:{worker_id}")

    # Lifecycle
    def start(self) -> None:
        if self._running:
            return
        if self.heartbeat_interval_secs is None:
            self._client = WorkQueueClient(self.broker_url, self.worker_id)
        else:
            self._client = WorkQueueClient(
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
    def create_queue(self, queue: str) -> None:
        self._check()
        self._client.create_queue(queue)

    def delete_queue(self, queue: str) -> None:
        self._check()
        self._client.delete_queue(queue)

    # Producer
    def push(self, queue: str, value: bytes, metadata: Optional[Dict[str, str]] = None) -> str:
        self._check()
        return self._client.push(queue, value, metadata or {})

    def push_batch(self, queue: str, values: List[bytes]) -> List[str]:
        self._check()
        return self._client.push_batch(queue, values)

    # Consumer
    def claim(
        self, queue: str, batch_size: int = 1, timeout_ms: int = 5000
    ) -> List[WorkQueueRecord]:
        self._check()
        messages = self._client.claim(queue, batch_size, timeout_ms)
        return [WorkQueueRecord.from_message(m) for m in messages]

    def ack(
        self,
        queue: str,
        msg_ids: List[str],
        claim_tokens: Optional[List[str]] = None,
        state_namespace: Optional[str] = None,
        state_puts: Optional[Dict[str, bytes]] = None,
        state_deletes: Optional[List[str]] = None,
    ) -> int:
        self._check()
        return self._client.ack(
            queue,
            msg_ids,
            claim_tokens=claim_tokens,
            state_namespace=state_namespace,
            state_puts=state_puts,
            state_deletes=state_deletes,
        )

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
        self._check()
        return self._client.nack(
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
        self._check()
        return self._client.ack_and_forward(
            upstream_queue,
            upstream_msg_ids,
            upstream_claim_tokens,
            downstream_queue,
            downstream_payloads,
            state_namespace=state_namespace,
            state_puts=state_puts,
            state_deletes=state_deletes,
        )

    # State
    def state_get(self, namespace: str, keys: List[str]) -> Dict[str, bytes]:
        self._check()
        return self._client.state_get(namespace, keys)

    def state_put(
        self,
        namespace: str,
        puts: Optional[Dict[str, bytes]] = None,
        deletes: Optional[List[str]] = None,
    ) -> tuple:
        self._check()
        return self._client.state_put(namespace, puts, deletes)

    # Stats
    def get_stats(self, queue: str) -> Dict[str, int]:
        self._check()
        result = self._client.get_stats(queue)
        stats = result.get("queues", {}).get(queue, {})
        return {
            "pending_count": stats.get("pending_count", 0),
            "claimed_count": stats.get("claimed_count", 0),
            "total_pushed": stats.get("total_pushed", 0),
            "total_acked": stats.get("total_acked", 0),
        }

    def get_pending_count(self, queue: str) -> int:
        return self.get_stats(queue).get("pending_count", 0)

    # Queue Completion API
    def mark_queue_finished(self, queue: str) -> bool:
        """Mark queue as finished (no more messages will be pushed)."""
        self._check()
        return self._client.mark_queue_finished(queue)

    def is_queue_finished(self, queue: str) -> Dict[str, int]:
        """Check if queue is finished and safe to exit.

        Returns dict with: finished, drained, safe_to_exit, pending_count, claimed_count
        """
        self._check()
        return self._client.is_queue_finished(queue)

    def _check(self) -> None:
        if self._client is None:
            raise RuntimeError("Client not started")
