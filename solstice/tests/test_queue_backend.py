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

"""Tests for WorkQueue backend.

This module contains unit tests for WorkQueueBrokerManager + WorkQueueQueueClient:
- Single-queue multi-consumer model
- claim/ack operations
- ack_and_forward for exactly-once semantics

Test categories:
1. Basic operations: push, claim, ack
2. Batch operations: claim batches
3. Exactly-once semantics: ack_and_forward
4. Edge cases: empty queues, concurrent access
"""

import pytest

from solstice.queue import WorkQueueBrokerManager, WorkQueueQueueClient


# ============================================================================
# WorkQueue Tests (Broker + Client)
# Uses workqueue_broker_and_client fixture from conftest.py
# ============================================================================


@pytest.mark.slow
class TestWorkQueueBrokerManager:
    """Tests for WorkQueueBrokerManager (QueueBroker implementation)."""

    def test_start_stop(self, workqueue_broker_and_client):
        """Test broker lifecycle."""
        broker, client = workqueue_broker_and_client
        assert broker.is_running()

    def test_get_broker_url(self, workqueue_broker_and_client):
        """Test getting broker URL."""
        broker, client = workqueue_broker_and_client
        broker_url = broker.get_broker_url()
        assert ":" in broker_url
        port = int(broker_url.split(":")[1])
        assert 1024 < port < 65535


@pytest.mark.slow
class TestWorkQueueQueueClient:
    """Tests for WorkQueueQueueClient (claim/ack operations)."""

    def test_health_check(self, workqueue_broker_and_client):
        """Test client health check."""
        broker, client = workqueue_broker_and_client
        assert client.health_check()

    def test_create_queue(self, workqueue_broker_and_client):
        """Test queue creation."""
        broker, client = workqueue_broker_and_client
        client.create_queue("test-queue")
        # Should not raise

    def test_push_claim_ack(self, workqueue_broker_and_client):
        """Test push, claim, and ack."""
        broker, client = workqueue_broker_and_client
        queue = "test-queue"
        client.create_queue(queue)

        # Push
        msg_id = client.push(queue, b"hello workqueue")
        assert msg_id  # Should be a non-empty string

        # Claim
        records = client.claim(queue, batch_size=1, timeout_ms=1000)
        assert len(records) == 1
        assert records[0].value == b"hello workqueue"
        assert records[0].msg_id == msg_id

        # Ack
        acked = client.ack(queue, [msg_id])
        assert acked == 1

        # Claim again should be empty
        records = client.claim(queue, batch_size=1, timeout_ms=100)
        assert len(records) == 0

    def test_push_batch(self, workqueue_broker_and_client):
        """Test batch push."""
        broker, client = workqueue_broker_and_client
        queue = "test-queue"
        client.create_queue(queue)

        # Push batch
        values = [f"msg-{i}".encode() for i in range(5)]
        msg_ids = client.push_batch(queue, values)
        assert len(msg_ids) == 5

        # Claim all
        records = client.claim(queue, batch_size=10, timeout_ms=1000)
        assert len(records) == 5

    def test_nack_returns_to_queue(self, workqueue_broker_and_client):
        """Test nack returns message to queue."""
        broker, client = workqueue_broker_and_client
        queue = "test-queue"
        client.create_queue(queue)

        # Push and claim
        msg_id = client.push(queue, b"test message")
        records = client.claim(queue, batch_size=1, timeout_ms=1000)
        assert len(records) == 1

        # Nack
        nacked = client.nack(queue, [msg_id])
        assert nacked == 1

        # Should be able to claim again
        records = client.claim(queue, batch_size=1, timeout_ms=1000)
        assert len(records) == 1
        assert records[0].msg_id == msg_id

    def test_get_stats(self, workqueue_broker_and_client):
        """Test getting queue statistics."""
        broker, client = workqueue_broker_and_client
        queue = "test-queue"
        client.create_queue(queue)

        # Push some messages
        for i in range(5):
            client.push(queue, f"msg-{i}".encode())

        # Get stats
        stats = client.get_stats(queue)
        assert stats["pending_count"] == 5
        assert stats["claimed_count"] == 0

        # Claim some
        records = client.claim(queue, batch_size=2, timeout_ms=1000)

        stats = client.get_stats(queue)
        assert stats["pending_count"] == 3
        assert stats["claimed_count"] == 2


@pytest.mark.slow
class TestWorkQueueAckAndForward:
    """Tests for ack_and_forward operation."""

    def test_ack_and_forward_basic(self, workqueue_broker_and_client):
        """Test atomic ack and forward operation."""
        broker, client = workqueue_broker_and_client
        upstream = "upstream-queue"
        downstream = "downstream-queue"
        client.create_queue(upstream)
        client.create_queue(downstream)

        # Push to upstream
        msg_id = client.push(upstream, b"input data")

        # Claim from upstream
        records = client.claim(upstream, batch_size=1, timeout_ms=1000)
        assert len(records) == 1

        # Ack and forward
        new_ids = client.ack_and_forward(
            upstream_queue=upstream,
            upstream_msg_ids=[msg_id],
            downstream_queue=downstream,
            downstream_payloads=[b"output data"],
        )
        assert len(new_ids) == 1

        # Upstream should be empty
        upstream_records = client.claim(upstream, batch_size=1, timeout_ms=100)
        assert len(upstream_records) == 0

        # Downstream should have the message
        downstream_records = client.claim(downstream, batch_size=1, timeout_ms=1000)
        assert len(downstream_records) == 1
        assert downstream_records[0].value == b"output data"


@pytest.mark.slow
class TestWorkQueueMultiClient:
    """Tests for multiple clients connecting to same broker."""

    def test_two_clients_communication(self, workqueue_broker_and_client):
        """Test two clients producing and consuming."""
        broker, client1 = workqueue_broker_and_client

        # Create second client
        client2 = WorkQueueQueueClient(broker.get_broker_url(), worker_id="client-2")
        client2.start()

        try:
            queue = "shared-queue"
            client1.create_queue(queue)

            # Client 1 pushes
            id1 = client1.push(queue, b"from client1")

            # Client 2 pushes
            id2 = client2.push(queue, b"from client2")

            # Both clients can claim messages
            records1 = client1.claim(queue, batch_size=1, timeout_ms=1000)
            records2 = client2.claim(queue, batch_size=1, timeout_ms=1000)

            # Both clients got one message each (competing consumers)
            assert len(records1) == 1
            assert len(records2) == 1

            # Messages are different
            assert records1[0].msg_id != records2[0].msg_id

        finally:
            client2.stop()
