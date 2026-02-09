"""Queue backend for inter-stage communication.

WorkQueue provides single-queue multi-consumer model with:
- claim: Atomically grab messages (with timeout-based lease)
- ack: Confirm message processing
- nack: Return message to queue for retry

Example:
    from _internal.queue import WorkQueueBrokerManager, WorkQueueQueueClient

    # On StageMaster - start broker
    broker = WorkQueueBrokerManager(db_path="file:///tmp/wq")
    broker.start()

    # Create client
    client = WorkQueueQueueClient(broker.get_broker_url(), worker_id="master")
    client.start()

    client.create_queue("my-queue")
    client.push("my-queue", b"message data")
    messages = client.claim("my-queue", batch_size=10)
    client.ack(
        "my-queue",
        [m.msg_id for m in messages],
        claim_tokens=[m.claim_token for m in messages],
    )

    client.stop()
    broker.stop()
"""

from _internal.queue.backend import Record
from _internal.queue.workqueue import (
    WorkQueueBrokerManager,
    WorkQueueQueueClient,
    WorkQueueRecord,
)
from _internal.queue.workqueue_storage import WorkQueueStorageReader

__all__ = [
    "Record",
    "WorkQueueBrokerManager",
    "WorkQueueQueueClient",
    "WorkQueueRecord",
    "WorkQueueStorageReader",
]
