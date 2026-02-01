"""Queue backend for inter-stage communication.

WorkQueue provides single-queue multi-consumer model with:
- claim: Atomically grab messages (with timeout-based lease)
- ack: Confirm message processing
- nack: Return message to queue for retry

Example:
    from solstice.queue import WorkQueueBrokerManager, WorkQueueQueueClient

    # On StageMaster - start broker
    broker = WorkQueueBrokerManager(db_path="file:///tmp/wq")
    broker.start()

    # Create client
    client = WorkQueueQueueClient(broker.get_broker_url(), worker_id="master")
    client.start()

    client.create_queue("my-queue")
    client.push("my-queue", b"message data")
    messages = client.claim("my-queue", batch_size=10)
    client.ack("my-queue", [m.msg_id for m in messages])

    client.stop()
    broker.stop()
"""

from solstice.queue.backend import Record
from solstice.queue.workqueue import (
    WorkQueueBrokerManager,
    WorkQueueQueueClient,
    WorkQueueRecord,
)

__all__ = [
    "Record",
    "WorkQueueBrokerManager",
    "WorkQueueQueueClient",
    "WorkQueueRecord",
]
