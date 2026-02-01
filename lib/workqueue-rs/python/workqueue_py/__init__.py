"""WorkQueue Python bindings - single-queue multi-consumer work queue."""

from typing import Optional

# Import Rust implementations
from workqueue_py.workqueue_py import (  # type: ignore
    BrokerConfig as _BrokerConfig,
    BrokerError as _BrokerError,
    WorkQueueBroker as _WorkQueueBroker,
)

# Re-export for better IDE support
BrokerConfig = _BrokerConfig
BrokerError = _BrokerError
WorkQueueBroker = _WorkQueueBroker


class BrokerEventHandler:
    """Base class for broker event callbacks.

    Subclass this and override methods to handle broker lifecycle events.

    Example:
        class MyHandler(BrokerEventHandler):
            def on_started(self, port: int) -> None:
                print(f"Broker started on port {port}")

            def on_fatal(self, error: BrokerError) -> None:
                print(f"Fatal error: {error}")

        handler = MyHandler()
        broker = WorkQueueBroker(config, event_handler=handler)
        broker.start()
    """

    def on_started(self, port: int) -> None:
        """Called when broker successfully starts.

        Args:
            port: The actual port the broker is listening on
        """
        pass

    def on_stopped(self) -> None:
        """Called when broker stops normally."""
        pass

    def on_error(self, error: "BrokerError") -> None:
        """Called when a recoverable error occurs.

        Args:
            error: The error that occurred
        """
        pass

    def on_fatal(self, error: "BrokerError") -> None:
        """Called when a fatal error occurs (broker will crash).

        Args:
            error: The error that occurred
        """
        pass


__all__ = [
    "BrokerConfig",
    "BrokerError",
    "WorkQueueBroker",
    "BrokerEventHandler",
]
