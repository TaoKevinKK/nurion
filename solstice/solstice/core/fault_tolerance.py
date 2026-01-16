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

"""Fault tolerance utilities for Solstice.

Provides:
- NodeBlacklist: Track and quarantine problematic nodes
- TimeoutMonitor: Detect and handle stuck workers
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set


@dataclass
class NodeBlacklistConfig:
    """Configuration for node blacklisting.

    Attributes:
        enabled: Whether blacklisting is enabled
        quarantine_ttl_seconds: How long a node stays blacklisted
        failures_to_blacklist: Number of failures before blacklisting
        max_blacklisted_nodes: Maximum nodes to blacklist (prevents cluster starvation)
        failure_window_seconds: Time window for counting failures
    """

    enabled: bool = True
    quarantine_ttl_seconds: float = 300.0  # 5 minutes
    failures_to_blacklist: int = 2
    max_blacklisted_nodes: int = 10
    failure_window_seconds: float = 600.0  # 10 minutes


@dataclass
class NodeFailure:
    """Record of a node failure."""

    node_id: str
    worker_id: str
    reason: str
    timestamp: float


@dataclass
class BlacklistedNode:
    """A blacklisted node with expiry time."""

    node_id: str
    blacklisted_at: float
    expires_at: float
    failure_count: int
    last_reason: str


class NodeBlacklist:
    """Track and quarantine problematic nodes.

    When a node experiences repeated failures (e.g., GPU errors, OOM),
    it gets added to the blacklist. Workers will not be scheduled on
    blacklisted nodes until the quarantine period expires.

    Thread-safe implementation.

    Usage:
        blacklist = NodeBlacklist(NodeBlacklistConfig())

        # Record failures
        blacklist.record_failure("node-1", "worker-0", "CUDA OOM")
        blacklist.record_failure("node-1", "worker-1", "GPU error")

        # Check if node is blacklisted
        if blacklist.is_blacklisted("node-1"):
            # Don't schedule on this node
            pass

        # Get scheduling options for Ray
        options = blacklist.get_scheduling_options()
        # options = {"scheduling_strategy": NodeAffinitySchedulingStrategy(...)}
    """

    def __init__(self, config: Optional[NodeBlacklistConfig] = None):
        """Initialize node blacklist.

        Args:
            config: Blacklist configuration
        """
        self._config = config or NodeBlacklistConfig()
        self._failures: Dict[str, List[NodeFailure]] = {}  # node_id -> failures
        self._blacklist: Dict[str, BlacklistedNode] = {}  # node_id -> info
        self._lock = threading.Lock()
        self._logger = logging.getLogger("NodeBlacklist")

    def record_failure(
        self,
        node_id: str,
        worker_id: str,
        reason: str,
    ) -> bool:
        """Record a failure on a node.

        Args:
            node_id: Ray node ID where failure occurred
            worker_id: Worker that failed
            reason: Description of the failure

        Returns:
            True if node was blacklisted as a result
        """
        if not self._config.enabled:
            return False

        with self._lock:
            now = time.time()

            # Create failure record
            failure = NodeFailure(
                node_id=node_id,
                worker_id=worker_id,
                reason=reason,
                timestamp=now,
            )

            # Initialize failure list for this node
            if node_id not in self._failures:
                self._failures[node_id] = []

            # Clean up old failures outside the window
            window_start = now - self._config.failure_window_seconds
            self._failures[node_id] = [
                f for f in self._failures[node_id] if f.timestamp > window_start
            ]

            # Add new failure
            self._failures[node_id].append(failure)

            # Check if we should blacklist
            failure_count = len(self._failures[node_id])
            if failure_count >= self._config.failures_to_blacklist:
                return self._blacklist_node(node_id, failure_count, reason)

            return False

    def _blacklist_node(
        self,
        node_id: str,
        failure_count: int,
        reason: str,
    ) -> bool:
        """Add a node to the blacklist.

        Must be called with lock held.

        Returns:
            True if node was blacklisted
        """
        # Check if already blacklisted
        if node_id in self._blacklist:
            # Extend the blacklist period
            self._blacklist[node_id].expires_at = time.time() + self._config.quarantine_ttl_seconds
            self._blacklist[node_id].failure_count = failure_count
            self._blacklist[node_id].last_reason = reason
            self._logger.warning(f"Extended blacklist for node {node_id}: {reason}")
            return True

        # Check max blacklisted nodes limit
        self._cleanup_expired()
        if len(self._blacklist) >= self._config.max_blacklisted_nodes:
            self._logger.warning(
                f"Max blacklisted nodes ({self._config.max_blacklisted_nodes}) reached, "
                f"not blacklisting {node_id}"
            )
            return False

        # Add to blacklist
        now = time.time()
        self._blacklist[node_id] = BlacklistedNode(
            node_id=node_id,
            blacklisted_at=now,
            expires_at=now + self._config.quarantine_ttl_seconds,
            failure_count=failure_count,
            last_reason=reason,
        )

        self._logger.warning(
            f"Blacklisted node {node_id} for {self._config.quarantine_ttl_seconds}s: "
            f"{failure_count} failures, last: {reason}"
        )
        return True

    def _cleanup_expired(self) -> None:
        """Remove expired entries from blacklist.

        Must be called with lock held.
        """
        now = time.time()
        expired = [node_id for node_id, info in self._blacklist.items() if info.expires_at <= now]
        for node_id in expired:
            del self._blacklist[node_id]
            self._logger.info(f"Node {node_id} removed from blacklist (expired)")

    def is_blacklisted(self, node_id: str) -> bool:
        """Check if a node is currently blacklisted.

        Args:
            node_id: Ray node ID to check

        Returns:
            True if node is blacklisted
        """
        if not self._config.enabled:
            return False

        with self._lock:
            self._cleanup_expired()
            return node_id in self._blacklist

    def get_blacklisted_nodes(self) -> Set[str]:
        """Get set of currently blacklisted node IDs.

        Returns:
            Set of blacklisted node IDs
        """
        with self._lock:
            self._cleanup_expired()
            return set(self._blacklist.keys())

    def get_scheduling_options(self) -> dict:
        """Get Ray scheduling options to exclude blacklisted nodes.

        Returns:
            Dictionary with scheduling_strategy if nodes are blacklisted,
            empty dict otherwise.

        Usage:
            options = blacklist.get_scheduling_options()
            actor = MyActor.options(**options).remote()
        """
        blacklisted = self.get_blacklisted_nodes()
        if not blacklisted:
            return {}

        # Note: Ray's NodeAffinitySchedulingStrategy with soft=True
        # and exclude_nodes is the way to avoid specific nodes
        # For now, return the list for manual handling
        return {"_excluded_nodes": list(blacklisted)}

    def remove_from_blacklist(self, node_id: str) -> bool:
        """Manually remove a node from the blacklist.

        Args:
            node_id: Node to remove

        Returns:
            True if node was in blacklist
        """
        with self._lock:
            if node_id in self._blacklist:
                del self._blacklist[node_id]
                self._logger.info(f"Node {node_id} manually removed from blacklist")
                return True
            return False

    def clear(self) -> None:
        """Clear all failures and blacklist entries."""
        with self._lock:
            self._failures.clear()
            self._blacklist.clear()

    def get_stats(self) -> dict:
        """Get blacklist statistics.

        Returns:
            Dictionary with blacklist state and history
        """
        with self._lock:
            self._cleanup_expired()
            return {
                "enabled": self._config.enabled,
                "blacklisted_count": len(self._blacklist),
                "blacklisted_nodes": [
                    {
                        "node_id": info.node_id,
                        "blacklisted_at": info.blacklisted_at,
                        "expires_at": info.expires_at,
                        "failure_count": info.failure_count,
                        "last_reason": info.last_reason,
                        "time_remaining": max(0, info.expires_at - time.time()),
                    }
                    for info in self._blacklist.values()
                ],
                "failure_counts": {
                    node_id: len(failures) for node_id, failures in self._failures.items()
                },
            }


@dataclass
class TimeoutConfig:
    """Configuration for timeout monitoring.

    Attributes:
        enabled: Whether timeout monitoring is enabled
        split_timeout_seconds: Maximum time for processing a single split
        check_interval_seconds: How often to check for timeouts
        grace_period_seconds: Extra time before declaring timeout
    """

    enabled: bool = True
    split_timeout_seconds: float = 300.0  # 5 minutes
    check_interval_seconds: float = 10.0
    grace_period_seconds: float = 30.0


@dataclass
class WorkerSplitInfo:
    """Information about a worker's current split."""

    worker_id: str
    split_id: str
    start_time: float
    last_heartbeat: float


class TimeoutMonitor:
    """Monitor workers for stuck/timed-out splits.

    Tracks which split each worker is processing and detects when
    processing takes too long. This helps identify:
    - Stuck workers (e.g., deadlock, infinite loop)
    - Slow workers (e.g., hardware issues)
    - Network issues (e.g., can't reach upstream)

    Usage:
        monitor = TimeoutMonitor(TimeoutConfig(split_timeout_seconds=300))

        # Worker starts processing
        monitor.record_split_start("worker-0", "split-123")

        # Worker sends heartbeat (optional, for long-running splits)
        monitor.record_heartbeat("worker-0")

        # Worker finishes
        monitor.record_split_complete("worker-0")

        # Check for timeouts
        timed_out = monitor.check_timeouts()
        for worker_id in timed_out:
            # Kill and restart worker
            pass
    """

    def __init__(self, config: Optional[TimeoutConfig] = None):
        """Initialize timeout monitor.

        Args:
            config: Timeout configuration
        """
        self._config = config or TimeoutConfig()
        self._workers: Dict[str, WorkerSplitInfo] = {}
        self._lock = threading.Lock()
        self._logger = logging.getLogger("TimeoutMonitor")

    def record_split_start(self, worker_id: str, split_id: str) -> None:
        """Record that a worker started processing a split.

        Args:
            worker_id: Worker identifier
            split_id: Split being processed
        """
        if not self._config.enabled:
            return

        with self._lock:
            now = time.time()
            self._workers[worker_id] = WorkerSplitInfo(
                worker_id=worker_id,
                split_id=split_id,
                start_time=now,
                last_heartbeat=now,
            )

    def record_heartbeat(self, worker_id: str) -> None:
        """Record a heartbeat from a worker.

        For long-running splits, workers can send heartbeats to indicate
        they're still making progress.

        Args:
            worker_id: Worker identifier
        """
        if not self._config.enabled:
            return

        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].last_heartbeat = time.time()

    def record_split_complete(self, worker_id: str) -> None:
        """Record that a worker completed processing a split.

        Args:
            worker_id: Worker identifier
        """
        with self._lock:
            self._workers.pop(worker_id, None)

    def check_timeouts(self) -> List[str]:
        """Check for timed-out workers.

        Returns:
            List of worker IDs that have timed out
        """
        if not self._config.enabled:
            return []

        with self._lock:
            now = time.time()
            timeout_threshold = self._config.split_timeout_seconds
            grace = self._config.grace_period_seconds
            timed_out = []

            for worker_id, info in self._workers.items():
                elapsed = now - info.start_time
                since_heartbeat = now - info.last_heartbeat

                # Check if exceeded timeout (with grace period)
                if elapsed > (timeout_threshold + grace):
                    timed_out.append(worker_id)
                    self._logger.warning(
                        f"Worker {worker_id} timed out processing split {info.split_id}: "
                        f"{elapsed:.1f}s elapsed (timeout: {timeout_threshold}s)"
                    )
                # Also check if no heartbeat for too long
                elif since_heartbeat > (timeout_threshold / 2 + grace):
                    timed_out.append(worker_id)
                    self._logger.warning(
                        f"Worker {worker_id} no heartbeat for {since_heartbeat:.1f}s "
                        f"while processing split {info.split_id}"
                    )

            return timed_out

    def get_worker_info(self, worker_id: str) -> Optional[WorkerSplitInfo]:
        """Get current split info for a worker.

        Args:
            worker_id: Worker identifier

        Returns:
            WorkerSplitInfo if worker is processing, None otherwise
        """
        with self._lock:
            return self._workers.get(worker_id)

    def get_all_workers(self) -> Dict[str, WorkerSplitInfo]:
        """Get info for all workers currently processing splits.

        Returns:
            Dictionary of worker_id -> WorkerSplitInfo
        """
        with self._lock:
            return dict(self._workers)

    def remove_worker(self, worker_id: str) -> None:
        """Remove a worker from tracking (e.g., when worker is killed).

        Args:
            worker_id: Worker identifier
        """
        with self._lock:
            self._workers.pop(worker_id, None)

    def clear(self) -> None:
        """Clear all worker tracking."""
        with self._lock:
            self._workers.clear()

    def get_stats(self) -> dict:
        """Get timeout monitor statistics.

        Returns:
            Dictionary with monitor state
        """
        with self._lock:
            now = time.time()
            return {
                "enabled": self._config.enabled,
                "timeout_seconds": self._config.split_timeout_seconds,
                "active_workers": len(self._workers),
                "workers": [
                    {
                        "worker_id": info.worker_id,
                        "split_id": info.split_id,
                        "elapsed_seconds": now - info.start_time,
                        "since_heartbeat_seconds": now - info.last_heartbeat,
                    }
                    for info in self._workers.values()
                ],
            }
