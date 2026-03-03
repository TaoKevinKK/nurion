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

"""Ray runtime for executing Nurion runtime jobs with queue-based architecture.

Architecture:
- Workers claim messages from upstream queues (competing consumers)
- Masters manage their output queue
- Message ID-based recovery via WorkQueue
- Optional autoscaling for dynamic worker management
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import ray

from _internal.core.job import Job

if TYPE_CHECKING:
    from _internal.core.stage import Stage
    from _internal.webui.job_webui import JobWebUI
    from _internal.webui.runtime_server import EmbeddedWebUIServer
from _internal.core.stage import StageRuntime
from _internal.core.stage_master import (
    StageMaster,
    QueueEndpoint,
)
from _internal.core.split_payload_store import (
    SplitPayloadStore,
    RaySplitPayloadStore,
    FsspecSplitPayloadStore,
)
from _internal.queue import WorkQueueBrokerManager
from _internal.runtime.autoscaler import SimpleAutoscaler
from _internal.runtime.backpressure import JobBackpressureController
from _internal.runtime.queue_stats import QueueStatsClient, StageQueueConfig
from _internal.utils.logging import create_ray_logger
from _internal.webui.state.writer import WorkQueueStateWriter


@dataclass
class JobStatus:
    """Status of the entire pipeline."""

    job_id: str
    is_running: bool
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    start_time: Optional[float] = None
    elapsed_time: float = 0.0
    error: Optional[str] = None


class RayJobRunner:
    """Job runner using queue-based architecture.

    Features:
    - StageMaster for simplified, output-queue only management
    - Workers claim from upstream queues (competing consumers)
    - Message ID-based recovery via WorkQueue
    - Async-first design

    Example:
        ```python
        job = Job(job_id="my_job")
        job.add_stage(source_stage)
        job.add_stage(transform_stage)
        job.add_stage(sink_stage)

        runner = RayJobRunner(job)
        await runner.run()
        ```
    """

    def __init__(self, job: Job):
        """Initialize the runner.

        Args:
            job: The job to run (configuration read from job.config)
        """
        self.job = job

        # Read configuration from job.config
        config = job.config
        self.workqueue_db_path = config.workqueue_db_path
        self._ray_init_kwargs = config.ray_init_kwargs or {}

        self.logger = create_ray_logger(f"RayJobRunner-{job.job_id}")

        # SplitPayloadStore - shared across all stages
        self._payload_store: Optional[SplitPayloadStore] = None

        # Stage masters (not Ray actors - they manage their own workers)
        self._masters: Dict[str, StageMaster] = {}
        self._master_tasks: Dict[str, asyncio.Task] = {}

        # Autoscaler (configured in run())
        self._autoscaler: Optional[SimpleAutoscaler] = None
        self._autoscale_task: Optional[asyncio.Task] = None

        # WebUI
        self._webui: Optional["JobWebUI"] = None
        self._webui_server: Optional["EmbeddedWebUIServer"] = None
        self._webui_port: Optional[int] = None
        self._webui_storage: Optional[Any] = None
        self._state_writer: Optional[WorkQueueStateWriter] = None

        # Shared WorkQueue broker for all stages (reduces resource usage and improves stability)
        self._shared_broker: Optional[WorkQueueBrokerManager] = None
        self._broker_endpoint: Optional[QueueEndpoint] = None
        self._queue_stats_client: Optional[QueueStatsClient] = None
        self._backpressure_controller: Optional[JobBackpressureController] = None
        self._stage_queue_configs: Dict[str, StageQueueConfig] = {}

        # State
        self._initialized = False
        self._running = False
        self._start_time: Optional[float] = None
        self._error: Optional[str] = None

        # DAG info
        self._reverse_dag: Dict[str, List[str]] = {}

    def _ensure_ray(self) -> None:
        """Ensure Ray is initialized."""
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, **self._ray_init_kwargs)

    async def _create_shared_broker(self) -> None:
        """Create a single shared WorkQueue broker for all stages.

        This improves stability by having one broker process instead of one per stage.
        All stages connect to this broker and create their own queues.
        """
        from _internal.utils.network import get_node_ip

        config = self.job.config
        self._shared_broker = WorkQueueBrokerManager(
            db_path=self.workqueue_db_path or "memory://",
            host=get_node_ip(),  # Use actual IP instead of 127.0.0.1 for cross-node access
            claim_timeout_secs=config.claim_timeout_secs,
            recovery_interval_secs=config.recovery_interval_secs,
        )
        self._shared_broker.start()

        broker_url = self._shared_broker.get_broker_url()
        host, port_str = broker_url.split(":")

        self._broker_endpoint = QueueEndpoint(
            host=host,
            port=int(port_str),
            storage_url=self.workqueue_db_path or "memory://",
        )

        self.logger.info(f"Created shared WorkQueue broker at {broker_url}")

    async def _stop_shared_broker(self) -> None:
        """Stop the shared WorkQueue broker."""
        if self._shared_broker:
            try:
                self._shared_broker.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping shared broker: {e}")
            self._shared_broker = None
            self._broker_endpoint = None

    def _create_payload_store(self) -> SplitPayloadStore:
        """Create a SplitPayloadStore based on job config URI.

        Returns:
            A ``RaySplitPayloadStore`` for ``ray://`` URIs (default), or a
            ``FsspecSplitPayloadStore`` for any other fsspec-compatible URI
            (e.g. ``s3://``, ``file://``).
        """
        uri = self.job.config.payload_store_uri
        if uri == "ray://" or uri.startswith("ray://"):
            store = RaySplitPayloadStore(name=f"payload_store_{self.job.job_id}")
            store.wait_ready()
            return store
        else:
            return FsspecSplitPayloadStore(
                base_uri=uri,
                job_id=self.job.job_id,
                storage_options=self.job.config.payload_store_options or None,
            )

    async def initialize(self) -> None:
        """Initialize the pipeline."""
        if self._initialized:
            return

        self._ensure_ray()
        self.logger.info(f"Initializing job {self.job.job_id}")

        # Create SplitPayloadStore - shared across all stages
        self._payload_store = self._create_payload_store()
        self.logger.info(f"Created SplitPayloadStore for job {self.job.job_id}")

        # Create shared WorkQueue broker for all stages (if using WorkQueue)
        await self._create_shared_broker()

        # Initialize state writer (gRPC) for WebUI metadata
        if self.job.config.webui.enabled and self._broker_endpoint:
            self._state_writer = WorkQueueStateWriter(
                job_id=self.job.job_id,
                broker_endpoint=self._broker_endpoint,
                claim_timeout_secs=self.job.config.claim_timeout_secs,
            )
            self._state_writer.start()

        # Create storage for WebUI (WorkQueue reader)
        if self.job.config.webui.enabled:
            self._webui_storage = await self._create_webui_storage()

        # Build reverse DAG (stage -> its upstreams)
        self._reverse_dag = self.job.build_reverse_dag()

        # Create masters in topological order
        processing_order = self._get_topological_order()

        for stage_id in processing_order:
            stage = self.job.stages[stage_id]
            upstream_ids = self._reverse_dag.get(stage_id, [])
            is_source = not upstream_ids

            # Determine upstream queue name (None for source stages)
            upstream_queue_name: Optional[str] = None

            upstream_partition_queue_names: Optional[tuple[str, ...]] = None

            if not is_source:
                # Non-source stage: get upstream queue name
                # TODO: Implement multi-upstream support (currently only uses first upstream)
                if len(upstream_ids) > 1:
                    self.logger.warning(
                        f"Stage {stage_id} has {len(upstream_ids)} upstreams but "
                        f"multi-upstream is not yet implemented. Using first upstream only."
                    )
                upstream_id = upstream_ids[0]
                upstream_master = self._masters[upstream_id]

                # Start upstream if needed to get its queue name
                if not upstream_master._running:
                    await upstream_master.start()

                # Check if upstream is a shuffle stage with partition queues
                partition_queues = upstream_master.get_partition_queue_names()
                if partition_queues:
                    # Downstream reads from upstream's partition queues
                    upstream_partition_queue_names = partition_queues
                    # Also set a regular upstream queue name for fallback/backpressure
                    upstream_queue_name = upstream_master._output_queue_name
                else:
                    upstream_queue_name = upstream_master._output_queue_name

            # Build immutable StageRuntime with all info
            runtime = self._build_stage_runtime(
                stage,
                upstream_queue_name,
                upstream_partition_queue_names,
            )

            # Create master using operator_config.master_class (or default StageMaster)
            master = self._create_master(stage, runtime)
            self._masters[stage_id] = master
            self.logger.info(f"Created {type(master).__name__} for stage {stage_id}")

        # Build queue config map and attach job-level backpressure controller
        self._stage_queue_configs = self._build_stage_queue_configs()
        self._queue_stats_client = self._create_queue_stats_client()
        if self._queue_stats_client:
            self._backpressure_controller = JobBackpressureController(
                queue_stats=self._queue_stats_client,
                stage_configs=self._stage_queue_configs,
                dag_edges=self.job.dag_edges,
            )
            for master in self._masters.values():
                master.set_backpressure_provider(self._backpressure_controller)

        # Initialize WebUI if enabled
        if self.job.config.webui.enabled:
            await self._initialize_webui()

        self._initialized = True
        self.logger.info(f"Initialized {len(self._masters)} stages")

    def _build_stage_queue_configs(self) -> Dict[str, StageQueueConfig]:
        configs: Dict[str, StageQueueConfig] = {}
        for stage_id, master in self._masters.items():
            cfg = StageQueueConfig(
                stage_id=stage_id,
                input_queue_name=master.get_backpressure_input_queue_name(),
                output_queue_name=master.get_backpressure_output_queue_name(),
                backpressure_threshold_lag=master.stage.backpressure_threshold_lag,
                backpressure_threshold_queue_size=master.stage.backpressure_threshold_queue_size,
            )
            configs[stage_id] = cfg
        return configs

    def _create_queue_stats_client(self) -> Optional[QueueStatsClient]:
        if not self._broker_endpoint:
            return None
        return QueueStatsClient(
            endpoint=self._broker_endpoint,
            claim_timeout_secs=self.job.config.claim_timeout_secs,
        )

    def _build_stage_runtime(
        self,
        stage: "Stage",
        upstream_queue_name: Optional[str] = None,
        upstream_partition_queue_names: Optional[tuple[str, ...]] = None,
    ) -> StageRuntime:
        """Build StageRuntime from job and runner configuration.

        Args:
            stage: The stage being configured
            upstream_queue_name: Queue name for upstream stage (None for source)
            upstream_partition_queue_names: Partition queue names if upstream
                is a shuffle stage (None for non-shuffle upstream)
        """
        return StageRuntime(
            broker_endpoint=self._broker_endpoint,
            upstream_queue_name=upstream_queue_name,
            claim_timeout_secs=self.job.config.claim_timeout_secs,
            upstream_partition_queue_names=upstream_partition_queue_names,
        )

    def _stage_info(self, stage: "Stage") -> Dict[str, Any]:
        """Get stage info dict for state events."""
        p = stage.parallelism
        return {
            "stage_id": stage.stage_id,
            "operator_type": type(stage.operator_config).__name__,
            "min_parallelism": p[0] if isinstance(p, tuple) else p,
            "max_parallelism": p[1] if isinstance(p, tuple) else p,
            "num_cpus": stage.num_cpus,
            "num_gpus": stage.num_gpus,
            "memory_mb": stage.memory_mb,
            "status": "PENDING",
        }

    def _write_job_state(self, status: str, end_time: Optional[float] = None) -> None:
        """Write job state and index into WorkQueue state."""
        if not self._state_writer:
            return
        start_time = self._start_time or time.time()
        job_data = {
            "job_id": self.job.job_id,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "dag_edges": self.job.dag_edges,
            "stages": [self._stage_info(s) for s in self.job.stages.values()],
        }
        summary = {
            "job_id": self.job.job_id,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
        }
        self._state_writer.write_job_index(summary)
        self._state_writer.write_job(job_data)

    def _create_master(
        self,
        stage: "Stage",
        runtime: StageRuntime,
    ) -> StageMaster:
        """Create appropriate master for a stage.

        Uses operator_config.master_class if specified (for special orchestration
        like CCIterateMaster), otherwise creates StageMaster with optional
        source strategy and sink committer from operator config.

        Args:
            stage: The stage definition
            runtime: Immutable runtime parameters
        """
        # Payload store must be initialized before creating masters
        assert self._payload_store is not None, "payload_store not initialized"

        return StageMaster(
            job_id=self.job.job_id,
            stage=stage,
            payload_store=self._payload_store,
            runtime=runtime,
        )

    def _get_topological_order(self) -> List[str]:
        """Get stages in topological order (sources first)."""
        # Simple BFS from sources
        in_degree = {
            stage_id: len(self._reverse_dag.get(stage_id, [])) for stage_id in self.job.stages
        }

        # Start with sources (no upstreams)
        queue = [s for s, d in in_degree.items() if d == 0]
        result = []

        while queue:
            stage_id = queue.pop(0)
            result.append(stage_id)

            # Find downstream stages
            for downstream_id, upstreams in self._reverse_dag.items():
                if stage_id in upstreams:
                    in_degree[downstream_id] -= 1
                    if in_degree[downstream_id] == 0:
                        queue.append(downstream_id)

        return result

    async def _notify_downstream_stages(self, finished_stage_id: str, all_finished: set) -> None:
        """Notify downstream stages that an upstream has finished.

        A downstream stage is notified when ALL its upstreams have finished.
        """
        # Find all stages that have this stage as an upstream
        for stage_id, upstream_ids in self._reverse_dag.items():
            if finished_stage_id in upstream_ids:
                # Check if ALL upstreams of this stage are finished
                all_upstreams_done = all(up_id in all_finished for up_id in upstream_ids)
                if all_upstreams_done and stage_id in self._masters:
                    await self._masters[stage_id].notify_upstream_finished()
                    self.logger.info(f"Notified stage {stage_id}: all upstreams finished")

    async def run(self, timeout: Optional[float] = None) -> JobStatus:
        """Run the pipeline until completion.

        Iterative stages (like CCIterateMaster) handle their own iteration
        internally - no special handling needed here.

        Args:
            timeout: Maximum time to wait (seconds), None for no timeout

        Returns:
            Final pipeline status
        """
        if not self._initialized:
            await self.initialize()
        self._running = True
        self._start_time = time.time()
        self._write_job_state(status="RUNNING")
        deadline = time.time() + timeout if timeout else None

        try:
            # Start all masters that haven't been started
            for stage_id, master in self._masters.items():
                if not master._running:
                    await master.start()

            # Create tasks for all master run loops
            self.logger.info(f"Creating run tasks for {len(self._masters)} masters")
            for stage_id, master in self._masters.items():
                if stage_id not in self._master_tasks:
                    self.logger.info(f"Creating run task for master {stage_id}")
                    task = asyncio.create_task(
                        master.run(),
                        name=f"master_{stage_id}",
                    )
                    self._master_tasks[stage_id] = task
            self.logger.info(f"Created {len(self._master_tasks)} master run tasks")

            # Start autoscaler if configured
            self._start_autoscaler()

            # Give asyncio tasks a chance to start executing
            await asyncio.sleep(0)
            self.logger.info("Entering main run loop")

            # Track which stages have finished (for upstream completion notification)
            finished_stages = set()

            # Wait for all masters to complete
            while self._running and self._master_tasks:
                # Check timeout
                if deadline and time.time() > deadline:
                    raise TimeoutError(f"Pipeline timeout after {timeout}s")

                # Check for completed tasks
                done_stages = []
                for stage_id, task in list(self._master_tasks.items()):
                    if task.done():
                        try:
                            result = task.result()
                            self.logger.info(f"Stage {stage_id} completed: {result}")
                        except Exception as e:
                            self._error = f"Stage {stage_id} failed: {e}"
                            self.logger.error(self._error)
                            raise
                        done_stages.append(stage_id)

                for stage_id in done_stages:
                    del self._master_tasks[stage_id]
                    finished_stages.add(stage_id)

                    # Notify downstream stages that this upstream has finished
                    await self._notify_downstream_stages(stage_id, finished_stages)

                if not self._master_tasks:
                    break

                await asyncio.sleep(0.1)

            self.logger.info("Pipeline completed successfully")
            return self.get_status()

        except Exception as e:
            self._error = str(e)
            raise
        finally:
            self._running = False
            # Note: Don't stop WebUI here - let caller decide when to stop
            # via explicit stop() call after any wait period

    async def stop(self) -> None:
        """Stop the pipeline."""
        self._running = False

        # Stop autoscaler
        await self._stop_autoscaler()

        # Cancel all running tasks
        for stage_id, task in list(self._master_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._master_tasks.clear()

        # Stop all masters (but don't clean up queues yet)
        for stage_id, master in self._masters.items():
            try:
                await master.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping stage {stage_id}: {e}")

        # Now clean up all queues (after all consumers are done)
        for stage_id, master in self._masters.items():
            try:
                await master.cleanup_queue()
            except Exception as e:
                self.logger.warning(f"Error cleaning up queue for {stage_id}: {e}")

        # Clean up SplitPayloadStore
        if self._payload_store:
            try:
                self._payload_store.clear()
            except Exception as e:
                self.logger.warning(f"Error cleaning up SplitPayloadStore: {e}")
            self._payload_store = None

        # Emit job completed event before stopping state infrastructure
        status = "FAILED" if self._error else "COMPLETED"
        self._write_job_state(status=status, end_time=time.time())
        if self._state_writer:
            self._state_writer.stop()
            self._state_writer = None

        # Stop queue stats client
        if self._queue_stats_client:
            self._queue_stats_client.stop()
            self._queue_stats_client = None
            self._backpressure_controller = None

        # Stop shared broker (after all stages are done)
        await self._stop_shared_broker()

        # Stop WebUI
        await self._stop_webui()

        self.logger.info("Pipeline stopped")

    def _start_autoscaler(self) -> None:
        """Start the autoscaler if configured."""
        autoscale_config = self.job.config.autoscale_config
        if autoscale_config is None:
            return

        self._autoscaler = SimpleAutoscaler(
            autoscale_config,
            queue_stats_client=self._queue_stats_client,
            stage_queue_configs=self._stage_queue_configs,
        )
        self._autoscale_task = asyncio.create_task(
            self._autoscaler.run_loop(self._masters),
            name="autoscaler",
        )
        self.logger.info("Autoscaler started")

    async def _stop_autoscaler(self) -> None:
        """Stop the autoscaler."""
        if self._autoscale_task and not self._autoscale_task.done():
            self._autoscale_task.cancel()
            try:
                await self._autoscale_task
            except asyncio.CancelledError:
                pass
            self._autoscale_task = None

        if self._autoscaler:
            self._autoscaler.stop()
            self._autoscaler = None

    def get_status(self) -> JobStatus:
        """Get current pipeline status."""
        stages = {}
        for stage_id, master in self._masters.items():
            status = master.get_status()
            stages[stage_id] = {
                "worker_count": status.worker_count,
                "output_queue_size": status.output_queue_size,
                "is_running": status.is_running,
                "is_finished": status.is_finished,
                "failed": status.failed,
            }

        elapsed = time.time() - self._start_time if self._start_time else 0

        return JobStatus(
            job_id=self.job.job_id,
            is_running=self._running,
            stages=stages,
            start_time=self._start_time,
            elapsed_time=elapsed,
            error=self._error,
        )

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    # === WebUI Integration ===

    async def _create_webui_storage(self):
        """Create WorkQueue-backed storage for WebUI."""
        from _internal.webui.state.manager import JobStateManager

        db_path = self.workqueue_db_path or "memory://"
        reader = self._shared_broker.get_storage_reader() if self._shared_broker else None
        self._webui_storage = JobStateManager(db_path, storage=reader)
        self.logger.info(f"WebUI storage using WorkQueue db: {db_path}")
        return self._webui_storage

    async def _initialize_webui(self) -> None:
        """Initialize WebUI components.

        - Creates JobWebUI instance using WorkQueue storage
        - Starts embedded WebUI server
        """
        try:
            from _internal.webui.job_webui import JobWebUI
            from _internal.webui.runtime_server import EmbeddedWebUIServer

            # Create JobWebUI using WorkQueue storage
            assert self._webui_storage is not None, "webui_storage not initialized"
            self._webui = JobWebUI(self, state_writer=self._state_writer)

            # Start WebUI
            await self._webui.start()

            # Start embedded WebUI server (runtime mode)
            self._webui_server = EmbeddedWebUIServer(
                job_id=self.job.job_id,
                storage=self._webui_storage,
                host="0.0.0.0",
                port_base=self.job.config.webui.port,
            )
            self._webui_port = self._webui_server.start()

            from _internal.utils.network import get_node_ip

            host = get_node_ip()
            self.logger.info(
                f"WebUI available at http://{host}:{self._webui_port}/jobs/{self.job.job_id}/"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize WebUI: {e}")
            # Don't fail the job if WebUI fails
            self._webui = None
            if self._webui_server:
                try:
                    self._webui_server.stop()
                except Exception:
                    pass
                self._webui_server = None
            self._webui_port = None

    async def _stop_webui(self) -> None:
        """Stop WebUI components."""
        if self._webui_server:
            try:
                self._webui_server.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping WebUI server: {e}")
            self._webui_server = None
            self._webui_port = None
        if self._webui:
            try:
                await self._webui.stop()
                self.logger.info("WebUI stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping WebUI: {e}")
            self._webui = None

    @property
    def webui_port(self) -> Optional[int]:
        """Get WebUI port if available.

        Returns:
            Embedded WebUI port where WebUI is accessible, or None
        """
        return self._webui_port

    @property
    def webui_path(self) -> Optional[str]:
        """Get WebUI path if available.

        Returns:
            WebUI path (e.g., "/jobs/{job_id}/"), or None
        """
        if self._webui_port:
            return f"/jobs/{self.job.job_id}/"
        return None


# Convenience function for simple pipeline execution
async def run_pipeline(
    job: Job,
    timeout: Optional[float] = None,
) -> JobStatus:
    """Run a pipeline and return its status.

    Configuration is read from job.config.

    Example:
        ```python
        job = Job(job_id="my_job")
        # ... add stages ...

        status = await run_pipeline(job)
        print(f"Completed in {status.elapsed_time:.2f}s")
        ```
    """
    runner = RayJobRunner(job)
    return await runner.run(timeout=timeout)
