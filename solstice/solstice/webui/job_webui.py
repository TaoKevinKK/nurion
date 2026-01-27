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

"""Job WebUI - per-job WebUI instance."""

import os
from typing import TYPE_CHECKING, Optional

from solstice.webui.storage import JobStorage
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.runtime.ray_runner import RayJobRunner
    from solstice.webui.state.manager import JobStateManager


class JobWebUI:
    """WebUI instance for a single Solstice job.

    This component stores job configuration at startup.

    Note: Metrics collection, worker tracking, and job archiving are handled
    by JobStateManager (push-based architecture).
    """

    def __init__(
        self,
        job_runner: "RayJobRunner",
        storage: JobStorage,
        attempt_id: str,
        state_manager: Optional["JobStateManager"] = None,
    ):
        """Initialize job WebUI.

        Args:
            job_runner: RayJobRunner instance
            storage: SlateDB storage instance
            attempt_id: Unique attempt ID for this run
            state_manager: JobStateManager for reading metrics (push-based)
        """
        self.job_runner = job_runner
        self.storage = storage
        self.job_id = job_runner.job.job_id
        self.attempt_id = attempt_id
        self.state_manager = state_manager

        self.logger = create_ray_logger(f"JobWebUI-{self.job_id}")

        self.logger.info("Job WebUI initialized")

    async def start(self) -> None:
        """Start the WebUI components."""
        # Store configuration at job start
        self._store_configuration()

        self.logger.info("Job WebUI started")

    def _store_configuration(self) -> None:
        """Store job configuration to storage."""
        try:
            job_runner = self.job_runner

            # Build stage configs
            stage_configs = {}
            for stage_id, master in job_runner._masters.items():
                stage = master.stage
                stage_configs[stage_id] = {
                    "operator_type": type(stage.operator_config).__name__,
                    "min_parallelism": stage.min_parallelism,
                    "max_parallelism": stage.max_parallelism,
                    "num_cpus": stage.num_cpus,
                    "num_gpus": stage.num_gpus,
                    "memory_mb": stage.memory_mb,
                }

            config_data = {
                "job_config": {
                    "job_id": job_runner.job.job_id,
                    "queue_type": job_runner.queue_type.value,
                    "tansu_storage_url": job_runner.tansu_storage_url,
                },
                "stage_configs": stage_configs,
                "dag_edges": job_runner.job.dag_edges,
                "environment": {
                    "SOLSTICE_LOG_LEVEL": os.getenv("SOLSTICE_LOG_LEVEL", "INFO"),
                    "RAY_PROMETHEUS_HOST": os.getenv("RAY_PROMETHEUS_HOST"),
                    "SOLSTICE_GRAFANA_URL": os.getenv("SOLSTICE_GRAFANA_URL"),
                },
            }

            self.storage.store_configuration(config_data)
            self.logger.debug("Configuration stored")

        except Exception as e:
            self.logger.warning(f"Failed to store configuration: {e}")

    async def stop(self) -> None:
        """Stop the WebUI components."""
        self.logger.info("Job WebUI stopped")
