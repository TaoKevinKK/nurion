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

"""WorkQueue state writer for WebUI metadata (gRPC)."""

from __future__ import annotations

from typing import Any, Dict

from _internal.queue import WorkQueueQueueClient
from _internal.utils.logging import create_ray_logger
from _internal.core.models import QueueEndpoint
from _internal.queue.workqueue import _compute_heartbeat_interval
from _internal.webui.state.schema import (
    config_key,
    encode_json,
    job_index_key,
    job_key,
    job_namespace,
    jobs_namespace,
    stage_key,
)


class WorkQueueStateWriter:
    """Write WebUI metadata into WorkQueue state (gRPC)."""

    def __init__(
        self,
        job_id: str,
        broker_endpoint: QueueEndpoint,
        claim_timeout_secs: float,
    ) -> None:
        self.job_id = job_id
        self.broker_endpoint = broker_endpoint
        self._client = WorkQueueQueueClient(
            f"{broker_endpoint.host}:{broker_endpoint.port}",
            worker_id=f"state-writer-{job_id}",
            heartbeat_interval_secs=_compute_heartbeat_interval(claim_timeout_secs),
        )
        self._running = False
        self.logger = create_ray_logger(f"WorkQueueStateWriter-{job_id}")

    def start(self) -> None:
        if self._running:
            return
        self._client.start()
        self._running = True

    def stop(self) -> None:
        if not self._running:
            return
        self._client.stop()
        self._running = False

    def write_job_index(self, summary: Dict[str, Any]) -> None:
        self._put(jobs_namespace(), {job_index_key(self.job_id): encode_json(summary)})

    def write_job(self, job_data: Dict[str, Any]) -> None:
        self._put(job_namespace(self.job_id), {job_key(): encode_json(job_data)})

    def write_config(self, config_data: Dict[str, Any]) -> None:
        self._put(job_namespace(self.job_id), {config_key(): encode_json(config_data)})

    def write_stage(self, stage_id: str, stage_data: Dict[str, Any]) -> None:
        self._put(job_namespace(self.job_id), {stage_key(stage_id): encode_json(stage_data)})

    def _put(self, namespace: str, puts: Dict[str, bytes]) -> None:
        if not self._running:
            self.start()
        try:
            self._client.state_put(namespace, puts=puts)
        except Exception as e:
            self.logger.warning(f"State put failed for {namespace}: {e}")
