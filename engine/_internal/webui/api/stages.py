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

"""Stages API - stage detail with queue_stats.

v2: Single endpoint. Metrics, throughput, offsets, and stage-level workers
    endpoints removed (see webui-api-v2.md for rationale).
"""

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["stages"])


@router.get("/jobs/{job_id}/stages/{stage_id}")
async def get_stage_detail(
    job_id: str,
    stage_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get detailed stage information including queue_stats."""
    storage = request.app.state.storage
    job_data: Dict[str, Any] | None = storage.get_job_archive(job_id)
    if job_data:
        stages = job_data.get("stages", [])
        for stage in stages:
            if stage.get("stage_id") == stage_id:
                return dict(stage)
        raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found")
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
