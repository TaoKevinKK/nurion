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

"""Stages API - stage metrics and details.

Architecture:
- JobRunner writes metadata to WorkQueue state (gRPC)
- WebUI reads directly from WorkQueue storage (pyO3)
"""

import time
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["stages"])


@router.get("/jobs/{job_id}/stages")
async def list_stages(job_id: str, request: Request) -> Dict[str, Any]:
    """List all stages for a job."""
    storage = request.app.state.storage
    job_data = storage.get_job_archive(job_id)
    if job_data:
        return {
            "job_id": job_id,
            "stages": job_data.get("stages", []),
            "dag_edges": job_data.get("dag_edges", {}),
        }
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


@router.get("/jobs/{job_id}/stages/{stage_id}")
async def get_stage_detail(
    job_id: str,
    stage_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get detailed stage information."""
    storage = request.app.state.storage
    job_data: Dict[str, Any] | None = storage.get_job_archive(job_id)
    if job_data:
        stages = job_data.get("stages", [])
        for stage in stages:
            if stage.get("stage_id") == stage_id:
                return dict(stage)
        raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found")
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


@router.get("/jobs/{job_id}/stages/{stage_id}/metrics")
async def get_stage_metrics_history(
    job_id: str,
    stage_id: str,
    request: Request,
    start_time: float = Query(0),
    end_time: float = Query(0),
) -> List[Dict[str, Any]]:
    """Get metrics history for a stage.

    Args:
        job_id: Job identifier
        stage_id: Stage identifier
        start_time: Start timestamp (Unix seconds)
        end_time: End timestamp (Unix seconds)

    Returns:
        List of metrics snapshots
    """
    # Default to last 5 minutes
    if end_time == 0:
        end_time = time.time()
    if start_time == 0:
        start_time = end_time - 300

    storage = request.app.state.storage
    result: List[Dict[str, Any]] = storage.get_metrics_history(
        job_id, stage_id, start_time, end_time
    )
    return result


@router.get("/jobs/{job_id}/stages/{stage_id}/workers")
async def list_stage_workers(
    job_id: str,
    stage_id: str,
    request: Request,
) -> List[Dict[str, Any]]:
    """List workers for a specific stage.

    Args:
        job_id: Job identifier
        stage_id: Stage identifier

    Returns:
        List of worker info
    """
    storage = request.app.state.storage
    # Use list_workers which is better optimized
    workers = storage.list_workers(job_id, stage_id=stage_id, limit=500)
    return workers


@router.get("/jobs/{job_id}/stages/{stage_id}/offsets")
async def get_stage_partition_offsets(
    job_id: str,
    stage_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get partition offsets (Gauge) for all workers in a stage."""
    storage = request.app.state.storage
    offsets = storage.get_partition_offsets(job_id, stage_id=stage_id)
    return {
        "job_id": job_id,
        "stage_id": stage_id,
        "worker_offsets": offsets,
    }


@router.get("/jobs/{job_id}/stages/{stage_id}/throughput")
async def get_stage_throughput(
    job_id: str,
    stage_id: str,
    request: Request,
    time_range_s: float = Query(60.0, description="Time range for rate calculation"),
) -> Dict[str, Any]:
    """Get throughput using Prometheus-style rate() on Counter metrics.

    rate = (v2 - v1) / (t2 - t1)
    """
    storage = request.app.state.storage
    result = storage.get_throughput(job_id, stage_id=stage_id, time_range_s=time_range_s)
    result["job_id"] = job_id
    result["stage_id"] = stage_id
    return result
