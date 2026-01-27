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

"""Workers API - worker status, logs, and debugging.

Architecture:
- JobRunner writes to JobStorage (SlateDB) via JobStateManager
- Portal/History Server reads from JobStorage (read-only)

Note: Logs and stacktrace endpoints require running workers (Ray actors).
They use Ray State API to find actors, not cross-process state.

Note: storage is guaranteed to exist (app won't start without it).
"""

import subprocess
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

router = APIRouter(tags=["workers"])


@router.get("/jobs/{job_id}/workers")
async def list_workers(job_id: str, request: Request) -> List[Dict[str, Any]]:
    """List all workers for a job."""
    storage = request.app.state.storage
    return storage.list_workers(job_id, limit=1000)


@router.get("/jobs/{job_id}/workers/{worker_id}")
async def get_worker_detail(
    job_id: str,
    worker_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get worker state (latest Counter/Gauge values)."""
    storage = request.app.state.storage
    history = storage.get_worker_history(job_id, worker_id)
    if history:
        return history
    raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")


@router.get("/jobs/{job_id}/workers/{worker_id}/metrics")
async def get_worker_metrics(
    job_id: str,
    worker_id: str,
    request: Request,
    start_time: float = Query(0, description="Start timestamp (Unix seconds)"),
    end_time: float = Query(0, description="End timestamp (Unix seconds)"),
) -> Dict[str, Any]:
    """Get time-series samples and calculated rates for a worker.

    Returns raw Counter/Gauge samples for charting, plus rate() calculations.
    """
    import time as time_module

    now = time_module.time()

    if end_time == 0:
        end_time = now
    if start_time == 0:
        start_time = end_time - 300

    storage = request.app.state.storage
    time_range = end_time - start_time

    samples = storage.get_metrics_samples(job_id, worker_id, start_time, end_time)
    rates = {
        "input_records_per_sec": storage.rate(job_id, worker_id, "input_records", time_range),
        "output_records_per_sec": storage.rate(job_id, worker_id, "output_records", time_range),
        "splits_per_sec": storage.rate(job_id, worker_id, "processed_count", time_range),
    }

    return {
        "job_id": job_id,
        "worker_id": worker_id,
        "start_time": start_time,
        "end_time": end_time,
        "samples": samples,
        "rates": rates,
    }


@router.get("/jobs/{job_id}/workers/{worker_id}/logs")
async def get_worker_logs(
    job_id: str,
    worker_id: str,
    request: Request,
    tail: int = Query(100, ge=1, le=10000),
) -> PlainTextResponse:
    """Get worker logs.

    Requires the job to be running (worker must be a live Ray actor).

    Args:
        job_id: Job identifier
        worker_id: Worker identifier
        tail: Number of lines to return from the end

    Returns:
        Plain text log content
    """
    try:
        # Find the actor using Ray State API
        from ray.util.state import list_actors, get_log

        actors = list_actors(filters=[("name", "=", worker_id)])
        if not actors:
            return PlainTextResponse(
                f"Actor {worker_id} not found. Logs only available for running jobs."
            )

        actor = actors[0]
        actor_id = actor.get("actor_id")

        if not actor_id:
            return PlainTextResponse("Could not determine actor ID")

        # Get logs using actor_id
        try:
            logs = get_log(actor_id=actor_id, tail=tail)
            if logs:
                if isinstance(logs, (list, tuple)):
                    return PlainTextResponse("\n".join(logs))
                return PlainTextResponse(str(logs))
        except Exception as e:
            # Fallback: try reading from log files via Ray Dashboard API
            node_id = actor.get("node_id")
            pid = actor.get("pid")

            if node_id and pid:
                import httpx

                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(
                            f"http://localhost:8265/api/v0/logs/file"
                            f"?node_id={node_id}&pid={pid}&lines={tail}",
                            timeout=5.0,
                        )
                        if resp.status_code == 200:
                            return PlainTextResponse(resp.text)
                except Exception:
                    pass

            return PlainTextResponse(
                f"Could not retrieve logs: {e}\nActor: {actor_id}, Node: {node_id}, PID: {pid}"
            )

        return PlainTextResponse("No logs available")

    except ImportError:
        return PlainTextResponse("Ray not available")
    except Exception as e:
        return PlainTextResponse(f"Error getting logs: {e}")


@router.get("/jobs/{job_id}/workers/{worker_id}/stacktrace")
async def get_worker_stacktrace(
    job_id: str,
    worker_id: str,
    request: Request,
) -> PlainTextResponse:
    """Get worker stacktrace using py-spy.

    Requires the job to be running (worker must be a live Ray actor).

    Args:
        job_id: Job identifier
        worker_id: Worker identifier

    Returns:
        Plain text stacktrace
    """
    try:
        # Find the actor using Ray State API
        from ray.util.state import list_actors

        actors = list_actors(filters=[("name", "=", worker_id)])
        if not actors:
            return PlainTextResponse(
                f"Actor {worker_id} not found. Stacktrace only available for running jobs."
            )

        actor = actors[0]
        pid = actor.get("pid")

        if not pid:
            return PlainTextResponse("Could not determine worker PID")

        # Use py-spy to dump stacktrace
        try:
            result = subprocess.run(
                ["py-spy", "dump", "--pid", str(pid)],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                return PlainTextResponse(result.stdout)
            else:
                return PlainTextResponse(
                    f"py-spy failed: {result.stderr}\n\n"
                    f"Make sure py-spy is installed: pip install py-spy"
                )

        except FileNotFoundError:
            return PlainTextResponse(
                f"py-spy not found.\n\nInstall with: pip install py-spy\nWorker PID: {pid}"
            )
        except subprocess.TimeoutExpired:
            return PlainTextResponse("py-spy timeout after 10 seconds")

    except ImportError:
        return PlainTextResponse("Ray not available")
    except Exception as e:
        return PlainTextResponse(f"Error getting stacktrace: {e}")
