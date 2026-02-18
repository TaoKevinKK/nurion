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

"""Workers API - worker list, logs, and stacktrace.

v2: 3 endpoints. Worker detail and metrics endpoints removed.
    list_workers now reads from persistent worker metadata (not event scanning).
    Supports ?stage_id= and ?worker_id= filters.
"""

import subprocess
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import PlainTextResponse

router = APIRouter(tags=["workers"])


@router.get("/jobs/{job_id}/workers")
async def list_workers(
    job_id: str,
    request: Request,
    stage_id: Optional[str] = Query(None),
    worker_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    """List workers from persistent metadata."""
    storage = request.app.state.storage
    return storage.list_workers(
        job_id,
        stage_id=stage_id,
        worker_id=worker_id,
        limit=limit,
        offset=offset,
    )


@router.get("/jobs/{job_id}/workers/{worker_id}/logs")
async def get_worker_logs(
    job_id: str,
    worker_id: str,
    request: Request,
    tail: int = Query(100, ge=1, le=10000),
) -> PlainTextResponse:
    """Get worker logs.

    Requires the job to be running (worker must be a live Ray actor).
    """
    try:
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

        try:
            logs = get_log(actor_id=actor_id, tail=tail)
            if logs:
                if isinstance(logs, (list, tuple)):
                    return PlainTextResponse("\n".join(logs))
                return PlainTextResponse(str(logs))
        except Exception as e:
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
    """
    try:
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
