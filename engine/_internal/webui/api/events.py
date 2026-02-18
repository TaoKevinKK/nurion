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

"""Events API - unified event query for ack, nack, and timeout events."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["events"])


@router.get("/jobs/{job_id}/events")
async def list_events(
    job_id: str,
    request: Request,
    stage_id: Optional[str] = Query(None),
    worker_id: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None, description="Comma-separated: ack,nack,timeout"),
    limit: int = Query(100, ge=1, le=10000),
) -> List[Dict[str, Any]]:
    """List events for a job with optional filters.

    Replaces the old /exceptions endpoint. Supports filtering by
    stage_id, worker_id, and event_type (comma-separated).
    """
    storage = request.app.state.storage
    return storage.list_events(
        job_id,
        stage_id=stage_id,
        worker_id=worker_id,
        event_type=event_type,
        limit=limit,
    )
