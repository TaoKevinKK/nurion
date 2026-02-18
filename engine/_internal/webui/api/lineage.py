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

"""Lineage API - split lineage and trace.

v2: 2 endpoints. Overview and stage splits endpoints removed
    (see webui-api-v2.md for rationale).
"""

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["lineage"])


@router.get("/jobs/{job_id}/lineage/splits/{split_id}")
async def get_split_lineage(
    job_id: str,
    split_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get single split's lineage details (O(1) lookup)."""
    if request.app.state.storage:
        lineage: Dict[str, Any] | None = request.app.state.storage.get_split_lineage(
            job_id, split_id
        )
        if lineage:
            return lineage

    raise HTTPException(status_code=404, detail=f"Split {split_id} not found")


@router.get("/jobs/{job_id}/lineage/splits/{split_id}/trace")
async def get_split_trace(
    job_id: str,
    split_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get complete lineage trace for a split (O(depth) walk)."""
    if request.app.state.storage:
        result: Dict[str, Any] = request.app.state.storage.get_split_trace(job_id, split_id)
        return result

    return {"splits": [], "edges": [], "root_split_id": split_id}
