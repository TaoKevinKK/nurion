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

"""Serve API - model service monitoring endpoints."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["serve"])


@router.get("/serve/models")
async def list_serve_models(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """List deployed models."""
    storage = request.app.state.storage
    return storage.list_serve_models(limit=limit)


@router.get("/serve/workers")
async def list_serve_workers(
    request: Request,
    model_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """List InferenceWorker lifecycle status."""
    storage = request.app.state.storage
    return storage.list_serve_workers(model_id=model_id, limit=limit)


@router.get("/serve/events")
async def list_serve_events(
    request: Request,
    model_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
) -> List[Dict[str, Any]]:
    """List serve event history."""
    storage = request.app.state.storage
    return storage.list_serve_events(model_id=model_id, limit=limit)
