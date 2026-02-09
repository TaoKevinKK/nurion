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

"""FastAPI application factory for Nurion WebUI.

Provides shared utilities and app factory for runtime and history modes.
Storage is injected via JobStateManager.
"""

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from _internal.webui.state.manager import JobStateManager
from _internal.utils.logging import create_ray_logger


# Paths
WEBUI_DIR = Path(__file__).parent
TEMPLATES_DIR = WEBUI_DIR / "templates"
STATIC_DIR = WEBUI_DIR / "static"


def create_webui_app(
    storage: JobStateManager,
    title: str = "Nurion WebUI",
    base_path: str = "",
) -> FastAPI:
    """Create the Nurion WebUI FastAPI application.

    This is the unified app factory used by runtime and history modes.
    All routes read from the injected storage adapter.

    Args:
        storage: JobStateManager instance for reading data
        title: Application title
        base_path: URL prefix for all routes (e.g., "/solstice" for Portal, "" for History Server)

    Returns:
        FastAPI application with all routes configured
    """
    logger = create_ray_logger("NurionWebUI")

    # Normalize base_path (ensure no trailing slash, can be empty)
    base_path = base_path.rstrip("/")

    # Validate directories
    if not STATIC_DIR.exists():
        raise RuntimeError(f"Static directory not found: {STATIC_DIR}")
    if not TEMPLATES_DIR.exists():
        raise RuntimeError(f"Templates directory not found: {TEMPLATES_DIR}")

    # Create app
    app = FastAPI(title=title, version="0.1.0")

    # Setup templates
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    setup_template_filters(templates)

    # Add global template variables
    templates.env.globals["base_path"] = base_path

    # Mount static files
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Store references
    app.state.storage = storage
    app.state.templates = templates
    app.state.logger = logger
    app.state.base_path = base_path

    # =========================================================================
    # HTML Routes (server-rendered pages)
    # =========================================================================

    @app.get("/", response_class=HTMLResponse)
    async def portal_home(request: Request):
        """Home page - list all jobs."""
        running_jobs = storage.list_jobs(status="RUNNING", limit=20)
        completed_jobs = storage.list_jobs(status="COMPLETED", limit=20)

        return templates.TemplateResponse(
            "portal.html",
            {
                "request": request,
                "running_jobs": running_jobs,
                "completed_jobs": completed_jobs,
                "ray_dashboard_url": get_ray_dashboard_url(),
            },
        )

    @app.get("/running", response_class=HTMLResponse)
    async def running_jobs_page(request: Request):
        """Running jobs list page."""
        running_jobs = storage.list_jobs(status="RUNNING", limit=100)
        return templates.TemplateResponse(
            "running_jobs.html",
            {"request": request, "jobs": running_jobs},
        )

    @app.get("/completed", response_class=HTMLResponse)
    async def completed_jobs_page(request: Request):
        """Completed jobs list page."""
        completed_jobs = storage.list_jobs(status="COMPLETED", limit=100)
        return templates.TemplateResponse(
            "completed_jobs.html",
            {"request": request, "jobs": completed_jobs},
        )

    @app.get("/jobs/{job_id}/", response_class=HTMLResponse)
    async def job_detail_page(job_id: str, request: Request):
        """Job detail page."""
        job_data = storage.get_job_archive(job_id)
        if not job_data:
            job_data = {"job_id": job_id, "status": "NOT_FOUND"}

        return templates.TemplateResponse(
            "job_detail.html",
            {
                "request": request,
                "job": job_data,
                "stages": job_data.get("stages", []),
                "dag_edges": job_data.get("dag_edges", {}),
            },
        )

    @app.get("/jobs/{job_id}/stages/{stage_id}", response_class=HTMLResponse)
    async def stage_detail_page(job_id: str, stage_id: str, request: Request):
        """Stage detail page."""
        stage_data = {"stage_id": stage_id, "status": "NOT_FOUND"}

        job_data = storage.get_job_archive(job_id)
        if job_data:
            for s in job_data.get("stages", []):
                if s.get("stage_id") == stage_id:
                    stage_data = s
                    break

        # Get workers for this stage
        workers = storage.list_workers(job_id, stage_id=stage_id, limit=500)

        return templates.TemplateResponse(
            "stage_detail.html",
            {
                "request": request,
                "job_id": job_id,
                "stage": stage_data,
                "workers": workers,
            },
        )

    @app.get("/jobs/{job_id}/workers", response_class=HTMLResponse)
    async def workers_list_page(job_id: str, request: Request):
        """Workers list page."""
        job_data = storage.get_job_archive(job_id) or {"job_id": job_id, "status": "UNKNOWN"}
        stages = job_data.get("stages", [])
        workers = storage.list_workers(job_id, limit=500)

        return templates.TemplateResponse(
            "workers.html",
            {
                "request": request,
                "job": job_data,
                "stages": stages,
                "workers": workers,
            },
        )

    @app.get("/jobs/{job_id}/workers/{worker_id}", response_class=HTMLResponse)
    async def worker_detail_page(job_id: str, worker_id: str, request: Request):
        """Worker detail page."""
        import time as time_module

        now = time_module.time()
        worker_data = storage.get_worker_history(job_id, worker_id) or {
            "worker_id": worker_id,
            "stage_id": "",
            "status": "UNKNOWN",
        }
        worker_events = storage.list_worker_events(job_id, worker_id=worker_id, limit=50)

        # Live debugging: query Ray actor info
        try:
            from ray.util.state import list_actors

            actors = list_actors(
                filters=[("class_name", "=", "StageWorker"), ("state", "=", "ALIVE")]
            )
            for actor in actors:
                if worker_id in actor.get("name", ""):
                    worker_data["actor_id"] = actor.get("actor_id")
                    worker_data["node_id"] = actor.get("node_id")
                    worker_data["pid"] = actor.get("pid")
                    break
        except Exception:
            pass

        return templates.TemplateResponse(
            "worker_detail.html",
            {
                "request": request,
                "job_id": job_id,
                "worker": worker_data,
                "worker_events": worker_events,
                "now": now,
            },
        )

    @app.get("/jobs/{job_id}/checkpoints", response_class=HTMLResponse)
    async def checkpoints_page(job_id: str, request: Request):
        """Checkpoints page.

        Shows the current checkpoint info from job archive.
        Since we only keep one checkpoint, this is simplified.
        """
        job_archive = storage.get_job_archive(job_id)
        checkpoint = None
        if job_archive:
            checkpoint = job_archive.get("checkpoint")
        return templates.TemplateResponse(
            "checkpoints.html",
            {"request": request, "job_id": job_id, "checkpoint": checkpoint},
        )

    @app.get("/jobs/{job_id}/lineage", response_class=HTMLResponse)
    async def lineage_page(job_id: str, request: Request):
        """Lineage page."""
        return templates.TemplateResponse(
            "lineage.html",
            {"request": request, "job_id": job_id, "lineage": []},
        )

    @app.get("/jobs/{job_id}/configuration", response_class=HTMLResponse)
    async def configuration_page(job_id: str, request: Request):
        """Configuration page."""
        config_data = storage.get_configuration(job_id) or {
            "job_config": {},
            "stage_configs": {},
            "environment": {},
        }
        return templates.TemplateResponse(
            "configuration.html",
            {"request": request, "job_id": job_id, "config": config_data},
        )

    # =========================================================================
    # API Routes
    # =========================================================================

    from _internal.webui.api.jobs import router as jobs_router
    from _internal.webui.api.stages import router as stages_router
    from _internal.webui.api.workers import router as workers_router
    from _internal.webui.api.lineage import router as lineage_router
    from _internal.webui.api.exceptions import router as exceptions_router

    app.include_router(jobs_router, prefix="/api")
    app.include_router(stages_router, prefix="/api")
    app.include_router(workers_router, prefix="/api")
    app.include_router(lineage_router, prefix="/api")
    app.include_router(exceptions_router, prefix="/api")

    @app.get("/health")
    async def health():
        """Health check."""
        return {"status": "ok", "service": "nurion-webui"}

    logger.info("WebUI app created")
    return app


def get_ray_dashboard_url() -> str:
    """Get Ray Dashboard URL for external links.

    Returns:
        Ray Dashboard URL (defaults to http://localhost:8265)
    """
    import ray

    # Get from environment or use default
    if ray.is_initialized():
        # Ray Dashboard typically runs on port 8265
        return os.getenv("RAY_DASHBOARD_URL", "http://localhost:8265")

    return os.getenv("RAY_DASHBOARD_URL", "http://localhost:8265")


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 15m", "45s", "1.2s")
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_bytes(bytes_value: int) -> str:
    """Format bytes in human-readable form.

    Args:
        bytes_value: Size in bytes

    Returns:
        Formatted string (e.g., "1.2GB", "45MB")
    """
    value: float = float(bytes_value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024.0:
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}PB"


def format_number(num: int) -> str:
    """Format large numbers with abbreviations.

    Args:
        num: Number to format

    Returns:
        Formatted string (e.g., "1.2M", "45K")
    """
    if num is None:
        return "0"
    if num >= 1e9:
        return f"{num / 1e9:.1f}B"
    elif num >= 1e6:
        return f"{num / 1e6:.1f}M"
    elif num >= 1e3:
        return f"{num / 1e3:.1f}K"
    else:
        return str(num)


def format_datetime(timestamp: float) -> str:
    """Format Unix timestamp as human-readable datetime.

    Args:
        timestamp: Unix timestamp in seconds

    Returns:
        Formatted string (e.g., "2026-01-06 12:53:23")
    """
    from datetime import datetime

    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, TypeError):
        return "Unknown"


# Register template filters
def setup_template_filters(templates: Jinja2Templates):
    """Register custom template filters."""
    templates.env.filters["format_duration"] = format_duration
    templates.env.filters["format_bytes"] = format_bytes
    templates.env.filters["format_number"] = format_number
    templates.env.filters["format_datetime"] = format_datetime
