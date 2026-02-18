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
Serves the React SPA from static/dist/ (built by Vite).
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from _internal.webui.state.manager import JobStateManager
from _internal.utils.logging import create_ray_logger


# Paths
WEBUI_DIR = Path(__file__).parent
STATIC_DIR = WEBUI_DIR / "static"
SPA_DIST_DIR = STATIC_DIR / "dist"


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

    # Create app
    app = FastAPI(title=title, version="0.1.0")

    # Store references
    app.state.storage = storage
    app.state.logger = logger
    app.state.base_path = base_path

    # =========================================================================
    # API Routes
    # =========================================================================

    from _internal.webui.api.jobs import router as jobs_router
    from _internal.webui.api.stages import router as stages_router
    from _internal.webui.api.workers import router as workers_router
    from _internal.webui.api.lineage import router as lineage_router
    from _internal.webui.api.events import router as events_router
    from _internal.webui.api.serve import router as serve_router

    app.include_router(jobs_router, prefix="/api")
    app.include_router(stages_router, prefix="/api")
    app.include_router(workers_router, prefix="/api")
    app.include_router(lineage_router, prefix="/api")
    app.include_router(events_router, prefix="/api")
    app.include_router(serve_router, prefix="/api")

    @app.get("/health")
    async def health():
        """Health check."""
        return {"status": "ok", "service": "nurion-webui"}

    # =========================================================================
    # SPA — serve React build from static/dist/
    # =========================================================================

    spa_index = SPA_DIST_DIR / "index.html"
    if not spa_index.exists():
        raise RuntimeError(
            f"SPA build not found at {spa_index}. "
            "Run: cd engine/_internal/webui/frontend && npm run build"
        )

    # Read index.html once and inject base_path + fix asset paths
    _spa_html = spa_index.read_text()
    _spa_html = _spa_html.replace(
        "<head>",
        f'<head><script>window.__NURION_BASE_PATH__="{base_path}"</script>',
        1,
    )
    # Rewrite relative asset paths to absolute so deep routes work
    _spa_html = _spa_html.replace('"./assets/', '"/assets/')
    _spa_html = _spa_html.replace("'./assets/", "'/assets/")

    # Mount Vite assets directory
    spa_assets = SPA_DIST_DIR / "assets"
    if spa_assets.exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(spa_assets)),
            name="spa-assets",
        )

    @app.get("/{path:path}", response_class=HTMLResponse)
    async def spa_catch_all(path: str):
        """Serve SPA index.html for all non-API routes."""
        return HTMLResponse(_spa_html)

    logger.info("WebUI app created")
    return app
