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

"""Portal service - Ray Serve deployment for Nurion WebUI.

The Portal is the global entry point for accessing all Nurion jobs.
It reads directly from WorkQueue storage (pyO3).

Usage:
    from runtime.webui.portal import start_portal
    start_portal("file:///path/to/workqueue.db")
    # Access at http://localhost:8000/solstice/
"""

from fastapi import Request
from ray import serve

from runtime.webui.app import create_webui_app
from runtime.webui.state.manager import JobStateManager
from runtime.utils.logging import create_ray_logger


def create_portal_app(workqueue_db_path: str):
    """Create Portal FastAPI app.

    Args:
        workqueue_db_path: WorkQueue storage path

    Returns:
        FastAPI application
    """
    storage = JobStateManager(workqueue_db_path)
    # Portal runs at /solstice/ via Ray Serve route_prefix
    return create_webui_app(storage, title="Nurion Portal", base_path="/solstice")


@serve.deployment(
    name="nurion-portal",
    ray_actor_options={"num_cpus": 0.1, "num_gpus": 0},
)
class NurionPortal:
    """Ray Serve deployment for Nurion Portal.

    Wraps the FastAPI app and handles ASGI forwarding.
    """

    def __init__(self, workqueue_db_path: str):
        """Initialize portal with storage path."""
        self.app = create_portal_app(workqueue_db_path)
        self.logger = create_ray_logger("NurionPortal")
        self.logger.info(f"Portal initialized with storage: {workqueue_db_path}")

    async def __call__(self, request: Request):
        """Handle HTTP request by forwarding to FastAPI app."""
        scope = request.scope
        receive = request.receive

        response_started = False
        status_code = 200
        response_headers = []
        body_parts = []

        async def send(message):
            nonlocal response_started, status_code, response_headers
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                response_headers = message.get("headers", [])
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))

        await self.app(scope, receive, send)

        from starlette.responses import Response

        body = b"".join(body_parts)
        headers = {
            k.decode("latin-1") if isinstance(k, bytes) else k: v.decode("latin-1")
            if isinstance(v, bytes)
            else v
            for k, v in response_headers
        }
        return Response(content=body, status_code=status_code, headers=headers)


def start_portal(workqueue_db_path: str, port: int = 8000) -> str:
    """Start the global Nurion Portal service.

    Args:
        workqueue_db_path: WorkQueue storage path
        port: HTTP port for Ray Serve

    Returns:
        Portal URL path (e.g., "/solstice")
    """
    logger = create_ray_logger("PortalStarter")

    # Start Ray Serve
    try:
        serve.start(
            detached=True,
            http_options={"host": "0.0.0.0", "port": port},
        )
        logger.info(f"Started Ray Serve on port {port}")
    except Exception as e:
        logger.info(f"Ray Serve already running: {e}")

    # Deploy portal
    handle = NurionPortal.bind(workqueue_db_path)  # type: ignore[attr-defined]
    serve.run(handle, name="solstice-portal", route_prefix="/solstice")
    logger.info(f"Deployed Nurion Portal at /solstice with storage: {workqueue_db_path}")

    return "/solstice"


def portal_exists() -> bool:
    """Check if portal is already deployed."""
    try:
        status = serve.status()
        return "solstice-portal" in status.applications
    except Exception:
        return False
