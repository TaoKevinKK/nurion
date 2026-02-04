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

"""History Server for viewing completed Solstice jobs."""

import click
import uvicorn

from solstice.webui.app import create_webui_app
from solstice.webui.state.manager import JobStateManager


@click.command()
@click.option(
    "--workqueue-db-path",
    "-s",
    required=True,
    help="WorkQueue storage path (e.g., file:///tmp/workqueue.db)",
)
@click.option(
    "--host",
    "-h",
    default="0.0.0.0",
    help="Host to bind (default: 0.0.0.0)",
)
@click.option(
    "--port",
    "-p",
    default=8080,
    type=int,
    help="Port to bind (default: 8080)",
)
@click.option(
    "--reload",
    is_flag=True,
    help="Enable auto-reload for development",
)
def history_server(workqueue_db_path: str, host: str, port: int, reload: bool):
    """Start Solstice History Server for viewing completed jobs.

    The History Server provides read-only access to archived job data
    stored in WorkQueue storage. It uses the same WebUI interface as the embedded
    mode but reads data from storage instead of live jobs.

    Example:
        solstice history-server -s file:///tmp/workqueue.db -p 8080
    """
    click.echo("╔════════════════════════════════════════════╗")
    click.echo("║   Solstice History Server                 ║")
    click.echo("╚════════════════════════════════════════════╝")
    click.echo()
    click.echo(f"Storage:  {workqueue_db_path}")
    click.echo(f"Address:  http://{host}:{port}")
    click.echo()
    click.echo("Press Ctrl+C to stop")
    click.echo()

    # Initialize storage (read-only, caches readers per job)
    try:
        storage = JobStateManager(workqueue_db_path)
        click.echo("✓ Connected to storage (read-only)")
    except Exception as e:
        click.echo(f"✗ Failed to initialize storage: {e}", err=True)
        raise click.Abort()

    # Create history server app (no base_path prefix, runs at root)
    app = create_webui_app(storage, title="Solstice History Server", base_path="")
    app.add_event_handler("shutdown", storage.close)

    # Run server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        reload=reload,
    )


if __name__ == "__main__":
    history_server()
