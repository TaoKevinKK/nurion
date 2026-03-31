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

"""Per-node Arrow Flight payload server (subprocess).

Runs in an isolated process to avoid gRPC conflicts with Ray's internal gRPC
(shared C++ global state causes Flight server thread to silently die).

Design:
    - One per node, enforced by binding a fixed port (EADDRINUSE = already running).
    - Serves ALL .arrow files under the root dir — no per-job registration needed.
    - Exits on SIGTERM or idle timeout (1 hour with no reads).
    - Not tied to any parent process.

Protocol (stdout, read by first spawner):
    FLIGHT_READY:{port}       — server bound successfully
    FLIGHT_PORT_IN_USE:{port} — another server already running on this port
    FLIGHT_ERROR:{detail}     — startup failed
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc

_IDLE_TIMEOUT_S = 3600  # 1 hour


def _sanitize_key(key: str) -> str:
    return key.replace(":", "_").replace("/", "_")


class _FlightServer(flight.FlightServerBase):
    """Flight server that serves all .arrow files under a root directory."""

    def __init__(self, root_dir: str, port: int, max_concurrent_reads: int = 8):
        location = flight.Location.for_grpc_tcp("0.0.0.0", port)
        super().__init__(location)
        self._root_dir = root_dir
        self._semaphore = threading.Semaphore(max_concurrent_reads)
        self._last_read_time = time.monotonic()

    def do_get(self, context: flight.ServerCallContext, ticket: flight.Ticket):
        self._last_read_time = time.monotonic()
        key = ticket.ticket.decode()
        safe = _sanitize_key(key)
        prefix = safe[:2] if len(safe) >= 2 else "00"

        acquired = self._semaphore.acquire(timeout=30)
        if not acquired:
            raise flight.FlightUnavailableError("Server overloaded, retry later")
        try:
            # Scan all job dirs under root
            try:
                entries = os.scandir(self._root_dir)
            except OSError:
                raise flight.FlightUnavailableError(f"Root dir unreadable: {self._root_dir}")
            for entry in entries:
                if not entry.is_dir() or entry.name.startswith("."):
                    continue
                path = os.path.join(entry.path, prefix, f"{safe}.arrow")
                if os.path.exists(path):
                    source = pa.memory_map(path, "r")
                    reader = ipc.open_file(source)
                    table = reader.read_all()
                    return flight.RecordBatchStream(table)
            raise flight.FlightUnavailableError(f"Payload not found: {key}")
        finally:
            self._semaphore.release()

    @property
    def idle_seconds(self) -> float:
        return time.monotonic() - self._last_read_time


def main() -> None:
    parser = argparse.ArgumentParser(description="Arrow Flight payload server")
    parser.add_argument("--root-dir", required=True, help="NVMe root directory to serve")
    parser.add_argument("--port", type=int, required=True, help="Fixed port to bind")
    parser.add_argument("--max-concurrent-reads", type=int, default=8)
    args = parser.parse_args()

    # Pre-check: is the port already in use?  Arrow Flight wraps EADDRINUSE
    # into a generic "Server did not start properly" error, so we detect it
    # ourselves with a quick socket bind test.
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", args.port))
        sock.close()  # Port is free — proceed to start Flight server
    except OSError:
        sock.close()
        print(f"FLIGHT_PORT_IN_USE:{args.port}", flush=True)
        sys.exit(0)

    try:
        server = _FlightServer(args.root_dir, args.port, args.max_concurrent_reads)
    except Exception as e:
        print(f"FLIGHT_ERROR:{e}", flush=True)
        sys.exit(1)

    thread = threading.Thread(target=server.serve, daemon=True, name="flight-server")
    thread.start()

    # Wait for gRPC port binding (up to 10s)
    for _ in range(100):
        try:
            if server.port and server.port > 0:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        print("FLIGHT_ERROR:bind_failed", flush=True)
        sys.exit(1)

    print(f"FLIGHT_READY:{server.port}", flush=True)

    # Detach from parent
    try:
        sys.stdout.close()
    except Exception:
        pass
    try:
        sys.stdin.close()
    except Exception:
        pass

    # Wait for SIGTERM or idle timeout
    shutdown = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())

    while not shutdown.is_set():
        if server.idle_seconds > _IDLE_TIMEOUT_S:
            break
        shutdown.wait(timeout=10.0)

    server.shutdown()


if __name__ == "__main__":
    main()
