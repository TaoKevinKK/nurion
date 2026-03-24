#!/usr/bin/env python3
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

"""Standalone Arrow Flight server for container-based testing.

Implements the same do_get() protocol as FlightPayloadServer in
nvme_payload_store.py — no engine imports required.  Only depends on pyarrow.

Usage inside Docker:
    python flight_server.py --data-dir /data --port 8815

Protocol:
    do_get(ticket) where ticket.ticket = payload key (UTF-8 bytes)
    File layout:  {data_dir}/{prefix}/{sanitized_key}.arrow
    prefix      = first 2 chars of sanitized key
    sanitize    = replace ':' and '/' with '_'
"""

import argparse
import os
import signal
import sys
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc


def _sanitize_key(key: str) -> str:
    """Match split_payload_store._sanitize_key exactly."""
    return key.replace(":", "_").replace("/", "_")


class TestFlightServer(flight.FlightServerBase):
    """Minimal Flight server that serves Arrow IPC files from a directory."""

    def __init__(self, data_dir: str, port: int = 8815):
        self._data_dir = data_dir
        location = flight.Location.for_grpc_tcp("0.0.0.0", port)
        super().__init__(location)

    def do_get(self, context, ticket):
        key = ticket.ticket.decode()
        safe = _sanitize_key(key)
        prefix = safe[:2] if len(safe) >= 2 else "00"

        path = os.path.join(self._data_dir, prefix, f"{safe}.arrow")
        if not os.path.exists(path):
            raise flight.FlightUnavailableError(f"Payload not found: {key}")

        source = pa.memory_map(path, "r")
        reader = ipc.open_file(source)
        table = reader.read_all()
        return flight.RecordBatchStream(table)


def main():
    parser = argparse.ArgumentParser(description="Arrow Flight test server")
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--port", type=int, default=8815)
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)

    server = TestFlightServer(args.data_dir, port=args.port)

    # serve() blocks, so run in a daemon thread (same pattern as FlightPayloadServer).
    t = threading.Thread(target=server.serve, daemon=True)
    t.start()

    # Poll until the gRPC server has actually bound to a port.
    for _ in range(100):
        try:
            if server.port and server.port > 0:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        print("FLIGHT_ERROR: server failed to bind port", flush=True)
        sys.exit(1)

    # Readiness signal — testcontainers wait_for_logs() watches for this.
    print(f"FLIGHT_READY:{server.port}", flush=True)

    # Block until SIGTERM (Docker stop) or SIGINT (Ctrl-C).
    shutdown = threading.Event()

    def _shutdown(signum, frame):
        shutdown.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        shutdown.wait()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
