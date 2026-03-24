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

"""Shared container test helpers — Flight server, MinIO S3, Arrow IPC utilities.

Used by ``test_container_nvme_store.py``, ``test_container_nvme_workflow.py``,
and ``test_video_workflow.py``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

from _internal.core.split_payload_store import _sanitize_key
from tests.conftest import FLIGHT_INTERNAL_PORT

logger = logging.getLogger(__name__)


@dataclass
class FlightNode:
    """Holds a running Flight-server container and its host-visible endpoint."""

    container: Any
    host: str
    port: int
    data_dir: str  # host-side path mounted at /data in the container

    @property
    def endpoint(self) -> str:
        return f"grpc://{self.host}:{self.port}"


def start_flight_container(image_tag: str, data_dir: str) -> FlightNode:
    """Start a Flight server container and block until it is ready."""
    from testcontainers.core.container import DockerContainer  # type: ignore[import-untyped]
    from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore[import-untyped]

    container = (
        DockerContainer(image_tag)
        .with_exposed_ports(FLIGHT_INTERNAL_PORT)
        .with_volume_mapping(os.path.realpath(data_dir), "/data", "rw")
    )
    container.start()
    wait_for_logs(container, "FLIGHT_READY", timeout=120)

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(FLIGHT_INTERNAL_PORT))
    logger.info(f"Flight container ready at {host}:{port}  (data_dir={data_dir})")
    return FlightNode(container=container, host=host, port=port, data_dir=data_dir)


def minio_s3_options(minio_container) -> dict:
    """Build fsspec s3_options for a MinIO testcontainer.

    Disables checksum validation (incompatible between recent
    aiobotocore versions and MinIO's S3 implementation).
    """
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    return {
        "key": minio_container.access_key,
        "secret": minio_container.secret_key,
        "client_kwargs": {"endpoint_url": f"http://{host}:{port}"},
        "config_kwargs": {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    }


def write_arrow_ipc(data_dir: str, key: str, table: pa.Table) -> str:
    """Write Arrow IPC file in NvmeDisk-compatible layout.

    File goes to ``{data_dir}/{prefix}/{sanitized_key}.arrow`` where
    *prefix* is the first two characters of the sanitized key.
    """
    safe = _sanitize_key(key)
    prefix = safe[:2] if len(safe) >= 2 else "00"
    dir_path = os.path.join(data_dir, prefix)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{safe}.arrow")
    with pa.OSFile(file_path, "wb") as f:
        writer = ipc.new_file(f, table.schema)
        writer.write_table(table)
        writer.close()
    return file_path


def make_test_table(num_rows: int = 100, prefix: str = "") -> pa.Table:
    """Create a deterministic test Arrow table."""
    return pa.table(
        {
            "id": list(range(num_rows)),
            "value": [f"{prefix}row_{i}" for i in range(num_rows)],
            "score": [float(i) / max(num_rows, 1) for i in range(num_rows)],
        }
    )
