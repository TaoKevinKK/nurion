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

"""Container-based distributed tests for NvmeSplitPayloadStore (Layer B).

Uses testcontainers to spin up Docker containers running standalone Arrow
Flight servers, verifying cross-network reads, S3 (MinIO) failover, and
multi-node payload routing — all with real TCP/gRPC, no mocks.

Architecture:
    Test process (host)
      ├── Writes Arrow IPC files to mounted volumes
      ├── Connects to Flight server containers via gRPC
      ├── Connects to MinIO container (S3)
      └── Kills/restarts containers for failure tests

    Docker
      ├── flight-node(s) : python:3.13-slim + pyarrow, runs TestFlightServer
      └── minio           : S3-compatible storage (from conftest fixture)

Requirements: Docker daemon running.  Excluded from fast CI via `distributed` marker.
"""

import logging
import os
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

logger = logging.getLogger(__name__)

# Skip entire module if Docker SDK is missing
pytest.importorskip("docker", reason="docker SDK required for container tests")

from tests.utils.container_helpers import (  # noqa: E402
    make_test_table,
    minio_s3_options,
    start_flight_container,
    write_arrow_ipc,
)

pytestmark = pytest.mark.distributed


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def flight_node(flight_server_image, tmp_path):
    """A fresh Flight server container with an empty data directory."""
    data_dir = str(tmp_path / "nvme_data")
    os.makedirs(data_dir, exist_ok=True)
    node = start_flight_container(flight_server_image, data_dir)
    yield node
    try:
        node.container.stop()
    except Exception:
        pass


# ===========================================================================
# Layer B-1: Flight protocol across Docker network
# ===========================================================================


class TestContainerFlightProtocol:
    """Raw Arrow Flight reads from a container over Docker networking.

    These tests write Arrow IPC files to a host directory that is volume-
    mounted into a Docker container running a standalone Flight server,
    then read via gRPC from the host side.
    """

    def test_basic_read(self, flight_node):
        """Round-trip: host writes → container serves → host reads via Flight."""
        key = "test_payload_001"
        table = make_test_table(100)
        write_arrow_ipc(flight_node.data_dir, key, table)

        client = flight.FlightClient(flight_node.endpoint)
        result = client.do_get(flight.Ticket(key.encode())).read_all()

        assert result.equals(table)

    def test_large_payload(self, flight_node):
        """~10 MB table transferred via Flight."""
        key = "large_payload_001"
        table = pa.table(
            {
                "id": list(range(100_000)),
                "data": [f"x{i:010d}" * 10 for i in range(100_000)],
            }
        )
        write_arrow_ipc(flight_node.data_dir, key, table)

        client = flight.FlightClient(flight_node.endpoint)
        result = client.do_get(flight.Ticket(key.encode())).read_all()
        assert result.num_rows == 100_000
        assert result.equals(table)

    def test_multiple_keys(self, flight_node):
        """Read five different payloads from the same container."""
        tables: dict[str, pa.Table] = {}
        for i in range(5):
            key = f"multi_key_{i:03d}"
            tables[key] = make_test_table(50, prefix=f"t{i}_")
            write_arrow_ipc(flight_node.data_dir, key, tables[key])

        client = flight.FlightClient(flight_node.endpoint)
        for key, expected in tables.items():
            result = client.do_get(flight.Ticket(key.encode())).read_all()
            assert result.equals(expected), f"Mismatch for {key}"

    def test_nonexistent_key_raises(self, flight_node):
        """Requesting a missing key returns FlightUnavailableError."""
        client = flight.FlightClient(flight_node.endpoint)
        with pytest.raises(flight.FlightUnavailableError):
            client.do_get(flight.Ticket(b"no_such_key")).read_all()

    def test_key_with_colons(self, flight_node):
        """Keys containing ':' are sanitized consistently on both sides."""
        key = "job1:stage2:split_42"
        table = make_test_table(10)
        write_arrow_ipc(flight_node.data_dir, key, table)

        client = flight.FlightClient(flight_node.endpoint)
        result = client.do_get(flight.Ticket(key.encode())).read_all()
        assert result.equals(table)


# ===========================================================================
# Layer B-2: NvmeSplitPayloadStore integration with container Flight
# ===========================================================================


class TestContainerStoreIntegration:
    """Drive NvmeSplitPayloadStore.get_with_hint() against live containers."""

    def test_get_with_hint_reads_from_container(self, flight_node, tmp_path):
        """Empty local NVMe + hint → Flight to container → success."""
        from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy

        key = "remote_payload_001"
        table = make_test_table(200)
        write_arrow_ipc(flight_node.data_dir, key, table)

        local_dir = str(tmp_path / "local_nvme")
        os.makedirs(local_dir, exist_ok=True)
        store = NvmeSplitPayloadStore(
            root_dirs=[local_dir],
            job_id="container_test",
            write_policy=WritePolicy.WRITE_BACK,
            node_ip="127.0.0.1",
        )

        result = store.get_with_hint(key, {"flight": flight_node.endpoint})
        assert result is not None
        assert result.data.equals(table)
        assert store._metrics["remote_hits"] == 1

    def test_local_nvme_takes_precedence_over_container(self, flight_node, tmp_path):
        """If key exists locally, Flight container is never contacted."""
        from _internal.core.models import SplitPayload
        from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy

        key = "precedence_001"
        remote_table = make_test_table(10, prefix="remote_")
        local_table = make_test_table(10, prefix="local_")

        write_arrow_ipc(flight_node.data_dir, key, remote_table)

        local_dir = str(tmp_path / "local_nvme")
        os.makedirs(local_dir, exist_ok=True)
        store = NvmeSplitPayloadStore(
            root_dirs=[local_dir],
            job_id="container_test",
            write_policy=WritePolicy.WRITE_BACK,
            node_ip="127.0.0.1",
        )
        store.store(key, SplitPayload(data=local_table, split_id=key))

        result = store.get_with_hint(key, {"flight": flight_node.endpoint})
        assert result is not None
        assert result.data.equals(local_table), "Local data should take precedence"
        assert store._metrics["local_hits"] == 1
        assert store._metrics["remote_hits"] == 0


# ===========================================================================
# Layer B-3: Container failure → S3 (MinIO) fallback
# ===========================================================================


class TestContainerFailover:
    """Kill Flight containers, verify transparent S3 fallback."""

    def test_flight_kill_then_s3_fallback(self, flight_server_image, minio_container, tmp_path):
        """Flight alive → read OK · kill container → same key read from S3."""
        from _internal.core.models import SplitPayload
        from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy

        s3_uri = "s3://warehouse/nvme-failover"
        s3_options = minio_s3_options(minio_container)

        # --- Start Flight container ---
        data_dir = str(tmp_path / "flight_data")
        os.makedirs(data_dir, exist_ok=True)
        node = start_flight_container(flight_server_image, data_dir)

        try:
            key = "failover_001"
            table = make_test_table(200)
            payload = SplitPayload(data=table, split_id=key)

            # Populate container volume (remote NVMe)
            write_arrow_ipc(data_dir, key, table)

            # Populate S3 via a writer store
            writer = NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "writer_nvme")],
                job_id="nvme-failover",
                write_policy=WritePolicy.WRITE_THROUGH,
                s3_uri=s3_uri,
                s3_options=s3_options,
                node_ip="127.0.0.1",
            )
            writer._ensure_initialized()
            writer._write_s3(key, payload)
            s3_path = writer.get_location(key).get("s3")

            hint = {"flight": node.endpoint, "s3": s3_path}

            # Reader store (empty local NVMe + same S3)
            reader = NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "reader_nvme")],
                job_id="nvme-failover",
                write_policy=WritePolicy.WRITE_BACK,
                s3_uri=s3_uri,
                s3_options=s3_options,
                node_ip="127.0.0.1",
            )

            # Phase 1: Flight read (container alive)
            r1 = reader.get_with_hint(key, hint)
            assert r1 is not None and r1.data.num_rows == 200
            assert reader._metrics["remote_hits"] == 1

            # Phase 2: Kill container → next read falls back to S3
            logger.info("Killing Flight container for failover test")
            node.container.stop()
            time.sleep(1)

            r2 = reader.get_with_hint(key, hint)
            assert r2 is not None and r2.data.num_rows == 200
            assert reader._metrics["s3_hits"] == 1
        finally:
            try:
                node.container.stop()
            except Exception:
                pass

    def test_container_restart_recovery(self, flight_server_image, tmp_path):
        """Kill → restart with same mount → Flight reads recover."""
        data_dir = str(tmp_path / "restart_data")
        os.makedirs(data_dir, exist_ok=True)

        key = "restart_001"
        table = make_test_table(50)
        write_arrow_ipc(data_dir, key, table)

        # First container
        node1 = start_flight_container(flight_server_image, data_dir)
        client1 = flight.FlightClient(node1.endpoint)
        assert client1.do_get(flight.Ticket(key.encode())).read_all().equals(table)

        node1.container.stop()
        time.sleep(1)

        # Second container (same volume, new mapped port)
        node2 = start_flight_container(flight_server_image, data_dir)
        try:
            client2 = flight.FlightClient(node2.endpoint)
            assert client2.do_get(flight.Ticket(key.encode())).read_all().equals(table)
        finally:
            node2.container.stop()


# ===========================================================================
# Layer B-4: Multi-node payload routing
# ===========================================================================


class TestContainerMultiNode:
    """Two Flight containers with disjoint data — verify routing."""

    def test_two_node_routing(self, flight_server_image, tmp_path):
        """payload_loc directs reads to the correct container."""
        from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy

        dir_a = str(tmp_path / "node_a")
        dir_b = str(tmp_path / "node_b")
        os.makedirs(dir_a, exist_ok=True)
        os.makedirs(dir_b, exist_ok=True)

        table_a = make_test_table(100, prefix="A_")
        table_b = make_test_table(100, prefix="B_")
        key_a, key_b = "payload_node_a", "payload_node_b"

        write_arrow_ipc(dir_a, key_a, table_a)
        write_arrow_ipc(dir_b, key_b, table_b)

        node_a = start_flight_container(flight_server_image, dir_a)
        node_b = start_flight_container(flight_server_image, dir_b)
        try:
            store = NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "local_empty")],
                job_id="multinode",
                write_policy=WritePolicy.WRITE_BACK,
                node_ip="127.0.0.1",
            )

            # Correct routing
            r_a = store.get_with_hint(key_a, {"flight": node_a.endpoint})
            assert r_a is not None
            assert r_a.data.column("value")[0].as_py().startswith("A_")

            r_b = store.get_with_hint(key_b, {"flight": node_b.endpoint})
            assert r_b is not None
            assert r_b.data.column("value")[0].as_py().startswith("B_")

            # Mis-routing: key_a does not exist on node_b → None
            assert store.get_with_hint(key_a, {"flight": node_b.endpoint}) is None
        finally:
            node_a.container.stop()
            node_b.container.stop()

    def test_one_dead_one_alive_with_s3(self, flight_server_image, minio_container, tmp_path):
        """Kill one node; alive node serves via Flight, dead node falls back to S3."""
        from _internal.core.models import SplitPayload
        from _internal.core.nvme_payload_store import NvmeSplitPayloadStore, WritePolicy

        s3_uri = "s3://warehouse/nvme-multinode"
        s3_options = minio_s3_options(minio_container)

        dir_alive = str(tmp_path / "alive_node")
        dir_dead = str(tmp_path / "dead_node")
        os.makedirs(dir_alive, exist_ok=True)
        os.makedirs(dir_dead, exist_ok=True)

        key_alive, key_dead = "alive_payload", "dead_payload"
        table_alive = make_test_table(100, prefix="alive_")
        table_dead = make_test_table(100, prefix="dead_")

        write_arrow_ipc(dir_alive, key_alive, table_alive)
        write_arrow_ipc(dir_dead, key_dead, table_dead)

        node_alive = start_flight_container(flight_server_image, dir_alive)
        node_dead = start_flight_container(flight_server_image, dir_dead)

        try:
            # Write dead-node payload to S3 so fallback works
            w = NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "w_nvme")],
                job_id="nvme-multinode",
                write_policy=WritePolicy.WRITE_THROUGH,
                s3_uri=s3_uri,
                s3_options=s3_options,
                node_ip="127.0.0.1",
            )
            w._ensure_initialized()
            w._write_s3(key_dead, SplitPayload(data=table_dead, split_id=key_dead))
            s3_dead = w.get_location(key_dead).get("s3")

            # Kill the dead node
            node_dead.container.stop()
            time.sleep(1)

            # Reader
            reader = NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "r_nvme")],
                job_id="nvme-multinode",
                write_policy=WritePolicy.WRITE_BACK,
                s3_uri=s3_uri,
                s3_options=s3_options,
                node_ip="127.0.0.1",
            )

            # Alive node → Flight OK
            r1 = reader.get_with_hint(key_alive, {"flight": node_alive.endpoint})
            assert r1 is not None
            assert r1.data.column("value")[0].as_py().startswith("alive_")

            # Dead node → Flight fail → S3 fallback
            r2 = reader.get_with_hint(key_dead, {"flight": node_dead.endpoint, "s3": s3_dead})
            assert r2 is not None
            assert r2.data.column("value")[0].as_py().startswith("dead_")
            assert reader._metrics["s3_hits"] >= 1
        finally:
            try:
                node_alive.container.stop()
            except Exception:
                pass
            try:
                node_dead.container.stop()
            except Exception:
                pass
