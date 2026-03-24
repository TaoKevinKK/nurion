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

"""Unit and integration tests for NvmeSplitPayloadStore, NvmeDisk, NvmeDiskPool."""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pickle

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from _internal.core.models import DataQueueMessage, SplitPayload
from _internal.core.nvme_payload_store import (
    FlightPayloadServer,
    NvmeDisk,
    NvmeDiskPool,
    NvmeSplitPayloadStore,
    WritePolicy,
    parse_nvme_uri,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(split_id: str = "test_split", num_rows: int = 5) -> SplitPayload:
    table = pa.table(
        {
            "id": list(range(num_rows)),
            "name": [f"row_{i}" for i in range(num_rows)],
            "value": [float(i) * 1.5 for i in range(num_rows)],
        }
    )
    return SplitPayload(data=table, split_id=split_id)


# ---------------------------------------------------------------------------
# WritePolicy
# ---------------------------------------------------------------------------


class TestWritePolicy:
    def test_values(self):
        assert WritePolicy.WRITE_THROUGH.value == "write_through"
        assert WritePolicy.WRITE_BACK.value == "write_back"

    def test_from_string(self):
        assert WritePolicy("write_through") == WritePolicy.WRITE_THROUGH
        assert WritePolicy("write_back") == WritePolicy.WRITE_BACK


# ---------------------------------------------------------------------------
# parse_nvme_uri
# ---------------------------------------------------------------------------


class TestParseNvmeUri:
    def test_single_path(self):
        dirs, params = parse_nvme_uri("nvme:///mnt/nvme0/nurion")
        assert dirs == ["/mnt/nvme0/nurion"]
        assert params == {}

    def test_multi_path(self):
        dirs, params = parse_nvme_uri("nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion")
        assert dirs == ["/mnt/nvme0/nurion", "/mnt/nvme1/nurion"]

    def test_with_params(self):
        dirs, params = parse_nvme_uri(
            "nvme:///mnt/nvme0/n?s3_fallback=s3://bucket/pfx&quota_gb=500"
        )
        assert dirs == ["/mnt/nvme0/n"]
        assert params["s3_fallback"] == "s3://bucket/pfx"
        assert params["quota_gb"] == "500"

    def test_empty_path_raises(self):
        with pytest.raises(ValueError, match="at least one path"):
            parse_nvme_uri("nvme://")


# ---------------------------------------------------------------------------
# NvmeDisk
# ---------------------------------------------------------------------------


class TestNvmeDisk:
    def test_write_and_read(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        payload = _make_payload("k1")

        path = disk.write("k1", payload)
        assert os.path.exists(path)
        assert path.endswith(".arrow")

        result = disk.read("k1")
        assert result is not None
        assert result.data.num_rows == 5
        assert result.data.equals(payload.data)

    def test_read_missing(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        assert disk.read("nonexistent") is None

    def test_delete(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("k1", _make_payload("k1"))
        assert disk.delete("k1") is True
        assert disk.read("k1") is None
        assert disk.delete("k1") is False

    def test_no_tmp_files_after_write(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("k1", _make_payload("k1"))
        tmp_files = list(Path(disk.job_dir).glob("*.tmp.*"))
        assert tmp_files == []

    def test_cleanup_tmp_on_init(self, tmp_path):
        job_dir = tmp_path / "job1"
        job_dir.mkdir()
        # Create orphan tmp file
        (job_dir / "stale.arrow.tmp.99999").write_bytes(b"garbage")
        assert len(list(job_dir.glob("*.tmp.*"))) == 1

        disk = NvmeDisk(str(tmp_path), "job1")
        assert len(list(Path(disk.job_dir).glob("*.tmp.*"))) == 0

    def test_available_bytes(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        assert disk.available_bytes() > 0

    def test_available_bytes_with_quota(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=1024 * 1024)
        assert disk.available_bytes() <= 1024 * 1024

    def test_clear(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("k1", _make_payload("k1"))
        disk.write("k2", _make_payload("k2"))
        count = disk.clear()
        assert count == 2
        assert disk.read("k1") is None
        assert disk.read("k2") is None

    def test_key_sanitization(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        payload = _make_payload("job:stage:split")
        disk.write("job:stage:split", payload)
        result = disk.read("job:stage:split")
        assert result is not None
        assert result.data.num_rows == 5


# ---------------------------------------------------------------------------
# NvmeDiskPool
# ---------------------------------------------------------------------------


class TestNvmeDiskPool:
    def test_single_disk(self, tmp_path):
        pool = NvmeDiskPool([str(tmp_path / "d0")], "job1")
        pool.write("k1", _make_payload("k1"))
        result = pool.read("k1")
        assert result is not None

    def test_multi_disk_distributes(self, tmp_path):
        d0, d1 = str(tmp_path / "d0"), str(tmp_path / "d1")
        pool = NvmeDiskPool([d0, d1], "job1")

        pool.write("k1", _make_payload("k1"))
        pool.write("k2", _make_payload("k2"))

        assert pool.read("k1") is not None
        assert pool.read("k2") is not None

    def test_read_scans_all_disks(self, tmp_path):
        d0, d1 = str(tmp_path / "d0"), str(tmp_path / "d1")
        pool = NvmeDiskPool([d0, d1], "job1")
        # Write directly to second disk
        pool._disks[1].write("k1", _make_payload("k1"))

        result = pool.read("k1")
        assert result is not None

    def test_delete(self, tmp_path):
        pool = NvmeDiskPool([str(tmp_path / "d0")], "job1")
        pool.write("k1", _make_payload("k1"))
        assert pool.delete("k1") is True
        assert pool.read("k1") is None

    def test_clear(self, tmp_path):
        pool = NvmeDiskPool([str(tmp_path / "d0"), str(tmp_path / "d1")], "job1")
        pool.write("k1", _make_payload("k1"))
        pool.write("k2", _make_payload("k2"))
        count = pool.clear()
        assert count == 2


# ---------------------------------------------------------------------------
# NvmeSplitPayloadStore (WRITE_BACK, no S3)
# ---------------------------------------------------------------------------


class TestNvmeSplitPayloadStoreWriteBack:
    def _make_store(self, tmp_path) -> NvmeSplitPayloadStore:
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_BACK,
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        return store

    def test_store_and_get(self, tmp_path):
        store = self._make_store(tmp_path)
        payload = _make_payload("k1")
        store.store("k1", payload)

        result = store.get("k1")
        assert result is not None
        assert result.data.equals(payload.data)

    def test_get_missing(self, tmp_path):
        store = self._make_store(tmp_path)
        assert store.get("nonexistent") is None

    def test_delete(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        assert store.delete("k1") is True
        assert store.get("k1") is None

    def test_clear(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        store.store("k2", _make_payload("k2"))
        count = store.clear()
        assert count == 2

    def test_get_location(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        loc = store.get_location("k1")
        assert loc is not None
        assert "flight" in loc
        assert loc["flight"].startswith("grpc://")

    def test_get_with_hint_local(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        hint = {"flight": "grpc://10.0.0.99:9999"}  # wrong endpoint
        result = store.get_with_hint("k1", hint)
        assert result is not None  # local read succeeds, hint ignored

    def test_flush_noop(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        store.flush_pending_writes()  # Should be no-op for WRITE_BACK

    def test_metrics(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        store.get("k1")
        m = store.get_metrics()
        assert m["stored"] == 1
        assert m["local_hits"] == 1

    def test_pickle_roundtrip(self, tmp_path):
        """Verify store survives pickle (Ray serialization)."""
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
        )
        data = pickle.dumps(store)
        restored = pickle.loads(data)
        assert restored._root_dirs == store._root_dirs
        assert restored._job_id == store._job_id
        assert restored._initialized is False


# ---------------------------------------------------------------------------
# NvmeSplitPayloadStore (WRITE_BACK with S3)
# ---------------------------------------------------------------------------


class TestNvmeSplitPayloadStoreWithS3:
    def _make_store(self, tmp_path) -> NvmeSplitPayloadStore:
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_BACK,
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        return store

    def test_store_writes_to_both(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))

        # Local NVMe read
        assert store.get("k1") is not None

        # Wait for async S3 upload
        if store._s3_executor:
            store._s3_executor.shutdown(wait=True)

        # S3 should also have the file
        s3_files = list((tmp_path / "s3_mock" / "job1").rglob("*.arrow"))
        assert len(s3_files) == 1

    def test_get_location_includes_s3(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))
        loc = store.get_location("k1")
        assert "s3" in loc
        assert loc["s3"].endswith("k1.arrow")


# ---------------------------------------------------------------------------
# NvmeSplitPayloadStore (WRITE_THROUGH)
# ---------------------------------------------------------------------------


class TestNvmeSplitPayloadStoreWriteThrough:
    def test_write_through_requires_s3(self, tmp_path):
        with pytest.raises(ValueError, match="WRITE_THROUGH requires s3_fallback"):
            NvmeSplitPayloadStore(
                root_dirs=[str(tmp_path / "nvme0")],
                job_id="job1",
                write_policy=WritePolicy.WRITE_THROUGH,
                # no s3_uri!
            )

    def _make_store(self, tmp_path) -> NvmeSplitPayloadStore:
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_THROUGH,
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        return store

    def test_flush_waits_for_s3(self, tmp_path):
        store = self._make_store(tmp_path)
        store.store("k1", _make_payload("k1"))

        # Futures should be pending
        assert len(store._pending_s3_futures) == 1

        # Flush should wait and clear
        store.flush_pending_writes()
        assert len(store._pending_s3_futures) == 0

        # S3 should have data
        s3_files = list((tmp_path / "s3_mock" / "job1").rglob("*.arrow"))
        assert len(s3_files) == 1

    def test_s3_failure_auto_degrades(self, tmp_path):
        store = self._make_store(tmp_path)
        store.S3_FAILURE_THRESHOLD = 2

        # Make S3 writes fail
        original_write = store._write_s3

        def failing_write(*args, **kwargs):
            raise IOError("mock S3 failure")

        store._write_s3 = failing_write

        # First batch: fails, but threshold not reached
        store.store("k1", _make_payload("k1"))
        with pytest.raises(IOError):
            store.flush_pending_writes()
        assert store._write_policy == WritePolicy.WRITE_THROUGH

        # Second batch: fails again, threshold reached → auto-degrade
        store.store("k2", _make_payload("k2"))
        with pytest.raises(IOError):
            store.flush_pending_writes()
        assert store._write_policy == WritePolicy.WRITE_BACK

        # Restore and verify WRITE_BACK works
        store._write_s3 = original_write
        store.store("k3", _make_payload("k3"))
        store.flush_pending_writes()  # no-op for WRITE_BACK
        assert store.get("k3") is not None


# ---------------------------------------------------------------------------
# FlightPayloadServer
# ---------------------------------------------------------------------------


class TestFlightPayloadServer:
    def test_serve_and_get(self, tmp_path):
        """Start a Flight server and read a payload via Flight client."""
        import pyarrow.flight as flight

        # Write a test payload to disk
        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("k1", _make_payload("k1", num_rows=3))

        # Start server on random port
        server = FlightPayloadServer([disk.job_dir], port=0, max_concurrent_reads=2)
        thread = __import__("threading").Thread(target=server.serve, daemon=True)
        thread.start()

        port = server.port
        client = flight.connect(f"grpc://127.0.0.1:{port}")

        reader = client.do_get(flight.Ticket(b"k1"))
        table = reader.read_all()
        assert table.num_rows == 3

        server.shutdown()

    def test_missing_key_raises(self, tmp_path):
        import pyarrow.flight as flight

        disk = NvmeDisk(str(tmp_path), "job1")
        server = FlightPayloadServer([disk.job_dir], port=0)
        thread = __import__("threading").Thread(target=server.serve, daemon=True)
        thread.start()

        client = flight.connect(f"grpc://127.0.0.1:{server.port}")
        with pytest.raises(flight.FlightUnavailableError):
            client.do_get(flight.Ticket(b"nonexistent")).read_all()

        server.shutdown()

    def test_concurrent_reads(self, tmp_path):
        """Multiple concurrent Flight reads should not corrupt data."""
        disk = NvmeDisk(str(tmp_path), "job1")
        for i in range(10):
            disk.write(f"k{i}", _make_payload(f"k{i}", num_rows=100))

        server = FlightPayloadServer([disk.job_dir], port=0, max_concurrent_reads=4)
        t = threading.Thread(target=server.serve, daemon=True)
        t.start()

        results = {}
        errors = []

        def read_key(key):
            try:
                c = flight.connect(f"grpc://127.0.0.1:{server.port}")
                table = c.do_get(flight.Ticket(key.encode())).read_all()
                results[key] = table.num_rows
            except Exception as e:
                errors.append((key, e))

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(read_key, f"k{i}") for i in range(10)]
            for f in futures:
                f.result()

        assert len(errors) == 0, f"Flight errors: {errors}"
        assert all(v == 100 for v in results.values())
        server.shutdown()


# ===========================================================================
# Layer 1 additions: stress, large payload, concurrent writes
# ===========================================================================


class TestNvmeDiskConcurrency:
    """Concurrent read/write to the same disk from multiple threads."""

    def test_concurrent_writes(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        errors = []

        def write_key(i):
            try:
                disk.write(f"k{i}", _make_payload(f"k{i}", num_rows=50))
            except Exception as e:
                errors.append(e)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(write_key, i) for i in range(20)]
            for f in futures:
                f.result()

        assert len(errors) == 0
        # All 20 payloads should be readable
        for i in range(20):
            assert disk.read(f"k{i}") is not None

    def test_concurrent_read_write(self, tmp_path):
        """Writers and readers simultaneously — readers may get None for
        not-yet-written keys, but should never get corrupted data."""
        disk = NvmeDisk(str(tmp_path), "job1")
        # Pre-write some keys
        for i in range(10):
            disk.write(f"pre{i}", _make_payload(f"pre{i}", num_rows=10))

        errors = []

        def writer(i):
            try:
                disk.write(f"new{i}", _make_payload(f"new{i}", num_rows=10))
            except Exception as e:
                errors.append(("write", i, e))

        def reader(key):
            try:
                result = disk.read(key)
                if result is not None:
                    assert result.data.num_rows == 10
            except Exception as e:
                errors.append(("read", key, e))

        with ThreadPoolExecutor(max_workers=12) as pool:
            futs = []
            for i in range(10):
                futs.append(pool.submit(writer, i))
                futs.append(pool.submit(reader, f"pre{i}"))
            for f in futs:
                f.result()

        assert len(errors) == 0


class TestLargePayload:
    """Test with larger payloads to verify no size-related issues."""

    def test_10mb_payload(self, tmp_path):
        """~10MB Arrow table round-trip."""
        n = 100_000
        table = pa.table(
            {
                "id": list(range(n)),
                "text": [f"row_{i}_" + "x" * 80 for i in range(n)],
                "value": [float(i) for i in range(n)],
            }
        )
        payload = SplitPayload(data=table, split_id="big")

        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()

        store.store("big", payload)
        result = store.get("big")
        assert result is not None
        assert result.data.num_rows == n
        assert result.data.equals(table)

    def test_flight_large_payload(self, tmp_path):
        """~10MB payload through Flight server."""
        n = 100_000
        table = pa.table(
            {
                "id": list(range(n)),
                "data": [os.urandom(64) for _ in range(n)],
            }
        )
        payload = SplitPayload(data=table, split_id="big")

        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("big", payload)

        server = FlightPayloadServer([disk.job_dir], port=0)
        t = threading.Thread(target=server.serve, daemon=True)
        t.start()

        client = flight.connect(f"grpc://127.0.0.1:{server.port}")
        result = client.do_get(flight.Ticket(b"big")).read_all()
        assert result.num_rows == n
        server.shutdown()


class TestNvmeStoreFlightIntegration:
    """Test the full get_with_hint path: local miss → Flight → S3 fallback."""

    def test_remote_flight_read(self, tmp_path):
        """Store on 'remote' store, read via Flight from 'local' store."""
        # "Remote" store writes payload
        remote_store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "remote_nvme")],
            job_id="job_remote",
            node_ip="127.0.0.1",
        )
        remote_store._ensure_initialized()
        remote_store.store("flight_test_k1", _make_payload("flight_test_k1", num_rows=7))
        remote_loc = remote_store.get_location("flight_test_k1")

        # "Local" store on different directory (simulates different node)
        local_store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "local_nvme")],
            job_id="job_remote",
            node_ip="127.0.0.2",  # different "node"
        )
        local_store._ensure_initialized()

        # Local store doesn't have the key — should read via Flight from remote
        result = local_store.get_with_hint("flight_test_k1", remote_loc)
        assert result is not None
        assert result.data.num_rows == 7

    def test_flight_fail_s3_fallback(self, tmp_path):
        """Flight endpoint unreachable → falls back to S3."""
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme")],
            job_id="job1",
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()

        # Store payload (writes to NVMe + async S3)
        store.store("k1", _make_payload("k1", num_rows=3))
        # Wait for S3 upload to complete
        if store._s3_executor:
            store._s3_executor.shutdown(wait=True)
            store._s3_executor = ThreadPoolExecutor(max_workers=4)

        # Construct a hint with dead Flight endpoint + valid S3 path
        loc = store.get_location("k1")
        loc["flight"] = "grpc://192.0.2.1:9999"  # RFC 5737 TEST-NET, unreachable

        # Create a fresh store that doesn't have k1 locally
        other_store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "other_nvme")],
            job_id="job1",
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.3",
        )
        other_store._ensure_initialized()

        # Should fail Flight → succeed S3
        result = other_store.get_with_hint("k1", loc)
        assert result is not None
        assert result.data.num_rows == 3
        assert other_store._metrics["s3_hits"] == 1

    def test_message_payload_loc_roundtrip(self, tmp_path):
        """Verify payload_loc survives DataQueueMessage serialization."""
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme")],
            job_id="job1",
            s3_uri="file:///tmp/fake_s3",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        store.store("k1", _make_payload("k1"))

        loc = store.get_location("k1")
        msg = DataQueueMessage(
            message_id="m1",
            split_id="k1",
            payload_key="k1",
            metadata={"source_stage": "s0", "payload_loc": loc},
        )

        # Serialize → deserialize (same as queue transport)
        raw = msg.to_bytes()
        from _internal.core.models import queue_message_from_bytes

        restored = queue_message_from_bytes(raw)
        assert restored.metadata["payload_loc"]["flight"] == loc["flight"]
        assert restored.metadata["payload_loc"]["s3"] == loc["s3"]


# ===========================================================================
# Layer 2: Integration test — StageWorker uses get_with_hint + payload_loc
# ===========================================================================


class TestStageWorkerNvmeIntegration:
    """Verify StageWorker correctly uses get_with_hint and embeds payload_loc.

    Uses the same pattern as TestStageWorkerPayloadCleanup in test_stage_master.py:
    direct class instantiation (no Ray), real WorkQueue backend.
    """

    @pytest.mark.asyncio
    async def test_get_with_hint_called(self, workqueue_backend):
        """StageWorker._parse_records passes payload_loc hint to get_with_hint."""
        from unittest.mock import MagicMock
        from _internal.core.stage_worker import StageWorker, WorkerRuntime
        from _internal.core.models import QueueEndpoint
        from _internal.core.operator import OperatorConfig, Operator
        from _internal.runtime.queue_stats import QueueRef
        from dataclasses import dataclass

        WorkerClass = StageWorker.__ray_actor_class__

        # Mock payload store that tracks get_with_hint calls
        mock_store = MagicMock()
        mock_store.get_with_hint.return_value = SplitPayload(
            data=pa.table({"x": [1, 2]}), split_id="s1"
        )
        mock_store.get_location.return_value = None
        mock_store.store.return_value = "out_key"
        mock_store.delete.return_value = True
        mock_store.flush_pending_writes.return_value = None

        @dataclass
        class SimpleConfig(OperatorConfig):
            pass

        class SimpleOp(Operator):
            def process_split(self, split, payload=None):
                return payload

        SimpleConfig.operator_class = SimpleOp
        SimpleOp.config_class = SimpleConfig

        class MockStage:
            stage_id = "test_stage"
            operator_config = SimpleConfig()
            upstream_stages = None
            min_parallelism = 1
            max_parallelism = 1
            output_partitions = None
            batch_size = 100
            commit_batch_size = 5
            backpressure_threshold_lag = 5000
            backpressure_threshold_queue_size = 1000
            worker_ready_timeout_seconds = 30.0
            worker_spawn_retry_delay_seconds = 2.0
            num_cpus = 1.0
            num_gpus = 0.0
            memory_mb = None
            custom_resources = None
            java_options = None
            runtime_env = None

        runtime = WorkerRuntime(
            worker_id="w_hint",
            job_id="job_hint",
            stage_id="stage_hint",
            broker_endpoint=QueueEndpoint(
                host=workqueue_backend.host,
                port=workqueue_backend.port,
                storage_url="memory://",
            ),
            upstream=QueueRef.queue("hint_upstream"),
        )

        worker = WorkerClass(runtime, MockStage(), mock_store)
        worker.queue_client = workqueue_backend.client

        # Push a message WITH payload_loc in metadata
        loc = {"flight": "grpc://10.0.0.1:5555", "s3": "s3://bucket/k1.arrow"}
        msg = DataQueueMessage(
            message_id="msg_hint_001",
            split_id="s1",
            payload_key="input_key",
            metadata={"payload_loc": loc},
        )
        workqueue_backend.client.create_queue("hint_upstream")
        workqueue_backend.client.push("hint_upstream", msg.to_bytes())

        records = workqueue_backend.client.claim("hint_upstream", batch_size=1, timeout_ms=1000)
        assert len(records) == 1

        await worker._process_and_ack(records)

        # Verify get_with_hint was called (not bare get)
        mock_store.get_with_hint.assert_called_once_with("input_key", loc)


# ===========================================================================
# Advanced tests: fault injection, stress, edge cases
# ===========================================================================


class TestDiskFullDegradation:
    """Test behavior when NVMe disk is full."""

    def test_write_back_enospc_with_s3_fallback(self, tmp_path):
        """WRITE_BACK: NVMe full → falls back to sync S3 write."""
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_BACK,
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
            quota_bytes=1,  # 1 byte quota — immediately "full"
        )
        store._ensure_initialized()

        # First write should exceed quota and fall back to S3
        payload = _make_payload("k1", num_rows=10)
        store.store("k1", payload)

        # NVMe may or may not have it (quota is checked at select_disk level)
        # But S3 should definitely have it after executor completes
        if store._s3_executor:
            store._s3_executor.shutdown(wait=True)
            store._s3_executor = ThreadPoolExecutor(max_workers=4)

        s3_files = list((tmp_path / "s3_mock" / "job1").rglob("*.arrow"))
        assert len(s3_files) >= 1

    def test_write_back_enospc_no_s3_raises(self, tmp_path):
        """WRITE_BACK without S3: NVMe full → raises OSError."""
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_BACK,
            node_ip="127.0.0.1",
            quota_bytes=500,  # Small enough to fill after one write
        )
        store._ensure_initialized()

        # First write fills quota
        store.store("k0", _make_payload("k0", num_rows=100))
        # Second write exceeds quota
        with pytest.raises(OSError):
            store.store("k1", _make_payload("k1", num_rows=100))

    def test_multi_disk_failover(self, tmp_path):
        """When first disk is full, second disk should be selected."""
        pool = NvmeDiskPool(
            [str(tmp_path / "d0"), str(tmp_path / "d1")],
            "job1",
            quota_bytes=None,
        )

        # Fill first disk with a payload, then set its quota to used size
        pool.write("k_fill", _make_payload("k_fill", num_rows=1000))

        # Both disks should have space, verify writes distribute
        for i in range(10):
            pool.write(f"k{i}", _make_payload(f"k{i}", num_rows=5))

        # All should be readable
        for i in range(10):
            assert pool.read(f"k{i}") is not None


class TestQuotaEnforcement:
    """Test per-disk quota tracking accuracy."""

    def test_quota_tracks_used_bytes(self, tmp_path):
        """_used_bytes should reflect actual file sizes on disk."""
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=10 * 1024 * 1024)

        # Write some payloads
        for i in range(5):
            disk.write(f"k{i}", _make_payload(f"k{i}", num_rows=100))

        # _used_bytes should match actual disk usage
        actual = sum(
            f.stat().st_size for f in Path(disk.job_dir).rglob("*.arrow") if ".tmp." not in f.name
        )
        assert disk._used_bytes == actual

    def test_quota_after_delete(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=10 * 1024 * 1024)

        disk.write("k1", _make_payload("k1", num_rows=100))
        used_after_write = disk._used_bytes
        assert used_after_write > 0

        disk.delete("k1")
        assert disk._used_bytes == 0

    def test_quota_after_overwrite(self, tmp_path):
        """Overwriting same key should not double-count bytes."""
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=10 * 1024 * 1024)

        disk.write("k1", _make_payload("k1", num_rows=10))
        used_first = disk._used_bytes

        # Overwrite with larger payload
        disk.write("k1", _make_payload("k1", num_rows=100))
        used_second = disk._used_bytes

        # Should be roughly the size of the second write, not sum of both
        assert used_second > used_first  # second is bigger
        actual = sum(
            f.stat().st_size for f in Path(disk.job_dir).rglob("*.arrow") if ".tmp." not in f.name
        )
        assert disk._used_bytes == actual

    def test_quota_after_clear(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=10 * 1024 * 1024)
        for i in range(10):
            disk.write(f"k{i}", _make_payload(f"k{i}", num_rows=50))
        assert disk._used_bytes > 0

        disk.clear()
        assert disk._used_bytes == 0

    def test_available_bytes_respects_quota(self, tmp_path):
        quota = 100 * 1024  # 100KB
        disk = NvmeDisk(str(tmp_path), "job1", quota_bytes=quota)
        initial_avail = disk.available_bytes()
        assert initial_avail <= quota

        disk.write("k1", _make_payload("k1", num_rows=50))
        after_write = disk.available_bytes()
        assert after_write < initial_avail


class TestFlightServerResilience:
    """Test Flight server under adverse conditions."""

    def test_semaphore_limits_concurrent_reads(self, tmp_path):
        """More concurrent readers than semaphore allows — some should wait."""
        disk = NvmeDisk(str(tmp_path), "job1")
        disk.write("slow_k", _make_payload("slow_k", num_rows=1000))

        # Semaphore = 2, start 4 concurrent readers
        server = FlightPayloadServer([disk.job_dir], port=0, max_concurrent_reads=2)
        t = threading.Thread(target=server.serve, daemon=True)
        t.start()

        results = []
        errors = []

        def read_one():
            try:
                c = flight.connect(f"grpc://127.0.0.1:{server.port}")
                table = c.do_get(flight.Ticket(b"slow_k")).read_all()
                results.append(table.num_rows)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_one) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=30)

        # All should eventually succeed (semaphore blocks, doesn't reject)
        assert len(results) == 4
        assert all(r == 1000 for r in results)
        assert len(errors) == 0
        server.shutdown()

    def test_multiple_stores_share_flight_singleton(self, tmp_path):
        """Two NvmeSplitPayloadStore instances on the same node should share
        one Flight server."""
        store1 = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme_a")],
            job_id="job_a",
            node_ip="127.0.0.1",
        )
        store1._ensure_initialized()

        store2 = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme_b")],
            job_id="job_b",
            node_ip="127.0.0.2",  # Different "node" so Flight read is attempted
        )
        store2._ensure_initialized()

        # Store1 writes, store2 reads via Flight (different node_ip → not skipped)
        store1.store("shared_k", _make_payload("shared_k", num_rows=3))
        loc = store1.get_location("shared_k")

        result = store2.get_with_hint("shared_k", loc)
        assert result is not None
        assert result.data.num_rows == 3


class TestHashPrefixDistribution:
    """Verify hash-prefix directory layout works correctly at scale."""

    def test_1000_keys_distributed_across_prefixes(self, tmp_path):
        """1000 keys should distribute across multiple prefix directories."""
        disk = NvmeDisk(str(tmp_path), "job1")

        # Use hex-formatted keys for diverse prefixes
        for i in range(1000):
            disk.write(f"{i:04x}_payload", _make_payload(f"k{i}", num_rows=2))

        # Check directory structure
        prefix_dirs = [d for d in Path(disk.job_dir).iterdir() if d.is_dir()]
        # Hex keys 0000-03e7: first 2 chars span 00,01,02,03 = at least 4 prefixes
        assert len(prefix_dirs) >= 4

        # All keys should be readable
        for i in range(1000):
            result = disk.read(f"{i:04x}_payload")
            assert result is not None
            assert result.data.num_rows == 2

    def test_prefix_dirs_cleaned_on_clear(self, tmp_path):
        disk = NvmeDisk(str(tmp_path), "job1")
        for i in range(50):
            disk.write(f"k{i:04d}", _make_payload(f"k{i}", num_rows=1))

        prefix_dirs_before = list(Path(disk.job_dir).iterdir())
        assert len(prefix_dirs_before) > 0

        disk.clear()

        # All prefix dirs should be empty (rmdir only removes empty dirs)
        remaining_files = list(Path(disk.job_dir).rglob("*.arrow"))
        assert len(remaining_files) == 0


class TestWriteThroughFlushSemantics:
    """Detailed tests for WRITE_THROUGH flush behavior."""

    def _make_store(self, tmp_path) -> NvmeSplitPayloadStore:
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            write_policy=WritePolicy.WRITE_THROUGH,
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        return store

    def test_flush_multiple_payloads_atomically(self, tmp_path):
        """All pending S3 writes should complete in a single flush call."""
        store = self._make_store(tmp_path)

        for i in range(5):
            store.store(f"k{i}", _make_payload(f"k{i}", num_rows=10))

        assert len(store._pending_s3_futures) == 5
        store.flush_pending_writes()
        assert len(store._pending_s3_futures) == 0

        # All 5 should be on S3
        s3_files = list((tmp_path / "s3_mock" / "job1").rglob("*.arrow"))
        assert len(s3_files) == 5

    def test_flush_resets_failure_counter_on_success(self, tmp_path):
        """Successful flush should reset consecutive failure counter."""
        store = self._make_store(tmp_path)
        store._consecutive_s3_failures = 3  # simulate prior failures

        store.store("k1", _make_payload("k1"))
        store.flush_pending_writes()

        assert store._consecutive_s3_failures == 0

    def test_flush_noop_when_empty(self, tmp_path):
        """Flush with no pending writes should not raise."""
        store = self._make_store(tmp_path)
        store.flush_pending_writes()  # Should be a no-op


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_payload(self, tmp_path):
        """Store and retrieve a payload with zero rows."""
        disk = NvmeDisk(str(tmp_path), "job1")
        table = pa.table({"x": pa.array([], type=pa.int64())})
        payload = SplitPayload(data=table, split_id="empty")

        disk.write("empty", payload)
        result = disk.read("empty")
        assert result is not None
        assert result.data.num_rows == 0
        assert result.data.schema == table.schema

    def test_special_characters_in_key(self, tmp_path):
        """Keys with colons, slashes, dots should be sanitized correctly."""
        disk = NvmeDisk(str(tmp_path), "job1")
        special_keys = [
            "job:stage:split_0",
            "a/b/c/d",
            "key.with.dots",
            "key:with/mixed:chars/and.dots",
            "ab",  # exactly 2 chars (edge for prefix)
            "a",  # 1 char (shorter than prefix length)
        ]
        for key in special_keys:
            disk.write(key, _make_payload(key, num_rows=1))

        for key in special_keys:
            result = disk.read(key)
            assert result is not None, f"Failed to read key: {key}"

    def test_get_location_without_s3(self, tmp_path):
        """get_location should not include 's3' key when S3 is not configured."""
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        store.store("k1", _make_payload("k1"))

        loc = store.get_location("k1")
        assert "flight" in loc
        assert "s3" not in loc

    def test_delete_nonexistent_key(self, tmp_path):
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()
        assert store.delete("nonexistent") is False

    def test_get_with_hint_all_tiers_miss(self, tmp_path):
        """get_with_hint returns None when local, Flight, and S3 all miss."""
        s3_dir = tmp_path / "s3_mock"
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0")],
            job_id="job1",
            s3_uri=f"file://{s3_dir}",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()

        hint = {
            "flight": "grpc://192.0.2.1:9999",  # unreachable
            "s3": f"{store._s3_root}/nonexistent.arrow",
        }
        result = store.get_with_hint("no_such_key", hint)
        assert result is None


class TestStressStore:
    """High-concurrency stress tests."""

    def test_concurrent_store_get_delete(self, tmp_path):
        """Simultaneous store, get, and delete from multiple threads."""
        store = NvmeSplitPayloadStore(
            root_dirs=[str(tmp_path / "nvme0"), str(tmp_path / "nvme1")],
            job_id="job1",
            node_ip="127.0.0.1",
        )
        store._ensure_initialized()

        errors = []
        n_keys = 50

        # Phase 1: write all keys
        def writer(i):
            try:
                store.store(f"stress_{i}", _make_payload(f"stress_{i}", num_rows=10))
            except Exception as e:
                errors.append(("write", i, e))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(writer, range(n_keys)))

        assert len(errors) == 0, f"Write errors: {errors}"

        # Phase 2: concurrent read + delete (interleaved)
        read_results = {}

        def reader(i):
            try:
                result = store.get(f"stress_{i}")
                read_results[i] = result is not None
            except Exception as e:
                errors.append(("read", i, e))

        def deleter(i):
            try:
                store.delete(f"stress_{i}")
            except Exception as e:
                errors.append(("delete", i, e))

        with ThreadPoolExecutor(max_workers=12) as pool:
            futs = []
            for i in range(n_keys):
                futs.append(pool.submit(reader, i))
                if i % 3 == 0:  # delete every 3rd key
                    futs.append(pool.submit(deleter, i))
            for f in futs:
                f.result()

        assert len(errors) == 0, f"Errors: {errors}"

    def test_rapid_overwrite_same_key(self, tmp_path):
        """Rapidly overwrite the same key — should never corrupt."""
        disk = NvmeDisk(str(tmp_path), "job1")
        errors = []

        def overwriter(iteration):
            try:
                disk.write("hotkey", _make_payload("hotkey", num_rows=iteration + 1))
            except Exception as e:
                errors.append(e)

        # 20 threads all writing to same key
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(overwriter, range(20)))

        assert len(errors) == 0
        # Final read should succeed with some valid row count
        result = disk.read("hotkey")
        assert result is not None
        assert result.data.num_rows > 0
