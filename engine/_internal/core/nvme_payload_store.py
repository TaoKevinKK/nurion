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

"""NVMe-tiered PayloadStore with Arrow Flight cross-node data plane.

Write: Arrow IPC -> local NVMe (sync) + S3 (configurable: sync or async)
Read:  local NVMe (mmap) -> remote NVMe (Arrow Flight) -> S3 (fallback)

Two write policies:
  WRITE_THROUGH: S3 pipelined, NVMe cache. Data durable before ack.
  WRITE_BACK:    NVMe first, S3 async (if configured). Default.

Location info is embedded in queue message metadata — no centralized registry.

Usage::

    # Single NVMe disk
    store = NvmeSplitPayloadStore(
        root_dirs=["/mnt/nvme0/nurion"],
        job_id="job_123",
    )

    # Multi-disk with S3 durability
    store = NvmeSplitPayloadStore(
        root_dirs=["/mnt/nvme0/nurion", "/mnt/nvme1/nurion"],
        job_id="job_123",
        write_policy=WritePolicy.WRITE_THROUGH,
        s3_uri="s3://bucket/shuffle",
    )
"""

from __future__ import annotations

import errno
import logging
import os
import select
import shutil
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Tuple
from urllib.parse import parse_qs

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc

from _internal.core.models import SplitPayload
from _internal.core.split_payload_store import SplitPayloadStore, _sanitize_key

logger = logging.getLogger(__name__)


# =============================================================================
# Write Policy
# =============================================================================


class WritePolicy(str, Enum):
    """Payload write durability policy.

    WRITE_THROUGH: S3 write is pipelined with compute and confirmed before ack.
        Use when recompute is expensive (LLM inference, external API calls).
    WRITE_BACK:    NVMe is written synchronously; S3 upload is async/best-effort.
        Use when recompute is cheap (resize, filter, format conversion).
        Without ``s3_fallback`` in the URI this becomes NVMe-only.
    """

    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


# =============================================================================
# URI Parsing
# =============================================================================


def parse_nvme_uri(uri: str) -> Tuple[List[str], Dict[str, str]]:
    """Parse an ``nvme://`` URI into root directories and parameters.

    Format::

        nvme:///mnt/nvme0/nurion,/mnt/nvme1/nurion?s3_fallback=s3://bucket/pfx&quota_gb=500

    Returns:
        ``(root_dirs, params)`` where *root_dirs* is a list of local paths and
        *params* is a flat dict of query-string parameters.
    """
    # Strip scheme
    rest = uri
    if rest.startswith("nvme://"):
        rest = rest[len("nvme://") :]

    # Split path from query string
    if "?" in rest:
        path_part, query_part = rest.split("?", 1)
    else:
        path_part, query_part = rest, ""

    root_dirs = [p.strip() for p in path_part.split(",") if p.strip()]
    if not root_dirs:
        raise ValueError(f"nvme:// URI must contain at least one path: {uri}")

    params: Dict[str, str] = {}
    if query_part:
        for k, v_list in parse_qs(query_part).items():
            params[k] = v_list[0]

    return root_dirs, params


# =============================================================================
# NvmeDisk — single disk management
# =============================================================================


class NvmeDisk:
    """Manages a single NVMe disk mount point for one job.

    Directory layout (hash-prefix bucketing)::

        {root_dir}/                <- Nurion root on this disk (user-specified)
            {job_id}/              <- Per-job isolation
                a1/                <- 2-char hex prefix of sanitized key
                    a1b2c3.arrow
                f0/
                    f0e1d2.arrow

    The 2-char prefix keeps each subdirectory under ~N/256 files, avoiding
    filesystem performance degradation at high file counts (ext4/xfs readdir
    is O(N) per directory).

    Space tracking uses an in-memory counter (``_used_bytes``) instead of
    scanning the directory, keeping ``available_bytes()`` O(1).
    """

    def __init__(
        self,
        root_dir: str,
        job_id: str,
        quota_bytes: Optional[int] = None,
    ):
        self._root_dir = root_dir
        self._job_dir = os.path.join(root_dir, job_id)
        self._quota_bytes = quota_bytes
        self._used_bytes = 0
        os.makedirs(self._job_dir, exist_ok=True)
        self._cleanup_tmp_files()
        self._rebuild_used_bytes()

    @property
    def job_dir(self) -> str:
        return self._job_dir

    # -- Path helpers --------------------------------------------------------

    def _key_to_path(self, key: str, suffix: str = ".arrow") -> str:
        """Map key to hash-prefixed file path: ``{job_dir}/{prefix}/{safe}{suffix}``."""
        safe = _sanitize_key(key)
        prefix = safe[:2] if len(safe) >= 2 else "00"
        return os.path.join(self._job_dir, prefix, f"{safe}{suffix}")

    def _ensure_prefix_dir(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # -- I/O -----------------------------------------------------------------

    def write(self, key: str, payload: SplitPayload) -> str:
        """Atomic write: Arrow IPC to tmp file, then rename.

        Returns the final file path.
        """
        final_path = self._key_to_path(key)
        self._ensure_prefix_dir(final_path)
        # Subtract old size if overwriting an existing file
        try:
            old_size = os.path.getsize(final_path)
        except FileNotFoundError:
            old_size = 0
        # Include thread ID to avoid collisions between concurrent writes in same process
        tid = threading.get_ident()
        tmp_path = final_path + f".tmp.{os.getpid()}_{tid}"
        with open(tmp_path, "wb") as f:
            writer = ipc.new_file(f, payload.data.schema)
            writer.write_table(payload.data)
            writer.close()
        os.rename(tmp_path, final_path)
        new_size = os.path.getsize(final_path)
        self._used_bytes += new_size - old_size
        return final_path

    def read(self, key: str) -> Optional[SplitPayload]:
        """Read via memory-map (zero-copy when page-cache is hot)."""
        path = self._key_to_path(key)
        if not os.path.exists(path):
            return None
        try:
            source = pa.memory_map(path, "r")
            reader = ipc.open_file(source)
            table = reader.read_all()
            return SplitPayload.from_arrow(table, split_id=key)
        except (FileNotFoundError, OSError):
            # Race: file deleted between exists() and open()
            return None

    def delete(self, key: str) -> bool:
        path = self._key_to_path(key)
        try:
            size = os.path.getsize(path)
            os.unlink(path)
            self._used_bytes = max(0, self._used_bytes - size)
            return True
        except FileNotFoundError:
            return False

    # -- Space ---------------------------------------------------------------

    def available_bytes(self) -> int:
        """O(1): uses in-memory counter, no directory scan."""
        _, _, fs_free = shutil.disk_usage(self._root_dir)
        if self._quota_bytes is None:
            return fs_free
        quota_free = max(0, self._quota_bytes - self._used_bytes)
        return min(fs_free, quota_free)

    def _rebuild_used_bytes(self) -> None:
        """Scan once on init to calibrate the in-memory counter."""
        total = 0
        for f in Path(self._job_dir).rglob("*.arrow"):
            if ".tmp." not in f.name:
                total += f.stat().st_size
        self._used_bytes = total

    # -- Cleanup -------------------------------------------------------------

    def _cleanup_tmp_files(self) -> None:
        """Remove stale .tmp files left by crashed workers.

        Only deletes tmp files whose PID indicates a dead process,
        to avoid deleting tmp files being actively written by concurrent workers.
        Tmp filename format: ``key.arrow.tmp.{pid}_{tid}``
        """
        for tmp in Path(self._job_dir).rglob("*.arrow.tmp.*"):
            try:
                # Extract PID from filename: "key.arrow.tmp.{pid}_{tid}"
                suffix = tmp.name.rsplit(".tmp.", 1)[-1]
                pid_str = suffix.split("_")[0]
                pid = int(pid_str)
                # Only delete if the process is no longer alive
                try:
                    os.kill(pid, 0)  # signal 0 = check existence, no actual signal
                except OSError:
                    tmp.unlink(missing_ok=True)  # Process dead → safe to delete
            except (ValueError, OSError):
                pass

    def clear(self) -> int:
        """Delete all payload files for this job. Returns count deleted."""
        count = 0
        if os.path.exists(self._job_dir):
            for f in Path(self._job_dir).rglob("*.arrow"):
                f.unlink(missing_ok=True)
                count += 1
            # Clean up empty prefix dirs
            for d in Path(self._job_dir).iterdir():
                if d.is_dir():
                    try:
                        d.rmdir()  # Only removes if empty
                    except OSError:
                        pass
        self._used_bytes = 0
        return count


# =============================================================================
# NvmeDiskPool — multi-disk management
# =============================================================================


class NvmeDiskPool:
    """Manages multiple NVMe disks. Writes to the disk with most free space."""

    def __init__(
        self,
        root_dirs: List[str],
        job_id: str,
        quota_bytes: Optional[int] = None,
    ):
        self._disks = [NvmeDisk(d, job_id, quota_bytes) for d in root_dirs]
        self._key_to_disk: Dict[str, int] = {}

    def write(self, key: str, payload: SplitPayload) -> str:
        """Select disk with most space, write payload. Returns file path."""
        disk = self._select_disk()
        path = disk.write(key, payload)
        self._key_to_disk[key] = self._disks.index(disk)
        return path

    def read(self, key: str) -> Optional[SplitPayload]:
        """Check tracked disk first, then scan all disks."""
        idx = self._key_to_disk.get(key)
        if idx is not None:
            return self._disks[idx].read(key)
        for i, disk in enumerate(self._disks):
            result = disk.read(key)
            if result is not None:
                self._key_to_disk[key] = i
                return result
        return None

    def delete(self, key: str) -> bool:
        idx = self._key_to_disk.pop(key, None)
        if idx is not None:
            return self._disks[idx].delete(key)
        return any(d.delete(key) for d in self._disks)

    def clear(self) -> int:
        self._key_to_disk.clear()
        return sum(d.clear() for d in self._disks)

    @property
    def all_job_dirs(self) -> List[str]:
        return [d.job_dir for d in self._disks]

    def _select_disk(self) -> NvmeDisk:
        best = max(self._disks, key=lambda d: d.available_bytes())
        if best.available_bytes() <= 0:
            raise OSError(errno.ENOSPC, "All NVMe disks full or over quota")
        return best


# =============================================================================
# Arrow Flight Server
# =============================================================================


class FlightPayloadServer(flight.FlightServerBase):
    """Per-process Arrow Flight server serving local NVMe Arrow IPC files.

    Runs as a daemon thread. Concurrent reads are limited by a semaphore to
    prevent OOM when many remote consumers request large payloads at once.
    """

    _instances: ClassVar[Dict[int, "FlightPayloadServer"]] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        job_dirs: List[str],
        port: int = 0,
        max_concurrent_reads: int = 8,
    ):
        location = flight.Location.for_grpc_tcp("0.0.0.0", port)
        super().__init__(location)
        self._job_dirs = list(job_dirs)
        self._semaphore = threading.Semaphore(max_concurrent_reads)
        self._thread: Optional[threading.Thread] = None

    def do_get(self, context: flight.ServerCallContext, ticket: flight.Ticket):
        key = ticket.ticket.decode()
        safe = _sanitize_key(key)
        prefix = safe[:2] if len(safe) >= 2 else "00"

        acquired = self._semaphore.acquire(timeout=30)
        if not acquired:
            raise flight.FlightUnavailableError("Server overloaded, retry later")
        try:
            for job_dir in self._job_dirs:
                path = os.path.join(job_dir, prefix, f"{safe}.arrow")
                if os.path.exists(path):
                    source = pa.memory_map(path, "r")
                    reader = ipc.open_file(source)
                    table = reader.read_all()
                    return flight.RecordBatchStream(table)
            raise flight.FlightUnavailableError(f"Payload not found: {key}")
        finally:
            self._semaphore.release()

    # -- Lifecycle -----------------------------------------------------------

    @classmethod
    def get_or_start(
        cls,
        job_dirs: List[str],
        port: int = 0,
        max_concurrent_reads: int = 8,
    ) -> "FlightPayloadServer":
        """Return the singleton server for *port*, starting it if needed."""
        with cls._lock:
            existing = cls._instances.get(port)
            if (
                existing is not None
                and existing._thread is not None
                and existing._thread.is_alive()
            ):
                # Add any new job dirs
                for d in job_dirs:
                    if d not in existing._job_dirs:
                        existing._job_dirs.append(d)
                return existing

            server = cls(job_dirs, port, max_concurrent_reads)
            thread = threading.Thread(target=server.serve, daemon=True, name="flight-server")
            thread.start()
            server._thread = thread

            # Read actual port after server starts (if port=0, OS assigns)
            actual_port = server.port
            cls._instances[actual_port] = server
            # Also store under requested port so subsequent get_or_start(0) finds it
            if port != actual_port:
                cls._instances[port] = server
            logger.info(f"Flight server started on port {actual_port}")
            return server


# =============================================================================
# Flight Server via Ray Actor (gRPC isolation + lifecycle management)
# =============================================================================

# Well-known port for the per-node Flight payload server.
FLIGHT_SERVER_PORT = 18815


import ray  # noqa: E402 — deferred import, only used by _FlightServerActor below


@ray.remote(num_cpus=0)
class _FlightServerActor:
    """Ray actor that owns a Flight server subprocess.

    Runs one per node.  The actor's lifecycle is managed by Ray (survives
    worker actor exits).  The subprocess provides gRPC isolation from Ray.
    """

    def __init__(self, root_dir: str, port: int):
        self._port = port
        self._root_dir = root_dir
        self._proc: Optional[subprocess.Popen] = None
        self._start_server()

    def _start_server(self) -> None:
        # Run _flight_server_proc.py as a script (not -m module) because
        # inside Ray workers the code lives in a temporary working_dir that
        # isn't on the subprocess's PYTHONPATH.  The script only imports
        # pyarrow (no _internal), so running it directly works everywhere.
        script = os.path.join(os.path.dirname(__file__), "_flight_server_proc.py")

        self._proc = subprocess.Popen(
            [
                sys.executable,
                script,
                "--root-dir",
                self._root_dir,
                "--port",
                str(self._port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
        )

        assert self._proc.stdout is not None
        ready, _, _ = select.select([self._proc.stdout], [], [], 10.0)
        if not ready:
            stderr_tail = ""
            if self._proc.stderr:
                stderr_tail = self._proc.stderr.read(2048).decode(errors="replace")
            self._proc.kill()
            self._proc.wait()
            raise RuntimeError(f"Flight server subprocess timed out. stderr: {stderr_tail}")

        line = self._proc.stdout.readline().decode().strip()
        self._proc.stdout.close()
        if self._proc.stderr:
            self._proc.stderr.close()

        if line.startswith("FLIGHT_READY:"):
            self._port = int(line.split(":", 1)[1])
        elif line.startswith("FLIGHT_PORT_IN_USE:"):
            self._proc.wait()
            self._proc = None  # Already running from another actor
        else:
            raise RuntimeError(f"Flight server failed: {line}")

    def get_port(self) -> int:
        return self._port

    def is_alive(self) -> bool:
        if self._proc is None:
            return True  # Port in use = someone else owns it
        return self._proc.poll() is None


class FlightServerProcess:
    """Per-node Arrow Flight server managed by a Ray actor.

    The Ray actor owns the Flight subprocess, so it survives worker exits
    (Ray actor lifecycle is independent of stage worker actors).

    Singleton per node enforced by fixed port + Ray named actor.
    """

    _lock: ClassVar[threading.Lock] = threading.Lock()
    _cache: ClassVar[Dict[str, "FlightServerProcess"]] = {}

    def __init__(self, port: int):
        self.port = port

    @classmethod
    def get_or_start(
        cls,
        job_dirs: List[str],
        port: int = FLIGHT_SERVER_PORT,
        max_concurrent_reads: int = 8,
    ) -> "FlightServerProcess":
        """Return per-node Flight server, starting a Ray actor if needed."""
        from _internal.utils.network import get_node_ip

        node_ip = get_node_ip()
        actor_name = f"flight_server_{node_ip}"
        root = os.path.dirname(job_dirs[0]) if job_dirs else "/tmp"

        with cls._lock:
            # Check cache first (same process, already resolved)
            if actor_name in cls._cache:
                return cls._cache[actor_name]

            # Try to connect to existing named actor
            try:
                actor = ray.get_actor(actor_name)
                actual_port = ray.get(actor.get_port.remote(), timeout=5)
                instance = cls(actual_port)
                cls._cache[actor_name] = instance
                logger.info(f"Reusing existing Flight server actor on {node_ip}:{actual_port}")
                return instance
            except (ValueError, ray.exceptions.GetTimeoutError):
                pass  # Actor doesn't exist or unresponsive

            # Schedule actor on THIS node
            current_node = ray.get_runtime_context().get_node_id()
            actor = _FlightServerActor.options(  # type: ignore[attr-defined]
                name=actor_name,
                lifetime="detached",
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=current_node,
                    soft=False,
                ),
            ).remote(root, port)

            actual_port = ray.get(actor.get_port.remote(), timeout=15)
            instance = cls(actual_port)
            cls._cache[actor_name] = instance
            logger.info(f"Started Flight server actor on {node_ip}:{actual_port}")
            return instance


# =============================================================================
# NvmeSplitPayloadStore
# =============================================================================


class NvmeSplitPayloadStore(SplitPayloadStore):
    """NVMe + S3 two-tier storage with Arrow Flight cross-node reads.

    Write policies:
      WRITE_THROUGH — S3 pipelined, confirmed before ack. NVMe is cache.
      WRITE_BACK    — NVMe first, S3 async if configured. Default.

    Location info is embedded in queue message metadata (no registry needed).
    """

    # Shared Flight servers (per-process, survives pickle round-trip)
    _flight_server_port: ClassVar[Optional[int]] = None

    # Auto-degrade threshold: switch from WRITE_THROUGH to WRITE_BACK
    S3_FAILURE_THRESHOLD = 5

    def __init__(
        self,
        root_dirs: List[str],
        job_id: str,
        write_policy: WritePolicy = WritePolicy.WRITE_BACK,
        s3_uri: Optional[str] = None,
        s3_options: Optional[Dict[str, Any]] = None,
        flight_port: int = 0,
        quota_bytes: Optional[int] = None,
        node_ip: Optional[str] = None,
    ):
        if write_policy == WritePolicy.WRITE_THROUGH and not s3_uri:
            raise ValueError(
                "WRITE_THROUGH requires s3_fallback in URI "
                "(S3 must be configured for durable writes)"
            )

        self._root_dirs = root_dirs
        self._job_id = job_id
        self._write_policy = write_policy
        self._s3_uri = s3_uri
        self._s3_options = s3_options or {}
        self._flight_port_config = flight_port
        self._quota_bytes = quota_bytes
        self._node_ip_override = node_ip

        # Lazily initialized in _ensure_initialized() (after pickle to worker)
        self._initialized = False
        self._disk_pool: Optional[NvmeDiskPool] = None
        self._s3_fs: Optional[Any] = None
        self._s3_root: Optional[str] = None
        self._s3_executor: Optional[ThreadPoolExecutor] = None
        self._pending_s3_futures: Dict[str, Future] = {}
        self._flight_clients: Dict[str, flight.FlightClient] = {}
        self._flight_endpoint: Optional[str] = None
        self._consecutive_s3_failures = 0

        # Metrics
        self._metrics = {
            "stored": 0,
            "local_hits": 0,
            "remote_hits": 0,
            "s3_hits": 0,
            "s3_writes": 0,
        }

    # -- Pickle support (Ray serialization) ----------------------------------

    def __getstate__(self) -> dict:
        """Exclude non-picklable runtime objects."""
        return {
            "root_dirs": self._root_dirs,
            "job_id": self._job_id,
            "write_policy": self._write_policy,
            "s3_uri": self._s3_uri,
            "s3_options": self._s3_options,
            "flight_port_config": self._flight_port_config,
            "quota_bytes": self._quota_bytes,
            "node_ip_override": self._node_ip_override,
        }

    def __setstate__(self, state: dict) -> None:
        """Reconstruct from pickled state — lazy init on first use."""
        NvmeSplitPayloadStore.__init__(
            self,
            root_dirs=state["root_dirs"],
            job_id=state["job_id"],
            write_policy=state["write_policy"],
            s3_uri=state["s3_uri"],
            s3_options=state["s3_options"],
            flight_port=state["flight_port_config"],
            quota_bytes=state["quota_bytes"],
            node_ip=state.get("node_ip_override"),
        )

    # -- Helpers -------------------------------------------------------------

    def _resolve_node_ip(self) -> str:
        if self._node_ip_override:
            return self._node_ip_override
        from _internal.utils.network import get_node_ip

        return get_node_ip()

    # -- Lazy initialization -------------------------------------------------

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        # Disk pool
        self._disk_pool = NvmeDiskPool(self._root_dirs, self._job_id, self._quota_bytes)

        # S3 tier
        if self._s3_uri:
            import fsspec.core

            full_path = f"{self._s3_uri.rstrip('/')}/{self._job_id}"
            self._s3_fs, self._s3_root = fsspec.core.url_to_fs(full_path, **self._s3_options)
            self._s3_fs.mkdirs(self._s3_root, exist_ok=True)
            self._s3_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="s3-upload")

        # Flight server — subprocess for gRPC isolation from Ray.
        # Arrow Flight's gRPC shares C++ global state with Ray's internal gRPC;
        # running both in one process causes the Flight server thread to silently
        # die.  Subprocess isolation eliminates this conflict.
        server = FlightServerProcess.get_or_start(
            self._disk_pool.all_job_dirs, self._flight_port_config
        )
        node_ip = self._resolve_node_ip()
        self._flight_endpoint = f"grpc://{node_ip}:{server.port}"
        NvmeSplitPayloadStore._flight_server_port = server.port

        self._initialized = True
        logger.info(
            f"NvmeSplitPayloadStore initialized: "
            f"disks={self._root_dirs} policy={self._write_policy.value} "
            f"s3={self._s3_uri} flight={self._flight_endpoint}"
        )

    # -- SplitPayloadStore interface -----------------------------------------

    def store(self, key: str, payload: SplitPayload) -> str:
        self._ensure_initialized()
        assert self._disk_pool is not None

        if self._write_policy == WritePolicy.WRITE_THROUGH:
            return self._store_write_through(key, payload)
        else:
            return self._store_write_back(key, payload)

    def get(self, key: str) -> Optional[SplitPayload]:
        self._ensure_initialized()
        assert self._disk_pool is not None

        # Local NVMe
        result = self._disk_pool.read(key)
        if result is not None:
            self._metrics["local_hits"] += 1
            return result

        # S3 fallback
        if self._s3_fs:
            result = self._read_s3(key)
            if result is not None:
                self._metrics["s3_hits"] += 1
                return result

        return None

    def get_with_hint(
        self, key: str, location_hint: Optional[Dict[str, Any]] = None
    ) -> Optional[SplitPayload]:
        self._ensure_initialized()
        assert self._disk_pool is not None

        # Tier 1: Local NVMe
        result = self._disk_pool.read(key)
        if result is not None:
            self._metrics["local_hits"] += 1
            return result

        if not location_hint:
            # Local NVMe already missed above — try S3 directly
            if self._s3_fs:
                result = self._read_s3(key)
                if result is not None:
                    self._metrics["s3_hits"] += 1
                    return result
            return None

        # Tier 2: Remote NVMe via Arrow Flight
        endpoint = location_hint.get("flight")
        if endpoint and endpoint != self._flight_endpoint:
            try:
                table = self._flight_get(endpoint, key)
                if table is not None:
                    self._metrics["remote_hits"] += 1
                    return SplitPayload.from_arrow(table, split_id=key)
            except Exception as e:
                logger.debug(f"Flight get failed for {key} from {endpoint}: {e}")

        # Tier 3: S3
        s3_path = location_hint.get("s3")
        if s3_path:
            result = self._read_s3_path(s3_path, key)
            if result is not None:
                self._metrics["s3_hits"] += 1
                return result

        return None

    def get_location(self, key: str) -> Optional[Dict[str, Any]]:
        self._ensure_initialized()
        loc: Dict[str, Any] = {"flight": self._flight_endpoint}
        if self._s3_root:
            safe = _sanitize_key(key)
            loc["s3"] = f"{self._s3_root}/{safe}.arrow"
        return loc

    def flush_pending_writes(self) -> None:
        if not self._pending_s3_futures:
            return

        errors: list = []
        for key, future in self._pending_s3_futures.items():
            try:
                future.result(timeout=120)
            except Exception as e:
                errors.append((key, e))

        self._pending_s3_futures.clear()

        if errors:
            self._consecutive_s3_failures += len(errors)
            if self._consecutive_s3_failures >= self.S3_FAILURE_THRESHOLD:
                logger.warning(
                    f"S3 failed {self._consecutive_s3_failures} times consecutively, "
                    f"auto-degrading from WRITE_THROUGH to WRITE_BACK"
                )
                self._write_policy = WritePolicy.WRITE_BACK
            raise IOError(f"S3 write failed for {len(errors)} payloads: {errors[0][1]}")
        else:
            self._consecutive_s3_failures = 0

    def delete(self, key: str) -> bool:
        self._ensure_initialized()
        assert self._disk_pool is not None
        return self._disk_pool.delete(key)

    def clear(self) -> int:
        self._ensure_initialized()
        assert self._disk_pool is not None

        count = self._disk_pool.clear()

        # Best-effort S3 cleanup
        if self._s3_fs and self._s3_root:
            try:
                self._s3_fs.rm(self._s3_root, recursive=True)
            except Exception as e:
                logger.warning(f"S3 cleanup failed: {e}")

        return count

    def get_metrics(self) -> dict:
        return dict(self._metrics)

    # -- Write policies ------------------------------------------------------

    def _store_write_through(self, key: str, payload: SplitPayload) -> str:
        """S3 first (pipelined), then NVMe cache."""
        assert self._disk_pool is not None

        # Start S3 upload (awaited in flush_pending_writes)
        if self._s3_executor:
            future = self._s3_executor.submit(self._write_s3, key, payload)
            self._pending_s3_futures[key] = future

        # NVMe cache (best-effort — S3 has the data)
        try:
            self._disk_pool.write(key, payload)
        except OSError:
            pass

        self._metrics["stored"] += 1
        return key

    def _store_write_back(self, key: str, payload: SplitPayload) -> str:
        """NVMe first. S3 async in background if configured."""
        assert self._disk_pool is not None

        try:
            self._disk_pool.write(key, payload)
        except OSError as e:
            if e.errno == errno.ENOSPC and self._s3_executor:
                # NVMe full — degrade to sync S3 write
                self._write_s3(key, payload)
                self._metrics["stored"] += 1
                return key
            raise

        # Async S3 upload (fire-and-forget)
        if self._s3_executor:
            self._s3_executor.submit(self._write_s3, key, payload)

        self._metrics["stored"] += 1
        return key

    # -- S3 I/O --------------------------------------------------------------

    def _write_s3(self, key: str, payload: SplitPayload) -> None:
        """Write Arrow IPC to S3 (called from executor thread)."""
        assert self._s3_fs is not None and self._s3_root is not None
        safe = _sanitize_key(key)
        s3_path = f"{self._s3_root}/{safe}.arrow"
        with self._s3_fs.open(s3_path, "wb") as f:
            writer = ipc.new_file(f, payload.data.schema)
            writer.write_table(payload.data)
            writer.close()
        self._metrics["s3_writes"] += 1

    def _read_s3(self, key: str) -> Optional[SplitPayload]:
        """Read from S3 using key."""
        safe = _sanitize_key(key)
        s3_path = f"{self._s3_root}/{safe}.arrow"
        return self._read_s3_path(s3_path, key)

    def _read_s3_path(self, s3_path: str, key: str) -> Optional[SplitPayload]:
        """Read from S3 using explicit path."""
        if self._s3_fs is None:
            return None
        try:
            with self._s3_fs.open(s3_path, "rb") as f:
                reader = ipc.open_file(f)
                table = reader.read_all()
            return SplitPayload.from_arrow(table, split_id=key)
        except FileNotFoundError:
            return None

    # -- Flight client -------------------------------------------------------

    # Timeout for Arrow Flight reads (connect + do_get).  Fail fast — don't
    # block workers for minutes on unreachable Flight servers.
    FLIGHT_TIMEOUT_S = 10

    def _flight_get(self, endpoint: str, key: str) -> Optional[pa.Table]:
        """Fetch from remote node via Arrow Flight (with timeout)."""
        client = self._get_or_create_client(endpoint)
        try:
            opts = flight.FlightCallOptions(timeout=self.FLIGHT_TIMEOUT_S)
            reader = client.do_get(flight.Ticket(key.encode()), opts)
            return reader.read_all()
        except Exception:
            # Any Flight error (unavailable, timeout, internal) — drop cached connection
            self._flight_clients.pop(endpoint, None)
            return None

    def _get_or_create_client(self, endpoint: str) -> flight.FlightClient:
        if endpoint not in self._flight_clients:
            self._flight_clients[endpoint] = flight.connect(
                endpoint,
                generic_options=[("grpc.keepalive_timeout_ms", 5000)],
            )
        return self._flight_clients[endpoint]
