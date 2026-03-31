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

"""SplitPayloadStore - Abstract interface for storing SplitPayload data.

This module provides a flexible storage abstraction for SplitPayload objects.
Different implementations can use various backends:
- Ray Object Store (default, for distributed in-memory storage)
- Fsspec-compatible filesystems (S3, GCS, local/shared filesystems)

Usage:
    # Create a Ray-backed store
    store = RaySplitPayloadStore(name="my_store")

    # Create an fsspec-backed store (S3, shared filesystem, local disk, etc.)
    store = FsspecSplitPayloadStore(base_uri="s3://bucket/prefix", job_id="my_job")
    store = FsspecSplitPayloadStore(base_uri="file:///mnt/shared", job_id="my_job")

    # Store payload (synchronous API - same across all implementations)
    store.store("key1", payload)

    # Retrieve payload
    payload = store.get("key1")

    # Delete when done
    store.delete("key1")

    # Clear all
    store.clear()
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import ray

from _internal.core.models import SplitPayload
from _internal.utils.logging import create_ray_logger

logger = logging.getLogger(__name__)


class SplitPayloadStore(ABC):
    """Abstract base class for SplitPayload storage backends.

    All implementations provide a synchronous interface for simplicity.
    The underlying implementation may use async/actors internally.
    """

    @abstractmethod
    def store(self, key: str, payload: SplitPayload) -> str:
        """Store a SplitPayload with the given key.

        Args:
            key: Unique identifier for this payload
            payload: The SplitPayload to store

        Returns:
            The key (for confirmation/chaining)
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[SplitPayload]:
        """Retrieve a SplitPayload by key.

        Args:
            key: The key used when storing

        Returns:
            The SplitPayload, or None if not found
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a stored payload.

        Args:
            key: The key to delete

        Returns:
            True if deleted, False if key not found
        """
        pass

    @abstractmethod
    def clear(self) -> int:
        """Clear all stored payloads.

        Returns:
            Number of payloads cleared
        """
        pass

    # -- Optional methods with default implementations (backward-compatible) --

    def get_with_hint(
        self, key: str, location_hint: Optional[Dict[str, Any]] = None
    ) -> Optional[SplitPayload]:
        """Retrieve payload using an optional location hint for faster access.

        Location-aware stores (e.g., NVMe) use the hint to try a remote Flight
        endpoint or S3 path before falling back to a full lookup. The hint is
        typically embedded in ``DataQueueMessage.metadata["payload_loc"]`` by
        the producing worker.

        Default implementation ignores the hint and delegates to :meth:`get`.
        """
        return self.get(key)

    def get_location(self, key: str) -> Optional[Dict[str, Any]]:
        """Return location metadata for a stored payload.

        The returned dict (e.g., ``{"flight": "grpc://...", "s3": "s3://..."}``)
        is embedded in the downstream queue message so consumers can read
        directly without a registry lookup.

        Default implementation returns ``None`` (no location tracking).
        """
        return None

    def flush_pending_writes(self) -> None:
        """Block until all pending async writes are durable.

        Called by ``StageWorker`` before ``ack_and_scatter`` to ensure that
        WRITE_THROUGH payloads have been confirmed by S3 before the upstream
        messages are acknowledged.

        Default implementation is a no-op.
        """
        pass


# =============================================================================
# Ray Object Store Implementation
# =============================================================================


@ray.remote
class _RaySplitPayloadStoreActor:
    """Internal Ray actor that manages ObjectRef mappings.

    This actor stores key -> ObjectRef mappings. The actual objects are put
    by callers with _owner=actor to prevent GC when original workers exit.
    """

    def __init__(self):
        self._refs: dict[str, ray.ObjectRef] = {}
        self._logger = create_ray_logger("RaySplitPayloadStoreActor")

        # Metrics tracking
        self._total_stored = 0
        self._total_deleted = 0
        self._estimated_bytes = 0

    def ping(self) -> bool:
        """Health check - returns True when actor is ready."""
        return True

    def register(self, key: str, ref_wrapper: dict) -> str:
        """Register an ObjectRef (wrapped in dict to prevent auto-deref) with a key."""
        self._refs[key] = ref_wrapper["ref"]
        self._total_stored += 1
        self._logger.debug(f"Registered payload for key {key}")
        return key

    def get_ref(self, key: str) -> Optional[dict]:
        """Get the ObjectRef (wrapped in dict) for a key."""
        ref = self._refs.get(key)
        if ref is None:
            return None
        return {"ref": ref}

    def delete(self, key: str) -> bool:
        if key in self._refs:
            del self._refs[key]
            self._total_deleted += 1
            return True
        return False

    def clear(self) -> int:
        count = len(self._refs)
        self._refs.clear()
        self._logger.info(f"Cleared {count} payloads")
        return count

    def get_metrics(self) -> dict:
        """Get storage metrics.

        Returns:
            Dictionary with storage statistics
        """
        return {
            "total_objects": len(self._refs),
            "total_stored": self._total_stored,
            "total_deleted": self._total_deleted,
            "estimated_bytes": self._estimated_bytes,
        }


class RaySplitPayloadStore(SplitPayloadStore):
    """Ray Object Store backed implementation of SplitPayloadStore.

    This class wraps an internal Ray actor that stores SplitPayload objects
    in Ray's distributed object store.

    The interface is synchronous - all Ray actor calls are wrapped with ray.get()
    to provide a consistent API across different storage backends.

    Usage:
        store = RaySplitPayloadStore(name="my_store")
        store.wait_ready()  # Ensure actor is initialized before use

        store.store("key", payload)
        payload = store.get("key")
        store.delete("key")
        store.clear()
    """

    def __init__(self, name: str):
        """Initialize the store.

        Args:
            name: Name for the Ray actor (required for discovery and debugging)
        """
        if not name:
            raise ValueError("RaySplitPayloadStore requires a non-empty name")
        self._actor_name = name
        self._actor = _RaySplitPayloadStoreActor.options(name=name).remote()  # type: ignore[attr-defined]

    @property
    def actor_name(self) -> str:
        """Get the actor name."""
        return self._actor_name

    def wait_ready(self, timeout: float = 30.0) -> None:
        """Wait for the actor to be fully initialized.

        Call this before starting any workers that will use the store.

        Args:
            timeout: Maximum seconds to wait

        Raises:
            TimeoutError: If actor doesn't respond within timeout
        """
        try:
            ray.get(self._actor.ping.remote(), timeout=timeout)
        except ray.exceptions.GetTimeoutError:
            raise TimeoutError(
                f"SplitPayloadStore actor '{self._actor_name}' did not become ready within {timeout}s"
            )

    def store(self, key: str, payload: SplitPayload) -> str:
        # Put directly to object store with actor as owner
        # This avoids serializing payload twice (once to actor, once to object store)
        ref = ray.put(payload, _owner=self._actor)
        # Wrap ObjectRef in dict to prevent Ray from auto-dereferencing it
        return ray.get(self._actor.register.remote(key, {"ref": ref}))

    # Prefix for JVM-written Arrow data keys (embedded directly in payload_key)
    JVM_ARROW_PREFIX = "_jvm_arrow:"

    def get(self, key: str) -> Optional[SplitPayload]:
        # Check for JVM direct Arrow data key
        # Format: _jvm_arrow:{base64_encoded_arrow_ipc}
        if key.startswith(self.JVM_ARROW_PREFIX):
            return self._get_from_arrow_data(key)

        # Standard path: lookup from actor's registered refs.
        # Fail fast with timeout — don't block indefinitely on dead actors/objects.
        _TIMEOUT = 30
        try:
            ref_wrapper = ray.get(self._actor.get_ref.remote(key), timeout=_TIMEOUT)
        except ray.exceptions.GetTimeoutError:
            logger.warning(f"Timeout getting ref for key {key} after {_TIMEOUT}s")
            return None
        if ref_wrapper is None:
            return None

        try:
            data = ray.get(ref_wrapper["ref"], timeout=_TIMEOUT)
        except ray.exceptions.GetTimeoutError:
            logger.warning(f"Timeout getting payload data for key {key} after {_TIMEOUT}s")
            return None
        return self._convert_to_payload(data, split_id=key)

    def _get_from_arrow_data(self, key: str) -> Optional[SplitPayload]:
        """Extract Arrow data directly from key.

        This is used when JVM writes directly to queue with payload_key
        containing the base64-encoded Arrow IPC bytes.

        This approach embeds data directly in the message, avoiding
        ObjectRef serialization issues between JVM and Python.
        """
        import base64

        # Extract base64-encoded Arrow IPC data
        arrow_b64 = key[len(self.JVM_ARROW_PREFIX) :]
        arrow_bytes = base64.b64decode(arrow_b64)

        return self._convert_to_payload(arrow_bytes, split_id=key)

    def _convert_to_payload(self, data, split_id: str) -> SplitPayload:
        """Convert various data types to SplitPayload."""
        # Already a SplitPayload (from Python writers)
        if isinstance(data, SplitPayload):
            return data

        # Arrow IPC bytes (from JVM writers)
        if isinstance(data, bytes):
            import pyarrow.ipc as ipc
            import io

            table = ipc.open_stream(io.BytesIO(data)).read_all()
            return SplitPayload.from_arrow(table, split_id=split_id)

        # Arrow Table (direct)
        import pyarrow as pa

        if isinstance(data, pa.Table):
            return SplitPayload.from_arrow(data, split_id=split_id)

        raise ValueError(f"Unsupported data type in store: {type(data)}")

    def delete(self, key: str) -> bool:
        return ray.get(self._actor.delete.remote(key))

    def clear(self) -> int:
        return ray.get(self._actor.clear.remote())

    def get_metrics(self) -> dict:
        """Get storage metrics for monitoring.

        Returns:
            Dictionary with:
            - total_objects: Current number of stored objects
            - total_stored: Lifetime count of stored objects
            - total_deleted: Lifetime count of deleted objects
            - estimated_bytes: Estimated storage size (placeholder)
        """
        return ray.get(self._actor.get_metrics.remote())


def _sanitize_key(key: str) -> str:
    """Sanitize a payload key for use as a filesystem path component.

    Keys like ``job1:stage1:split_0`` contain colons which are invalid on
    some filesystems (e.g. Windows, some S3 clients).
    """
    return key.replace(":", "_").replace("/", "_")


# =============================================================================
# Fsspec (S3 / Shared Filesystem / Local Disk) Implementation
# =============================================================================


class FsspecSplitPayloadStore(SplitPayloadStore):
    """Fsspec-backed implementation of SplitPayloadStore.

    Works with any fsspec-compatible filesystem URI:
    - ``s3://bucket/prefix`` -- Amazon S3
    - ``gs://bucket/prefix`` -- Google Cloud Storage
    - ``file:///mnt/shared`` -- shared filesystem (NFS/EFS/Lustre) or local disk

    Payloads are serialized as Arrow IPC streaming bytes and stored as
    individual files under ``{base_uri}/{job_id}/{sanitized_key}.arrow``.

    Usage:
        store = FsspecSplitPayloadStore(
            base_uri="s3://my-bucket/payloads",
            job_id="job_123",
        )
        store.store("key1", payload)
        payload = store.get("key1")
        store.delete("key1")
        store.clear()
    """

    def __init__(
        self,
        base_uri: str,
        job_id: str,
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the fsspec-backed store.

        Args:
            base_uri: Fsspec-compatible URI for the storage root
                (e.g. ``s3://bucket/prefix``, ``file:///mnt/shared``).
            job_id: Job identifier used to namespace payloads.
            storage_options: Extra options passed to ``fsspec.core.url_to_fs``
                (e.g. S3 credentials, custom endpoint).
        """
        import fsspec.core

        self._base_uri = base_uri.rstrip("/")
        self._job_id = job_id
        full_path = f"{self._base_uri}/{job_id}"
        self._fs, self._root = fsspec.core.url_to_fs(full_path, **(storage_options or {}))
        self._fs.mkdirs(self._root, exist_ok=True)

        # Metrics tracking
        self._total_stored = 0
        self._total_deleted = 0

        logger.info(
            f"FsspecSplitPayloadStore initialized: "
            f"root={full_path} fs_type={type(self._fs).__name__}"
        )

    def _key_to_path(self, key: str) -> str:
        """Map a payload key to a filesystem path."""
        safe = _sanitize_key(key)
        return f"{self._root}/{safe}.arrow"

    def store(self, key: str, payload: SplitPayload) -> str:
        import pyarrow.ipc as ipc

        path = self._key_to_path(key)
        with self._fs.open(path, "wb") as f:
            writer = ipc.new_file(f, payload.data.schema)
            writer.write_table(payload.data)
            writer.close()
        self._total_stored += 1
        return key

    def get(self, key: str) -> Optional[SplitPayload]:
        import pyarrow.ipc as ipc

        path = self._key_to_path(key)
        if not self._fs.exists(path):
            return None
        with self._fs.open(path, "rb") as f:
            reader = ipc.open_file(f)
            table = reader.read_all()
        return SplitPayload.from_arrow(table, split_id=key)

    def delete(self, key: str) -> bool:
        path = self._key_to_path(key)
        if not self._fs.exists(path):
            return False
        self._fs.rm(path)
        self._total_deleted += 1
        return True

    def clear(self) -> int:
        try:
            files = self._fs.ls(self._root, detail=False)
        except FileNotFoundError:
            return 0
        count = len(files)
        if count > 0:
            self._fs.rm(self._root, recursive=True)
            self._fs.mkdirs(self._root, exist_ok=True)
        return count

    def get_metrics(self) -> dict:
        """Get storage metrics for monitoring.

        Returns:
            Dictionary with:
            - total_stored: Lifetime count of stored objects
            - total_deleted: Lifetime count of deleted objects
            - fs_type: Filesystem backend type name
        """
        return {
            "total_stored": self._total_stored,
            "total_deleted": self._total_deleted,
            "fs_type": type(self._fs).__name__,
        }
