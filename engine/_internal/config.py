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

"""Centralized engine configuration.

Every tunable constant lives here as a field on :class:`EngineConfig`.
Values are read **once per process** from environment variables
(prefix ``NURION_``) with sensible defaults.

**Distributed sync** — Ray propagates ``runtime_env.env_vars`` to all
workers, so env-var–based config is naturally consistent across the
cluster.  :func:`configure` is the programmatic entry-point: call it
*before* ``job.run()`` and the runner will inject non-default values
into ``runtime_env`` automatically.

Quick reference::

    # Via environment variable
    export NURION_BROKER_CLAIM_TIMEOUT_S=120

    # Via Python (must be called before job.run())
    from _internal.config import configure
    configure(broker_claim_timeout_s=120)

    # Read current value
    from _internal.config import get_config
    cfg = get_config()
    print(cfg.broker_claim_timeout_s)  # 120.0
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Any

_ENV_PREFIX = "NURION_"

# Module-level cache — one per process
_cached_config: EngineConfig | None = None


@dataclass(frozen=True)
class EngineConfig:
    """All tunable engine constants.

    Each field has a corresponding environment variable:
    ``NURION_<FIELD_NAME_UPPER>``  (e.g. ``NURION_BROKER_CLAIM_TIMEOUT_S``).
    """

    # ── Broker (Anvil) ────────────────────────────────────────────────
    broker_startup_timeout_s: float = 30.0
    broker_claim_timeout_s: float = 60.0
    broker_recovery_interval_s: float = 10.0
    broker_acked_retention_s: float = 3600.0
    broker_gc_interval_s: float = 60.0

    # ── Heartbeat ─────────────────────────────────────────────────────
    heartbeat_min_interval_s: float = 0.1
    heartbeat_max_interval_s: float = 5.0

    # ── Stage ─────────────────────────────────────────────────────────
    stage_no_progress_timeout_s: float = 600.0
    stage_completion_poll_interval_s: float = 0.1
    stage_completion_max_errors: int = 10
    stage_mark_finished_max_retries: int = 3

    # ── Worker ────────────────────────────────────────────────────────
    worker_idle_timeout_s: float = 300.0
    worker_claim_timeout_ms: int = 1000
    worker_idle_sleep_s: float = 0.05
    worker_error_sleep_s: float = 0.1
    worker_stop_timeout_s: float = 5.0
    worker_queue_full_max_retries: int = 30
    worker_queue_full_retry_sleep_s: float = 1.0

    # ── Source ────────────────────────────────────────────────────────
    source_backpressure_check_interval: int = 10
    source_backpressure_pause_sleep_s: float = 0.1
    source_produce_max_retries: int = 3
    source_queue_full_max_retries: int = 60
    source_queue_full_retry_sleep_s: float = 1.0

    # ── Autoscaler (defaults, overridable per-stage via StageAutoscaleConfig)
    autoscaler_check_interval_s: float = 10.0
    autoscaler_scale_up_lag: int = 500
    autoscaler_scale_down_lag: int = 100
    autoscaler_cooldown_up_s: float = 15.0
    autoscaler_cooldown_down_s: float = 60.0
    autoscaler_max_scale_step: int = 32

    # ── Flight / NVMe payload store ───────────────────────────────────
    flight_server_port: int = 18815
    flight_max_concurrent_reads: int = 8
    flight_read_timeout_s: float = 10.0
    flight_semaphore_timeout_s: float = 30.0
    flight_subprocess_start_timeout_s: float = 10.0

    # ── S3 (NVMe write-through) ──────────────────────────────────────
    s3_failure_threshold: int = 5
    s3_upload_workers: int = 4
    s3_write_timeout_s: float = 120.0

    # ── Misc ──────────────────────────────────────────────────────────
    main_loop_sleep_s: float = 0.1

    def env_vars(self) -> dict[str, str]:
        """Return non-default values as a ``{NURION_...: value}`` dict.

        Intended for injection into ``runtime_env["env_vars"]`` so that
        Ray workers see the same config as the driver.
        """
        defaults = EngineConfig()
        result: dict[str, str] = {}
        for f in fields(self):
            val = getattr(self, f.name)
            if val != getattr(defaults, f.name):
                result[_ENV_PREFIX + f.name.upper()] = str(val)
        return result


_TYPE_MAP: dict[str, type] = {
    "float": float,
    "int": int,
    "str": str,
}

def _read_from_env() -> EngineConfig:
    """Build an EngineConfig by reading env vars, falling back to defaults."""
    kwargs: dict[str, Any] = {}
    for f in fields(EngineConfig):
        env_key = _ENV_PREFIX + f.name.upper()
        raw = os.environ.get(env_key)
        if raw is not None:
            # f.type is a string (due to __future__.annotations), resolve it
            converter = _TYPE_MAP.get(f.type, str) if isinstance(f.type, str) else f.type
            kwargs[f.name] = converter(raw)
    return EngineConfig(**kwargs)


def get_config() -> EngineConfig:
    """Return the cached engine config (reads env vars on first call)."""
    global _cached_config
    if _cached_config is None:
        _cached_config = _read_from_env()
    return _cached_config


def configure(**kwargs: object) -> EngineConfig:
    """Set config values programmatically.

    Sets the corresponding ``NURION_`` env vars and resets the cache so
    that :func:`get_config` returns an updated config.  Must be called
    **before** ``job.run()`` so that the runner can propagate the values
    to Ray workers.

    Returns the new config.
    """
    valid = {f.name for f in fields(EngineConfig)}
    for key, value in kwargs.items():
        if key not in valid:
            raise ValueError(f"Unknown config key: {key!r}. Valid keys: {sorted(valid)}")
        os.environ[_ENV_PREFIX + key.upper()] = str(value)

    global _cached_config
    _cached_config = None  # force re-read
    return get_config()


def reset_config() -> None:
    """Reset the cached config (forces re-read from env on next access).

    Primarily for testing.
    """
    global _cached_config
    _cached_config = None
