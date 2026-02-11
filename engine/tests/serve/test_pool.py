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

"""Tests for ModelPool autoscaling behavior.

Uses FakeInferenceWorker (no GPU, no vLLM) to verify that the autoscaler
correctly scales workers up/down based on inflight request metrics
(pending + running), not just pending queue depth.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import httpx
import pytest
import ray

from _internal.serve.config import AutoscaleConfig, ModelConfig
from _internal.serve.pool import ModelPool
from _internal.serve.registry import ModelRegistry
from _internal.utils.network import find_free_port


# ---------------------------------------------------------------------------
# Fake worker & testable pool (module-level for Ray serialization)
# ---------------------------------------------------------------------------


class FakeInferenceWorker:
    """Mock InferenceWorker that registers with the registry without GPU/vLLM.

    - Registers as ready on ``start()``
    - Does NOT send heartbeats — the test drives metrics directly via HTTP
    - Unregisters on ``shutdown()``
    """

    def __init__(
        self,
        config: ModelConfig,
        registry: ray.actor.ActorHandle,
        port: int,
        worker_id: str,
    ) -> None:
        self._config = config
        self._registry = registry
        self._port = port
        self._worker_id = worker_id
        self._endpoint = f"http://127.0.0.1:{port}"
        self._is_ready = False
        self._registry_url: Optional[str] = None
        self._http_client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._registry_url = ray.get(self._registry.get_http_url.remote())
        self._http_client = httpx.AsyncClient(timeout=5.0)
        await self._http_client.post(
            f"{self._registry_url}/register",
            json={
                "model_id": self._config.model_id,
                "endpoint": self._endpoint,
                "status": {"is_ready": True, "pending": 0, "running": 0},
            },
        )
        self._is_ready = True

    def is_ready(self) -> bool:
        return self._is_ready

    def is_failed(self) -> bool:
        return False

    async def shutdown(self) -> None:
        if self._http_client and self._registry_url:
            try:
                await self._http_client.post(
                    f"{self._registry_url}/unregister",
                    json={
                        "model_id": self._config.model_id,
                        "endpoint": self._endpoint,
                    },
                )
                await self._http_client.aclose()
            except Exception:
                pass
        self._is_ready = False


class _TestableModelPool(ModelPool):
    """ModelPool that spawns ``FakeInferenceWorker`` instead of real ones."""

    async def _spawn_worker(self) -> tuple[str, ray.actor.ActorHandle]:
        port = find_free_port()
        worker_id = f"{self._config.model_id}_worker_{port}"

        worker = (
            ray.remote(FakeInferenceWorker)
            .options(name=worker_id, num_cpus=0)
            .remote(
                self._config,
                registry=self._registry,
                port=port,
                worker_id=worker_id,
            )
        )
        await worker.start.remote()

        self._workers[worker_id] = worker
        self._worker_ports[worker_id] = port
        return worker_id, worker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model_config(model_id: str = "test_model") -> ModelConfig:
    """ModelConfig with no GPU requirement."""
    return ModelConfig(
        model_id=model_id,
        model_source="fake/model",
        tensor_parallel_size=1,
        min_workers=2,
        max_workers=4,
        worker_resources={"num_cpus": 0},
    )


def _make_autoscale_config(**overrides: Any) -> AutoscaleConfig:
    """AutoscaleConfig with fast intervals for testing."""
    defaults: dict[str, Any] = {
        "enabled": True,
        "check_interval_seconds": 0.5,
        "scale_up_threshold": 5,
        "scale_down_idle_seconds": 2.0,
        "cooldown_seconds": 1.0,
        "max_scale_step": 1,
    }
    defaults.update(overrides)
    return AutoscaleConfig(**defaults)


async def _set_metrics(
    http_url: str, endpoint: str, pending: int, running: int
) -> None:
    """Update a worker's metrics in the registry via HTTP heartbeat."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(
            f"{http_url}/heartbeat",
            json={
                "endpoint": endpoint,
                "status": {"is_ready": True, "pending": pending, "running": running},
            },
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def registry(ray_cluster):
    """Fresh ModelRegistry actor with HTTP server."""
    actor = ray.remote(ModelRegistry).remote()
    http_url = ray.get(actor.start.remote())
    yield actor, http_url
    ray.get(actor.stop.remote())


@pytest.fixture
async def pool_env(registry):
    """TestableModelPool scaled to *min_workers* with autoscaler NOT started.

    Returns ``(pool_handle, http_url, model_config)``.
    """
    registry_actor, http_url = registry
    config = _make_model_config()

    pool = (
        ray.remote(_TestableModelPool)
        .options(name="test_pool", num_cpus=0)
        .remote(config, registry_actor, False)
    )

    await pool.scale_to.remote(config.min_workers)
    ready = await pool.wait_ready.remote(timeout=10.0)
    assert ready, "FakeInferenceWorker failed to become ready"

    yield pool, http_url, config

    await pool.shutdown.remote()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestModelPoolAutoscaling:
    """Verify autoscaler scales based on total inflight (pending + running)."""

    # -- scale-up ----------------------------------------------------------

    async def test_scale_up_on_high_running(self, pool_env) -> None:
        """Scale up when *running* is high even if *pending* ≈ 0.

        This is the primary scenario the bug-fix addresses: vLLM's chunked
        prefill moves requests from waiting → running almost immediately, so
        pending stays near zero while GPU is saturated.
        """
        pool, http_url, _ = pool_env

        status = await pool.get_status.remote()
        assert status["total_workers"] == 2
        endpoints = status["endpoints"]

        # Simulate: GPU saturated, waiting queue near-empty
        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=0, running=20)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))

        # Wait: cooldown (1 s) + check (0.5 s) + spawn time + buffer
        await asyncio.sleep(3.0)

        # inflight = 40 > threshold = 5 × 2 = 10 → scale up
        status = await pool.get_status.remote()
        assert status["total_workers"] > 2, (
            f"Expected scale-up from running load, still at {status['total_workers']}"
        )

    async def test_scale_up_on_high_pending(self, pool_env) -> None:
        """Traditional overload: high pending, low running → still scales up."""
        pool, http_url, _ = pool_env

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=15, running=0)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))
        await asyncio.sleep(3.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] > 2

    async def test_no_scale_up_below_threshold(self, pool_env) -> None:
        """No scale-up when total inflight is below the threshold."""
        pool, http_url, _ = pool_env

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        # total inflight = 2 × (1 + 1) = 4 < threshold = 5 × 2 = 10
        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=1, running=1)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))
        await asyncio.sleep(3.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] == 2

    async def test_respects_max_workers(self, pool_env) -> None:
        """Autoscaler never exceeds *max_workers*."""
        pool, http_url, config = pool_env

        await pool.scale_to.remote(config.max_workers)
        await asyncio.sleep(0.5)

        status = await pool.get_status.remote()
        assert status["total_workers"] == config.max_workers
        endpoints = status["endpoints"]

        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=100, running=100)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))
        await asyncio.sleep(3.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] == config.max_workers

    # -- scale-down --------------------------------------------------------

    async def test_scale_down_when_idle(self, pool_env) -> None:
        """Scale down after sustained zero-inflight period."""
        pool, http_url, config = pool_env

        # Start above min so there is room to scale down
        await pool.scale_to.remote(3)
        await asyncio.sleep(0.5)

        status = await pool.get_status.remote()
        assert status["total_workers"] == 3
        endpoints = status["endpoints"]

        # All workers idle (inflight = 0)
        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=0, running=0)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))

        # Wait: cooldown (1 s) + idle detection start + idle_seconds (2 s) + buffer
        await asyncio.sleep(5.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] < 3, (
            f"Expected scale-down, still at {status['total_workers']}"
        )
        assert status["total_workers"] >= config.min_workers

    async def test_no_scale_down_with_running_requests(self, pool_env) -> None:
        """Workers with *running* > 0 (but pending = 0) should NOT be idle."""
        pool, http_url, config = pool_env

        await pool.scale_to.remote(3)
        await asyncio.sleep(0.5)

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        # pending = 0 but running > 0 → not idle
        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=0, running=1)

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))
        await asyncio.sleep(5.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] == 3, (
            "Should NOT scale down while workers have running requests"
        )

    # -- guards ------------------------------------------------------------

    async def test_cooldown_blocks_scaling(self, pool_env) -> None:
        """No scaling during cooldown period."""
        pool, http_url, _ = pool_env

        # Very long cooldown — longer than the test sleeps
        ray.get(
            pool.start_autoscaler.remote(
                _make_autoscale_config(cooldown_seconds=60.0)
            )
        )

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=50, running=50)

        await asyncio.sleep(3.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] == 2

    async def test_frozen_autoscaler_does_not_scale(self, pool_env) -> None:
        """Frozen autoscaler makes no decisions regardless of load."""
        pool, http_url, _ = pool_env

        ray.get(pool.start_autoscaler.remote(_make_autoscale_config()))
        ray.get(pool.freeze_autoscaler.remote())

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        for ep in endpoints:
            await _set_metrics(http_url, ep, pending=50, running=50)

        await asyncio.sleep(3.0)

        status = await pool.get_status.remote()
        assert status["total_workers"] == 2

    # -- status API --------------------------------------------------------

    async def test_get_status_includes_running(self, pool_env) -> None:
        """``get_status`` returns *total_running* alongside *total_pending*."""
        pool, http_url, _ = pool_env

        status = await pool.get_status.remote()
        endpoints = status["endpoints"]

        await _set_metrics(http_url, endpoints[0], pending=3, running=7)
        await _set_metrics(http_url, endpoints[1], pending=5, running=10)

        status = await pool.get_status.remote()
        assert status["total_pending"] == 8
        assert status["total_running"] == 17
