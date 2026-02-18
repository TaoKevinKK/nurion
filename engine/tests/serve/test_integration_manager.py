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

"""Integration tests for ModelServiceManager — full deploy/discover/infer/undeploy flow.

Uses ``backend="fake"`` in ModelConfig so that InferenceWorker starts a
lightweight fake server subprocess (``_internal.serve.fake_server``) instead
of real vLLM/SGLang. This exercises the real InferenceWorker lifecycle
(subprocess management, health polling, registration, heartbeat) without
requiring GPUs or a real vLLM installation.

Ray is initialized with ``num_gpus=16`` (fake) so the allocator and scheduling
strategies work correctly.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
import pytest_asyncio

from _internal.serve.client import ModelClient
from _internal.serve.config import AutoscaleConfig, ModelConfig
from _internal.serve.manager import ModelServiceManager

pytestmark = [pytest.mark.integration, pytest.mark.slow]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def manager(ray_cluster_with_gpus):
    """Real ModelServiceManager running in-process with fake backend.

    Uses ``backend="fake"`` so InferenceWorker starts a lightweight fake server
    subprocess instead of real vLLM. The real InferenceWorker lifecycle
    (subprocess management, health polling, registration, heartbeat) is fully
    exercised.

    The Manager runs in the **test process** (not as a Ray actor) for simpler
    testing. Workers still run as real Ray actors.
    """
    _ManagerCls = ModelServiceManager.__ray_metadata__.modified_class

    autoscale_config = AutoscaleConfig(enabled=False)
    mgr = _ManagerCls(autoscale_config)
    yield mgr

    try:
        await mgr.shutdown()
    except Exception:
        pass


@pytest_asyncio.fixture
async def manager_for_compaction(ray_cluster_with_gpus, monkeypatch):
    """Manager with short spawn timeout for compaction tests.

    The first spawn in ``spawn_with_compaction`` is expected to fail (no free GPUs).
    We shorten the timeout so tests don't wait 120s for it.
    """
    monkeypatch.setattr("_internal.serve.pool._SPAWN_WAIT_TIMEOUT_SECONDS", 5.0)
    _ManagerCls = ModelServiceManager.__ray_metadata__.modified_class
    autoscale_config = AutoscaleConfig(enabled=False)
    mgr = _ManagerCls(autoscale_config)
    yield mgr

    try:
        await mgr.shutdown()
    except Exception:
        pass


@pytest_asyncio.fixture
async def manager_with_autoscale(ray_cluster_with_gpus):
    """Manager with autoscaling enabled (fast intervals for testing)."""
    _ManagerCls = ModelServiceManager.__ray_metadata__.modified_class
    autoscale_config = AutoscaleConfig(
        enabled=True,
        check_interval_seconds=0.5,
        scale_up_threshold=5,
        scale_down_idle_seconds=2.0,
        cooldown_seconds=1.0,
        max_scale_step=2,
    )
    mgr = _ManagerCls(autoscale_config)
    yield mgr

    try:
        await mgr.shutdown()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _wait_all_workers_ready(manager: Any, model_id: str, timeout: float = 60.0) -> None:
    """Wait until all workers in a pool are ready and registered with the registry.

    With the fake backend, InferenceWorker polls /health every 2s, so workers
    become ready asynchronously after deploy_model returns. After becoming ready,
    the worker registers with the registry, so we also wait for the endpoint
    count to match the worker count.
    """
    import time

    pool = manager._pools[model_id]
    expected = len(pool._workers)
    deadline = time.time() + timeout
    while time.time() < deadline:
        results = await asyncio.gather(*[w.is_ready.remote() for w in pool._workers.values()])
        if all(results):
            # Also wait for registry to have all endpoints
            status = await pool.get_status()
            if len(status.get("endpoints", [])) >= expected:
                return
        await asyncio.sleep(1.0)
    raise TimeoutError(f"Not all workers for {model_id} became ready within {timeout}s")


async def _set_worker_metrics(manager: Any, model_id: str, pending: int, running: int) -> None:
    """Set load metrics on all workers of a model via the fake server.

    POSTs to the fake server's ``/internal/set_metrics`` endpoint via
    ``InferenceWorker.set_test_metrics()``. The next heartbeat cycle picks up
    the updated values from ``/metrics`` and propagates them to the registry.
    """
    pool = manager._pools[model_id]
    tasks = [w.set_test_metrics.remote(pending, running) for w in pool._workers.values()]
    await asyncio.gather(*tasks)
    # Wait for at least one heartbeat to propagate metrics to the registry
    await asyncio.sleep(3.0)


def _make_config(
    model_id: str = "test_model",
    tp: int = 1,
    min_workers: int = 1,
    max_workers: int = 2,
    **kwargs: Any,
) -> ModelConfig:
    # Explicitly set num_cpus=0 so fake workers don't consume CPU slots.
    # Without this, Ray defaults to 1 CPU/actor, limiting concurrency
    # to num_cpus in the test cluster (8).
    kwargs.setdefault("worker_resources", {"num_gpus": tp, "num_cpus": 0})
    return ModelConfig(
        model_id=model_id,
        model_source=f"fake/{model_id}",
        backend="fake",
        tensor_parallel_size=tp,
        min_workers=min_workers,
        max_workers=max_workers,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Tests — Deploy single model
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerDeploySingle:
    """Deploy a single model through the full Manager flow."""

    async def test_deploy_single_model_tp1(self, manager) -> None:
        """Deploy a TP=1 model, verify ready and endpoints registered."""
        config = _make_config("small_model", tp=1, min_workers=1, max_workers=2)
        result = await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        assert result["status"] == "ready"
        assert len(result["endpoints"]) == 1
        assert result["model_id"] == "small_model"

    async def test_deploy_single_model_tp4(self, manager) -> None:
        """Deploy a TP=4 model, verify 4 GPUs reserved per worker."""
        config = _make_config("medium_model", tp=4, min_workers=1, max_workers=2)
        result = await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        assert result["status"] == "ready"
        assert len(result["endpoints"]) == 1

    async def test_deploy_single_model_tp8(self, manager) -> None:
        """Deploy a TP=8 model. Uses 8 of 16 fake GPUs."""
        config = _make_config("large_model", tp=8, min_workers=1, max_workers=2)
        result = await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        assert result["status"] == "ready"
        assert len(result["endpoints"]) == 1

    async def test_deploy_min_workers_respected(self, manager) -> None:
        """Deploy with min_workers=2, verify 2 endpoints appear."""
        config = _make_config("multi_worker", tp=1, min_workers=2, max_workers=4)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)
        await _wait_all_workers_ready(manager, "multi_worker")

        status = await manager._pools["multi_worker"].get_status()
        assert len(status.get("endpoints", [])) == 2

    async def test_deploy_duplicate_model_raises(self, manager) -> None:
        """Deploying the same model_id twice raises an error."""
        config = _make_config("dup_model", tp=1, min_workers=1, max_workers=1)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        with pytest.raises(ValueError, match="already deployed"):
            await manager.deploy_model(config, wait_ready=True, timeout=30.0)


# ---------------------------------------------------------------------------
# Tests — Deploy multiple models (anti-fragmentation ordering)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerDeployMultiple:
    """Deploy multiple models together — exercises anti-fragmentation ordering."""

    async def test_deploy_multiple_models_different_tp(self, manager) -> None:
        """Deploy TP=4 and TP=1 together. Large deploys first."""
        configs = [
            _make_config("small", tp=1, min_workers=1, max_workers=2),
            _make_config("large", tp=4, min_workers=1, max_workers=2),
        ]
        results = await manager.deploy_model(configs, wait_ready=True, timeout=30.0)

        assert len(results) == 2
        for r in results:
            assert r["status"] == "ready"

        models = manager.list_models()
        assert set(models) == {"small", "large"}

    async def test_deploy_mixed_tp_values(self, manager) -> None:
        """Deploy TP=1 + TP=2 + TP=4 simultaneously (total 7 GPUs, fits in 16)."""
        configs = [
            _make_config("tp1", tp=1, min_workers=1, max_workers=1),
            _make_config("tp2", tp=2, min_workers=1, max_workers=1),
            _make_config("tp4", tp=4, min_workers=1, max_workers=1),
        ]
        results = await manager.deploy_model(configs, wait_ready=True, timeout=30.0)

        assert len(results) == 3
        assert all(r["status"] == "ready" for r in results)


# ---------------------------------------------------------------------------
# Tests — Undeploy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerUndeploy:
    """Undeploy models and verify cleanup."""

    async def test_undeploy_removes_model(self, manager) -> None:
        """Undeploy removes model from list and unregisters from registry."""
        config = _make_config("to_undeploy", tp=1, min_workers=1, max_workers=1)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        result = await manager.undeploy_model("to_undeploy")
        assert result["status"] == "undeployed"

        models = manager.list_models()
        assert "to_undeploy" not in models

    async def test_undeploy_frees_gpu_resources(self, manager) -> None:
        """After undeploying a TP=8 model, its GPUs become available."""
        config_large = _make_config("gpu_hog", tp=8, min_workers=2, max_workers=2)
        # 2 workers x 8 GPUs = 16 GPUs (all consumed)
        await manager.deploy_model(config_large, wait_ready=True, timeout=30.0)

        # Undeploy to free GPUs
        await manager.undeploy_model("gpu_hog")

        # Deploy a new model — should succeed because GPUs are free
        config_new = _make_config("new_model", tp=4, min_workers=1, max_workers=1)
        result = await manager.deploy_model(config_new, wait_ready=True, timeout=30.0)
        assert result["status"] == "ready"

    async def test_undeploy_nonexistent_raises(self, manager) -> None:
        """Undeploying a model that doesn't exist raises an error."""
        with pytest.raises(ValueError, match="not found"):
            await manager.undeploy_model("nonexistent")


# ---------------------------------------------------------------------------
# Tests — Scale
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerScale:
    """Scale workers up and down through the Manager API."""

    async def test_scale_up(self, manager) -> None:
        """Scale from 1 worker to 3 workers."""
        config = _make_config("scalable", tp=1, min_workers=1, max_workers=4)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        result = await manager.scale_model("scalable", target=3)
        assert result["current_workers"] == 3

    async def test_scale_down(self, manager) -> None:
        """Scale from 3 workers down to 1."""
        config = _make_config("scalable", tp=1, min_workers=1, max_workers=4)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)
        await manager.scale_model("scalable", target=3)

        result = await manager.scale_model("scalable", target=1)
        assert result["current_workers"] == 1

    async def test_scale_respects_min_max(self, manager) -> None:
        """Scale below min or above max is clamped."""
        config = _make_config("bounded", tp=1, min_workers=2, max_workers=3)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        # Try to scale to 0 — should clamp to min=2
        result = await manager.scale_model("bounded", target=0)
        assert result["current_workers"] == 2

        # Try to scale to 10 — should clamp to max=3
        result = await manager.scale_model("bounded", target=10)
        assert result["current_workers"] == 3


# ---------------------------------------------------------------------------
# Tests — Client endpoint discovery and inference
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestClientEndpointDiscovery:
    """Full flow: deploy → discover via ModelClient → infer via HTTP."""

    async def test_client_discovers_deployed_endpoints(self, manager) -> None:
        """ModelClient discovers endpoints registered by deployed workers."""
        config = _make_config("discoverable", tp=1, min_workers=2, max_workers=2)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)
        await _wait_all_workers_ready(manager, "discoverable")

        registry = manager.get_registry()
        client = ModelClient(registry, cache_ttl_seconds=0.0)

        endpoints = await client.get_endpoints("discoverable")
        assert len(endpoints) == 2
        for ep in endpoints:
            assert ep.startswith("http://")

        await client.close()

    async def test_client_can_make_inference_request(self, manager) -> None:
        """Deploy → discover → send chat completion request → verify response."""
        config = _make_config("inference_test", tp=2, min_workers=1, max_workers=1)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        registry = manager.get_registry()
        client = ModelClient(registry, cache_ttl_seconds=0.0)
        endpoints = await client.get_endpoints("inference_test")

        async with httpx.AsyncClient(timeout=10.0) as http:
            response = await http.post(
                f"{endpoints[0]}/v1/chat/completions",
                json={
                    "model": "inference_test",
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["object"] == "chat.completion"
            assert "Fake response" in data["choices"][0]["message"]["content"]
            assert "TP=2" in data["choices"][0]["message"]["content"]

        await client.close()

    async def test_endpoints_removed_after_undeploy(self, manager) -> None:
        """After undeploying, ModelClient should no longer find endpoints."""
        config = _make_config("ephemeral", tp=1, min_workers=1, max_workers=1)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        registry = manager.get_registry()
        client = ModelClient(registry, cache_ttl_seconds=0.0)

        # Verify endpoint exists
        endpoints = await client.get_endpoints("ephemeral")
        assert len(endpoints) == 1

        # Undeploy
        await manager.undeploy_model("ephemeral")

        # Verify endpoint is gone
        client.invalidate_cache("ephemeral")
        with pytest.raises(RuntimeError, match="No endpoints available"):
            await client.get_endpoints("ephemeral")

        await client.close()


# ---------------------------------------------------------------------------
# Tests — TP parametrized
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestTPParametrized:
    """Test all TP values: 1, 2, 4, 8."""

    @pytest.mark.parametrize("tp_size", [1, 2, 4, 8])
    async def test_deploy_and_infer_with_tp(self, manager, tp_size: int) -> None:
        """Deploy with TP={tp_size}, discover, infer, verify TP in response."""
        model_id = f"tp{tp_size}_model"
        config = _make_config(model_id, tp=tp_size, min_workers=1, max_workers=1)
        result = await manager.deploy_model(config, wait_ready=True, timeout=30.0)
        assert result["status"] == "ready"

        registry = manager.get_registry()
        client = ModelClient(registry, cache_ttl_seconds=0.0)
        endpoints = await client.get_endpoints(model_id)

        async with httpx.AsyncClient(timeout=10.0) as http:
            response = await http.post(
                f"{endpoints[0]}/v1/chat/completions",
                json={
                    "model": model_id,
                    "messages": [{"role": "user", "content": "test"}],
                },
            )
            assert response.status_code == 200
            content = response.json()["choices"][0]["message"]["content"]
            assert f"TP={tp_size}" in content

        await client.close()

    @pytest.mark.parametrize("tp_size", [1, 2, 4, 8])
    async def test_gpu_allocation_correct_for_tp(self, manager, tp_size: int) -> None:
        """Deploy with TP={tp_size}, verify model appears in list."""
        model_id = f"alloc_tp{tp_size}"
        config = _make_config(model_id, tp=tp_size, min_workers=1, max_workers=1)
        result = await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        assert result["status"] == "ready"
        models = manager.list_models()
        assert model_id in models


# ---------------------------------------------------------------------------
# Tests — Shutdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerShutdown:
    """Verify Manager.shutdown() cleans up everything."""

    async def test_shutdown_cleans_up_all_models(self, manager) -> None:
        """Deploy 3 models, shutdown, verify all gone."""
        configs = [_make_config(f"model_{i}", tp=1, min_workers=1, max_workers=1) for i in range(3)]
        await manager.deploy_model(configs, wait_ready=True, timeout=30.0)

        models = manager.list_models()
        assert len(models) == 3

        await manager.shutdown()

        # Manager has shut down — pools and configs should be empty
        models = manager.list_models()
        assert len(models) == 0


# ---------------------------------------------------------------------------
# Tests — Freeze / Unfreeze
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestManagerFreezeUnfreeze:
    """Verify freeze/unfreeze autoscaler via Manager API."""

    async def test_freeze_and_unfreeze(self, manager) -> None:
        """Freeze and unfreeze should not raise."""
        config = _make_config("freezable", tp=1, min_workers=1, max_workers=4)
        await manager.deploy_model(config, wait_ready=True, timeout=30.0)

        manager.freeze_model("freezable")
        manager.unfreeze_model("freezable")

    async def test_freeze_nonexistent_raises(self, manager) -> None:
        """Freezing a nonexistent model raises an error."""
        with pytest.raises(ValueError, match="not found"):
            manager.freeze_model("ghost")


# ---------------------------------------------------------------------------
# Tests — Multi-model deployment with compaction / defragmentation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestMultiModelCompaction:
    """Compaction: evict small workers to free contiguous GPU blocks for large models.

    Uses ``manager_for_compaction`` fixture with a 5s spawn timeout so the
    first (expected-to-fail) spawn in ``spawn_with_compaction`` completes
    quickly instead of waiting the default 120s.
    """

    async def test_compaction_evicts_small_to_fit_large(self, manager_for_compaction) -> None:
        """Fill 16 GPUs with TP=4 workers, then spawn TP=8 via compaction.

        Scenario:
        - Deploy model_a (TP=4, 4 workers = 16 GPUs consumed)
        - Create model_b pool (TP=8) without spawning
        - Call spawn_with_compaction("model_b") → first spawn fails (no GPUs),
          compaction evicts 2 model_a workers (frees 8 GPUs), retry succeeds
        """
        mgr = manager_for_compaction

        config_a = _make_config("model_a", tp=4, min_workers=4, max_workers=4)
        await mgr.deploy_model(config_a, wait_ready=True, timeout=60.0)
        await _wait_all_workers_ready(mgr, "model_a")
        assert await _pool_worker_count(mgr, "model_a") == 4

        # Create model_b pool manually (skip deploy_model to avoid auto-spawn)
        config_b = _make_config("model_b", tp=8, min_workers=1, max_workers=2)
        _create_empty_pool(mgr, config_b)

        # spawn_with_compaction: fail → plan compaction → evict → retry → succeed
        worker_id, _ = await mgr.spawn_with_compaction("model_b")
        assert worker_id is not None

        assert await _pool_worker_count(mgr, "model_b") == 1
        # 2 TP=4 workers evicted (8 GPUs freed for the TP=8 worker)
        assert await _pool_worker_count(mgr, "model_a") == 2

    async def test_compaction_with_mixed_tp_models(self, manager_for_compaction) -> None:
        """Mixed TP values: compaction picks cheapest eviction plan.

        Scenario (16 GPUs total):
        - model_tp4: TP=4, 3 workers = 12 GPUs
        - model_tp1: TP=1, 4 workers = 4 GPUs
        - Spawn new_tp4 (TP=4) via compaction → needs 4 GPUs
        - Cheapest plan: evict 1 TP=4 worker (frees 4 GPUs, 1 eviction)
          rather than 4 TP=1 workers (frees 4 GPUs, 4 evictions)
        """
        mgr = manager_for_compaction

        # Deploy sequentially: 7 total workers avoids GPU scheduling race
        config_tp4 = _make_config("model_tp4", tp=4, min_workers=3, max_workers=3)
        await mgr.deploy_model(config_tp4, wait_ready=True, timeout=60.0)
        await _wait_all_workers_ready(mgr, "model_tp4")

        config_tp1 = _make_config("model_tp1", tp=1, min_workers=4, max_workers=4)
        await mgr.deploy_model(config_tp1, wait_ready=True, timeout=60.0)
        await _wait_all_workers_ready(mgr, "model_tp1")

        assert await _pool_worker_count(mgr, "model_tp4") == 3
        assert await _pool_worker_count(mgr, "model_tp1") == 4

        config_new = _make_config("new_tp4", tp=4, min_workers=1, max_workers=2)
        _create_empty_pool(mgr, config_new)

        worker_id, _ = await mgr.spawn_with_compaction("new_tp4")
        assert worker_id is not None

        tp4_count = await _pool_worker_count(mgr, "model_tp4")
        tp1_count = await _pool_worker_count(mgr, "model_tp1")
        new_count = await _pool_worker_count(mgr, "new_tp4")

        assert new_count == 1
        # Compaction should evict 1 TP=4 worker (cheapest), not 4 TP=1 workers
        assert tp4_count == 2
        assert tp1_count == 4

    async def test_compaction_unfreezes_autoscaler_after_spawn(
        self, manager_for_compaction
    ) -> None:
        """Autoscalers for affected pools are frozen during compaction, then unfrozen."""
        mgr = manager_for_compaction

        config_a = _make_config("fz_model_a", tp=4, min_workers=4, max_workers=4)
        await mgr.deploy_model(config_a, wait_ready=True, timeout=60.0)
        await _wait_all_workers_ready(mgr, "fz_model_a")

        # Start autoscaler for model_a
        pool_a = mgr._pools["fz_model_a"]
        pool_a.start_autoscaler(AutoscaleConfig(enabled=True, check_interval_seconds=1.0))
        assert not pool_a._autoscale_frozen

        config_b = _make_config("fz_model_b", tp=8, min_workers=1, max_workers=1)
        _create_empty_pool(mgr, config_b)

        await mgr.spawn_with_compaction("fz_model_b")

        # After compaction completes, autoscaler should be unfrozen
        assert not pool_a._autoscale_frozen

    async def test_undeploy_frees_gpus_for_new_large_model(self, manager_for_compaction) -> None:
        """Undeploy to free GPUs, then deploy large model — no compaction needed."""
        mgr = manager_for_compaction

        config_a = _make_config("fill_model", tp=4, min_workers=4, max_workers=4)
        await mgr.deploy_model(config_a, wait_ready=True, timeout=60.0)

        await mgr.undeploy_model("fill_model")

        config_b = _make_config("large_tp8", tp=8, min_workers=2, max_workers=2)
        await mgr.deploy_model(config_b, wait_ready=True, timeout=60.0)
        await _wait_all_workers_ready(mgr, "large_tp8")

        status = await mgr._pools["large_tp8"].get_status()
        assert len(status.get("endpoints", [])) == 2


def _create_empty_pool(manager: Any, config: ModelConfig) -> Any:
    """Create a ModelPool registered in the manager without spawning workers.

    Used by compaction tests to create a pool whose first spawn is expected
    to fail (triggering the compaction path).
    """
    from _internal.serve.pool import ModelPool

    pool = ModelPool(
        config=config,
        registry=manager._registry,
        detached=False,
        allocator=manager._allocator,
    )
    manager._pools[config.model_id] = pool
    manager._configs[config.model_id] = config
    return pool


async def _pool_worker_count(manager: Any, model_id: str) -> int:
    """Get the current worker count for a model."""
    status = await manager._pools[model_id].get_status()
    return status["total_workers"]


# ---------------------------------------------------------------------------
# Tests — Multi-model autoscaler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestMultiModelAutoscaler:
    """Autoscaler with multiple models deployed — each scales independently."""

    async def test_independent_scale_up_under_load(self, manager_with_autoscale) -> None:
        """Deploy two models. Apply load to one, verify only that one scales up."""
        mgr = manager_with_autoscale

        config_a = _make_config("auto_a", tp=1, min_workers=1, max_workers=4)
        config_b = _make_config("auto_b", tp=1, min_workers=1, max_workers=4)
        await mgr.deploy_model([config_a, config_b], wait_ready=True, timeout=30.0)

        # Inject high load on model_a only (propagated via heartbeat)
        await _set_worker_metrics(mgr, "auto_a", pending=20, running=20)

        # Wait for autoscaler to act
        await asyncio.sleep(4.0)

        count_a = await _pool_worker_count(mgr, "auto_a")
        count_b = await _pool_worker_count(mgr, "auto_b")

        assert count_a > 1, f"auto_a should have scaled up, got {count_a}"
        assert count_b == 1, f"auto_b should remain at 1, got {count_b}"

    async def test_scale_down_after_load_removed(self, manager_with_autoscale) -> None:
        """Scale up under load, then scale down after load removed."""
        mgr = manager_with_autoscale

        config = _make_config("scaledown_model", tp=1, min_workers=1, max_workers=4)
        await mgr.deploy_model(config, wait_ready=True, timeout=30.0)

        # Inject high load → trigger scale up
        await _set_worker_metrics(mgr, "scaledown_model", pending=30, running=30)

        await asyncio.sleep(4.0)
        count_after_load = await _pool_worker_count(mgr, "scaledown_model")
        assert count_after_load > 1, f"Should have scaled up, got {count_after_load}"

        # Remove all load → trigger scale down after idle period
        await _set_worker_metrics(mgr, "scaledown_model", pending=0, running=0)

        await asyncio.sleep(6.0)
        count_after_idle = await _pool_worker_count(mgr, "scaledown_model")
        assert count_after_idle < count_after_load, (
            f"Should have scaled down from {count_after_load}, got {count_after_idle}"
        )

    async def test_autoscaler_respects_max_workers_with_different_tp(
        self, manager_with_autoscale
    ) -> None:
        """Models with different TP values scale independently up to max_workers."""
        mgr = manager_with_autoscale

        # TP=2 model uses 2 GPUs per worker, max 3 workers = 6 GPUs
        config_tp2 = _make_config("as_tp2", tp=2, min_workers=1, max_workers=3)
        # TP=1 model uses 1 GPU per worker, max 4 workers = 4 GPUs
        config_tp1 = _make_config("as_tp1", tp=1, min_workers=1, max_workers=4)
        await mgr.deploy_model([config_tp2, config_tp1], wait_ready=True, timeout=30.0)

        # Apply heavy load to both
        await _set_worker_metrics(mgr, "as_tp2", pending=50, running=50)
        await _set_worker_metrics(mgr, "as_tp1", pending=50, running=50)

        # Wait for multiple autoscaler cycles
        await asyncio.sleep(8.0)

        count_tp2 = await _pool_worker_count(mgr, "as_tp2")
        count_tp1 = await _pool_worker_count(mgr, "as_tp1")

        assert count_tp2 <= 3, f"TP=2 should respect max_workers=3, got {count_tp2}"
        assert count_tp1 <= 4, f"TP=1 should respect max_workers=4, got {count_tp1}"
        # At least one should have scaled up
        assert count_tp2 > 1 or count_tp1 > 1

    async def test_frozen_model_does_not_scale_while_others_do(
        self, manager_with_autoscale
    ) -> None:
        """Frozen model's autoscaler stays frozen while other models scale normally."""
        mgr = manager_with_autoscale

        config_a = _make_config("frozen_a", tp=1, min_workers=1, max_workers=4)
        config_b = _make_config("active_b", tp=1, min_workers=1, max_workers=4)
        await mgr.deploy_model([config_a, config_b], wait_ready=True, timeout=30.0)

        # Freeze model_a
        mgr.freeze_model("frozen_a")

        # Apply load to both (frozen model still receives load metrics)
        await _set_worker_metrics(mgr, "frozen_a", pending=30, running=30)
        await _set_worker_metrics(mgr, "active_b", pending=30, running=30)

        await asyncio.sleep(4.0)

        count_a = await _pool_worker_count(mgr, "frozen_a")
        count_b = await _pool_worker_count(mgr, "active_b")

        assert count_a == 1, f"Frozen model should stay at 1, got {count_a}"
        assert count_b > 1, f"Active model should scale up, got {count_b}"
