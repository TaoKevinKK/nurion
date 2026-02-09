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

"""Test script to validate ModelRegistry actor lifecycle.

Submit via:
    ray job submit --address http://localhost:8265 -- python scripts/test_registry_lifecycle.py

Tests:
1. Registry creation and HTTP server start
2. Register/unregister endpoints
3. ModelClient discovers registry and gets endpoints
4. Registry survives across async operations (simulating Nurion engine workflow)
5. Registry visibility from actors created by the same job
"""

import asyncio
import logging
import time

import ray

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def test_registry_basic():
    """Test 1: Basic registry creation and HTTP queries."""
    from _internal.serve.registry import get_or_create_registry, REGISTRY_ACTOR_NAME

    logger.info("=== Test 1: Registry basic lifecycle ===")

    # Create registry
    registry = get_or_create_registry()
    http_url = ray.get(registry.get_http_url.remote())
    logger.info(f"Registry created, HTTP URL: {http_url}")

    # Verify it's findable
    found = ray.get_actor(REGISTRY_ACTOR_NAME)
    logger.info(f"Registry findable via ray.get_actor: {found}")

    # Register a fake endpoint
    import httpx
    client = httpx.Client(timeout=5.0)

    resp = client.post(f"{http_url}/register", json={
        "model_id": "test_model",
        "endpoint": "http://fake:8000",
        "status": {"is_ready": True, "pending": 0, "running": 0},
    })
    assert resp.status_code == 200, f"Register failed: {resp.text}"
    logger.info("Registered fake endpoint")

    # Query endpoints
    resp = client.get(f"{http_url}/endpoints/test_model/status")
    assert resp.status_code == 200
    endpoints = resp.json()
    logger.info(f"Endpoints: {endpoints}")
    assert len(endpoints) == 1
    assert endpoints[0]["endpoint"] == "http://fake:8000"

    # Health check
    resp = client.get(f"{http_url}/health")
    assert resp.status_code == 200
    logger.info("Health check OK")

    client.close()
    logger.info("Test 1 PASSED")

    # Keep registry actor handle alive
    return registry


def test_model_client(registry_handle):
    """Test 2: ModelClient endpoint discovery."""
    from _internal.serve.client import ModelClient
    from _internal.serve.registry import REGISTRY_ACTOR_NAME

    logger.info("=== Test 2: ModelClient endpoint discovery ===")

    # Ping via handle to confirm actor is alive
    logger.info(f"Pinging registry via handle: {ray.get(registry_handle.get_http_url.remote())}")

    # Then verify via get_actor
    try:
        actor = ray.get_actor(REGISTRY_ACTOR_NAME)
        url = ray.get(actor.get_http_url.remote())
        logger.info(f"Registry still alive before ModelClient: {url}")
    except Exception as e:
        logger.error(f"Registry DEAD before ModelClient: {e}")

        # Check if it crashed - list dead actors
        from ray.util.state import list_actors
        for a in list_actors(filters=[('class_name', '=', 'ModelRegistry')]):
            logger.error(f"  ModelRegistry actor: state={a.get('state')}, "
                        f"death_cause={a.get('death_cause', 'N/A')}, "
                        f"pid={a.get('pid')}")
        raise AssertionError("Registry died before ModelClient test")

    client = ModelClient()
    endpoint = client.get_endpoint("test_model")
    logger.info(f"ModelClient.get_endpoint('test_model') = {endpoint}")
    assert endpoint == "http://fake:8000"

    client.close()
    logger.info("Test 2 PASSED")


def test_registry_visible_from_actor():
    """Test 3: Registry visible from another actor in the same job."""
    from _internal.serve.registry import REGISTRY_ACTOR_NAME

    logger.info("=== Test 3: Registry visibility from actor ===")

    @ray.remote
    class TestActor:
        def check_registry(self) -> str:
            try:
                registry = ray.get_actor(REGISTRY_ACTOR_NAME)
                url = ray.get(registry.get_http_url.remote())
                return f"FOUND: {url}"
            except ValueError as e:
                return f"NOT_FOUND: {e}"

        def check_namespace(self) -> str:
            return ray.get_runtime_context().namespace

    actor = TestActor.remote()

    actor_ns = ray.get(actor.check_namespace.remote())
    driver_ns = ray.get_runtime_context().namespace
    logger.info(f"Driver namespace: {driver_ns}")
    logger.info(f"Actor namespace: {actor_ns}")
    logger.info(f"Same namespace: {driver_ns == actor_ns}")

    result = ray.get(actor.check_registry.remote())
    logger.info(f"Registry from actor: {result}")
    assert result.startswith("FOUND:"), f"Registry not visible from actor: {result}"

    ray.kill(actor)
    logger.info("Test 3 PASSED")


async def test_registry_survives_async():
    """Test 4: Registry survives across async operations."""
    from _internal.serve.registry import REGISTRY_ACTOR_NAME

    logger.info("=== Test 4: Registry survives async ===")

    # Simulate some async work (like Solstice job runner)
    await asyncio.sleep(1)

    # Can we still find it?
    try:
        registry = ray.get_actor(REGISTRY_ACTOR_NAME)
        url = ray.get(registry.get_http_url.remote())
        logger.info(f"Registry still alive after async: {url}")
    except ValueError as e:
        logger.error(f"Registry DEAD after async: {e}")
        raise AssertionError(f"Registry died: {e}")

    logger.info("Test 4 PASSED")


async def test_registry_with_simulated_workflow():
    """Test 5: Full simulated workflow (no GPU needed)."""
    from _internal.serve.client import ModelClient
    from _internal.serve.registry import REGISTRY_ACTOR_NAME, get_or_create_registry

    logger.info("=== Test 5: Simulated workflow ===")

    # STEP 1: "Deploy model" (register endpoints)
    registry = get_or_create_registry()
    http_url = ray.get(registry.get_http_url.remote())
    logger.info(f"STEP 1: Registry at {http_url}")

    import httpx
    client = httpx.Client(timeout=5.0)
    for port in [8001, 8002, 8003]:
        client.post(f"{http_url}/register", json={
            "model_id": "workflow_model",
            "endpoint": f"http://worker:{port}",
            "status": {"is_ready": True, "pending": port % 10, "running": 0},
        })
    client.close()
    logger.info("STEP 1: Registered 3 endpoints")

    # STEP 2: Simulate StageWorker discovering endpoints
    @ray.remote
    class SimulatedStageWorker:
        def discover_and_select(self) -> dict:
            """Simulate what ExternalLLMOperator does."""
            from _internal.serve.client import ModelClient

            mc = ModelClient()
            try:
                endpoint = mc.get_endpoint("workflow_model")
                return {"status": "ok", "endpoint": endpoint}
            except Exception as e:
                return {"status": "error", "error": str(e)}
            finally:
                mc.close()

    workers = [SimulatedStageWorker.remote() for _ in range(4)]
    results = ray.get([w.discover_and_select.remote() for w in workers])

    for i, r in enumerate(results):
        logger.info(f"  Worker {i}: {r}")
        assert r["status"] == "ok", f"Worker {i} failed: {r}"

    for w in workers:
        ray.kill(w)

    logger.info("Test 5 PASSED")


def main():
    ray.init(address="auto")
    logger.info(f"Connected. Namespace: {ray.get_runtime_context().namespace}")

    try:
        registry_handle = test_registry_basic()
        test_model_client(registry_handle)
        test_registry_visible_from_actor()
        asyncio.run(test_registry_survives_async())
        asyncio.run(test_registry_with_simulated_workflow())
        logger.info("\n" + "=" * 60)
        logger.info("ALL TESTS PASSED")
        logger.info("=" * 60)
    except Exception:
        logger.exception("TEST FAILED")
        raise


if __name__ == "__main__":
    main()
