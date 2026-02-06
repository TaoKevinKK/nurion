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

"""Tests for solstice.serve.registry with HTTP server."""

import pytest
import ray
import httpx

from solstice.serve.registry import ModelRegistry


@pytest.fixture
async def registry(ray_cluster):
    """Create a fresh registry with HTTP server for each test."""
    registry_actor = ray.remote(ModelRegistry).remote()
    http_url = ray.get(registry_actor.start.remote())
    yield registry_actor, http_url
    # Cleanup
    ray.get(registry_actor.stop.remote())


@pytest.mark.asyncio
class TestModelRegistryHTTP:
    """Tests for ModelRegistry HTTP endpoints."""

    async def test_health_endpoint(self, registry) -> None:
        """Test health check endpoint."""
        _, http_url = registry
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{http_url}/health")
            assert response.status_code == 200
            assert response.json() == {"status": "healthy"}

    async def test_register_via_http(self, registry) -> None:
        """Test registering endpoint via HTTP."""
        registry_actor, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register an endpoint
            response = await client.post(
                f"{http_url}/register",
                json={
                    "model_id": "test_model",
                    "endpoint": "http://worker1:8001",
                    "status": {"is_ready": True, "pending": 0},
                },
            )
            assert response.status_code == 200
            assert response.json() == {"ok": True}

            # Verify via HTTP GET (query parameter)
            response = await client.get(f"{http_url}/endpoints", params={"model_id": "test_model"})
            assert response.status_code == 200
            assert response.json() == ["http://worker1:8001"]

    async def test_unregister_via_http(self, registry) -> None:
        """Test unregistering endpoint via HTTP."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register first
            await client.post(
                f"{http_url}/register",
                json={"model_id": "test_model", "endpoint": "http://worker1:8001"},
            )

            # Unregister
            response = await client.post(
                f"{http_url}/unregister",
                json={"model_id": "test_model", "endpoint": "http://worker1:8001"},
            )
            assert response.status_code == 200

            # Verify empty
            response = await client.get(f"{http_url}/endpoints", params={"model_id": "test_model"})
            assert response.json() == []

    async def test_heartbeat_via_http(self, registry) -> None:
        """Test heartbeat updates via HTTP."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register first
            await client.post(
                f"{http_url}/register",
                json={"model_id": "test_model", "endpoint": "http://worker1:8001"},
            )

            # Send heartbeat with updated status
            response = await client.post(
                f"{http_url}/heartbeat",
                json={
                    "endpoint": "http://worker1:8001",
                    "status": {"is_ready": True, "pending": 5, "running": 2},
                },
            )
            assert response.status_code == 200

            # Verify status via endpoints_status (query parameter)
            response = await client.get(
                f"{http_url}/endpoints_status", params={"model_id": "test_model"}
            )
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["endpoint"] == "http://worker1:8001"
            assert data[0]["pending"] == 5
            assert data[0]["running"] == 2

    async def test_get_endpoints_with_status(self, registry) -> None:
        """Test getting endpoints with status."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register multiple endpoints
            for i in range(3):
                await client.post(
                    f"{http_url}/register",
                    json={
                        "model_id": "test_model",
                        "endpoint": f"http://worker{i}:800{i}",
                        "status": {"is_ready": True, "pending": i * 2},
                    },
                )

            # Get with status
            response = await client.get(
                f"{http_url}/endpoints_status", params={"model_id": "test_model"}
            )
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 3

            # Check fields are present
            for item in data:
                assert "endpoint" in item
                assert "pending" in item
                assert "is_ready" in item
                assert "last_heartbeat_age_s" in item

    async def test_get_all_models(self, registry) -> None:
        """Test getting all model IDs."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register endpoints for different models
            await client.post(
                f"{http_url}/register",
                json={"model_id": "model_a", "endpoint": "http://worker1:8001"},
            )
            await client.post(
                f"{http_url}/register",
                json={"model_id": "model_b", "endpoint": "http://worker2:8002"},
            )

            response = await client.get(f"{http_url}/models")
            assert response.status_code == 200
            models = response.json()
            assert set(models) == {"model_a", "model_b"}

    async def test_get_status(self, registry) -> None:
        """Test getting full registry status."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            # Register some endpoints
            await client.post(
                f"{http_url}/register",
                json={"model_id": "model_a", "endpoint": "http://worker1:8001"},
            )
            await client.post(
                f"{http_url}/register",
                json={"model_id": "model_a", "endpoint": "http://worker2:8002"},
            )

            response = await client.get(f"{http_url}/status")
            assert response.status_code == 200
            status = response.json()

            assert status["total_models"] == 1
            assert status["total_workers"] == 2
            assert "model_a" in status["models"]
            assert status["models"]["model_a"]["worker_count"] == 2

    async def test_nonexistent_model_returns_empty(self, registry) -> None:
        """Test that querying nonexistent model returns empty list."""
        _, http_url = registry

        async with httpx.AsyncClient() as client:
            response = await client.get(f"{http_url}/endpoints", params={"model_id": "nonexistent"})
            assert response.status_code == 200
            assert response.json() == []


@pytest.mark.asyncio
class TestModelRegistryRayMethods:
    """Tests for ModelRegistry Ray Actor methods (control plane)."""

    async def test_get_http_url(self, registry) -> None:
        """Test getting HTTP URL via Ray."""
        registry_actor, expected_url = registry
        url = ray.get(registry_actor.get_http_url.remote())
        assert url == expected_url
        assert url.startswith("http://")
