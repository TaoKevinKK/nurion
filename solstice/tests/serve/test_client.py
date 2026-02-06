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

"""Tests for solstice.serve.client."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from solstice.serve.client import EndpointCache, EndpointInfo, ModelClient


class TestEndpointInfo:
    """Tests for EndpointInfo dataclass."""

    def test_defaults(self) -> None:
        info = EndpointInfo(endpoint="http://localhost:8000")
        assert info.endpoint == "http://localhost:8000"
        assert info.pending == 0
        assert info.running == 0
        assert info.is_ready is True
        assert info.last_heartbeat_age_s == 0.0

    def test_full_init(self) -> None:
        info = EndpointInfo(
            endpoint="http://localhost:8000",
            pending=5,
            running=3,
            is_ready=False,
            last_heartbeat_age_s=10.0,
        )
        assert info.pending == 5
        assert info.running == 3
        assert info.is_ready is False


class TestEndpointCache:
    """Tests for EndpointCache dataclass."""

    def test_is_fresh(self) -> None:
        import time

        cache = EndpointCache(cached_at=time.time())
        assert cache.is_fresh(ttl=30.0) is True
        assert cache.is_fresh(ttl=0.0) is False

    def test_from_registry_response(self) -> None:
        response = [
            {"endpoint": "http://localhost:8001", "pending": 5, "is_ready": True},
            {"endpoint": "http://localhost:8002", "pending": 10, "is_ready": False},
        ]
        cache = EndpointCache.from_registry_response(response)

        assert len(cache.endpoints) == 2
        assert cache.endpoints[0].endpoint == "http://localhost:8001"
        assert cache.endpoints[0].pending == 5
        assert cache.endpoints[1].is_ready is False


class TestModelClientEndpointSelection:
    """Tests for endpoint selection (load balancing) logic."""

    def _make_client(self, local_pending: dict | None = None) -> ModelClient:
        client = ModelClient.__new__(ModelClient)
        client._local_pending = local_pending or {}
        return client

    def test_prefers_lowest_pending(self) -> None:
        client = self._make_client()
        endpoints = [
            EndpointInfo(endpoint="http://host1:8001", pending=10, is_ready=True),
            EndpointInfo(endpoint="http://host2:8002", pending=3, is_ready=True),
            EndpointInfo(endpoint="http://host3:8003", pending=5, is_ready=True),
        ]
        assert client._select_endpoint(endpoints) == "http://host2:8002"

    def test_considers_local_pending(self) -> None:
        client = self._make_client({"http://host2:8002": 20})
        endpoints = [
            EndpointInfo(endpoint="http://host1:8001", pending=10, is_ready=True),
            EndpointInfo(endpoint="http://host2:8002", pending=3, is_ready=True),
        ]
        # host2: 3 + 20 = 23, host1: 10 + 0 = 10
        assert client._select_endpoint(endpoints) == "http://host1:8001"

    def test_prefers_ready_endpoints(self) -> None:
        client = self._make_client()
        endpoints = [
            EndpointInfo(endpoint="http://host1:8001", pending=0, is_ready=False),
            EndpointInfo(endpoint="http://host2:8002", pending=10, is_ready=True),
        ]
        assert client._select_endpoint(endpoints) == "http://host2:8002"

    def test_falls_back_to_not_ready(self) -> None:
        client = self._make_client()
        endpoints = [
            EndpointInfo(endpoint="http://host1:8001", pending=10, is_ready=False),
            EndpointInfo(endpoint="http://host2:8002", pending=5, is_ready=False),
        ]
        assert client._select_endpoint(endpoints) == "http://host2:8002"

    def test_empty_list_returns_none(self) -> None:
        client = self._make_client()
        assert client._select_endpoint([]) is None


class TestModelClientPendingTracking:
    """Tests for local pending count tracking."""

    def test_track_and_untrack(self) -> None:
        client = ModelClient.__new__(ModelClient)
        client._local_pending = {}
        ep = "http://host:8000"

        client.track_pending(ep)
        assert client._local_pending[ep] == 1
        client.track_pending(ep)
        assert client._local_pending[ep] == 2
        client.untrack_pending(ep)
        assert client._local_pending[ep] == 1
        client.untrack_pending(ep)
        assert client._local_pending[ep] == 0

    def test_untrack_never_goes_negative(self) -> None:
        client = ModelClient.__new__(ModelClient)
        client._local_pending = {}
        client.untrack_pending("http://host:8000")
        assert client._local_pending.get("http://host:8000", 0) == 0


@pytest.mark.asyncio
class TestModelClientGetEndpoint:
    """Tests for get_endpoint with mocked HTTP."""

    async def test_returns_least_loaded(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry)
        client._registry_url = "http://registry:18000"
        client._endpoint_cache["m"] = EndpointCache.from_registry_response([
            {"endpoint": "http://h1:8001", "pending": 10, "is_ready": True},
            {"endpoint": "http://h2:8002", "pending": 3, "is_ready": True},
        ])
        assert await client.get_endpoint("m") == "http://h2:8002"

    async def test_no_endpoints_raises(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry)
        client._registry_url = "http://registry:18000"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        client._http_client = mock_http

        with pytest.raises(RuntimeError, match="No endpoints available"):
            await client.get_endpoint("missing_model")

    async def test_refreshes_cache_on_expiry(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry, cache_ttl_seconds=0.0)  # always expired
        client._registry_url = "http://registry:18000"
        client._endpoint_cache["m"] = EndpointCache(
            endpoints=[EndpointInfo(endpoint="http://old:8000")],
            cached_at=0.0,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"endpoint": "http://new:8001", "pending": 0, "is_ready": True}
        ]
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        client._http_client = mock_http

        assert await client.get_endpoint("m") == "http://new:8001"
        mock_http.get.assert_called_once()

    async def test_invalidate_cache(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry)
        client._endpoint_cache["m"] = EndpointCache.from_registry_response(
            [{"endpoint": "http://h:8001", "pending": 0, "is_ready": True}]
        )
        client.invalidate_cache("m")
        assert "m" not in client._endpoint_cache


@pytest.mark.asyncio
class TestModelClientRegistryRefresh:
    """Tests for registry URL refresh on connection failure."""

    async def test_refresh_registry_url(self) -> None:
        """When registry URL is None, it should be resolved from the actor."""
        mock_registry = MagicMock()
        mock_registry.get_http_url.remote = AsyncMock(
            return_value="http://new-registry:18000"
        )

        client = ModelClient(registry=mock_registry)
        assert client._registry_url is None

        url = await client._get_registry_url()
        assert url == "http://new-registry:18000"
        assert client._registry_url == "http://new-registry:18000"

    async def test_get_endpoint_refreshes_on_connect_error(self) -> None:
        """On ConnectError, client should reset registry URL and retry."""
        mock_registry = MagicMock()
        mock_registry.get_http_url.remote = AsyncMock(
            return_value="http://new-registry:18000"
        )

        client = ModelClient(registry=mock_registry, cache_ttl_seconds=0.0)
        client._registry_url = "http://dead:18000"

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("Connection refused")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = [
                {"endpoint": "http://h:8001", "pending": 0, "is_ready": True}
            ]
            resp.raise_for_status = MagicMock()
            return resp

        mock_http = AsyncMock()
        mock_http.get = mock_get
        client._http_client = mock_http

        endpoint = await client.get_endpoint("m")
        assert endpoint == "http://h:8001"
        assert call_count == 2


@pytest.mark.asyncio
class TestModelClientClose:
    """Tests for close/cleanup."""

    async def test_close(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry)
        mock_http = AsyncMock()
        client._http_client = mock_http
        await client.close()
        mock_http.aclose.assert_called_once()
        assert client._http_client is None

    async def test_close_idempotent(self) -> None:
        mock_registry = MagicMock()
        client = ModelClient(registry=mock_registry)
        mock_http = AsyncMock()
        client._http_client = mock_http
        await client.close()
        await client.close()  # should not raise
        mock_http.aclose.assert_called_once()
