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

"""Workflow tests for image captioning pipelines.

Tests both embedded (EmbeddedLLMOperator) and external (ExternalLLMOperator)
captioning workflows with mocked GPU and vLLM.

- **Embedded test**: Inject mock ``vllm`` and ``PIL`` packages into Ray
  workers via ``runtime_env.py_modules``.
- **External test**: Deploy model with ``backend="fake"`` via the real serve
  layer, then call the imported ``run_workflow`` from
  ``run_image_captioning_external``.
"""

from __future__ import annotations

import asyncio
import struct
import time
import zlib
from pathlib import Path

import lance
import pyarrow as pa
import pytest
import ray

from tests.conftest import RAY_RUNTIME_EXCLUDES

pytestmark = pytest.mark.workflow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_TEST_ROWS = 5
TEST_IMAGE_FIELD = "image_res_2048"
FAKE_EMBEDDED_CAPTION = "Mock caption from embedded vLLM engine."

# ---------------------------------------------------------------------------
# Mock package source code (written to disk, loaded by Ray workers)
# ---------------------------------------------------------------------------

MOCK_VLLM_SOURCE = '''
"""Mock vllm package for workflow integration tests."""


class SamplingParams:
    """Mock vLLM SamplingParams."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _CompletionOutput:
    def __init__(self, text):
        self.text = text


class _RequestOutput:
    def __init__(self, text="Mock caption from embedded vLLM engine."):
        self.outputs = [_CompletionOutput(text)]


class LLM:
    """Mock vLLM LLM engine — no GPU, no model loading."""

    def __init__(self, **kwargs):
        self._model = kwargs.get("model", "mock")

    def generate(self, inputs, sampling_params=None):
        """Return a fake RequestOutput for each input."""
        return [_RequestOutput() for _ in inputs]
'''

MOCK_PIL_INIT_SOURCE = '''
"""Mock PIL (Pillow) package for workflow integration tests."""
'''

MOCK_PIL_IMAGE_SOURCE = '''
"""Mock PIL.Image module for workflow integration tests."""


class _MockImage:
    """Minimal mock PIL Image object."""

    def __init__(self, data=None):
        self._data = data
        self.mode = "RGB"
        self.size = (1, 1)

    def convert(self, mode):
        self.mode = mode
        return self

    def close(self):
        pass


def open(fp, *args, **kwargs):
    """Mock Image.open — reads bytes, returns mock image."""
    if hasattr(fp, "read"):
        data = fp.read()
    else:
        data = fp
    return _MockImage(data)


def new(mode, size, color=0):
    """Mock Image.new."""
    return _MockImage()
'''

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_minimal_png() -> bytes:
    """Create a valid 1x1 pixel PNG using raw struct + zlib (no Pillow)."""

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        payload = chunk_type + data
        crc = struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)
        length = struct.pack(">I", len(data))
        return length + payload + crc

    signature = b"\x89PNG\r\n\x1a\n"
    # IHDR: 1x1, 8-bit RGB
    ihdr_data = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
    ihdr = _chunk(b"IHDR", ihdr_data)
    # IDAT: filter=None(0x00) + R G B
    raw_data = b"\x00\xff\x00\x00"
    idat = _chunk(b"IDAT", zlib.compress(raw_data))
    # IEND
    iend = _chunk(b"IEND", b"")

    return signature + ihdr + idat + iend


def _create_test_lance_dataset(path: str, num_rows: int, image_field: str) -> None:
    """Create a Lance dataset with id + image columns."""
    png_bytes = _create_minimal_png()
    records = [{"id": i, image_field: png_bytes} for i in range(num_rows)]
    table = pa.Table.from_pylist(records)
    lance.write_dataset(table, path, mode="overwrite")


def _kill_all_actors() -> None:
    try:
        for actor_info in ray.state.actors().values():
            if actor_info.get("State") == "ALIVE":
                try:
                    actor = ray.get_actor(actor_info.get("Name", ""))
                    ray.kill(actor)
                except Exception:
                    pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Fixtures — Embedded test
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mock_packages_dir(tmp_path_factory):
    """Create temp directory with mock vllm and PIL packages for Ray workers."""
    base = tmp_path_factory.mktemp("mock_packages")

    # Mock vllm
    vllm_pkg = base / "vllm"
    vllm_pkg.mkdir()
    (vllm_pkg / "__init__.py").write_text(MOCK_VLLM_SOURCE)

    # Mock PIL
    pil_pkg = base / "PIL"
    pil_pkg.mkdir()
    (pil_pkg / "__init__.py").write_text(MOCK_PIL_INIT_SOURCE)
    (pil_pkg / "Image.py").write_text(MOCK_PIL_IMAGE_SOURCE)

    return str(base)


@pytest.fixture(scope="function")
def ray_cluster_with_mock_vllm(mock_packages_dir):
    """Ray cluster with fake GPUs and mock vllm/PIL injected via py_modules."""
    if ray.is_initialized():
        ray.shutdown()
        time.sleep(1.0)

    mock_dir = Path(mock_packages_dir)
    ray.init(
        num_cpus=8,
        num_gpus=4,
        runtime_env={
            "excludes": RAY_RUNTIME_EXCLUDES,
            "py_modules": [
                str(mock_dir / "vllm"),
                str(mock_dir / "PIL"),
            ],
        },
        ignore_reinit_error=True,
    )

    yield

    _kill_all_actors()
    ray.shutdown()
    time.sleep(3.0)


# ---------------------------------------------------------------------------
# Fixtures — External test
# ---------------------------------------------------------------------------

_PROXY_ENV_CLEAR = {
    "http_proxy": "",
    "https_proxy": "",
    "HTTP_PROXY": "",
    "HTTPS_PROXY": "",
    "all_proxy": "",
    "ALL_PROXY": "",
}


@pytest.fixture(scope="function")
def ray_cluster_serve():
    """Ray cluster with fake GPUs + proxy env vars cleared.

    Proxy env vars (SOCKS / HTTP) are cleared so that httpx in Ray workers
    can reach fake server endpoints on 127.0.0.1.
    """
    if ray.is_initialized():
        ray.shutdown()
        time.sleep(1.0)

    ray.init(
        num_cpus=8,
        num_gpus=16,
        runtime_env={
            "excludes": RAY_RUNTIME_EXCLUDES,
            "env_vars": _PROXY_ENV_CLEAR,
        },
        ignore_reinit_error=True,
    )

    yield

    _kill_all_actors()
    ray.shutdown()
    time.sleep(3.0)


# ---------------------------------------------------------------------------
# Tests — Embedded captioning workflow
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
class TestEmbeddedCaptioningWorkflow:
    """Test run_image_captioning.py with mocked vLLM engine."""

    def test_end_to_end(self, ray_cluster_with_mock_vllm, tmp_path) -> None:
        """Full pipeline: LanceSource -> EmbeddedLLMOperator -> LanceSink."""
        input_path = str(tmp_path / "input.lance")
        output_path = str(tmp_path / "output.lance")

        _create_test_lance_dataset(input_path, NUM_TEST_ROWS, TEST_IMAGE_FIELD)

        from workflows.run_image_captioning import run_workflow

        asyncio.run(
            run_workflow(
                input_path=input_path,
                output_path=output_path,
                model_source="mock-model",
                tensor_parallel_size=1,
                max_model_len=1024,
                gpu_memory_utilization=0.9,
                image_field=TEST_IMAGE_FIELD,
                split_size=NUM_TEST_ROWS,
            )
        )

        # Verify output
        assert Path(output_path).exists(), "Output Lance dataset not created"

        result = lance.dataset(output_path).to_table()
        assert result.num_rows == NUM_TEST_ROWS, (
            f"Expected {NUM_TEST_ROWS} rows, got {result.num_rows}"
        )
        assert "caption" in result.column_names, (
            f"Missing 'caption' column. Columns: {result.column_names}"
        )
        assert "id" in result.column_names, "Original 'id' column lost"

        captions = result.column("caption").to_pylist()
        for i, cap in enumerate(captions):
            assert isinstance(cap, str) and len(cap) > 0, (
                f"Row {i}: caption is empty or not a string: {cap!r}"
            )


# ---------------------------------------------------------------------------
# Tests — External captioning workflow
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
@pytest.mark.asyncio
class TestExternalCaptioningWorkflow:
    """Test run_image_captioning_external using the real serve layer."""

    async def test_end_to_end(self, ray_cluster_serve, tmp_path) -> None:
        """Deploy model via serve layer, then run the imported workflow."""
        from _internal.serve.config import AutoscaleConfig, ModelConfig
        from _internal.serve.manager import ModelServiceManager

        _ManagerCls = ModelServiceManager.__ray_metadata__.modified_class
        mgr = _ManagerCls(AutoscaleConfig(enabled=False))

        config = ModelConfig(
            model_id="test_caption",
            model_source="fake/caption_model",
            backend="fake",
            tensor_parallel_size=1,
            min_workers=1,
            max_workers=2,
            worker_resources={"num_gpus": 1, "num_cpus": 0},
        )
        await mgr.deploy_model(config, wait_ready=True)

        try:
            input_path = str(tmp_path / "input.lance")
            output_path = str(tmp_path / "output.lance")

            _create_test_lance_dataset(input_path, NUM_TEST_ROWS, TEST_IMAGE_FIELD)

            from workflows.run_image_captioning_external import run_workflow

            await run_workflow(
                input_path=input_path,
                output_path=output_path,
                model_id="test_caption",
                registry=mgr._registry,
                image_field=TEST_IMAGE_FIELD,
                split_size=NUM_TEST_ROWS,
            )

            # Verify output
            assert Path(output_path).exists(), "Output Lance dataset not created"

            result = lance.dataset(output_path).to_table()
            assert result.num_rows == NUM_TEST_ROWS, (
                f"Expected {NUM_TEST_ROWS} rows, got {result.num_rows}"
            )
            assert "caption" in result.column_names, (
                f"Missing 'caption' column. Columns: {result.column_names}"
            )
            assert "id" in result.column_names, "Original 'id' column lost"

            captions = result.column("caption").to_pylist()
            for i, cap in enumerate(captions):
                assert isinstance(cap, str) and len(cap) > 0, (
                    f"Row {i}: caption is empty or not a string: {cap!r}"
                )
        finally:
            await mgr.shutdown()
