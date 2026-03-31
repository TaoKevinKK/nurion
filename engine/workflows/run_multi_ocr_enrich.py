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

"""Multi-model OCR + Fusion workflow.

Pipeline: Lance Source → Multi-OCR Fusion (transform) → Lance Sink

Three models are served via nurion.serve (ModelServiceManager) on vLLM:
1. DeepSeek-OCR  (~7B, TP=1)       — OCR with grounding bbox output
2. HunyuanOCR   (1B, TP=1)         — OCR with coordinate bbox output
3. Qwen3-VL-235B (235B, TP=8, fp8) — Fuse & enrich both OCR results

OCR models use raw prompt format via /v1/completions (no chat template).
Fusion model uses /v1/chat/completions with vision messages.

Output columns:
    - deepseek_ocr_raw:     Raw DeepSeek output (<|ref|>...<|det|> grounding)
    - deepseek_ocr_json:    Parsed JSON [{"bbox_2d": [...], "text_content": "..."}]
    - hunyuan_ocr_raw:      Raw HunyuanOCR output (text(x1,y1),(x2,y2))
    - hunyuan_ocr_json:     Parsed JSON (same unified format)
    - qwen_ocr_caption_v1:  Raw Qwen3-VL fusion output (for debugging)
    - qwen_ocr_json:        Validated JSON with enriched metadata
                            (font-family, font-style, color, language, decorative)

Serve modes:
    --detached    Deploy models as detached actors (survive job exit)
    --connect     Reuse already-running detached models (skip deployment)
    --shutdown-models  Tear down detached models and exit

Usage:
    # Deploy detached models (slow, one-time)
    python workflows/run_multi_ocr_enrich.py --detached \\
        --input s3://bucket/images.lance --output s3://bucket/out.lance

    # Reuse models (fast iteration)
    python workflows/run_multi_ocr_enrich.py --connect \\
        --input s3://bucket/images.lance --output s3://bucket/out.lance

    # Tear down models
    python workflows/run_multi_ocr_enrich.py --shutdown-models
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import re
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Optional

import pyarrow as pa
import ray
from PIL import Image

from nurion import (
    ModelConfig,
    ModelServiceManager,
    Operator,
    OperatorConfig,
    OperatorRuntime,
    Split,
    SplitPayload,
    create_manager,
    operator,
)

from _internal.operators.llm.client import ChatCompletionsClient

# ---------------------------------------------------------------------------
# Image preprocessing
# ---------------------------------------------------------------------------

MAX_SHORT_SIDE = 1024
MAX_LONG_SIDE = 2048
MAX_RATIO_FILTER = 10


def resize_image(image_bytes: bytes) -> Optional[bytes]:
    """Resize image (short side <= 1024, long side <= 2048), return JPEG bytes."""
    try:
        Image.MAX_IMAGE_PIXELS = None
        with BytesIO(image_bytes) as bio:
            img = Image.open(bio).convert("RGB")
            w, h = img.size
            img.info = {}

            short_side = min(w, h)
            long_side = max(w, h)
            if short_side > 0 and long_side / short_side > MAX_RATIO_FILTER:
                return None

            if short_side <= MAX_SHORT_SIDE and long_side <= MAX_LONG_SIDE:
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=90)
                return buf.getvalue()

            scale = min(MAX_SHORT_SIDE / short_side, MAX_LONG_SIDE / long_side)
            img = img.resize(
                (int(round(w * scale)), int(round(h * scale))),
                Image.LANCZOS,
            )
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=90)
            return buf.getvalue()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# OCR output parsers -> unified [{"bbox_2d": [...], "text_content": "..."}]
# ---------------------------------------------------------------------------


def parse_deepseek_ocr(ocr_text: str) -> list[dict]:
    """Parse DeepSeek OCR: <|ref|>TEXT<|/ref|><|det|>[[x1,y1,x2,y2]]<|/det|>"""
    if not ocr_text:
        return []
    results = []
    pattern = (
        r"<\|ref\|>(.*?)<\|/ref\|><\|det\|>\[\[(\d+)[,\s]+(\d+)[,\s]+(\d+)[,\s]+(\d+)\]\]<\|/det\|>"
    )
    for m in re.finditer(pattern, ocr_text):
        coords = [max(0, min(999, int(m.group(i)))) for i in range(2, 6)]
        results.append({"bbox_2d": coords, "text_content": m.group(1)})
    return results


def parse_hunyuan_ocr(ocr_text: str) -> list[dict]:
    """Parse HunyuanOCR: TEXT(x1,y1),(x2,y2)"""
    if not ocr_text:
        return []
    results = []
    pattern = r"([^\(\)]+?)\((\d+),(\d+)\),\((\d+),(\d+)\)"
    for m in re.finditer(pattern, ocr_text):
        coords = [max(0, min(999, int(m.group(i)))) for i in range(2, 6)]
        results.append({"bbox_2d": coords, "text_content": m.group(1).strip()})
    return results


def parse_json_response(response: str) -> list[dict]:
    """Parse JSON array from model response, stripping markdown fences."""
    if not response:
        return []

    cleaned = response.strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        result = json.loads(cleaned)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    try:
        match = re.search(r"\[[\s\S]*\]", cleaned)
        if match:
            result = json.loads(match.group(0))
            if isinstance(result, list):
                return result
    except json.JSONDecodeError:
        pass

    return []


# ---------------------------------------------------------------------------
# Prompts & model-specific formats
# ---------------------------------------------------------------------------

# DeepSeek-OCR: raw prompt with grounding token, no chat template
_GROUNDING = "<" + "|grounding" + "|>"
DEEPSEEK_PROMPT = f"<image>\n{_GROUNDING}OCR this image."
DEEPSEEK_SAMPLING_PARAMS: dict[str, Any] = {
    "temperature": 0.0,
    # max_model_len=8192, reserve 1024 for input tokens (image + prompt)
    "max_tokens": 7168,
    "skip_special_tokens": False,
    # NGram logit processor args to prevent repetition in table OCR
    "extra_args": {
        "ngram_size": 30,
        "window_size": 90,
        "whitelist_token_ids": [128821, 128822],  # whitelist: <td>, </td>
    },
}

# Passthrough chat template: outputs text content as-is, no formatting.
# Used for HunyuanOCR which needs special Unicode tokens preserved.
PASSTHROUGH_CHAT_TEMPLATE = (
    "{% for message in messages %}"
    "{% if message.content is string %}{{ message.content }}"
    "{% else %}{% for item in message.content %}"
    "{% if item.type == 'text' %}{{ item.text }}{% endif %}"
    "{% endfor %}{% endif %}"
    "{% endfor %}"
)

# HunyuanOCR: raw prompt with special Unicode tokens, no chat template
_HUNYUAN_IMAGE_PLACEHOLDER = (
    "<\uff5chy_place\u2581holder\u2581no\u2581100\uff5c>"
    "<\uff5chy_place\u2581holder\u2581no\u2581102\uff5c>"
    "<\uff5chy_place\u2581holder\u2581no\u2581101\uff5c>"
)
HUNYUAN_PROMPT = (
    f"<\uff5chy_begin\u2581of\u2581sentence\uff5c>{_HUNYUAN_IMAGE_PLACEHOLDER}"
    "\u691c\u51fa\u5e76\u8bc6\u522b\u56fe\u7247\u4e2d\u7684\u6587\u5b57\uff0c\u5c06\u6587\u672c\u5750\u6807\u683c\u5f0f\u5316\u8f93\u51fa\u3002<\uff5chy_User\uff5c>"
)
HUNYUAN_SAMPLING_PARAMS: dict[str, Any] = {
    "temperature": 0.0,
    # max_model_len=8192, reserve 1024 for input tokens (image + prompt)
    "max_tokens": 7168,
    "top_k": 1,
    "repetition_penalty": 1.0,
}

# Qwen3-VL fusion: standard chat completions with vision
FUSION_SAMPLING_PARAMS: dict[str, Any] = {
    "temperature": 0.7,
    "top_p": 0.8,
    "top_k": 20,
    # Actual output is ~2800 tokens; 8192 provides 3x headroom.
    # max_model_len=20480, images can use up to ~7K input tokens.
    "max_tokens": 8192,
}

FUSION_PROMPT = """You are an expert OCR reviewer. You have the original image and OCR results from TWO different OCR systems.

IMAGE: [Shown above]

OCR SYSTEM A:
```json
{ocr_a_json}
```

OCR SYSTEM B:
```json
{ocr_b_json}
```

Your task is to produce the FINAL textbox list by:

1. **ACCEPT**: Keep a textbox from one OCR system if it's correct
2. **REJECT**: Skip textboxes that are:
   - Duplicates (same text appearing in both systems)
   - False positives (not actual text in the image)
   - Illegible or purely decorative elements that aren't readable text
3. **COMBINE**: Merge multiple textboxes into ONE when they form a logical unit:
   - Multi-line paragraphs (OCR often splits each line)
   - Title + subtitle pairs
   - Related text that should stay together

---

## MERGING RULES (IMPORTANT)

**DO NOT MERGE** textboxes if they differ in ANY of these properties:
- `font-family` (e.g., sans-serif vs serif)
- `color` (e.g., white vs black)
- `decorative` (e.g., normal text vs decorative/stylized text)

**CAN MERGE** textboxes even if they differ in:
- `font-style` (bold vs regular vs italic) -> merged result uses `"font-style": "mixed"` and markdown in text
- `language` -> merged result lists all languages

When combining bounding boxes:
- Take MINIMUM x1, y1 and MAXIMUM x2, y2
- Join text with `\\n` for multi-line content

---

## OUTPUT FORMAT

Output a JSON array with one entry per line:

```json
[
    {{"bbox_2d": [709, 22, 976, 82], "text_content": "TITLE", "font-family": "sans-serif", "font-style": "bold", "decorative": false, "color": "black", "language": ["en"]}},
    {{"bbox_2d": [144, 117, 867, 152], "text_content": "Line 1\\nLine 2\\nLine 3", "font-family": "serif", "font-style": "regular", "decorative": false, "color": "white", "language": ["en"]}},
    {{"bbox_2d": [369, 148, 633, 171], "text_content": "Regular and **bold** and *italic* text", "font-family": "sans-serif", "font-style": "mixed", "decorative": false, "color": "blue", "language": ["en", "zh"]}}
]
```

### Field Specifications:

**bbox_2d**: `[x1, y1, x2, y2]` in 0-1000 normalized scale

**text_content**: The text content
- Use `\\n` for multi-line text within a single textbox
- When `font-style` is `"mixed"`, use markdown: `**bold**`, `*italic*`, `***bold italic***` to mark styled portions

**font-family**: The typeface category
- **For English text**: `"sans-serif"`, `"serif"`, `"monospace"`, `"cursive"`, `"fantasy"`, `"display"`, ...
- **For Chinese text**: `"黑体"`, `"宋体"`, `"楷体"`, `"仿宋"`, `"行书"`, `"隶书"`, `"艺术字"`, ...

**font-style**: `"regular"`, `"bold"`, `"italic"`, `"bold italic"`, `"mixed"`

**decorative**: `true` or `false`

**color**: Simple color names: `"white"`, `"black"`, `"red"`, `"blue"`, `"yellow"`, `"green"`, `"orange"`, `"pink"`, `"purple"`, `"gray"`, `"gold"`, `"silver"`

**language**: Array of language codes: `["en"]`, `["zh"]`, `["en", "zh"]`

---

## CRITICAL RULES

1. **NO DUPLICATES** - each piece of text appears in exactly ONE textbox
2. **PRESERVE ORDER** - output textboxes in reading order (top-to-bottom, left-to-right)
3. **MERGE WISELY** - combine related text ONLY if font-family, color, and decorative match
4. **STYLE FLEXIBILITY** - different font-style CAN be merged using "mixed" + markdown
5. **LANGUAGE FLEXIBILITY** - different languages CAN be merged into one textbox

Output ONLY a valid JSON array with no additional text."""

# ---------------------------------------------------------------------------
# Model IDs (used for both serve registration and ModelClient discovery)
# ---------------------------------------------------------------------------

DEEPSEEK_OCR_MODEL_ID = "deepseek-ai/DeepSeek-OCR"
HUANYUAN_OCR_MODEL_ID = "tencent/HunyuanOCR"
ENRICH_MODEL_ID = "Qwen/Qwen3-VL-235B-A22B-Instruct"

# ---------------------------------------------------------------------------
# Operator
# ---------------------------------------------------------------------------


@dataclass
class MultiOCRFusionConfig(OperatorConfig):
    """Config for multi-model OCR + fusion operator."""

    registry: Optional[ray.actor.ActorHandle] = None
    image_field: str = "image"

    deepseek_model_id: str = DEEPSEEK_OCR_MODEL_ID
    huanyuan_model_id: str = HUANYUAN_OCR_MODEL_ID
    enrich_model_id: str = ENRICH_MODEL_ID

    fusion_prompt: str = FUSION_PROMPT

    timeout: float = 600.0  # High concurrency increases per-request latency
    max_retries: int = 3


@operator(MultiOCRFusionConfig)
class MultiOCRFusionOperator(Operator):
    """Calls two OCR models and one fusion model per image."""

    def __init__(self, config: MultiOCRFusionConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self._cfg = config
        self._client: Optional[ChatCompletionsClient] = None

    def _get_client(self) -> ChatCompletionsClient:
        if self._client is None:
            assert self._cfg.registry is not None, "registry must be set"
            self._client = ChatCompletionsClient(
                registry=self._cfg.registry,
                timeout=self._cfg.timeout,
                max_retries=self._cfg.max_retries,
            )
        return self._client

    # ----- OCR calls (chat completions with vision) -----

    async def _call_deepseek_ocr(self, image_b64: str) -> str:
        body: dict[str, Any] = {
            "model": self._cfg.deepseek_model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"{_GROUNDING}OCR this image."},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                        },
                    ],
                }
            ],
            **DEEPSEEK_SAMPLING_PARAMS,
        }
        return await self._get_client().generate(self._cfg.deepseek_model_id, body)

    async def _call_hunyuan_ocr(self, image_b64: str) -> str:
        body: dict[str, Any] = {
            "model": self._cfg.huanyuan_model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        # Full prompt with special Unicode tokens for coordinate output
                        {"type": "text", "text": HUNYUAN_PROMPT},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                        },
                    ],
                }
            ],
            # Passthrough template preserves special tokens (no chat formatting)
            "chat_template": PASSTHROUGH_CHAT_TEMPLATE,
            **HUNYUAN_SAMPLING_PARAMS,
        }
        return await self._get_client().generate(self._cfg.huanyuan_model_id, body)

    # ----- Fusion call (chat completions with vision) -----

    async def _call_fusion(self, image_b64: str, deepseek_json: str, hunyuan_json: str) -> str:
        prompt_text = self._cfg.fusion_prompt.format(
            ocr_a_json=deepseek_json,
            ocr_b_json=hunyuan_json,
        )
        body: dict[str, Any] = {
            "model": self._cfg.enrich_model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt_text},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_b64}",
                                "detail": "high",
                            },
                        },
                    ],
                }
            ],
            **FUSION_SAMPLING_PARAMS,
        }
        return await self._get_client().generate(self._cfg.enrich_model_id, body)

    # ----- Per-image processing -----

    async def _process_one(self, image_bytes: bytes) -> Optional[dict[str, str]]:
        resized = resize_image(image_bytes)
        if resized is None:
            return None

        image_b64 = base64.b64encode(resized).decode("utf-8")

        deepseek_raw, hunyuan_raw = await asyncio.gather(
            self._call_deepseek_ocr(image_b64),
            self._call_hunyuan_ocr(image_b64),
        )

        deepseek_parsed = parse_deepseek_ocr(deepseek_raw)
        hunyuan_parsed = parse_hunyuan_ocr(hunyuan_raw)
        deepseek_json = json.dumps(deepseek_parsed, ensure_ascii=False)
        hunyuan_json = json.dumps(hunyuan_parsed, ensure_ascii=False)

        fusion_raw = await self._call_fusion(image_b64, deepseek_json, hunyuan_json)
        fusion_parsed = parse_json_response(fusion_raw)
        fusion_json = json.dumps(fusion_parsed, ensure_ascii=False)

        return {
            "deepseek_ocr_caption_v1": deepseek_raw,
            "deepseek_ocr_json_v1": deepseek_json,
            "hunyuan_ocr_caption_v1": hunyuan_raw,
            "hunyuan_ocr_json_v1": hunyuan_json,
            "qwen_ocr_caption_v1": fusion_raw,
            "qwen_ocr_json_v1": fusion_json,
        }

    # ----- Operator interface -----

    _OUTPUT_COLS = (
        "deepseek_ocr_caption_v1",
        "deepseek_ocr_json_v1",
        "hunyuan_ocr_caption_v1",
        "hunyuan_ocr_json_v1",
        "qwen_ocr_caption_v1",
        "qwen_ocr_json_v1",
    )
    _EMPTY = {c: ("[]" if c.endswith("_json_v1") else "") for c in _OUTPUT_COLS}

    async def process_split(
        self,
        split: Split,
        payload: Optional[SplitPayload] = None,
    ) -> Optional[SplitPayload]:
        if payload is None:
            return None

        table = payload.to_table()

        # Preserve _rowid as a regular column for output traceability
        if "_rowid" in table.column_names:
            table = table.append_column("original_row_id", table["_rowid"].cast(pa.int64()))

        images: list[bytes] = table[self._cfg.image_field].to_pylist()

        # Process all images in the split concurrently
        all_results: list[Optional[dict[str, str]]] = list(
            await asyncio.gather(*(self._process_one(img) for img in images))
        )

        filled = [r if r is not None else self._EMPTY for r in all_results]
        for col in self._OUTPUT_COLS:
            table = table.append_column(col, pa.array([r[col] for r in filled], type=pa.string()))

        return SplitPayload(
            data=table,
            split_id=f"{split.split_id}_{self.worker_id}",
        )

    def close(self) -> None:
        if self._client is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._client.close())
            except RuntimeError:
                pass
            self._client = None
        super().close()


# ---------------------------------------------------------------------------
# Model deployment
# ---------------------------------------------------------------------------


async def deploy_models(
    deepseek_source: str,
    huanyuan_source: str,
    enrich_source: str,
    deepseek_tp: int,
    huanyuan_tp: int,
    enrich_tp: int,
    gpu_mem_util: float,
    detached: bool = False,
) -> ray.actor.ActorHandle:
    """Deploy three models via ModelServiceManager.

    Returns the Manager actor handle. The allocator automatically handles
    anti-fragmentation: large models (TP=8) get full nodes, small models
    (TP=1, gpu_memory_utilization=0.3 -> auto num_gpus=0.5) pack onto
    remaining GPUs.
    """
    logger = logging.getLogger(__name__)
    manager = create_manager(detached=detached)

    models = [
        ModelConfig(
            model_id=ENRICH_MODEL_ID,
            model_source=enrich_source,
            tensor_parallel_size=enrich_tp,
            max_model_len=32768,
            gpu_memory_utilization=gpu_mem_util,
            quantization="fp8",
            trust_remote_code=True,
            min_workers=3,
            max_workers=4,
            extra_engine_kwargs={
                "kv_cache_dtype": "fp8_e4m3",
                "enable_chunked_prefill": True,
                "enable_prefix_caching": True,
            },
        ),
        # DeepSeek-OCR: gpu_memory_utilization=0.3 -> auto num_gpus=0.5
        ModelConfig(
            model_id=DEEPSEEK_OCR_MODEL_ID,
            model_source=deepseek_source,
            tensor_parallel_size=deepseek_tp,
            max_model_len=8192,
            gpu_memory_utilization=0.3,
            trust_remote_code=True,
            min_workers=2,
            max_workers=4,
            extra_engine_kwargs={
                "max_num_seqs": 64,
                "enable_chunked_prefill": True,
                "max_num_batched_tokens": 16384,
                "enable_prefix_caching": False,
                "mm_processor_cache_gb": 0,
                "enforce_eager": True,
                "limit_mm_per_prompt": {"image": 1},
            },
        ),
        # HunyuanOCR: gpu_memory_utilization=0.3 -> auto num_gpus=0.5
        ModelConfig(
            model_id=HUANYUAN_OCR_MODEL_ID,
            model_source=huanyuan_source,
            tensor_parallel_size=huanyuan_tp,
            max_model_len=8192,
            gpu_memory_utilization=0.3,
            trust_remote_code=True,
            min_workers=2,
            max_workers=4,
            extra_engine_kwargs={
                "max_num_seqs": 64,
                "enable_chunked_prefill": True,
                "max_num_batched_tokens": 16384,
                "enable_prefix_caching": False,
                "mm_processor_cache_gb": 0,
                "enforce_eager": True,
                "limit_mm_per_prompt": {"image": 1},
            },
        ),
    ]

    # deploy_model with a list handles ordering automatically:
    # large models (TP=8) sequentially first, then small models in parallel
    results = await manager.deploy_model.remote(models, wait_ready=True, timeout=1800.0)
    for r in results:
        if isinstance(r, dict) and r.get("status") == "ready":
            logger.info(
                f"  -> {r['model_id']} ready in {r['duration_s']:.1f}s, endpoints={r['endpoints']}"
            )

    return manager


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


async def run_workflow(
    input_path: str,
    output_path: str,
    image_field: str,
    split_size: int,
    parallelism: int,
    registry: ray.actor.ActorHandle,
) -> None:
    """Build and run the multi-OCR fusion pipeline."""
    from nurion import Job, JobConfig, LanceSinkConfig, LanceTableSourceConfig, Stage

    logger = logging.getLogger(__name__)

    job = Job(
        job_id="multi_ocr_fusion",
        config=JobConfig(anvil_db_path="memory://"),
    )

    source = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=split_size,
            columns=[image_field],  # Only read image column, avoid schema conflicts
            # _rowid is auto-included by Lance (with_row_id=True)
        ),
        parallelism=1,
    )

    transform = Stage(
        stage_id="transform",
        operator_config=MultiOCRFusionConfig(
            registry=registry,
            image_field=image_field,
        ),
        parallelism=parallelism,
    )

    sink = Stage(
        stage_id="sink",
        operator_config=LanceSinkConfig(
            table_path=output_path,
            mode="overwrite",
        ),
        parallelism=1,
    )

    job.add_stage(source)
    job.add_stage(transform, upstream_stages=["source"])
    job.add_stage(sink, upstream_stages=["transform"])

    logger.info("Starting multi-OCR fusion pipeline")
    logger.info(f"  Input:  {input_path}")
    logger.info(f"  Output: {output_path}")

    runner = job.create_ray_runner()
    await runner.run()

    logger.info("Pipeline completed!")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main_async(args: argparse.Namespace) -> None:
    """Deploy or connect to models, run pipeline."""
    logger = logging.getLogger(__name__)

    # Shutdown mode: tear down detached models and exit
    if args.shutdown_models:
        logger.info("Shutting down detached models...")
        manager = ModelServiceManager.connect()
        await manager.shutdown.remote()
        logger.info("All models shut down, GPUs released.")
        return

    # Validate required args for pipeline modes
    if not args.input or not args.output:
        raise ValueError("--input and --output are required for pipeline runs")

    # Connect mode: reuse existing detached models
    if args.connect:
        logger.info("=" * 60)
        logger.info("Connecting to existing detached models")
        logger.info("=" * 60)

        manager = ModelServiceManager.connect()
        models = ray.get(manager.list_models.remote())
        logger.info(f"Connected to {len(models)} model(s): {models}")

        logger.info("=" * 60)
        logger.info("Running OCR + fusion pipeline")
        logger.info("=" * 60)

        await run_workflow(
            input_path=args.input,
            output_path=args.output,
            image_field=args.image_field,
            split_size=args.split_size,
            parallelism=args.parallelism,
            registry=ray.get(manager.get_registry.remote()),
        )
        logger.info("Pipeline done. Models still running (detached).")

    else:
        # Deploy mode (default or --detached)
        detached = args.detached
        logger.info("=" * 60)
        logger.info(f"STEP 1: Deploying inference models (detached={detached})")
        logger.info("=" * 60)

        manager = await deploy_models(
            deepseek_source=args.deepseek_source,
            huanyuan_source=args.huanyuan_source,
            enrich_source=args.enrich_source,
            deepseek_tp=args.deepseek_tp,
            huanyuan_tp=args.huanyuan_tp,
            enrich_tp=args.enrich_tp,
            gpu_mem_util=args.gpu_memory_utilization,
            detached=detached,
        )

        try:
            logger.info("=" * 60)
            logger.info("STEP 2: Running OCR + fusion pipeline")
            logger.info("=" * 60)

            await run_workflow(
                input_path=args.input,
                output_path=args.output,
                image_field=args.image_field,
                split_size=args.split_size,
                parallelism=args.parallelism,
                registry=ray.get(manager.get_registry.remote()),
            )
        finally:
            if detached:
                logger.info("Pipeline done. Models still running (detached).")
            else:
                logger.info("Shutting down serve layer...")
                await manager.shutdown.remote()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Multi-model OCR + Qwen3-VL fusion pipeline (engine-served)"
    )

    parser.add_argument("--input", default="", help="Input Lance table path")
    parser.add_argument("--output", default="", help="Output Lance table path")
    parser.add_argument("--image-field", default="image", help="Column containing image bytes")

    parser.add_argument(
        "--deepseek-source",
        default="deepseek-ai/DeepSeek-OCR",
        help="DeepSeek-OCR model source (default: DeepSeek-OCR, ~7B)",
    )
    parser.add_argument(
        "--huanyuan-source",
        default="tencent/HunyuanOCR",
        help="HunyuanOCR model source (default: 1B OCR expert model)",
    )
    parser.add_argument(
        "--enrich-source",
        default="Qwen/Qwen3-VL-235B-A22B-Instruct",
        help="Qwen3-VL fusion model source",
    )

    parser.add_argument("--deepseek-tp", type=int, default=1, help="TP for DeepSeek-OCR")
    parser.add_argument("--huanyuan-tp", type=int, default=1, help="TP for HunyuanOCR")
    parser.add_argument("--enrich-tp", type=int, default=8, help="TP for Qwen3-VL-235B")

    parser.add_argument(
        "--gpu-memory-utilization",
        type=float,
        default=0.9,
        help="Fraction of GPU memory to use",
    )

    parser.add_argument(
        "--split-size", type=int, default=4, help="Rows per split (small for pipeline overlap)"
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=64,
        help="Transform workers (many small splits for pipeline overlap)",
    )

    # Serve mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--detached",
        action="store_true",
        help="Deploy models as detached actors (survive job exit).",
    )
    mode_group.add_argument(
        "--connect",
        action="store_true",
        help="Connect to already-running detached models (skip deployment).",
    )
    mode_group.add_argument(
        "--shutdown-models",
        action="store_true",
        help="Shutdown detached models and exit (no pipeline run).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not ray.is_initialized():
        ray.init(address="auto")
    logging.info(f"Connected to Ray cluster: {ray.cluster_resources()}")

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
