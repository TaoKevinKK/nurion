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

"""Example: MinHash deduplication workflow with Union-Find Service.

This example demonstrates:
1. Creating test documents with near-duplicates
2. Deploying a Union-Find Service cluster
3. Running the MinHash dedup pipeline (Encode -> BucketUnion -> Filter)
4. Verifying deduplication results

Architecture:
- Union-Find Service: long-lived Ray actors holding cluster state
- Pipeline operators are stateless; UF state survives operator OOM
- 3-stage pipeline: MinHashEncoder -> BucketUnion -> DedupFilter

Run:
    cd solstice
    python examples/minhash_dedup_example.py
"""

import asyncio
import logging
import tempfile
from pathlib import Path

import lance
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_test_data(path: str) -> int:
    """Create test documents with near-duplicates."""
    documents = [
        # Group 1: Near-duplicates (fox)
        {
            "doc_id": "doc_001",
            "text": "The quick brown fox jumps over the lazy dog. Classic pangram.",
        },
        {
            "doc_id": "doc_002",
            "text": "The quick brown fox jumps over the lazy dog! A classic pangram.",
        },
        # Group 2: Near-duplicates (ML)
        {
            "doc_id": "doc_003",
            "text": "Machine learning is AI that enables computers to learn from data.",
        },
        {
            "doc_id": "doc_004",
            "text": "Machine learning is AI enabling computers to learn from data.",
        },
        # Group 3: Unique
        {"doc_id": "doc_005", "text": "Python is a high-level programming language."},
        {"doc_id": "doc_006", "text": "Data engineering builds systems for data at scale."},
        {"doc_id": "doc_007", "text": "Cloud computing provides on-demand resources."},
    ]

    table = pa.Table.from_pylist(documents)
    lance.write_dataset(table, path, mode="overwrite")
    logger.info(f"Created {len(documents)} test documents at {path}")
    return len(documents)


async def run_example():
    """Run the MinHash dedup workflow."""
    from workflows.minhash_dedup import run_dedup_pipeline

    logger.info("=" * 60)
    logger.info("MinHash Deduplication Example (Union-Find Service)")
    logger.info("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = str(Path(tmpdir) / "input.lance")
        output_path = str(Path(tmpdir) / "output.lance")

        # Step 1: Create test data
        logger.info("\n[Step 1] Creating test data with duplicates...")
        total_docs = create_test_data(input_path)

        # Step 2: Run full pipeline
        logger.info("\n[Step 2] Running MinHash dedup pipeline...")
        config = {
            "input": input_path,
            "output": output_path,
            "content_column": "text",
            "id_column": "doc_id",
            "num_buckets": 8,
            "hashes_per_bucket": 4,
            "ngram_size": 3,
            "num_shards": 4,
            "workqueue_db_path": "memory://",
            "output_format": "lance",
            "num_partitions": 4,
        }

        result = await run_dedup_pipeline("minhash_dedup_example", config)

        # Step 3: Verify results
        logger.info("\n[Step 3] Verifying results...")
        if Path(output_path).exists():
            result_ds = lance.dataset(output_path)
            result_count = result_ds.count_rows()
            logger.info(f"Input: {total_docs} documents")
            logger.info(f"Output: {result_count} documents")
            logger.info(f"Cluster mappings: {result['cluster_mappings']}")
            logger.info(f"Cross-shard resolution: {result['cross_shard_resolution']}")
            logger.info(f"Duration: {result['duration_s']:.1f}s")

            # With near-duplicates, we expect fewer output docs
            # Group 1 (2 docs) -> 1, Group 2 (2 docs) -> 1, Unique (3 docs) -> 3
            expected = 5
            if result_count <= expected + 1:
                logger.info(f"Deduplication successful (expected ~{expected})")
            else:
                logger.warning(f"More docs than expected ({result_count} > {expected})")
        else:
            logger.warning("Output not found - pipeline may have failed")

        logger.info("\n" + "=" * 60)
        logger.info("Example completed!")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
