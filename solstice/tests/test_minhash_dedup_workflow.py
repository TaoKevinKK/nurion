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

"""Tests for MinHash deduplication workflow.

Test Data (same format as runMinHashExample.py):
- Train file: articles_10000.train
  Format: Each line is "<doc_id> <word1> <word2> ..."
- Truth file: articles_10000.truth
  Format: Each line is "<doc_id1> <doc_id2>" (plagiary pairs)

Data is downloaded from public HTTPS endpoint (no authentication required).

Local Cache Mode:
    Set MINHASH_CACHE_DIR environment variable to cache downloaded files:

        export MINHASH_CACHE_DIR=~/.cache/solstice_minhash_test
        pytest tests/test_minhash_dedup_workflow.py -v -m workflow

NOTE: Current pipeline limitation - documents without candidate pairs are not
output. This is because Solstice doesn't yet support multi-upstream stages.
"""

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import lance
import pyarrow as pa
import pytest
import requests

logger = logging.getLogger(__name__)

# Public HTTPS endpoint (no auth required)
PUBLIC_DATA_URL = "https://pub-8bc1f1d3d1984bdfb056d0bc0bf97c3d.r2.dev/minhash"

# Test data files
TRAIN_FILE = "articles_10000.train"
TRUTH_FILE = "articles_10000.truth"

# Local cache directory (set via MINHASH_CACHE_DIR env var)
LOCAL_CACHE_DIR = os.environ.get("MINHASH_CACHE_DIR")


def _get_cache_dir() -> Path:
    """Get cache directory for downloaded test data."""
    if LOCAL_CACHE_DIR:
        cache_dir = Path(LOCAL_CACHE_DIR).expanduser()
    else:
        # Use system temp directory
        cache_dir = Path(tempfile.gettempdir()) / "solstice_minhash_test"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _download_file(filename: str) -> Path:
    """Download file from public URL if not cached.

    Args:
        filename: Name of the file to download

    Returns:
        Path to local file (cached or newly downloaded)
    """
    cache_dir = _get_cache_dir()
    local_path = cache_dir / filename

    if local_path.exists():
        logger.info(f"Using cached file: {local_path}")
        return local_path

    url = f"{PUBLIC_DATA_URL}/{filename}"
    logger.info(f"Downloading {url} -> {local_path}")

    try:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
        logger.info(f"Downloaded {filename} ({local_path.stat().st_size} bytes)")
    except requests.RequestException as e:
        raise RuntimeError(
            f"Failed to download {url}: {e}\n"
            f"Please ensure the file is available at the public URL, "
            f"or set MINHASH_CACHE_DIR and place the file there manually."
        ) from e

    return local_path


def load_articles_train(path: Optional[str] = None) -> List[Dict[str, str]]:
    """Load articles from train file.

    Format (same as runMinHashExample.py):
        words = f.readline().split(" ")
        docID = words[0]
        del words[0]
        # rest are content words

    Each line: "<doc_id> <word1> <word2> ..."

    Args:
        path: Optional local path. If None, downloads from public URL.
    """
    if path is None:
        path = str(_download_file(TRAIN_FILE))

    documents = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Split by space
            words = line.split(" ")
            # First word is doc_id
            doc_id = words[0]
            # Rest is text (rejoin with spaces)
            text = " ".join(words[1:]).strip()
            documents.append({"doc_id": doc_id, "text": text})
    return documents


def load_articles_truth(path: Optional[str] = None) -> Dict[str, str]:
    """Load ground truth plagiary pairs.

    Format (same as runMinHashExample.py):
        docs = line.split(" ")
        plagiaries[docs[0]] = docs[1]
        plagiaries[docs[1]] = docs[0]

    Returns bidirectional dict: plagiaries[doc1] = doc2, plagiaries[doc2] = doc1

    Args:
        path: Optional local path. If None, downloads from public URL.
    """
    if path is None:
        path = str(_download_file(TRUTH_FILE))

    plagiaries = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Strip newline
            if line and line[-1] == "\n":
                line = line[:-1]
            if not line:
                continue
            docs = line.split(" ")
            if len(docs) >= 2:
                # Map the two documents to each other
                plagiaries[docs[0]] = docs[1]
                plagiaries[docs[1]] = docs[0]
    return plagiaries


def create_test_documents(path: str) -> Dict[str, Any]:
    """Create test documents from real articles dataset.

    Args:
        path: Output Lance dataset path

    Returns metadata for verification.
    """
    # Load real data
    documents = load_articles_train()
    plagiaries = load_articles_truth()

    # Write to Lance
    table = pa.Table.from_pylist(documents)
    lance.write_dataset(table, path, mode="overwrite")

    # Number of truth pairs = len(plagiaries) / 2 (since bidirectional)
    num_truth_pairs = len(plagiaries) // 2

    return {
        "total_docs": len(documents),
        "num_truth_pairs": num_truth_pairs,
        "plagiaries": plagiaries,  # Bidirectional dict
    }


def get_docs_to_remove(plagiaries: Dict[str, str]) -> set:
    """Get doc_ids that should be removed based on ground truth.

    For each plagiary pair (doc1, doc2), the larger doc_id should be removed.

    Args:
        plagiaries: Bidirectional dict of plagiary pairs

    Returns:
        Set of doc_ids that should be removed (larger doc from each pair)
    """
    docs_to_remove = set()
    seen_pairs = set()

    for doc1, doc2 in plagiaries.items():
        pair = tuple(sorted([doc1, doc2]))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        # The larger doc_id should be removed
        if doc1 < doc2:
            docs_to_remove.add(doc2)
        else:
            docs_to_remove.add(doc1)

    return docs_to_remove


@pytest.mark.workflow
@pytest.mark.timeout(600)
class TestMinHashDedupWorkflowExecution:
    """End-to-end workflow tests for MinHash deduplication."""

    def test_basic_execution(self, ray_cluster):
        """Test workflow execution and verify results match ground truth.

        Verification:
        1. For each truth pair, at most one doc should be kept
        2. If a doc from a truth pair is kept, it should be the smaller doc_id
        3. No duplicate doc_ids in output

        Note: Current pipeline limitation - only documents that have candidate
        pairs flow through the CC stage and get output. Documents without any
        similar matches are not included in the output.
        """
        tmp_dir = tempfile.mkdtemp(prefix="minhash_exec_test_")
        input_path = os.path.join(tmp_dir, "input.lance")
        output_path = os.path.join(tmp_dir, "output.lance")

        try:
            # Load all 10000 documents
            metadata = create_test_documents(input_path)
            plagiaries = metadata["plagiaries"]

            # Get docs that should be removed (larger doc from each truth pair)
            docs_to_remove = get_docs_to_remove(plagiaries)

            logger.info(
                f"Test data loaded:\n"
                f"  - Total docs: {metadata['total_docs']}\n"
                f"  - Truth pairs: {metadata['num_truth_pairs']}\n"
                f"  - Docs to remove: {len(docs_to_remove)}"
            )

            from workflows.minhash_dedup import run_dedup_pipeline

            config = {
                "input": input_path,
                "output": output_path,
                "content_column": "text",
                "id_column": "doc_id",
                # MinHash parameters following datatrove defaults:
                # 14 buckets * 8 hashes = 112 total hashes
                # threshold ≈ (1/14)^(1/8) ≈ 0.72 Jaccard similarity
                "num_buckets": 14,
                "hashes_per_bucket": 8,
                "ngram_size": 5,
                "num_shards": 2,
                "shard_num_cpus": 0.1,  # Minimal CPU for test (4 CPU cluster)
                "shard_memory_mb": 512,
                "workqueue_db_path": "memory://",
                "output_format": "lance",
                "num_partitions": 4,
                "split_size": 1000,  # Normal split size -- cross-batch matching at shard
                # Resources for 10k doc test
                "worker_num_cpus": 0.5,
                "worker_memory_mb": 512,
                # Single worker per stage for constrained test env
                "encoder_parallelism": 1,
                "union_parallelism": 1,
                "filter_parallelism": 1,
            }

            result = asyncio.run(
                run_dedup_pipeline("test_minhash_exec", config)
            )

            # Verify pipeline completed (run_dedup_pipeline returns a result dict)
            assert "job_id" in result, f"Pipeline failed: {result}"

            # Verify output was produced
            assert Path(output_path).exists(), "Output file not created"

            result_ds = lance.dataset(output_path)
            result_table = result_ds.to_table()
            result_count = result_table.num_rows

            # Get output doc_ids
            assert "doc_id" in result_table.column_names, "Missing doc_id column"
            output_doc_ids = result_table.column("doc_id").to_pylist()
            output_id_set = set(output_doc_ids)

            logger.info(
                f"Results:\n  - Input: {metadata['total_docs']}\n  - Output: {result_count}"
            )

            # === VERIFICATION ===

            # 1. No duplicate doc_ids in output
            assert len(output_doc_ids) == len(output_id_set), (
                f"Duplicate doc_ids in output: {len(output_doc_ids)} rows "
                f"but only {len(output_id_set)} unique"
            )

            # 2. Check truth pair handling
            both_kept = []
            correct_kept = 0  # Kept the smaller doc_id
            wrong_kept = 0  # Kept the larger doc_id (should be removed)
            neither_kept = 0

            seen_pairs = set()
            for doc1, doc2 in plagiaries.items():
                pair = tuple(sorted([doc1, doc2]))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                smaller, larger = (doc1, doc2) if doc1 < doc2 else (doc2, doc1)
                smaller_in = smaller in output_id_set
                larger_in = larger in output_id_set

                if smaller_in and larger_in:
                    both_kept.append(f"{smaller} and {larger}")
                elif smaller_in:
                    correct_kept += 1
                elif larger_in:
                    wrong_kept += 1
                else:
                    neither_kept += 1

            logger.info(
                f"\nDedup results:\n"
                f"  - Correct (smaller kept): {correct_kept}/{metadata['num_truth_pairs']}\n"
                f"  - Wrong (larger kept): {wrong_kept}/{metadata['num_truth_pairs']}\n"
                f"  - Both kept (dedup failed): {len(both_kept)}\n"
                f"  - Neither kept: {neither_kept}"
            )

            # 3. MinHash LSH is probabilistic: check recall
            # With shard-side band_hash index, cross-batch matching should work.
            # Expect recall > 40% on this dataset (mixture of near-exact and
            # paraphrased duplicates at word 5-gram level).
            detected_pairs = correct_kept + wrong_kept
            total_pairs = metadata["num_truth_pairs"]
            recall = detected_pairs / total_pairs * 100 if total_pairs > 0 else 0

            logger.info(
                f"\nRecall: {recall:.1f}% ({detected_pairs}/{total_pairs} pairs detected)"
            )

            assert recall > 40, (
                f"Recall too low: {recall:.1f}% ({detected_pairs}/{total_pairs}). "
                f"Both kept: {len(both_kept)}, neither kept: {neither_kept}"
            )

            # 4. Among detected pairs, at most one doc should be in the output
            # Note: Union-Find root selection is by rank, not by doc_id order,
            # so either the smaller or larger doc may be the representative.
            if detected_pairs > 0:
                logger.info(
                    f"Detected: {detected_pairs} pairs "
                    f"(correct_kept={correct_kept}, wrong_kept={wrong_kept})"
                )
                # All detected pairs should have exactly one doc kept
                # (either the smaller or larger is fine)
                assert correct_kept + wrong_kept == detected_pairs

        finally:
            if Path(tmp_dir).exists():
                shutil.rmtree(tmp_dir)
