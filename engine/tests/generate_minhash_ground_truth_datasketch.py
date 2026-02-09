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

"""Generate ground truth using datasketch library (industry standard MinHash).

This script uses datasketch's MinHash and MinHashLSH implementations to generate
a reference ground truth for the MinHash deduplication test. This provides an
independent validation of our custom implementation.

Usage:
    cd engine
    python tests/generate_minhash_ground_truth_datasketch.py
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Set

from datasketch import MinHash, MinHashLSH

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.test_minhash_dedup_workflow import (
    load_articles_train,
    load_articles_truth,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def tokenize_ngrams(text: str, n: int = 5) -> list[str]:
    """Tokenize text into word-level n-grams (same as our implementation)."""
    words = text.lower().split()
    if len(words) < n:
        return [" ".join(words)] if words else []
    return [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]


def create_minhash(text: str, num_perm: int = 112, ngram_size: int = 5, seed: int = 42) -> MinHash:
    """Create MinHash signature using datasketch library.
    
    Args:
        text: Document text
        num_perm: Number of hash functions (num_buckets * hashes_per_bucket)
        ngram_size: N-gram size for shingling
        seed: Random seed for reproducibility
    
    Returns:
        MinHash object with computed signature
    """
    m = MinHash(num_perm=num_perm, seed=seed)
    ngrams = tokenize_ngrams(text, ngram_size)
    for ngram in ngrams:
        m.update(ngram.encode('utf-8'))
    return m


def deduplicate_with_lsh(
    documents: list[Dict[str, str]],
    num_buckets: int = 14,
    hashes_per_bucket: int = 8,
    ngram_size: int = 5,
    seed: int = 42,
) -> Dict[str, str]:
    """Deduplicate documents using datasketch MinHashLSH.
    
    Args:
        documents: List of {doc_id, text} dicts
        num_buckets: Number of LSH bands
        hashes_per_bucket: Number of hash functions per band
        ngram_size: N-gram size for shingling
        seed: Random seed for reproducibility
    
    Returns:
        Dict mapping doc_id -> cluster_representative_id
    """
    num_perm = num_buckets * hashes_per_bucket
    
    # LSH threshold: (1/b)^(1/r) where b=bands, r=rows
    threshold = (1.0 / num_buckets) ** (1.0 / hashes_per_bucket)
    logger.info(f"MinHashLSH threshold: {threshold:.3f} (num_perm={num_perm})")
    
    # Create LSH index (seed is only used in MinHash, not LSH)
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    
    # Index all documents
    minhashes = {}
    logger.info(f"Computing MinHash signatures for {len(documents)} documents...")
    for doc in documents:
        doc_id = doc["doc_id"]
        text = doc["text"]
        m = create_minhash(text, num_perm=num_perm, ngram_size=ngram_size, seed=seed)
        minhashes[doc_id] = m
        lsh.insert(doc_id, m)
    
    # Find duplicate pairs using LSH
    logger.info("Finding duplicate pairs via LSH queries...")
    duplicate_pairs: Set[tuple[str, str]] = set()
    for doc_id, m in minhashes.items():
        results = lsh.query(m)
        for match_id in results:
            if match_id != doc_id:
                # Store pairs in canonical order (smaller first)
                pair = tuple(sorted([doc_id, match_id]))
                duplicate_pairs.add(pair)
    
    logger.info(f"Found {len(duplicate_pairs)} duplicate pairs via LSH")
    
    # Union-Find clustering
    parent = {}
    
    def find(x):
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    
    def union(x, y):
        root_x = find(x)
        root_y = find(y)
        if root_x != root_y:
            # Use lexicographic order for deterministic root selection
            if root_x < root_y:
                parent[root_y] = root_x
            else:
                parent[root_x] = root_y
    
    # Union all duplicate pairs
    for a, b in duplicate_pairs:
        union(a, b)
    
    # Build cluster mapping
    cluster_map = {}
    for doc in documents:
        doc_id = doc["doc_id"]
        cluster_rep = find(doc_id)
        cluster_map[doc_id] = cluster_rep
    
    return cluster_map


def analyze_results(
    cluster_map: Dict[str, str],
    plagiaries: Dict[str, str],
) -> Dict[str, any]:
    """Analyze deduplication results against ground truth.
    
    Args:
        cluster_map: Mapping from doc_id to cluster representative
        plagiaries: Ground truth plagiary pairs (bidirectional)
    
    Returns:
        Dict with analysis metrics
    """
    # Deduplicated set: keep only cluster representatives
    cluster_reps = set(cluster_map.values())
    deduplicated_docs = [doc_id for doc_id, rep in cluster_map.items() if doc_id == rep]
    
    logger.info(f"Total documents: {len(cluster_map)}")
    logger.info(f"Unique clusters: {len(cluster_reps)}")
    logger.info(f"Deduplicated output: {len(deduplicated_docs)}")
    
    # Check truth pairs
    both_kept = []
    correct_kept = 0  # Kept the smaller doc_id
    wrong_kept = 0  # Kept the larger doc_id
    neither_kept = 0
    
    output_id_set = set(deduplicated_docs)
    seen_pairs = set()
    
    for doc1, doc2 in plagiaries.items():
        pair = tuple(sorted([doc1, doc2]))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        
        smaller, larger = (doc1, doc2) if doc1 < doc2 else (doc2, doc1)
        
        # Check if they're in the same cluster
        in_same_cluster = cluster_map.get(smaller) == cluster_map.get(larger)
        
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
    
    detected_pairs = correct_kept + wrong_kept
    total_pairs = len(seen_pairs)
    recall = detected_pairs / total_pairs * 100 if total_pairs > 0 else 0
    
    return {
        "output_count": len(deduplicated_docs),
        "detected_pairs": detected_pairs,
        "correct_kept": correct_kept,
        "wrong_kept": wrong_kept,
        "both_kept": len(both_kept),
        "neither_kept": neither_kept,
        "total_truth_pairs": total_pairs,
        "recall": recall,
    }


def main(seed: int = 42):
    """Generate ground truth using datasketch."""
    logger.info("=" * 80)
    logger.info("MinHash Ground Truth Generation using datasketch")
    logger.info("=" * 80)
    
    # Load test data
    logger.info("Loading test data...")
    documents = load_articles_train()
    plagiaries = load_articles_truth()
    
    logger.info(f"Loaded {len(documents)} documents")
    logger.info(f"Ground truth: {len(plagiaries) // 2} plagiary pairs")
    
    # Run deduplication
    logger.info(f"\nRunning MinHash deduplication (seed={seed})...")
    cluster_map = deduplicate_with_lsh(
        documents,
        num_buckets=14,
        hashes_per_bucket=8,
        ngram_size=5,
        seed=seed,
    )
    
    # Analyze results
    logger.info("\nAnalyzing results...")
    results = analyze_results(cluster_map, plagiaries)
    
    # Print results
    logger.info("\n" + "=" * 80)
    logger.info("DATASKETCH GROUND TRUTH RESULTS")
    logger.info("=" * 80)
    logger.info(f"Total documents: {len(documents)}")
    logger.info(f"Output documents: {results['output_count']}")
    logger.info(f"Removed documents: {len(documents) - results['output_count']}")
    logger.info(f"\nTruth pairs detected: {results['detected_pairs']}/{results['total_truth_pairs']} ({results['recall']:.1f}% recall)")
    logger.info(f"  - Correct (smaller kept): {results['correct_kept']}")
    logger.info(f"  - Wrong (larger kept): {results['wrong_kept']}")
    logger.info(f"  - Both kept (failed): {results['both_kept']}")
    logger.info(f"  - Neither kept: {results['neither_kept']}")
    logger.info("=" * 80)
    
    # Print expected results format
    print("\n" + "=" * 80)
    print("EXPECTED_RESULTS for test_minhash_dedup_workflow.py:")
    print("=" * 80)
    print("EXPECTED_RESULTS = {")
    print(f'    "seed": {seed},')
    print(f'    "output_count": {results["output_count"]},')
    print(f'    "detected_pairs": {results["detected_pairs"]},')
    print(f'    "correct_kept": {results["correct_kept"]},')
    print(f'    "wrong_kept": {results["wrong_kept"]},')
    print(f'    "both_kept": {results["both_kept"]},')
    print(f'    "neither_kept": {results["neither_kept"]},')
    print("}")
    print("=" * 80)
    print("\nNote: This is the reference ground truth from datasketch library.")
    print("Our implementation should match or be close to these values.")
    print("=" * 80)
    
    return results


if __name__ == "__main__":
    main(seed=42)
