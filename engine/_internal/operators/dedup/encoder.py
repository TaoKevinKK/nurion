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

"""MinHash encoder operator with xxhash + numpy vectorization.

Improvements over the original minhash/compute.py:
1. xxhash64 instead of SHA-256 (~50x faster hashing)
2. Numpy vectorized signature computation (batch all shingles at once)
3. No signature data amplification: output only (doc_id, bucket_id, band_hash)
   instead of carrying the full 1KB signature per band row
4. Word-level n-grams (following datatrove) instead of character shingles

Algorithm:
1. Tokenize text into word n-grams (shingles)
2. Hash each shingle using xxhash64
3. Apply MinHash: signature[i] = min(a[i] * h + b[i]) mod p for all shingles
4. Divide signature into bands; hash each band to get band_hash
5. Output one row per (doc_id, bucket_id) with the band_hash values

Output schema:
    - doc_id: Original document ID
    - bucket_id: Band index (0 to num_buckets-1)
    - band_hash: Hash of the band (int64, for bucketing/union)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pyarrow as pa
import xxhash

from _internal.core.operator import OperatorRuntime, operator
from _internal.operators.shuffle import ShuffleOperator, ShuffleOperatorConfig

# Mersenne prime for universal hashing
_MERSENNE_PRIME = np.uint64((1 << 61) - 1)


def _xxhash64(s: str) -> int:
    """Fast non-cryptographic hash using xxhash64.

    ~50x faster than SHA-256 for our use case (hashing shingles).
    Returns a 64-bit unsigned integer.
    """
    return xxhash.xxh64_intdigest(s.encode("utf-8"))


def _tokenize_ngrams(text: str, n: int) -> list[str]:
    """Tokenize text into word-level n-grams.

    Args:
        text: Input text
        n: N-gram size (number of words per shingle)

    Returns:
        List of n-gram strings (space-joined words)
    """
    # Simple whitespace tokenization + lowercasing
    words = text.lower().split()
    if len(words) < n:
        return [" ".join(words)] if words else []
    return [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]


@dataclass
class MinHashEncoderConfig(ShuffleOperatorConfig):
    """Configuration for MinHash encoding.

    Attributes:
        content_column: Column containing text to hash
        id_column: Column containing document ID
        num_buckets: Number of LSH buckets (bands)
        hashes_per_bucket: Number of hash functions per bucket
        ngram_size: Word n-gram size for shingling
        seed: Random seed for reproducibility
    """

    content_column: str = "content"
    id_column: str = "id"
    num_buckets: int = 14
    hashes_per_bucket: int = 8
    ngram_size: int = 5
    seed: int = 1

    def __post_init__(self) -> None:
        # Partition by bucket_id for downstream BucketUnion stage
        self.partition_keys = ["bucket_id"]

    @property
    def num_hashes(self) -> int:
        """Total number of hash functions."""
        return self.num_buckets * self.hashes_per_bucket


@operator(MinHashEncoderConfig)
class MinHashEncoderOperator(ShuffleOperator):
    """MinHash signature computation with xxhash + numpy vectorization.

    Output: One row per (document, bucket) pair with:
    - doc_id: Document identifier
    - bucket_id: Band index (0 to num_buckets-1)
    - band_hash: Hash of the band signature values (int64)

    Key difference from the old MinHashComputeOperator:
    - Does NOT carry the full signature in each row (~25x less data)
    - Uses xxhash64 instead of SHA-256 (~50x faster)
    - Numpy vectorized: all shingles processed in a single matrix operation
    """

    def __init__(self, config: MinHashEncoderConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        self.encoder_config = config
        self._init_hash_params()

    def _init_hash_params(self) -> None:
        """Pre-compute random hash function parameters."""
        gen = np.random.RandomState(self.encoder_config.seed)
        num_hashes = self.encoder_config.num_hashes
        self._hash_a = gen.randint(1, _MERSENNE_PRIME, dtype=np.uint64, size=(1, num_hashes))
        self._hash_b = gen.randint(0, _MERSENNE_PRIME, dtype=np.uint64, size=(1, num_hashes))

    def _compute_signature(self, text: str) -> np.ndarray:
        """Compute MinHash signature using vectorized numpy operations.

        Args:
            text: Input text

        Returns:
            Numpy array of shape (num_hashes,) with uint64 MinHash values
        """
        config = self.encoder_config
        ngrams = _tokenize_ngrams(text, config.ngram_size)

        if not ngrams:
            return np.full(config.num_hashes, np.iinfo(np.uint64).max, dtype=np.uint64)

        # Hash all shingles at once using xxhash64
        shingle_hashes = np.fromiter(
            (_xxhash64(s) for s in ngrams),
            dtype=np.uint64,
        ).reshape(-1, 1)  # Shape: (num_shingles, 1)

        # Vectorized MinHash: (shingle_hashes * a + b) % prime
        # Broadcast: (num_shingles, 1) * (1, num_hashes) -> (num_shingles, num_hashes)
        permuted = (shingle_hashes * self._hash_a + self._hash_b) % _MERSENNE_PRIME

        # MinHash: take minimum across all shingles per hash function
        signature = np.min(permuted, axis=0).astype(np.uint64)

        return signature

    def _hash_band(self, band_values: np.ndarray) -> int:
        """Hash a band of signature values into a single int64.

        Uses struct packing + xxhash for deterministic cross-process hashing.
        """
        band_bytes = band_values.tobytes()
        # Use positive int63 to stay in Arrow int64 range
        return xxhash.xxh64_intdigest(band_bytes) & 0x7FFFFFFFFFFFFFFF

    def process_data(self, table: pa.Table) -> Optional[pa.Table]:
        """Compute MinHash signatures and expand into bucket rows."""
        config = self.encoder_config

        if config.content_column not in table.column_names:
            raise ValueError(f"Content column '{config.content_column}' not found")
        if config.id_column not in table.column_names:
            raise ValueError(f"ID column '{config.id_column}' not found")

        contents = table.column(config.content_column).to_pylist()
        doc_ids = table.column(config.id_column).to_pylist()

        out_doc_ids: list = []
        out_bucket_ids: list[int] = []
        out_band_hashes: list[int] = []

        for doc_id, content in zip(doc_ids, contents):
            if content is None or not content:
                continue

            signature = self._compute_signature(str(content))

            # Split into bands and hash each band
            for bucket_id in range(config.num_buckets):
                start = bucket_id * config.hashes_per_bucket
                end = start + config.hashes_per_bucket
                band_values = signature[start:end]
                band_hash = self._hash_band(band_values)

                out_doc_ids.append(doc_id)
                out_bucket_ids.append(bucket_id)
                out_band_hashes.append(band_hash)

        if not out_doc_ids:
            return None

        return pa.table(
            {
                "doc_id": out_doc_ids,
                "bucket_id": pa.array(out_bucket_ids, type=pa.int32()),
                "band_hash": pa.array(out_band_hashes, type=pa.int64()),
            }
        )
