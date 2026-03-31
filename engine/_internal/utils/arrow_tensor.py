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

"""Arrow ↔ ndarray conversion utilities.

Provides a clean API for converting between Python lists of numpy arrays
and PyArrow arrays, choosing the optimal Arrow type automatically:

- **1D arrays** (embeddings): ``pa.array()`` → ``list<float>`` or ``FixedSizeList``.
  Compatible with Lance vector indexes.
- **N-D arrays** (images, feature maps): ``FixedShapeTensorArray`` (PyArrow 22+).
  Preserves shape metadata natively — no manual flatten/reshape needed.

Usage in Nurion operators::

    from _internal.utils.arrow_tensor import ndarray_list_to_arrow, arrow_to_ndarray_list

    # Write: list[np.ndarray] → pa.Array
    arr = ndarray_list_to_arrow(values)

    # Read: pa.ChunkedArray → list[np.ndarray]
    values = arrow_to_ndarray_list(col)
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pyarrow as pa


def ndarray_list_to_arrow(values: list) -> tuple[pa.Array, Optional[dict]]:
    """Convert a list of values to an Arrow array with tensor-aware encoding.

    Returns:
        (array, ndarray_meta_or_None)
        - ndarray_meta is only set for N-D FixedShapeTensor columns (for
          backward compat with older readers that don't understand the
          extension type).

    Type dispatch:
        - list[np.ndarray] with ndim > 1 → FixedShapeTensorArray (PyArrow 22+)
        - list[np.ndarray] with ndim == 1 → pa.array() → list<value_type>
        - list[scalar/str/...] → pa.array() (default)
    """
    sample = next((v for v in values if v is not None), None)

    if sample is not None and isinstance(sample, np.ndarray) and sample.ndim > 1:
        return _write_nd_tensor(values, sample), None

    # 1D arrays and scalars: pa.array() produces list<T> for 1D arrays,
    # which is Lance vector-index compatible.
    try:
        return pa.array(values), None
    except Exception:
        import pickle

        return pa.array([pickle.dumps(v) for v in values], type=pa.binary()), None


def arrow_to_ndarray_list(col: pa.Array | pa.ChunkedArray) -> list[np.ndarray]:
    """Convert an Arrow column back to a list of numpy arrays.

    Handles:
        - FixedShapeTensorArray → to_numpy_ndarray() (PyArrow 22+)
        - FixedSizeListArray with ndarray metadata → manual reshape (legacy)
        - Regular list array → to_pylist() fallback
    """
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()

    # PyArrow 22+ FixedShapeTensorArray
    if isinstance(col.type, pa.FixedShapeTensorType):
        batch = col.to_numpy_ndarray()  # shape: (N, *tensor_shape)
        return [batch[i] for i in range(len(batch))]

    # Legacy: FixedSizeList with metadata (from older adapters)
    if isinstance(col.type, pa.FixedSizeListType):
        return _read_fixed_size_list(col)

    # Fallback: regular list array → Python list
    return col.to_pylist()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _write_nd_tensor(values: list[np.ndarray], sample: np.ndarray) -> pa.Array:
    """N-D arrays → FixedShapeTensorArray (PyArrow 22+)."""
    shape = sample.shape
    dtype = sample.dtype

    # Stack into a single contiguous array: (N, *shape)
    stacked = np.stack(
        [v if isinstance(v, np.ndarray) else np.zeros(shape, dtype=dtype) for v in values]
    )
    return pa.FixedShapeTensorArray.from_numpy_ndarray(stacked)


def _read_fixed_size_list(col: pa.FixedSizeListArray) -> list[np.ndarray]:
    """Legacy path: FixedSizeList → list of 1D arrays."""
    flat = col.values.to_numpy()
    size = col.type.list_size
    n = len(col)
    return [flat[i * size : (i + 1) * size].copy() for i in range(n)]
