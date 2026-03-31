# Arrow & Lance Tensor Type Reference

> Quick reference for storing ndarray / torch.Tensor / safetensors in Arrow tables and Lance datasets.
>
> **Last Updated**: 2026-03-31
> **PyArrow version**: 22.0+ required (`pyarrow>=22.0.0` in pyproject.toml)

---

## Arrow Types for Tensors

| Type | Use Case | Lance Vector Index | Shape Metadata |
|---|---|---|---|
| `FixedSizeList<float>[N]` | 1D embeddings, vector search | Yes | No (just list size) |
| `FixedShapeTensorType` (1D) | 1D embeddings with semantics | Yes | Yes (shape, dim_names) |
| `FixedShapeTensorType` (N-D) | Images, feature maps, attention | Storage only | Yes |
| `VariableShapeTensor` | Ragged sequences | Not in PyArrow yet | Yes |
| `LargeBinary` | Arbitrary blobs, checkpoints | No | No |

### FixedShapeTensorType (Recommended for N-D)

Arrow canonical extension type. Storage: `FixedSizeList<value_type>[product(shape)]` with JSON metadata.

```python
import pyarrow as pa
import numpy as np

# Define: each row is a [224, 224, 3] float32 image tensor
tensor_type = pa.fixed_shape_tensor(pa.float32(), [224, 224, 3])

# numpy -> Arrow (first axis = rows)
images = np.random.randn(100, 224, 224, 3).astype(np.float32)
arr = pa.FixedShapeTensorArray.from_numpy_ndarray(images)
table = pa.table({"image": arr})

# Arrow -> numpy (zero-copy)
recovered = table["image"].chunk(0).to_numpy_ndarray()  # (100, 224, 224, 3)
```

**Constraint**: All tensors in a column must have identical shape.

### FixedSizeList (Recommended for 1D Embeddings)

```python
# 768-dim embedding column
embedding_type = pa.list_(pa.float32(), 768)
embeddings = pa.FixedSizeListArray.from_arrays(
    pa.array(flat_values, type=pa.float32()), 768
)
```

Universally supported. Lance vector indexes (IVF_PQ, etc.) require this type or 1D FixedShapeTensor.

---

## Lance-Specific Support

| Feature | Type Required | Notes |
|---|---|---|
| Vector index (IVF_PQ, etc.) | `FixedSizeList` or 1D `FixedShapeTensor` | N-D tensors rejected |
| Vector search | Same as above | Returns `_distance` column |
| bf16 storage | `lance.arrow.BFloat16Type` | Extension on `fixed_size_binary[2]` |
| Image tensor | `FixedShapeImageTensorArray` | Lance extension wrapping FST |
| Encoded image (JPEG/PNG) | `EncodedImageArray` | Compressed storage, can decode |
| Blob columns | `LargeBinary` with `lance-encoding:blob` | For large binary data |

File size is essentially identical across FSL, FST, and binary for the same data.

---

## Interop Patterns

### torch.Tensor -> Arrow -> Lance

```python
import torch
import pyarrow as pa

# GPU tensor -> Arrow
tensor = model(input)                    # shape: (batch, 768)
fst = pa.FixedShapeTensorArray.from_numpy_ndarray(tensor.cpu().numpy())
table = pa.table({"embedding": fst})

# Arrow -> torch
arr = table["embedding"].chunk(0).to_numpy_ndarray()
tensor = torch.from_numpy(arr)           # zero-copy if contiguous
```

**GPU limitation**: Must `.cpu()` first. No direct GPU->Arrow path.

### safetensors -> Arrow

```python
from safetensors.numpy import load_file
import pyarrow as pa

weights = load_file("model.safetensors")  # dict[str, np.ndarray]
for name, arr in weights.items():
    fst = pa.FixedShapeTensorArray.from_numpy_ndarray(arr)
    # Store in table or Lance dataset
```

### bf16 Handling

Arrow has no native bf16. Two options:
1. **Upcast**: `tensor.bfloat16().float().numpy()` -> store as float32
2. **Lance extension**: `lance.arrow.BFloat16Type` -> store as 2-byte fixed binary

---

## Recommendation for Nurion Operators

| Scenario | Column Type | Why |
|---|---|---|
| Embedding output (e.g., CLIP) | `FixedSizeList<float32>[dim]` | Lance vector index compatible |
| Image tensor storage | `FixedShapeTensorType` | Preserves shape metadata through IPC + Lance |
| Variable-length token embeddings | `LargeBinary` + metadata | Until `VariableShapeTensor` lands in PyArrow |
| Model weights / checkpoints | `LargeBinary` blob column | Lance blob encoding |
| bf16 embeddings | `BFloat16Type` in FSL | Lance-specific, preserves precision |

### In SplitPayload

`SplitPayload.data` is a `pa.Table`. Tensor columns are just regular columns with the above types:

```python
# Operator that produces embeddings
def process_split(self, split, payload):
    images = payload.data["image"].to_numpy()  # or .to_pylist()
    embeddings = self.model.encode(images)      # shape: (N, 768)

    return SplitPayload(
        data=pa.table({
            "id": payload.data["id"],
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(embeddings.flatten(), type=pa.float32()), 768
            ),
        }),
        split_id=split.split_id,
    )
```

---

## Version Requirements

- `pyarrow>=22.0.0`: `FixedShapeTensorType` stable, DLPack export, IPC/Parquet roundtrip
- `pylance>=0.38.0`: Extension type preservation, BFloat16Type, vector index
- `VariableShapeTensor`: Spec finalized, Rust implemented, **PyArrow not yet** (as of 23.0.1)
