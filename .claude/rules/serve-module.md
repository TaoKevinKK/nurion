---
globs:
  - engine/_internal/serve/**
  - engine/tests/serve/**
---

# Serve Module

> Model inference serving built on Ray. **Update when component responsibilities change.**

---

## Components

| Component | File | Role |
|---|---|---|
| `ModelServiceManager` | `serve/manager.py` | Ray actor, control plane — deploy/undeploy models, owns pools and allocator |
| `ModelPool` | `serve/pool.py` | Plain object per model — track workers, handle scale up/down |
| `GPUAllocator` | `serve/allocator.py` | Plain object — GPU bin-packing, anti-fragmentation |
| `InferenceWorker` | `serve/worker.py` | Ray actor — runs vLLM or SGLang server |
| `ModelRegistry` | `serve/registry.py` | Ray Named Actor — service discovery (model_id → worker URLs) |
| `ModelClient` | `serve/client.py` | Plain object — HTTP client, round-robin load balancing |
| `ModelConfig` | `serve/config.py` | Dataclass — model_id, model_source, tensor_parallel_size, min/max_workers, AutoscaleConfig |

---

## Manager Modes

```python
# Attached (default): actor dies with job
manager = create_manager()
await manager.deploy_model.remote(config)

# Detached: actor survives job exit
manager = create_manager(detached=True)
# Reconnect from another job:
manager = ModelServiceManager.connect()
```

---

## Lifecycle

```
create_manager()
  └── ModelServiceManager.__init__()
        ├── GPUAllocator()
        └── {}  # empty pools dict

deploy_model(config: ModelConfig)
  ├── GPUAllocator.allocate(config.tensor_parallel_size)
  ├── ModelPool(config) → spawn InferenceWorker actors
  └── ModelRegistry.register(model_id, worker_urls)

undeploy_model(model_id)
  ├── ModelPool.shutdown() → stop InferenceWorker actors
  ├── GPUAllocator.release(model_id)
  └── ModelRegistry.deregister(model_id)
```

---

## Client Usage

```python
client = ModelClient(model_id="my-model")
response = await client.generate(prompt="...", max_tokens=100)
# Internally: ModelRegistry.get_workers() → round-robin URL selection → HTTP POST
```

---

## Testing

```python
@pytest.mark.distributed
def test_serve(ray_cluster_with_gpus):
    # Monkeypatch InferenceWorker with FakeInferenceServer
    # FakeInferenceServer is an HTTP stub, no actual vLLM/SGLang needed
    manager = create_manager()
    ...
```

- Always use `ray_cluster_with_gpus` fixture (16 fake GPUs)
- Monkeypatch `InferenceWorker` with `FakeInferenceServer` (`serve/fake_server.py`)
- Do NOT call real vLLM/SGLang in unit/integration tests

---

## Key Invariants

- All scheduling logic is local in `ModelServiceManager` — no cross-actor RPC for allocation
- `GPUAllocator` uses bin-packing to minimize GPU fragmentation
- `ModelRegistry` is the single source of truth for live worker URLs
- `ModelPool` is a plain Python object (not a Ray actor) — owned by manager
