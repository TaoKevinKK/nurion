---
globs:
  - engine/_internal/serve/**
  - engine/tests/serve/**
---

# Serve Module

- ModelServiceManager: control plane Ray actor, deploys/manages models
- ModelPool: worker pool per model (scale up/down)
- GPUAllocator: GPU bin-packing, anti-fragmentation
- InferenceWorker: Ray actor running vLLM/SGLang
- ModelClient: client-side load balancing
- ModelRegistry: service discovery via Ray Named Actors
- Config: ModelConfig (model_id, model_source, tensor_parallel_size, min/max_workers)
- Testing: use `ray_cluster_with_gpus` fixture, monkeypatch InferenceWorker with fakes
