# GPU Scheduling and Length-Based Routing

_Design document for anti-fragmentation GPU scheduling and length-based model routing
in the multi-model inference service (`_internal.serve`)._

_Created: February 2026_

---

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| **GPUAllocator** | ❌ Not Implemented | `_internal/serve/allocator.py` — best-fit bin-packing, plain class owned by Manager |
| **Manager as Ray actor** | ❌ Not Implemented | Manager becomes `@ray.remote` actor; Pool demoted to plain class inside Manager |
| **Fractional GPU** | ❌ Not Implemented | Auto `num_gpus=0.5` when TP=1 and `gpu_memory_utilization < 0.5` |
| **deploy_models()** | ❌ Not Implemented | Two-phase ordered deployment (large-first sequential, then small parallel) |
| **Compaction** | ❌ Not Implemented | Manager-coordinated freeze → evict → spawn → unfreeze |
| **Worker node reporting** | ❌ Not Implemented | `get_node_id()` via `ray.get_runtime_context()` |
| **ModelRoutingConfig** | ❌ Not Implemented | Length-based routing config in `ExternalLLMOperatorConfig` |
| **Per-row routing** | ❌ Not Implemented | Token estimation + group-by-model routing in operator |

---

## 1. Problem: GPU Fragmentation in Multi-Model Serving

When deploying multiple models with heterogeneous GPU requirements (e.g., TP=8
needs a full 8-GPU node, TP=1 needs 1 GPU), Ray's default scheduler may scatter
1-GPU workers across multiple nodes. This **fragments** GPU resources, making it
impossible to find a full node for TP=8 models.

```
Example: 3 nodes × 8 GPUs = 24 GPUs total

Without anti-fragmentation:
  NodeA: [TP1][   ][   ][   ][   ][   ][   ][   ]   1/8 used
  NodeB: [TP1][   ][   ][   ][   ][   ][   ][   ]   1/8 used
  NodeC: [TP1][TP1][   ][   ][   ][   ][   ][   ]   2/8 used
  → No node has 8 free GPUs for a TP=8 model!

With anti-fragmentation (best-fit packing):
  NodeA: [   ][   ][   ][   ][   ][   ][   ][   ]   0/8 used (free for TP=8)
  NodeB: [   ][   ][   ][   ][   ][   ][   ][   ]   0/8 used (free for TP=8)
  NodeC: [TP1][TP1][TP1][TP1][   ][   ][   ][   ]   4/8 used (small models packed)
```

**Why Ray's default scheduling causes this**: Ray uses a hybrid spread/bin-pack
strategy that favors spreading actors across nodes for fault tolerance. This is
the opposite of what GPU-intensive inference needs.

### 1.1 Current Workaround

The only anti-fragmentation logic today is manual, per-workflow code in
`workflows/run_multi_ocr_enrich.py`:

```python
# Manual head node detection
head_ip = None
for node in ray.nodes():
    if node.get("Alive") and "node:__internal_head__" in node.get("Resources", {}):
        head_ip = node["NodeManagerAddress"]
        break

# Pin small models to head node
ModelConfig(
    model_id="deepseek-ocr",
    worker_resources={"num_gpus": 1, f"node:{head_ip}": 0.001},
)
```

Problems with this approach:

- Assumes static cluster topology (breaks when nodes join/leave)
- Wastes GPUs on "designated" nodes when that role is idle
- Does not adapt to TP=2, TP=4, or other mixed sizes
- Requires manual cluster knowledge in workflow code

---

## 2. Design: Unified Manager with Best-Fit GPU Bin-Packing

**Core principle**: Pack small GPU allocations into already-partially-used nodes,
keeping fully-free nodes available for large allocations. No static node roles.
No user configuration needed for the common case.

**Architectural principle**: The Manager is the single stateful Ray actor that
owns both pool state and allocator state. Pools are plain Python objects inside
the Manager, not separate actors. This eliminates cross-actor RPC for scheduling
decisions and makes compaction a local operation with no distributed coordination.

```
Workflow process
  └── ModelServiceManager (Ray Actor, named, detachable)
        │
        ├── GPUAllocator (plain object)
        │   ├── Per-node GPU tracking (float, supports fractional GPUs)
        │   ├── suggest_nodes()            ← best-fit bin-packing
        │   ├── suggest_workers_to_stop()  ← consolidate on scale-down
        │   └── plan_compaction()          ← eviction planning
        │
        ├── ModelPool (plain object, per model)
        │   ├── _spawn_worker()            ← creates InferenceWorker actors
        │   ├── _stop_worker()             ← graceful shutdown
        │   ├── autoscale loop             ← asyncio.Task in Manager's event loop
        │   └── worker tracking            ← worker_id → ActorHandle
        │
        ├── ModelPool (plain object, per model)
        │   └── ...
        │
        └── InferenceWorker (Ray Actor, per worker)
            └── vLLM/SGLang subprocess
```

### Why Manager-as-Actor, Not Pool-as-Actor

The previous design had Manager as a plain class and each Pool as a separate
Ray actor. This caused problems:

| Problem | Previous (Pool = Actor) | New (Pool = Plain Class) |
|---------|------------------------|--------------------------|
| Compaction coordination | Pool needs `sibling_pools` references; cross-actor RPC to freeze/evict/spawn | Manager calls local methods on its own pools |
| Allocator access | Every spawn/stop = 2 RPC round-trips to allocator actor | Direct method calls, microsecond latency |
| Autoscaler vs compaction race | Evicted pool's autoscaler may respawn on the cleared node before large model spawns | Manager freezes autoscaler locally before compaction, no race |
| `connect()` recovery | Must discover N pool actors by name | Find one Manager actor, all state is inside |
| Orphaned placements | Worker crash without `record_removal` → stale allocator state | Manager owns both pool and allocator; stop always calls `record_removal` |
| Detached mode | Each pool actor needs `lifetime="detached"` separately | Only the Manager actor is detached; pools live inside it |

### 2.1 Best-Fit Algorithm

When spawning a worker needing N GPUs, pick the node with the **fewest free GPUs
that still has >= N free**. This is the classic "best-fit" bin-packing heuristic.

Walkthrough (3 nodes, each 8 GPUs):

| Step | Event                        | NodeA | NodeB | NodeC | Algorithm choice                  |
|------|------------------------------|-------|-------|-------|-----------------------------------|
| 1    | Spawn TP=8                   | **0** | 8     | 8     | NodeA (any 8-free node)           |
| 2    | Spawn TP=8                   | 0     | **0** | 8     | NodeB (any 8-free node)           |
| 3    | Spawn TP=1                   | 0     | 0     | **7** | NodeC (only fit)                  |
| 4    | Spawn TP=1                   | 0     | 0     | **6** | NodeC (keeps packing)             |
| 5    | Spawn TP=1                   | 0     | 0     | **5** | NodeC (keeps packing)             |
| 6    | TP=8 on NodeA scales down    | **8** | 0     | 5     | —                                 |
| 7    | Spawn TP=1                   | 8     | 0     | **4** | NodeC (4 < 8, best-fit!)          |
| 8    | Spawn TP=8                   | **0** | 0     | 4     | NodeA (only node with >=8 free)   |

The critical step is 7: when both NodeA (8 free) and NodeC (5 free) can host a
TP=1 worker, best-fit picks NodeC because it is more packed. This **preserves
NodeA's full 8-GPU block** for a future TP=8 request.

### 2.2 Fractional GPU Support

When a model uses TP=1 with low `gpu_memory_utilization` (e.g., 0.3 for a small
OCR model), one physical GPU can host multiple workers. The allocator tracks GPU
resources as **floats**, and `get_worker_resources()` auto-infers `num_gpus=0.5`
when `gpu_memory_utilization < 0.5`:

```
NodeC with 8 GPUs, two OCR models each using num_gpus=0.5:

  GPU0: [OCR-A][OCR-B]   ← two workers share one GPU
  GPU1: [OCR-A][OCR-B]
  GPU2: [     ][     ]
  ...
  Allocator tracking: NodeC has 8.0 total, 2.0 used, 6.0 free
```

This allows packing more small models onto fewer GPUs, leaving full nodes free
for large TP=8 deployments.

### 2.3 Advisory, Not Authoritative

The allocator is a hint layer, not a hard scheduler:

- Suggestions are passed to Ray as `NodeAffinitySchedulingStrategy(soft=True)`.
- If the allocator's state is stale (race condition, node failure), Ray handles
  the actual placement. No correctness risk.
- After spawn, the worker reports its actual `node_id` back. The allocator
  self-corrects if the hint was overridden.

---

## 3. Component Design

### 3.1 GPUAllocator (Plain Class)

New file: `_internal/serve/allocator.py`

A plain Python class owned by the Manager actor. Tracks per-node GPU state
(as floats for fractional GPU support) and provides advisory placement decisions.
All methods are synchronous (fast, in-memory dict lookups). No Ray actor overhead.

```python
class GPUAllocator:
    """Cluster-aware GPU bin-packing allocator.

    Advisory layer on top of Ray's scheduler. Provides best-fit node
    suggestions passed as soft NodeAffinitySchedulingStrategy hints.
    If the suggestion is stale, Ray handles the fallback.

    Plain class owned by ModelServiceManager — not a Ray actor.
    All state is co-located with the Manager for zero-overhead access.
    """

    def __init__(self) -> None:
        self._node_total: dict[str, float] = {}       # node_id -> total GPUs
        self._placements: dict[str, tuple[str, float]] = {}  # worker_id -> (node_id, gpus)
```

**State management:**

```python
    def refresh_nodes(self) -> dict[str, float]:
        """Refresh node list from ray.nodes(). Returns {node_id: total_gpus}."""
        self._node_total = {}
        for node in ray.nodes():
            if node.get("Alive"):
                gpus = float(node["Resources"].get("GPU", 0))
                if gpus > 0:
                    self._node_total[node["NodeID"]] = gpus
        # Prune placements on dead nodes
        live = set(self._node_total)
        self._placements = {
            wid: (nid, g) for wid, (nid, g) in self._placements.items()
            if nid in live
        }
        return dict(self._node_total)

    def record_placement(self, worker_id: str, node_id: str, gpus: float) -> None:
        self._placements[worker_id] = (node_id, gpus)

    def record_removal(self, worker_id: str) -> None:
        self._placements.pop(worker_id, None)

    def get_node_free_gpus(self) -> dict[str, float]:
        """Free GPUs per node = total - sum(placements on that node)."""
        used: dict[str, float] = defaultdict(float)
        for _, (node_id, gpus) in self._placements.items():
            used[node_id] += gpus
        return {
            nid: total - used.get(nid, 0.0)
            for nid, total in self._node_total.items()
        }

    def reconcile(self, active_worker_ids: set[str]) -> int:
        """Remove placements for workers that no longer exist.

        Called by the Manager after worker crash recovery to clean up
        phantom placements. Returns number of stale entries removed.
        """
        stale = [wid for wid in self._placements if wid not in active_worker_ids]
        for wid in stale:
            del self._placements[wid]
        return len(stale)
```

**Best-fit placement:**

```python
    def suggest_nodes(self, gpus_per_worker: float, count: int) -> list[Optional[str]]:
        """Suggest best-fit nodes for `count` workers needing `gpus_per_worker` each.

        Simulates sequential placement so concurrent spawns from the same
        scale_to() call don't all target the same node.
        """
        free = self.get_node_free_gpus()
        results: list[Optional[str]] = []

        for _ in range(count):
            candidates = [
                (f, random.random(), nid)  # random tiebreaker for equal free GPUs
                for nid, f in free.items()
                if f >= gpus_per_worker - 1e-9  # float tolerance
            ]
            if not candidates:
                results.append(None)
                continue

            candidates.sort()  # ascending by free GPUs = best-fit
            best_node = candidates[0][2]
            results.append(best_node)
            free[best_node] -= gpus_per_worker  # tentative deduction

        return results
```

**Scale-down advisory:**

```python
    def suggest_workers_to_stop(
        self, worker_ids: list[str], count: int
    ) -> list[str]:
        """Pick which workers to stop to best consolidate free GPUs.

        Strategy: prefer workers on nodes with the most free GPUs (least
        packed). Removing them makes those nodes even emptier, consolidating
        free space into larger contiguous blocks.
        """
        node_workers: dict[str, list[str]] = defaultdict(list)
        for wid in worker_ids:
            if wid in self._placements:
                node_id, _ = self._placements[wid]
                node_workers[node_id].append(wid)

        free = self.get_node_free_gpus()
        sorted_nodes = sorted(
            node_workers.keys(),
            key=lambda nid: free.get(nid, 0),
            reverse=True,  # emptiest node first
        )

        to_stop: list[str] = []
        for nid in sorted_nodes:
            for wid in node_workers[nid]:
                if len(to_stop) >= count:
                    return to_stop
                to_stop.append(wid)
        return to_stop
```

**Compaction planner:**

```python
    def plan_compaction(
        self, gpus_needed: float
    ) -> Optional[tuple[str, list[str]]]:
        """Find the cheapest eviction plan to free gpus_needed GPUs on one node.

        Returns (node_id, [worker_ids_to_evict]) or None.
        Cheapest = fewest workers to evict.
        """
        free = self.get_node_free_gpus()
        node_workers: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for wid, (nid, gpus) in self._placements.items():
            node_workers[nid].append((wid, gpus))

        best: Optional[tuple[int, str, list[str]]] = None

        for nid, total in self._node_total.items():
            node_free = free.get(nid, 0.0)
            deficit = gpus_needed - node_free
            if deficit <= 1e-9:
                return (nid, [])  # already enough free

            workers_on_node = node_workers.get(nid, [])
            workers_on_node.sort(key=lambda x: x[1], reverse=True)

            evict_wids: list[str] = []
            freed = 0.0
            for wid, gpus in workers_on_node:
                evict_wids.append(wid)
                freed += gpus
                if node_free + freed >= gpus_needed - 1e-9:
                    break

            if node_free + freed >= gpus_needed - 1e-9:
                if best is None or len(evict_wids) < best[0]:
                    best = (len(evict_wids), nid, evict_wids)

        if best is None:
            return None
        return (best[1], best[2])
```

Key properties:

- **Single source of truth**: Owned by the Manager actor. All pools read/write
  the same allocator instance directly. No RPC overhead.
- **Float-based tracking**: Supports fractional GPUs (e.g., 0.5) for co-locating
  small models on the same physical GPU.
- **Fast**: All operations are in-memory dict lookups. O(N x M) where N = nodes,
  M = workers to place (both typically small).
- **Reconciliation**: `reconcile()` cleans up phantom placements after worker
  crashes, called by the Manager during recovery.

### 3.2 ModelPool (Plain Class)

Modified file: `_internal/serve/pool.py`

ModelPool is no longer a Ray actor. It is a plain Python class owned by the
Manager actor. Worker lifecycle, scaling, and autoscaling all execute within
the Manager's event loop.

```python
class ModelPool:
    """Manages InferenceWorkers for a single model.

    Plain class owned by ModelServiceManager. Not a Ray actor.
    The Manager's event loop runs autoscaling as an asyncio.Task.
    """

    def __init__(
        self,
        config: ModelConfig,
        registry: ray.actor.ActorHandle,
        detached: bool = False,
        allocator: Optional[GPUAllocator] = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._detached = detached
        self._allocator = allocator
        self._workers: dict[str, ray.actor.ActorHandle] = {}
        self._worker_ports: dict[str, int] = {}
        self._worker_nodes: dict[str, str] = {}  # worker_id -> node_id
        self._spawning_workers = 0
        self._shutdown_event = asyncio.Event()
        self._last_scale_time = 0.0
        self._registry_url: Optional[str] = None

        # Autoscaler state
        self._autoscale_config: Optional[AutoscaleConfig] = None
        self._autoscale_task: Optional[asyncio.Task] = None
        self._autoscale_frozen = False
        self._last_idle_time = 0.0
```

**`_spawn_worker()`** — query allocator (direct call, no RPC):

```python
    async def _spawn_worker(self) -> tuple[str, ray.actor.ActorHandle]:
        port = find_free_port()
        worker_id = f"{self._config.model_id}_worker_{port}"
        resources = self._config.get_worker_resources()
        actor_options: dict[str, Any] = {"name": worker_id, **resources}

        if self._detached:
            actor_options["lifetime"] = "detached"
            actor_options["namespace"] = SERVE_NAMESPACE

        # Best-fit node suggestion from allocator (direct call, no RPC)
        if self._allocator is not None:
            gpus = resources.get("num_gpus", 0)
            if gpus > 0:
                suggestions = self._allocator.suggest_nodes(float(gpus), 1)
                node_id = suggestions[0] if suggestions else None
                if node_id is not None:
                    actor_options["scheduling_strategy"] = (
                        NodeAffinitySchedulingStrategy(
                            node_id=node_id, soft=True
                        )
                    )

        worker: Optional[ray.actor.ActorHandle] = None
        self._spawning_workers += 1
        try:
            worker = (
                ray.remote(InferenceWorker)
                .options(**actor_options)
                .remote(self._config, registry=self._registry,
                        port=port, worker_id=worker_id)
            )
            await asyncio.wait_for(
                worker.start.remote(),
                timeout=_SPAWN_WAIT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if worker is not None:
                try:
                    ray.kill(worker)
                except Exception:
                    pass
            raise RuntimeError(
                f"Timed out spawning worker {worker_id} after "
                f"{_SPAWN_WAIT_TIMEOUT_SECONDS:.0f}s."
            )
        finally:
            self._spawning_workers = max(0, self._spawning_workers - 1)

        assert worker is not None
        self._workers[worker_id] = worker
        self._worker_ports[worker_id] = port

        # Report actual placement back to allocator (direct call, no RPC)
        actual_node = ray.get(worker.get_node_id.remote())
        self._worker_nodes[worker_id] = actual_node
        if self._allocator is not None:
            gpus = resources.get("num_gpus", 0)
            self._allocator.record_placement(worker_id, actual_node, float(gpus))

        return worker_id, worker
```

**`_stop_worker()`** — report removal (direct call):

```python
    async def _stop_worker(self, worker_id: str, graceful: bool = True) -> None:
        worker = self._workers.get(worker_id)
        if worker is None:
            return
        try:
            if graceful:
                await worker.shutdown.remote()
            else:
                ray.kill(worker)
        except Exception as e:
            logger.warning(f"Error stopping worker {worker_id}: {e}")

        self._workers.pop(worker_id, None)
        self._worker_ports.pop(worker_id, None)
        self._worker_nodes.pop(worker_id, None)

        # Direct call to allocator, no RPC
        if self._allocator is not None:
            self._allocator.record_removal(worker_id)
```

**`scale_to()`** — node-aware worker selection on scale-down:

```python
    async def scale_to(self, target: int) -> dict[str, Any]:
        target = max(self._config.min_workers, min(target, self._config.max_workers))
        current = len(self._workers)
        result: dict[str, Any] = {
            "model_id": self._config.model_id,
            "from_workers": current,
            "to_workers": target,
            "started_at": time.time(),
            "spawned": [], "stopped": [], "spawn_errors": [],
        }

        if target > current:
            tasks = [self._spawn_worker() for _ in range(target - current)]
            spawned = await asyncio.gather(*tasks, return_exceptions=True)
            for item in spawned:
                if isinstance(item, tuple):
                    result["spawned"].append(item[0])
                elif isinstance(item, Exception):
                    result["spawn_errors"].append(str(item))

        elif target < current:
            count = current - target
            if self._allocator is not None:
                workers_to_stop = self._allocator.suggest_workers_to_stop(
                    list(self._workers.keys()), count
                )
            else:
                workers_to_stop = list(self._workers.keys())[:count]

            for worker_id in workers_to_stop:
                await self._stop_worker(worker_id, graceful=True)
                result["stopped"].append(worker_id)

        result["completed_at"] = time.time()
        result["duration_s"] = result["completed_at"] - result["started_at"]
        result["current_workers"] = len(self._workers)
        self._last_scale_time = time.time()
        return result

    def stop_workers_by_ids(self, worker_ids: list[str]) -> list[str]:
        """Return the subset of worker_ids that belong to this pool."""
        return [wid for wid in worker_ids if wid in self._workers]
```

**Autoscaler** — runs as asyncio.Task in Manager's event loop (unchanged logic,
but no longer in a separate actor):

```python
    def start_autoscaler(self, config: Optional[AutoscaleConfig] = None) -> None:
        self._autoscale_config = config or AutoscaleConfig()
        if not self._autoscale_config.enabled:
            return
        if self._autoscale_task is not None:
            return
        self._autoscale_task = asyncio.get_event_loop().create_task(
            self._autoscale_loop()
        )

    # freeze_autoscaler(), unfreeze_autoscaler(), _autoscale_loop()
    # remain the same as current pool.py — just local method calls now.
```

### 3.3 ModelServiceManager (Ray Actor)

Modified file: `_internal/serve/manager.py`

The Manager becomes a Ray actor that owns all pools and the allocator. In detached
mode, the Manager actor survives job exit. `connect()` looks up one actor instead
of N pool actors.

```python
@ray.remote
class ModelServiceManager:
    """Control plane for multi-model inference service.

    Ray actor that owns ModelPool instances (plain objects) and a
    GPUAllocator (plain object). All scheduling and compaction logic
    is local — no cross-actor RPC.
    """

    def __init__(
        self,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> None:
        self._autoscale_config = autoscale_config or AutoscaleConfig()
        self._pools: dict[str, ModelPool] = {}
        self._configs: dict[str, ModelConfig] = {}
        self._allocator = GPUAllocator()
        self._allocator.refresh_nodes()

        # Create registry actor (still a separate actor for HTTP endpoint)
        self._registry = ray.remote(ModelRegistry).options(
            name=REGISTRY_ACTOR_NAME,
            lifetime="detached",
            namespace=SERVE_NAMESPACE,
        ).remote()
        ray.get(self._registry.start.remote())
```

**`deploy_models()`** — two-phase ordered deployment:

```python
    async def deploy_models(
        self,
        configs: list[ModelConfig],
        wait_ready: bool = True,
        timeout: float = 600.0,
    ) -> list[dict[str, Any]]:
        """Deploy multiple models with anti-fragmentation ordering.

        1. Refresh cluster topology in allocator.
        2. Sort by effective num_gpus descending.
        3. Phase 1: large models (num_gpus > threshold) deployed sequentially.
        4. Phase 2: small models deployed in parallel.
        """
        self._allocator.refresh_nodes()

        sorted_configs = sorted(
            configs,
            key=lambda c: c.get_worker_resources().get("num_gpus", 0),
            reverse=True,
        )

        # Dynamic threshold: half the largest node's GPU count
        max_node_gpus = max(self._allocator._node_total.values(), default=8)
        tp_threshold = max_node_gpus / 2

        large = [c for c in sorted_configs
                 if c.get_worker_resources().get("num_gpus", 0) > tp_threshold]
        small = [c for c in sorted_configs
                 if c.get_worker_resources().get("num_gpus", 0) <= tp_threshold]

        results: list[dict[str, Any]] = []

        # Phase 1: large models sequentially (need contiguous GPU blocks)
        for config in large:
            r = await self.deploy_model(config, wait_ready=wait_ready, timeout=timeout)
            results.append(r)

        # Phase 2: small models in parallel
        if small:
            small_results = await asyncio.gather(
                *(self.deploy_model(c, wait_ready=wait_ready, timeout=timeout)
                  for c in small),
                return_exceptions=True,
            )
            results.extend(small_results)

        return results
```

**`deploy_model()`** — creates pool as plain object:

```python
    async def deploy_model(
        self,
        config: ModelConfig,
        wait_ready: bool = True,
        timeout: float = 600.0,
        autoscale_config: Optional[AutoscaleConfig] = None,
    ) -> dict[str, Any]:
        model_id = config.model_id
        if model_id in self._pools:
            raise ValueError(f"Model {model_id} already deployed")

        # Pool is a plain object, not a Ray actor
        pool = ModelPool(
            config=config,
            registry=self._registry,
            detached=True,  # workers are always detached actors
            allocator=self._allocator,
        )
        self._pools[model_id] = pool
        self._configs[model_id] = config

        scale_result = await pool.scale_to(config.min_workers)

        effective_config = autoscale_config or self._autoscale_config
        pool.start_autoscaler(effective_config)

        if wait_ready:
            is_ready = await pool.wait_ready(timeout=timeout)
            if not is_ready:
                raise RuntimeError(f"Model {model_id} failed to start")

        pool_status = await pool.get_status()
        return {
            "model_id": model_id,
            "status": "ready",
            "endpoints": pool_status.get("endpoints", []),
            "scale_result": scale_result,
        }
```

**Compaction** — Manager coordinates directly, no cross-actor RPC:

```python
    async def spawn_with_compaction(
        self, model_id: str
    ) -> tuple[str, ray.actor.ActorHandle]:
        """Spawn a worker with one compaction retry on failure.

        Compaction is coordinated entirely within the Manager:
        1. Try normal spawn.
        2. On failure, plan compaction (local call to allocator).
        3. Freeze affected pools' autoscalers.
        4. Evict identified workers.
        5. Retry spawn.
        6. Unfreeze autoscalers (evicted pools auto-recover).
        """
        pool = self._pools[model_id]
        try:
            return await pool._spawn_worker()
        except Exception:
            gpus = float(pool._config.get_worker_resources().get("num_gpus", 0))
            if gpus <= 0:
                raise

            logger.warning(
                f"Spawn failed for {model_id} ({gpus} GPUs), "
                f"attempting compaction..."
            )

            plan = self._allocator.plan_compaction(gpus)  # direct call
            if plan is None:
                raise
            target_node, evict_wids = plan
            if not evict_wids:
                return await pool._spawn_worker()

            # Freeze autoscalers for affected pools
            affected_pools: list[ModelPool] = []
            for p in self._pools.values():
                owns = p.stop_workers_by_ids(evict_wids)
                if owns:
                    p.freeze_autoscaler()
                    affected_pools.append(p)

            # Evict workers
            for p in self._pools.values():
                owned_wids = p.stop_workers_by_ids(evict_wids)
                for wid in owned_wids:
                    await p._stop_worker(wid, graceful=True)

            # Retry spawn
            try:
                result = await pool._spawn_worker()
            finally:
                # Unfreeze — autoscalers will respawn evicted workers
                # on allocator-suggested nodes (not the cleared node)
                for p in affected_pools:
                    p.unfreeze_autoscaler()

            return result
```

**`connect()`** — simplified, only one actor to find:

```python
    @classmethod
    def connect(cls) -> ray.actor.ActorHandle:
        """Connect to an existing detached ModelServiceManager.

        Returns the Manager actor handle. All pools and allocator
        state live inside it — no need to discover N pool actors.
        """
        try:
            return ray.get_actor(
                MANAGER_ACTOR_NAME, namespace=SERVE_NAMESPACE
            )
        except ValueError:
            raise RuntimeError(
                "No detached serve layer found. "
                "Deploy models first with ModelServiceManager."
            )
```

**Creation helper** — for workflow code:

```python
# Module-level helper
MANAGER_ACTOR_NAME = "nurion_model_service_manager"

def create_manager(
    autoscale_config: Optional[AutoscaleConfig] = None,
    detached: bool = False,
) -> ray.actor.ActorHandle:
    """Create a new ModelServiceManager actor."""
    options: dict[str, Any] = {"name": MANAGER_ACTOR_NAME}
    if detached:
        options["lifetime"] = "detached"
        options["namespace"] = SERVE_NAMESPACE
    return (
        ray.remote(ModelServiceManager)
        .options(**options)
        .remote(autoscale_config)
    )
```

### 3.4 ModelConfig: Fractional GPU Auto-Inference

Changes to `_internal/serve/config.py`:

```python
@dataclass
class ModelConfig:
    ...
    tensor_parallel_size: int = 1
    gpu_memory_utilization: float = 0.9
    worker_resources: Optional[dict[str, Any]] = None

    def get_worker_resources(self) -> dict[str, Any]:
        """Return Ray resource dict for worker scheduling.

        Auto-infers num_gpus:
        - If worker_resources is explicitly set, use it as-is.
        - TP > 1: num_gpus = tensor_parallel_size (need dedicated GPUs).
        - TP = 1 and gpu_memory_utilization < 0.5: num_gpus = 0.5
          (allows two workers to share one physical GPU).
        - TP = 1 otherwise: num_gpus = 1.
        """
        if self.worker_resources is not None:
            return self.worker_resources
        if self.tensor_parallel_size > 1:
            return {"num_gpus": self.tensor_parallel_size}
        if self.gpu_memory_utilization < 0.5:
            return {"num_gpus": 0.5}
        return {"num_gpus": 1}
```

This means existing configs like:

```python
ModelConfig(
    model_id="deepseek-ocr",
    tensor_parallel_size=1,
    gpu_memory_utilization=0.3,  # < 0.5 → auto num_gpus=0.5
)
```

...automatically get `num_gpus=0.5`, allowing two OCR workers on the same GPU.
No `worker_resources` override needed for the common case.

### 3.5 InferenceWorker Changes

Changes to `_internal/serve/worker.py`:

```python
class InferenceWorker:
    def __init__(self, config, registry, port, worker_id):
        ...
        self._node_id: Optional[str] = None

    async def start(self):
        self._node_id = ray.get_runtime_context().get_node_id()
        ...  # existing start logic

    def get_node_id(self) -> Optional[str]:
        """Return the Ray node_id this worker is running on."""
        return self._node_id
```

---

## 4. Production Scenarios

### 4.1 Initial Deployment (3 nodes x 8 GPUs)

Deploy TP=8 (2 replicas) + TP=1 (4 replicas):

1. `deploy_models()` sorts: TP=8 first, then TP=1.
2. TP=8 worker 1: allocator sees 3 nodes all 8-free — picks NodeA.
3. TP=8 worker 2: NodeA=0, NodeB=8, NodeC=8 — picks NodeB.
4. TP=1 workers 1-4: NodeC is the only node with free GPUs — all pack there.

Result: NodeA=TP8, NodeB=TP8, NodeC=4xTP1. Clean, no fragmentation.

### 4.2 Elastic Scale-Down and Re-Up

1. TP=8 on NodeA scales down — NodeA now has 8 free GPUs.
2. TP=1 autoscales up, needs 1 GPU — allocator: NodeA=8, NodeC=4.
   Best-fit picks **NodeC** (4 < 8). NodeA stays clean.
3. Later, TP=8 needs to scale back up — NodeA has 8 free. Success.

This is the critical elastic scenario. Best-fit naturally preserves large free
blocks without any static node labeling.

### 4.3 Cluster Scale-Out (New Node Joins)

1. NodeD joins with 8 GPUs.
2. `allocator.refresh_nodes()` discovers it (called before spawns).
3. Next TP=1 spawn: NodeD=8 free is worst-fit — allocator prefers NodeC (4 free).
4. NodeD stays clean for future TP=8 deployment.

No configuration change needed.

### 4.4 Node Failure

1. NodeB dies, taking its TP=8 worker.
2. Pool detects worker death via existing recovery logic.
3. Manager calls `allocator.refresh_nodes()` — removes NodeB, prunes placements.
4. Allocator suggests NodeD (8 free) for respawn.
5. Worker respawns on NodeD.

Staleness does not cause failures because `soft=True` lets Ray handle fallback.

### 4.5 Mixed TP Sizes (TP=8, TP=4, TP=2, TP=1)

Best-fit handles all sizes naturally:

| Step | Event | NodeA(8) | NodeB(8) | NodeC(8) | Choice                                 |
|------|-------|----------|----------|----------|-----------------------------------------|
| 1    | TP=8  | **0**    | 8        | 8        | NodeA                                   |
| 2    | TP=4  | 0        | **4**    | 8        | NodeB                                   |
| 3    | TP=2  | 0        | **2**    | 8        | NodeB (4 free -> 2 free, packs tight)   |
| 4    | TP=1  | 0        | **1**    | 8        | NodeB (2 free -> 1 free)                |
| 5    | TP=1  | 0        | **0**    | 8        | NodeB (1 free -> 0, full)               |
| 6    | TP=4  | 0        | 0        | **4**    | NodeC                                   |

16 GPUs used across 2 nodes. NodeC still has a 4-GPU block free.

### 4.6 Fractional GPU Co-Location

Deploy TP=8 (1 replica) + OCR models with `gpu_memory_utilization=0.3` (auto
`num_gpus=0.5`):

| Step | Event                 | NodeA(8) | NodeB(8) | Choice                       |
|------|-----------------------|----------|----------|------------------------------|
| 1    | TP=8                  | **0.0**  | 8.0      | NodeA                        |
| 2    | OCR-A (0.5 GPU)       | 0.0      | **7.5**  | NodeB                        |
| 3    | OCR-B (0.5 GPU)       | 0.0      | **7.0**  | NodeB (packs onto same node) |
| 4    | OCR-A scale up (0.5)  | 0.0      | **6.5**  | NodeB                        |
| 5    | OCR-B scale up (0.5)  | 0.0      | **6.0**  | NodeB (4 OCR workers on 2 GPUs) |

4 OCR workers share 2 physical GPUs on NodeB. NodeA is fully dedicated to TP=8.

### 4.7 Heterogeneous Cluster (Mixed Node Sizes)

Cluster: NodeA=8 GPUs, NodeB=4 GPUs, NodeC=8 GPUs

| Step | Event | NodeA(8) | NodeB(4) | NodeC(8) | Choice                                          |
|------|-------|----------|----------|----------|-------------------------------------------------|
| 1    | TP=8  | **0**    | 4        | 8        | NodeA (NodeB too small)                         |
| 2    | TP=4  | 0        | **0**    | 8        | NodeB (4 = exact fit, best-fit over NodeC's 8)  |
| 3    | TP=1  | 0        | 0        | **7**    | NodeC (only fit)                                |
| 4    | TP=8  | 0        | 0        | 7        | No fit — compaction or wait for NodeA            |

Best-fit naturally routes TP=4 to the 4-GPU node, keeping 8-GPU nodes for TP=8.

### 4.8 Compaction

1. TP=1 workers have spilled: NodeA has 2 workers (2 used, 6 free),
   NodeB has 1 worker (1 used, 7 free).
2. TP=8 tries to spawn — no node with 8 free.
3. Manager calls `allocator.plan_compaction(8)`: NodeB needs only 1 eviction to
   free 8 GPUs (cheapest).
4. Manager freezes the affected pool's autoscaler.
5. Evicts the 1 worker on NodeB.
6. Retries TP=8 spawn — NodeB has 8 free. Success.
7. Unfreezes the affected pool's autoscaler. It respawns the evicted worker —
   allocator routes it to NodeA (6 free, best-fit over NodeB's 0 free).

Compaction is atomic within the Manager — no race between eviction and autoscaler.

---

## 5. Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Worker crash** (OOM, vLLM bug) | Pool loses one worker; allocator has phantom placement | Pool detects via `is_failed()`. Manager calls `allocator.reconcile()` with live worker IDs to clean up. Autoscaler respawns. |
| **Node death** | Workers on node die; allocator has stale node entry | `allocator.refresh_nodes()` removes dead node and prunes all placements on it. Pools respawn on surviving nodes. |
| **Manager actor crash** | All pool and allocator state lost | InferenceWorker actors (detached) and Registry survive. New Manager reconnects: discovers live workers via Registry, rebuilds pool state and allocator placements from actual worker `get_node_id()` reports. |
| **Registry actor crash** | Heartbeats fail, endpoint discovery breaks | Workers retry heartbeat; operators retry endpoint fetch. Manager can recreate Registry and workers re-register on next heartbeat. |
| **Allocator stale state** | Suboptimal placement hints | `soft=True` lets Ray handle actual scheduling. Allocator self-corrects on next `record_placement()` with actual node_id. No correctness risk. |

---

## 6. Length-Based Routing

### 6.1 Motivation

For VLM workloads, most requests are short (small images, <2K tokens) but some
are long (large documents, 10K-30K tokens). Deploying the same model with
different `max_model_len` settings and routing by estimated input length provides:

1. **Latency isolation**: Short requests never queue behind long prefills.
2. **Higher concurrency on short-context workers**: Lower `max_model_len` allows
   higher `max_num_seqs` (e.g., 128 vs 32), since worst-case per-sequence memory
   is bounded.
3. **Self-adjusting ratio**: Each deployment pool autoscales independently based
   on its own load. The short/long split ratio is not hardcoded.

### 6.2 Approach: Two Deployments + Per-Row Routing

Deploy the same model twice with different `max_model_len` values:

```
Deployment 1: qwen-8k   (max_model_len=8192,  max_num_seqs=128)
Deployment 2: qwen-32k  (max_model_len=32768, max_num_seqs=32)
```

The operator estimates each request's token count and routes to the appropriate
deployment. Each pool autoscales independently — no tiered-pool complexity.

### 6.3 Config

```python
@dataclass
class ModelRoute:
    """Routing rule: requests with estimated tokens <= max_tokens use this model."""
    model_id: str
    max_tokens: int

@dataclass
class ModelRoutingConfig:
    """Length-based routing across multiple deployments of the same model."""
    routes: list[ModelRoute]          # sorted by max_tokens ascending
    tokens_per_image: int = 1000      # estimated tokens per image (VLM)
    chars_per_token: float = 3.5      # rough chars-to-token ratio

    def __post_init__(self):
        if len(self.routes) < 1:
            raise ValueError("ModelRoutingConfig requires at least one route")
        for i in range(1, len(self.routes)):
            if self.routes[i].max_tokens <= self.routes[i - 1].max_tokens:
                raise ValueError("routes must have strictly increasing max_tokens")

# Added to ExternalLLMOperatorConfig:
model_routing: Optional[ModelRoutingConfig] = None
```

When `model_routing` is set, the operator ignores the `model` field and routes
per-row based on estimated token count.

### 6.4 Token Estimation

A rough estimate is sufficient for routing (not exact tokenization):

```python
def _estimate_tokens(self, messages: list[dict]) -> int:
    cfg = self._config.model_routing
    total = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total += len(content) / cfg.chars_per_token
        elif isinstance(content, list):
            for item in content:
                if item.get("type") == "text":
                    total += len(item.get("text", "")) / cfg.chars_per_token
                elif item.get("type") == "image_url":
                    total += cfg.tokens_per_image
    return int(total)

def _pick_model(self, estimated_tokens: int) -> str:
    for route in self._config.model_routing.routes:
        if estimated_tokens <= route.max_tokens:
            return route.model_id
    # Exceeds all routes — send to largest, log warning
    logger.warning(
        f"Estimated {estimated_tokens} tokens exceeds max route "
        f"({self._config.model_routing.routes[-1].max_tokens}), "
        f"routing to largest deployment"
    )
    return self._config.model_routing.routes[-1].model_id
```

### 6.5 Per-Row Routing in `process_split()`

Group rows by target model, then process each group with batched parallelism:

```python
async def _process_with_routing(self, messages_list):
    # 1. Group by target model
    groups: dict[str, list[tuple[int, list[dict]]]] = defaultdict(list)
    for i, messages in enumerate(messages_list):
        estimated = self._estimate_tokens(messages)
        model_id = self._pick_model(estimated)
        groups[model_id].append((i, messages))

    # 2. Process all groups in parallel
    async def _process_group(model_id, items):
        endpoints = await self._get_model_client().get_endpoints(model_id)
        selector = EndpointSelectPolicy(endpoints)
        results = []
        for batch_start in range(0, len(items), self._config.batch_size):
            batch = items[batch_start : batch_start + self._config.batch_size]
            batch_results = await asyncio.gather(
                *(self._generate_one(m, selector.next()) for _, m in batch)
            )
            results.extend(zip([idx for idx, _ in batch], batch_results))
        return results

    all_results = await asyncio.gather(
        *(_process_group(mid, items) for mid, items in groups.items())
    )

    # 3. Merge results in original row order
    outputs = [""] * len(messages_list)
    for group_results in all_results:
        for idx, result in group_results:
            outputs[idx] = result
    return outputs
```

---

## 7. Usage Example

```python
from _internal.serve.config import ModelConfig
from _internal.serve.manager import create_manager, ModelServiceManager

# Create Manager actor (detached — survives job exit)
manager = create_manager(detached=True)

# Deploy heterogeneous models — allocator handles placement automatically
models = [
    ModelConfig(
        model_id="qwen-8k",
        model_source="Qwen/Qwen2.5-VL-72B-Instruct",
        tensor_parallel_size=8,
        max_model_len=8192,
        min_workers=7, max_workers=10,
        extra_engine_kwargs={"max_num_seqs": 128},
    ),
    ModelConfig(
        model_id="qwen-32k",
        model_source="Qwen/Qwen2.5-VL-72B-Instruct",
        tensor_parallel_size=8,
        max_model_len=32768,
        min_workers=3, max_workers=5,
        extra_engine_kwargs={"max_num_seqs": 32},
    ),
    ModelConfig(
        model_id="deepseek-ocr",
        model_source="deepseek-ai/DeepSeek-VL2",
        tensor_parallel_size=1,
        gpu_memory_utilization=0.3,   # < 0.5 → auto num_gpus=0.5
        min_workers=4, max_workers=8,
    ),
]

await manager.deploy_models.remote(models)
# Allocator automatically:
#   - Deploys TP=8 models first, claiming full nodes
#   - Packs TP=1/0.5-GPU workers onto remaining partially-used nodes
#   - OCR workers share GPUs (2 per physical GPU)
#   - Maintains packing during autoscaling

# Later job: reconnect without redeploying
manager = ModelServiceManager.connect()
registry = ray.get(manager.get_registry.remote())

# Pipeline stage with length-based routing
Stage(
    stage_id="caption",
    operator_config=ExternalLLMOperatorConfig(
        use_model_client=True,
        registry=registry,
        model_routing=ModelRoutingConfig(
            routes=[
                ModelRoute(model_id="qwen-8k", max_tokens=7500),
                ModelRoute(model_id="qwen-32k", max_tokens=30000),
            ],
            tokens_per_image=1000,
        ),
        prompt="Describe this image.",
        image_field="image",
    ),
)
```

---

## 8. Files to Change

| File | Change |
|------|--------|
| `_internal/serve/allocator.py` | **New file.** `GPUAllocator` plain class: best-fit bin-packing, per-node float tracking, compaction planner, scale-down advisory, reconciliation |
| `_internal/serve/pool.py` | Demote from Ray actor to plain class; accept allocator; direct-call allocator for suggest/record; node-aware scale-down |
| `_internal/serve/manager.py` | Promote to `@ray.remote` actor; own pools and allocator as plain objects; `deploy_models()` with two-phase ordering; `spawn_with_compaction()` with freeze/evict/spawn/unfreeze; `connect()` simplified to single actor lookup; `create_manager()` helper |
| `_internal/serve/config.py` | `get_worker_resources()` auto-infers `num_gpus=0.5` when TP=1 and `gpu_memory_utilization < 0.5` |
| `_internal/serve/worker.py` | Add `get_node_id()` via `ray.get_runtime_context().get_node_id()` |
| `_internal/operators/llm/operator.py` | Add `ModelRoute`, `ModelRoutingConfig` with validation; implement `_estimate_tokens()`, `_pick_model()`, `_process_with_routing()` |
| `workflows/run_multi_ocr_enrich.py` | Remove manual head node detection and `node:{ip}` pinning; use `deploy_models()` and `ModelRoutingConfig` |

---

## 9. Design Decisions

### Architecture

| Decision | Rationale |
|----------|-----------|
| Manager as Ray actor, Pool as plain class | Compaction requires cross-pool coordination. Co-locating pools in the Manager makes compaction a local method call with no distributed coordination. Autoscaler freeze/unfreeze is atomic. |
| Allocator as plain class inside Manager | Eliminates 2 RPC round-trips per spawn (suggest + record). All scheduling decisions are microsecond-level local calls. No separate actor to crash or become stale. |
| Single Manager actor over N Pool actors | `connect()` finds one actor instead of N. All state is co-located. Single point of failure is acceptable because InferenceWorkers and Registry survive Manager crash, and state can be rebuilt from Registry. |

### Placement

| Decision | Rationale |
|----------|-----------|
| Dynamic best-fit over static node affinity | No assumptions about cluster topology. Adapts to node joins/leaves, heterogeneous nodes, and mixed TP sizes automatically. |
| Advisory (`soft=True`) over hard scheduling | The allocator is a hint layer. Stale state does not cause failures — Ray handles the actual scheduling as fallback. |
| Best-fit over first-fit or worst-fit | Best-fit minimizes wasted space by packing into the tightest-fitting node. Naturally preserves large free blocks for future large requests. |
| Fractional GPU auto-inference | TP=1 models with `gpu_memory_utilization < 0.5` get `num_gpus=0.5` automatically. Doubles small-model density without manual `worker_resources` override. |
| Compaction on demand over periodic defrag | Trigger only when a spawn actually fails. No unnecessary worker churn during steady state. |
| Two-phase deploy (large then small) | Large models need contiguous GPU blocks. Sequential deployment lets the allocator track their placements before small models compete. |
| Dynamic TP threshold | `max_node_gpus / 2` instead of hardcoded 4. Adapts to cluster topology (e.g., 4-GPU nodes vs 8-GPU nodes). |
| Random tiebreaker in best-fit | When multiple nodes have equal free GPUs, random selection avoids always picking the same node. |

### Routing

| Decision | Rationale |
|----------|-----------|
| Two deployments over tiered pool | Each pool autoscales independently. The short/long ratio self-adjusts based on actual load. |
| Rough token estimation over exact tokenization | Routing classifies "short vs long", not exact counts. `len(text) / 3.5 + images * 1000` is fast and sufficient. |
| Per-row routing in operator | The operator has access to actual message content (images, text) needed for estimation. The client only sees model IDs. |
| Group-then-batch processing | Grouping rows by target model preserves `asyncio.gather` parallelism. Processing groups concurrently prevents one model from blocking the other. |
| Warning on overflow routing | Requests exceeding all route thresholds get routed to the largest deployment with a warning log, rather than silently dropping or erroring. |
