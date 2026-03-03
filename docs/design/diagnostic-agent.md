# Diagnostic Agent

## Motivation

When running distributed data processing + LLM inference services, troubleshooting in nurion relies on manual log inspection and WebUI. We need a system-level agentic diagnostic actor that automatically collects signals, detects anomalies, performs deep root-cause analysis, and provides actionable recommendations.

## Architecture

```
┌───────────────────────────────────────────────┐
│           DiagnosticAgent (Ray Actor)         │
│   detached=True, namespace="nurion_serve"     │
├───────────────────────────────────────────────┤
│                                               │
│  Collector ──→ Rules Engine ──→ LLM Agent     │
│  (snapshots)   (fast detect)   (deep dive)    │
│                     │                │        │
│                     └── findings ────┘        │
│                              │                │
│                     DiagnosticReport          │
│                  (structured + natural lang)   │
└───────────────────────────────────────────────┘
```

### Standalone Sub-project

Decoupled from engine, independently deployable.

```
nurion/
├── engine/
├── control/
├── lib/
└── diagnostic/          ← new sub-project
    ├── pyproject.toml   # deps: ray, httpx, anthropic, pydantic
    ├── AGENTS.md
    └── diagnostic/
        ├── agent.py         # DiagnosticAgent Ray actor
        ├── collector/       # Data collection layer
        │   ├── pipeline.py  # WebUI API collector
        │   ├── serve.py     # Registry HTTP collector
        │   └── cluster.py   # Ray cluster API collector
        ├── rules/           # Rules engine
        │   ├── engine.py    # Rule framework
        │   ├── pipeline.py  # Pipeline rules
        │   ├── serve.py     # Serve rules
        │   └── resource.py  # Resource rules
        ├── llm/             # LLM agent loop
        │   ├── investigator.py  # Agent loop
        │   ├── tools.py         # Tool definitions + execution
        │   └── policy.py        # Trigger policy
        ├── models.py        # Finding, Report, Recommendation
        └── api.py           # HTTP API
```

**Zero engine dependency**: All data is obtained via HTTP APIs and Ray public APIs.

## Collector Layer

Collection interval: 30s. Non-intrusive, read-only collection.

### Data Sources

| Source | Method | Data |
|--------|--------|------|
| WebUI API | HTTP GET | Queue stats, worker status, events, serve models/workers |
| ModelRegistry | HTTP GET `/status`, `/endpoints_status` | Per-model worker count, pending/running |
| Ray Cluster | `ray.cluster_resources()`, `ray.nodes()` | CPU/GPU/memory total/available |
| ModelServiceManager | Ray RPC `list_models()` | Deployment config |

### Snapshot Data Structure

```python
@dataclass
class SystemSnapshot:
    timestamp: float
    queue_snapshots: dict[str, QueueSnapshot]    # per stage
    serve_snapshots: dict[str, ServeSnapshot]     # per model
    cluster: ClusterSnapshot                      # resources + nodes
```

## Rules Engine Layer

Deterministic rules, millisecond-level detection, covering known failure modes.

### Pipeline Rules

| Rule | Condition | Severity |
|------|-----------|----------|
| queue_stall | pending > 100 and claimed == 0 | critical |
| high_nack_rate | recent nack/total > 30% | warning |
| throughput_drop | total_acked delta is 0 for 3 consecutive cycles | warning |
| worker_all_dead | All worker metadata is FAILED/COMPLETED but job still RUNNING | critical |

### Serve Rules

| Rule | Condition | Severity |
|------|-----------|----------|
| autoscaler_stuck | inflight > threshold but worker_count == max_workers for 5 min | warning |
| worker_loading_timeout | status=LOADING for over 10 min | warning |
| all_workers_failed | ready_workers == 0 and total_workers > 0 | critical |
| gpu_fragmentation | Multiple nodes each have 1 GPU remaining but TP=2 required | info |

### Resource Rules

| Rule | Condition | Severity |
|------|-----------|----------|
| cluster_gpu_exhausted | available GPU == 0 | warning |
| node_offline | ray.nodes() has Alive=False | critical |
| memory_pressure | object_store_memory available < 20% | warning |

## LLM Agent Layer

Rules engine does triage, LLM does deep dive.

### Trigger Policy

```python
def should_invoke_llm(finding: Finding) -> bool:
    if finding.severity == "critical": return True
    if finding.category == "unknown_anomaly": return True
    if is_recurring(finding, window=10min, threshold=3): return True
    if finding.category == "user_triggered": return True
    if finding.category == "scheduled_review": return True  # hourly
    return False
```

### LLM Tools

The LLM agent investigates autonomously via tool-use. All tools are read-only:

| Tool | Description |
|------|-------------|
| `get_queue_stats` | Pending/claimed/acked for a given queue |
| `get_recent_events` | Last N events (ack/nack/timeout) |
| `get_worker_logs` | Ray actor logs (Ray State API) |
| `get_serve_status` | Model service status (Registry HTTP) |
| `get_metrics_history` | Metric time series (WebUI API) |
| `get_cluster_resources` | Ray cluster resources |
| `get_operator_config` | Stage operator/autoscaler config |
| `read_source_code` | Operator source code (for logic bug analysis) |
| `get_gpu_allocation_map` | GPU allocation details |

### Agent Loop

```python
async def investigate(self, trigger: Finding) -> DiagnosticReport:
    messages = [{"role": "user", "content": f"""
        You are an SRE diagnostic expert for the nurion distributed processing system.
        Anomaly detected: {trigger}
        Use the available tools to investigate the root cause and provide:
        1. Root cause analysis  2. Impact scope  3. Fix recommendations  4. Prevention measures
    """}]

    for _ in range(max_turns := 10):
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            tools=DIAGNOSTIC_TOOLS,
            messages=messages,
        )
        if response.stop_reason == "end_turn":
            return parse_report(response)
        # execute tool calls, append results
        ...
```

Model selection: Sonnet for routine checks (fast + cheap), Opus for critical issues (strongest reasoning).

### Hidden Issues LLM Can Discover

1. **Data skew stalls**: One worker's processing_ms far exceeds others → uneven split sizes
2. **Autoscaler oscillation**: Worker count repeatedly toggles → threshold/cooldown parameter conflicts
3. **Early memory leak signals**: object_store_memory linearly decreasing → unreleased references
4. **Queue config typos**: Messages written to a queue no one consumes
5. **Cross-system correlations**: Serve worker OOM + another model on the same node stealing memory → GPU fragmentation

## Output

```python
@dataclass
class DiagnosticReport:
    timestamp: float
    findings: list[Finding]
    recommendations: list[Recommendation]
    system_summary: SystemSummary

@dataclass
class Finding:
    severity: str        # critical, warning, info
    category: str        # queue_stall, autoscaler_stuck, ...
    message: str
    context: dict        # related metric snapshots

@dataclass
class Recommendation:
    action: str          # scale_up_model, trigger_gc, adjust_threshold
    target: str          # model_id / job_id / stage_id
    params: dict         # {"target_workers": 4}
    confidence: float    # 0-1
    auto_executable: bool
```

## Implementation Phases

### Phase 1: Passive Diagnostics (Read-only, Safest)

- Detached Ray actor, periodic collection + rule-based detection
- HTTP API to expose diagnostic reports
- Log findings with severity >= warning

### Phase 2: Intelligent Recommendations (LLM Integration)

- Rules engine triggers LLM deep analysis
- Historical trend comparison (retain last N snapshots for diffing)
- Structured DiagnosticReport output

### Phase 3: Auto-remediation (Optional, Use with Caution)

- `auto_executable=True` for low-risk operations (trigger GC, adjust autoscaler params)
- Requires explicit user opt-in
- All automated actions logged to audit trail

## Safety Boundaries

1. **Read-only**: All tools are read-only, no system state modifications
2. **Confirmation required**: Auto-remediation requires user opt-in
3. **Audit logging**: All LLM interactions are logged
4. **Fault isolation**: DiagnosticAgent crash does not affect pipeline or serve
5. **Cost control**: Rules engine filters 90% noise, each investigation capped at max_turns=10

## Startup

```bash
cd diagnostic && uv run python -m diagnostic.cli
```

```python
ray.init(address="auto")
agent = DiagnosticAgent.options(
    name="nurion_diagnostic_agent",
    namespace="nurion_serve",
    lifetime="detached",
).remote(config)
agent.run.remote()
```
