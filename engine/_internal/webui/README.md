# Nurion Runtime WebUI

A web-based debugging and monitoring interface for Nurion runtime jobs.

## Implementation Status

| Feature | Status |
|---------|--------|
| Portal Service (Ray Serve) | ✅ Complete |
| Unified Read-Only Architecture | ✅ Complete |
| Push-Based Metrics (Anvil) | ✅ Complete |
| Job/Stage/Worker Pages | ✅ Complete |
| Anvil Storage Reader | ✅ Complete |
| SSE Real-Time Updates | ❌ Pending |
| Lineage Visualization | ❌ Pending |
| Chart.js Metrics | ❌ Pending |

## Features

- **Real-time Monitoring**: Live metrics, progress tracking, and resource usage
- **History Server**: View completed jobs and perform post-mortem analysis
- **Multi-Job Support**: Monitor multiple jobs in the same Ray cluster
- **Prometheus Integration**: Export metrics for Grafana dashboards
- **Lineage Tracking**: Visualize data flow and split relationships
- **Exception Aggregation**: Track and analyze errors
- **Worker Debugging**: View logs, stacktraces, and resource usage

## Architecture

### Dual-Mode Design

1. **Embedded Mode**: WebUI runs inside the driver process (uvicorn)
2. **History Server Mode**: Standalone service for viewing archived jobs

### Storage Strategy

- **Anvil storage (pyO3)**: Job metadata, events, lineage
- **Prometheus**: Optional real-time metrics (records/s, lag, backpressure)

## Usage

### Embedded Mode (with Running Job)

```python
from nurion import Job, JobConfig, WebUIConfig

# Enable WebUI
job = Job(
    job_id="my_etl_job",
    config=JobConfig(
        anvil_db_path="file:///tmp/anvil.db",
        webui=WebUIConfig(
            enabled=True,
        ),
    ),
)

# Add stages...
job.add_stage(source_stage)
job.add_stage(transform_stage)
job.add_stage(sink_stage)

# Run
runner = job.create_ray_runner()
await runner.run()

# WebUI will be available at: http://localhost:<port>/jobs/{job_id}/
# (port starts at 5000 and increments until free)
```

### History Server Mode

```bash
# Start History Server
nurion history-server -s file:///tmp/anvil.db -p 8080

# Access at: http://localhost:8080
```

## Portal Structure

```
http://localhost:<port>/
├── /                           → All jobs (running + completed)
├── /running                    → Running jobs only
├── /completed                  → Completed jobs only
├── /jobs/{job_id}/             → Job detail page
│   ├── /stages/{stage_id}      → Stage detail
│   ├── /workers/{worker_id}    → Worker detail
│   ├── /exceptions             → Exception list
│   └── /lineage                → Lineage graph
└── /api/...                    → REST API
```

## Configuration

### WebUIConfig Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | False | Enable WebUI |
| `port` | int | 5000 | Embedded WebUI base port (auto-increment) |
| `lineage_sample_rate` | float | 0.0 | Split lineage sampling rate |

Anvil storage is configured via `JobConfig.anvil_db_path`.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RAY_DASHBOARD_URL` | Ray Dashboard URL (default: http://localhost:8265) |
| `SOLSTICE_GRAFANA_URL` | Legacy Grafana URL for external link |
| `RAY_PROMETHEUS_HOST` | Ray Prometheus endpoint (default: http://localhost:8080) |

## Prometheus Metrics

Nurion Engine exports the following metrics with `nurion_` prefix:

### Stage-Level Metrics

- `solstice_throughput_records_per_second{job_id, stage_id, direction}`
- `solstice_queue_lag{job_id, stage_id}`
- `solstice_queue_size{job_id, stage_id}`
- `solstice_backpressure_active{job_id, stage_id}`
- `solstice_skew_ratio{job_id, stage_id}`
- `solstice_worker_count{job_id, stage_id}`

### Partition-Level Metrics

- `solstice_partition_lag{job_id, stage_id, partition_id}`
- `solstice_partition_offset{job_id, stage_id, partition_id, offset_type}`

### Worker-Level Metrics

- `solstice_processing_time_seconds{job_id, stage_id}` (histogram)

## UI Pages

### Overview
- Cluster resources (CPU/Memory/GPU usage)
- Job statistics (running/completed/failed)
- External links (Ray Dashboard, Grafana)

### Job Detail
- Stage DAG visualization
- Progress tracking with ETA
- Timeline of events
- Metrics and throughput charts

### Stage Detail
- Partition-level metrics and offsets
- Data skew detection and visualization
- Backpressure status
- Worker list with resource usage

### Worker Detail
- CPU/Memory/GPU monitoring
- Processing statistics
- Real-time log viewer
- Stacktrace viewer (py-spy integration)
- Links to Ray Dashboard

### Exceptions
- Exception aggregation by type
- Occurrence counts and timestamps
- Full stacktrace viewer
- Root cause analysis hints

### Lineage
- Interactive DAG of split relationships
- Worker processing information
- Search and filtering

## Development

### Adding New API Endpoints

```python
# engine/webui/api/my_feature.py
from fastapi import APIRouter, Request

router = APIRouter(tags=["my_feature"])

@router.get("/my-endpoint")
async def my_endpoint(request: Request):
    # Access mode
    mode = request.app.state.mode
    
    # Access storage
    storage = request.app.state.storage
    
    # Access runner (embedded mode only)
    if mode == "embedded":
        runner = request.app.state.job_runner
    
    return {"data": "..."}

# Register in engine/webui/app.py
```

### Adding New Templates

Templates use Jinja2 and should extend `base.html`:

```html
{% extends "base.html" %}

{% block title %}My Page{% endblock %}

{% block content %}
<article>
    <h1>My Page</h1>
    <!-- Content here -->
</article>
{% endblock %}
```

## Styling

The UI uses:
- **Pico CSS** for base styles (10KB, semantic)
- **Custom styles** in `static/css/nurion.css`
- **HTMX** for dynamic updates
- **Alpine.js** for interactive components
- **Chart.js** for metrics visualization

### Key Classes

- `.badge-running`, `.badge-completed`, `.badge-failed`
- `.metric-card`, `.metric-value`, `.metric-label`
- `.progress-bar`, `.progress-fill`
- `.table-container`, `.sticky-col`
- `.log-viewer`, `.log-content`

## Performance Considerations

### Large Datasets

- **Pagination**: All list APIs support `page` and `page_size` parameters
- **Fixed Table Headers**: `position: sticky` for scrollable tables
- **Fixed First Column**: `sticky-col` class for wide tables
- **Log Limits**: Log viewer limited to 5000 lines in memory
- **Debounced Filtering**: 300ms debounce on search inputs

### API Limits

- Maximum page size: 1000 items
- Default page size: 100 items
- Worker logs tail: default 100 lines, max 10000

## Troubleshooting

### WebUI Not Starting

1. Check Ray is initialized: `ray.is_initialized()`
2. Check Ray Serve is available: `ray.serve.list_deployments()`
3. Check logs for Portal deployment errors

### Metrics Not Appearing

1. Check if Prometheus is scraping Ray metrics endpoint (if enabled)
2. Verify Anvil DB path is writable

### History Server Shows No Jobs

1. Check Anvil DB path is correct
2. Verify jobs are writing state via gRPC
3. Check logs for storage read errors

## Future Enhancements

- [ ] Grafana dashboard templates
- [ ] Alert rules based on metrics
- [ ] Query interface for searching splits/exceptions
- [ ] Export job report (PDF/HTML)
- [ ] Compare multiple job runs
- [ ] Resource recommendation based on history

