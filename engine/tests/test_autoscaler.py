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

"""Tests for the SimpleAutoscaler.

Tests the autoscaling functionality including:
- Threshold-based scaling decisions
- Cooldown periods
- Metrics collection from StageMaster + QueueStatsClient
- Scale execution (up/down with min/max bounds)
- Resource-aware scaling (proactive cluster resource check)
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from _internal.core.models import QueueStats
from _internal.runtime.autoscaler import StageAutoscaleConfig, SimpleAutoscaler, StageMetrics
from _internal.runtime.queue_stats import QueueRef, StageQueueConfig


# ============================================================================
# Mock Classes
# ============================================================================


class MockStageMaster:
    """Mock StageMaster for testing autoscaler."""

    def __init__(
        self,
        stage_id: str = "test_stage",
        worker_count: int = 2,
        min_workers: int = 1,
        max_workers: int = 8,
        input_queue_lag: int = 0,
        num_cpus: float = 0.5,
        num_gpus: float = 0.0,
    ):
        self.stage_id = stage_id
        self._workers = {f"worker_{i}": MagicMock() for i in range(worker_count)}
        self._source_manager = None  # Not a source stage by default

        # Stage (replaces config)
        self.stage = MagicMock()
        self.stage.min_parallelism = min_workers
        self.stage.max_parallelism = max_workers
        self.stage.num_cpus = num_cpus
        self.stage.num_gpus = num_gpus

    async def scale_up(self, count: int) -> int:
        """Scale up by spawning new workers."""
        to_add = min(count, self.stage.max_parallelism - len(self._workers))
        for _ in range(to_add):
            worker_id = f"worker_{len(self._workers)}"
            self._workers[worker_id] = MagicMock()
        return to_add

    async def scale_down(self, count: int) -> int:
        """Scale down by removing workers."""
        to_remove = min(count, len(self._workers) - self.stage.min_parallelism)
        for _ in range(to_remove):
            if self._workers:
                key = list(self._workers.keys())[-1]
                del self._workers[key]
        return to_remove


class FakeQueueStatsClient:
    def __init__(self, stats: dict[str, QueueStats]) -> None:
        self._stats = stats

    def get_ref_stats(self, ref: QueueRef | None) -> QueueStats:
        if not ref:
            return QueueStats()
        return self._stats.get(ref.name, QueueStats())


# ============================================================================
# Unit Tests
# ============================================================================


class TestStageAutoscaleConfig:
    """Tests for StageAutoscaleConfig."""

    def test_default_config(self):
        config = StageAutoscaleConfig()
        assert config.enabled is True
        assert config.check_interval_s == 10.0
        assert config.scale_up_lag_threshold == 500
        assert config.scale_down_lag_threshold == 100
        assert config.cooldown_up_s == 15.0
        assert config.cooldown_down_s == 60.0
        assert config.max_scale_step == 32

    def test_custom_config(self):
        config = StageAutoscaleConfig(
            enabled=False,
            check_interval_s=30.0,
            scale_up_lag_threshold=500,
        )
        assert config.enabled is False
        assert config.check_interval_s == 30.0
        assert config.scale_up_lag_threshold == 500


class TestStageMetrics:
    """Tests for StageMetrics."""

    def test_metrics_creation(self):
        metrics = StageMetrics(
            stage_id="test",
            worker_count=3,
            min_workers=1,
            max_workers=10,
            input_queue_lag=500,
        )
        assert metrics.stage_id == "test"
        assert metrics.worker_count == 3
        assert metrics.input_queue_lag == 500
        assert metrics.is_source is False


class TestSimpleAutoscaler:
    """Tests for SimpleAutoscaler."""

    def test_init(self):
        autoscaler = SimpleAutoscaler()
        assert autoscaler.config.enabled is True
        assert autoscaler._running is False

    def test_init_with_config(self):
        config = StageAutoscaleConfig(check_interval_s=10.0)
        autoscaler = SimpleAutoscaler(config)
        assert autoscaler.config.check_interval_s == 10.0


class TestScalingDecisions:
    """Tests for scaling decision logic."""

    @pytest.fixture
    def autoscaler(self):
        config = StageAutoscaleConfig(
            scale_up_lag_threshold=1000,
            scale_down_lag_threshold=100,
            max_scale_step=2,
        )
        return SimpleAutoscaler(config)

    def test_scale_up_on_high_lag(self, autoscaler):
        """Should scale up when lag exceeds threshold."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=1500,  # Above threshold
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" in decisions
        assert decisions["stage_a"] == 4  # 2 + max_scale_step(2)

    def test_scale_down_on_low_lag(self, autoscaler):
        """Should scale down when lag is below threshold."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=4,
                min_workers=1,
                max_workers=8,
                input_queue_lag=50,  # Below threshold
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" in decisions
        assert decisions["stage_a"] == 3  # 4 - 1

    def test_no_scale_down_with_claimed_inflight(self, autoscaler):
        """Should not scale down when messages are still claimed."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=4,
                min_workers=1,
                max_workers=8,
                input_queue_lag=50,  # Below threshold
                input_queue_claimed=3,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions

    def test_no_scale_in_normal_range(self, autoscaler):
        """Should not scale when lag is in normal range."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=500,  # Between thresholds
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions

    def test_respect_max_workers(self, autoscaler):
        """Should not scale above max_workers."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=7,
                min_workers=1,
                max_workers=8,
                input_queue_lag=2000,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert decisions["stage_a"] == 8  # Not 9

    def test_respect_min_workers(self, autoscaler):
        """Should not scale below min_workers."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=1,
                min_workers=1,
                max_workers=8,
                input_queue_lag=10,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions  # Already at min

    def test_source_with_fixed_parallelism_skipped(self, autoscaler):
        """Source stage with min==max (fixed parallelism) should not scale."""
        metrics = {
            "source": StageMetrics(
                stage_id="source",
                worker_count=4,
                min_workers=4,
                max_workers=4,
                input_queue_lag=5000,
                is_source=True,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "source" not in decisions

    def test_skip_finished_stages(self, autoscaler):
        """Should skip finished stages."""
        metrics = {
            "stage_a": StageMetrics(
                stage_id="stage_a",
                worker_count=2,
                min_workers=1,
                max_workers=8,
                input_queue_lag=5000,
                is_finished=True,
            )
        }

        decisions = autoscaler._compute_decisions(metrics)

        assert "stage_a" not in decisions


@pytest.mark.asyncio
class TestCooldown:
    """Tests for cooldown behavior."""

    async def test_cooldown_prevents_rapid_scaling(self):
        """Scaling should be blocked during cooldown period."""
        config = StageAutoscaleConfig(cooldown_up_s=60.0, cooldown_down_s=60.0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=2,
            input_queue_lag=2000,
        )
        masters = {"stage_a": master}

        # First scale
        decisions = {"stage_a": 4}
        await autoscaler._execute_decisions(masters, decisions)

        assert len(master._workers) == 4

        # Try to scale again immediately
        decisions = {"stage_a": 6}
        await autoscaler._execute_decisions(masters, decisions)

        # Should still be 4 due to cooldown
        assert len(master._workers) == 4

    async def test_scaling_after_cooldown(self):
        """Scaling should work after cooldown period."""
        config = StageAutoscaleConfig(
            cooldown_up_s=0.1, cooldown_down_s=0.1
        )  # Short cooldown for testing
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=2,
            input_queue_lag=2000,
        )
        masters = {"stage_a": master}

        # First scale
        await autoscaler._execute_decisions(masters, {"stage_a": 4})
        assert len(master._workers) == 4

        # Wait for cooldown
        await asyncio.sleep(0.15)

        # Should be able to scale now
        await autoscaler._execute_decisions(masters, {"stage_a": 6})
        assert len(master._workers) == 6


@pytest.mark.asyncio
class TestMetricsCollection:
    """Tests for metrics collection."""

    async def test_collect_metrics_from_masters(self):
        master = MockStageMaster(
            stage_id="stage_a",
            worker_count=3,
            input_queue_lag=500,
        )
        stage_cfg = StageQueueConfig(
            stage_id="stage_a",
            input=QueueRef.queue("input_stage_a"),
            output=QueueRef.queue("output_stage_a"),
            backpressure_threshold_lag=1000,
            backpressure_threshold_queue_size=1000,
        )
        stats_client = FakeQueueStatsClient(
            {
                "input_stage_a": QueueStats(pending_count=500, claimed_count=2),
                "output_stage_a": QueueStats(pending_count=10),
            }
        )
        autoscaler = SimpleAutoscaler(
            queue_stats_client=stats_client,
            stage_queue_configs={"stage_a": stage_cfg},
        )

        # Collect metrics - MockStageMaster is not a source stage
        metrics = await autoscaler._collect_metrics({"stage_a": master})

        assert "stage_a" in metrics
        assert metrics["stage_a"].worker_count == 3
        assert metrics["stage_a"].input_queue_lag == 500
        assert metrics["stage_a"].input_queue_claimed == 2
        assert metrics["stage_a"].is_source is False

    async def test_source_stage_marked_correctly(self):
        # Create a mock StageMaster with _source set (indicating it's a source stage)
        source = MagicMock()
        source.stage_id = "source"
        source._source = MagicMock()  # Non-None means it's a source stage
        source._workers = {"worker_0": MagicMock()}
        source._running = True
        source._finished = False
        source.stage = MagicMock()
        source.stage.min_parallelism = 1
        source.stage.max_parallelism = 1
        stage_cfg = StageQueueConfig(
            stage_id="source",
            input=None,
            output=QueueRef.queue("output_source"),
            backpressure_threshold_lag=1000,
            backpressure_threshold_queue_size=1000,
        )
        stats_client = FakeQueueStatsClient({"output_source": QueueStats(pending_count=25)})
        autoscaler = SimpleAutoscaler(
            queue_stats_client=stats_client,
            stage_queue_configs={"source": stage_cfg},
        )

        metrics = await autoscaler._collect_metrics({"source": source})

        assert metrics["source"].is_source is True


@pytest.mark.asyncio
class TestScaleExecution:
    """Tests for scale up/down execution."""

    async def test_scale_up_spawns_workers(self):
        autoscaler = SimpleAutoscaler()

        master = MockStageMaster(worker_count=2)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 5})

        assert len(master._workers) == 5

    async def test_scale_down_removes_workers(self):
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)  # No cooldown for testing
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=5, min_workers=1)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 2})

        assert len(master._workers) == 2

    async def test_scale_down_respects_min_workers(self):
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=3, min_workers=2)
        masters = {"stage_a": master}

        await autoscaler._execute_decisions(masters, {"stage_a": 1})

        # Should stop at min_workers
        assert len(master._workers) == 2


@pytest.mark.asyncio
class TestResourceAwareScaling:
    """Tests for proactive resource checking before scale-up."""

    async def test_scale_up_skipped_when_insufficient_cpus(self, monkeypatch):
        """Scale-up should be skipped when cluster lacks CPU resources."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=2, num_cpus=2.0, num_gpus=0.0)
        masters = {"stage_a": master}

        # Cluster has only 1 CPU available — worker needs 2
        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 1.0, "GPU": 0.0})

        await autoscaler._execute_decisions(masters, {"stage_a": 4})

        # Should still be 2 — scale-up skipped
        assert len(master._workers) == 2

    async def test_scale_up_skipped_when_insufficient_gpus(self, monkeypatch):
        """Scale-up should be skipped when cluster lacks GPU resources."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=2, num_cpus=0.5, num_gpus=1.0)
        masters = {"stage_a": master}

        # Cluster has CPUs but no GPUs
        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 10.0})

        await autoscaler._execute_decisions(masters, {"stage_a": 4})

        assert len(master._workers) == 2

    async def test_scale_up_proceeds_with_sufficient_resources(self, monkeypatch):
        """Scale-up should proceed when cluster has enough resources."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=2, num_cpus=1.0, num_gpus=1.0)
        masters = {"stage_a": master}

        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 8.0, "GPU": 4.0})

        await autoscaler._execute_decisions(masters, {"stage_a": 4})

        assert len(master._workers) == 4

    async def test_scale_up_proceeds_when_zero_cpu_required(self, monkeypatch):
        """Workers requiring 0 CPU should not be blocked by CPU check."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=2, num_cpus=0, num_gpus=0.0)
        masters = {"stage_a": master}

        # Even with 0 available resources, 0-requirement workers should pass
        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 0.0})

        await autoscaler._execute_decisions(masters, {"stage_a": 4})

        assert len(master._workers) == 4

    async def test_scale_down_not_affected_by_resource_check(self, monkeypatch):
        """Resource check should only gate scale-up, not scale-down."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=4, min_workers=1, num_cpus=2.0)
        masters = {"stage_a": master}

        # No resources available — but scale-down should still work
        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 0.0})

        await autoscaler._execute_decisions(masters, {"stage_a": 2})

        assert len(master._workers) == 2

    async def test_resource_check_error_does_not_block_scaling(self, monkeypatch):
        """If ray.available_resources() fails, scale-up should proceed."""
        config = StageAutoscaleConfig(cooldown_up_s=0, cooldown_down_s=0)
        autoscaler = SimpleAutoscaler(config)

        master = MockStageMaster(worker_count=2)
        masters = {"stage_a": master}

        def raise_error():
            raise RuntimeError("Ray not initialized")

        monkeypatch.setattr("ray.available_resources", raise_error)

        await autoscaler._execute_decisions(masters, {"stage_a": 4})

        # Should proceed despite error
        assert len(master._workers) == 4


# ============================================================================
# New Tests: Resource-Aware Scaling & Source-Aware Autoscaling
# ============================================================================


class TestGetSpawnableCount:
    """Tests for _get_spawnable_count."""

    def test_gpu_stage_limited_by_available_gpus(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"GPU": 8.0, "CPU": 100.0})
        autoscaler = SimpleAutoscaler()
        stage = MagicMock()
        stage.num_gpus = 1.0
        stage.num_cpus = 1.0
        assert autoscaler._get_spawnable_count(stage, max_needed=20) == 8

    def test_gpu_stage_limited_by_available_cpus(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"GPU": 16.0, "CPU": 4.0})
        autoscaler = SimpleAutoscaler()
        stage = MagicMock()
        stage.num_gpus = 1.0
        stage.num_cpus = 2.0
        # min(16 GPUs / 1, 4 CPUs / 2) = min(16, 2) = 2
        assert autoscaler._get_spawnable_count(stage, max_needed=20) == 2

    def test_cpu_stage(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"CPU": 32.0})
        autoscaler = SimpleAutoscaler()
        stage = MagicMock()
        stage.num_gpus = 0
        stage.num_cpus = 4.0
        assert autoscaler._get_spawnable_count(stage, max_needed=100) == 8

    def test_capped_by_max_needed(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"GPU": 96.0, "CPU": 500.0})
        autoscaler = SimpleAutoscaler()
        stage = MagicMock()
        stage.num_gpus = 1.0
        stage.num_cpus = 1.0
        assert autoscaler._get_spawnable_count(stage, max_needed=10) == 10

    def test_ray_unavailable_returns_max(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: (_ for _ in ()).throw(RuntimeError))
        autoscaler = SimpleAutoscaler()
        stage = MagicMock()
        stage.num_gpus = 1.0
        stage.num_cpus = 1.0
        assert autoscaler._get_spawnable_count(stage, max_needed=5) == 5


class TestSplitCooldowns:
    """Tests for AIMD-style split cooldowns (up fast, down slow)."""

    def test_default_cooldowns(self):
        config = StageAutoscaleConfig()
        assert config.cooldown_up_s == 15.0
        assert config.cooldown_down_s == 60.0


@pytest.mark.asyncio
class TestEagerFill:
    """Tests for eager_fill post-startup scaling."""

    async def test_eager_fill_scales_non_source_stages(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"GPU": 6.0, "CPU": 50.0})
        autoscaler = SimpleAutoscaler()

        # Source stage — should be skipped
        source = MockStageMaster(stage_id="source", worker_count=10, max_workers=100)
        source._source_manager = MagicMock()  # Mark as source

        # GPU stage — should be filled
        gpu_stage = MockStageMaster(
            stage_id="stage_0", worker_count=2, max_workers=96, num_gpus=1.0, num_cpus=1.0
        )

        masters = {"source": source, "stage_0": gpu_stage}
        await autoscaler.eager_fill(masters)

        # Source unchanged
        assert len(source._workers) == 10
        # GPU stage filled up to available (6 GPUs)
        assert len(gpu_stage._workers) == 8  # 2 + 6

    async def test_eager_fill_noop_when_already_at_max(self, monkeypatch):
        monkeypatch.setattr("ray.available_resources", lambda: {"GPU": 10.0, "CPU": 50.0})
        autoscaler = SimpleAutoscaler()

        stage = MockStageMaster(stage_id="stage_0", worker_count=8, max_workers=8, num_gpus=1.0)
        await autoscaler.eager_fill({"stage_0": stage})

        assert len(stage._workers) == 8  # No change
