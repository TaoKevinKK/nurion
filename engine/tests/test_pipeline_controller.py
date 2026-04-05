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

"""Tests for the unified PipelineController.

Tests scaling, flow control, and liveness detection — the three concerns
unified in PipelineController (replaces SimpleAutoscaler + BackpressureController
+ StageMaster.no_progress_timeout).
"""

import time
from unittest.mock import MagicMock

from _internal.core.models import QueueStats
from _internal.runtime.pipeline_controller import (
    PipelineController,
    ControllerConfig,
    StageMetrics,
)
from _internal.runtime.queue_stats import QueueRef, StageQueueConfig


# ============================================================================
# Mocks
# ============================================================================


class MockStageMaster:
    """Mock StageMaster for testing controller decisions."""

    def __init__(
        self,
        stage_id: str = "test_stage",
        worker_count: int = 2,
        min_workers: int = 1,
        max_workers: int = 8,
        num_cpus: float = 0.5,
        num_gpus: float = 0.0,
        is_source: bool = False,
        max_pending_total: int = 0,
    ):
        self.stage_id = stage_id
        self._workers = {f"worker_{i}": MagicMock() for i in range(worker_count)}
        self._source_manager = MagicMock() if is_source else None
        self._state = MagicMock()
        self._state.value = "running"
        self._source_paused = False
        self._failed = False
        self._fail_reason = None
        self._last_completion_time = time.monotonic()

        self.stage = MagicMock()
        self.stage.min_parallelism = min_workers
        self.stage.max_parallelism = max_workers
        self.stage.num_cpus = num_cpus
        self.stage.num_gpus = num_gpus

        self.runtime = MagicMock()
        self.runtime.max_pending_total = max_pending_total

    async def scale_up(self, count: int) -> int:
        to_add = min(count, self.stage.max_parallelism - len(self._workers))
        for _ in range(to_add):
            wid = f"worker_{len(self._workers)}"
            self._workers[wid] = MagicMock()
        return to_add

    async def scale_down(self, count: int) -> int:
        to_remove = min(count, len(self._workers) - self.stage.min_parallelism)
        for _ in range(to_remove):
            if self._workers:
                key = list(self._workers.keys())[-1]
                del self._workers[key]
        return to_remove

    def set_source_paused(self, paused: bool) -> None:
        self._source_paused = paused

    def fail(self, reason: str) -> None:
        self._failed = True
        self._fail_reason = reason

    def reset_progress_timer(self) -> None:
        self._last_completion_time = time.monotonic()

    def report_completion_age(self) -> float:
        if self._last_completion_time is None:
            return 0.0
        return time.monotonic() - self._last_completion_time


class FakeQueueStatsClient:
    def __init__(self, stats: dict[str, QueueStats]) -> None:
        self._stats = stats

    def get_ref_stats(self, ref: QueueRef | None) -> QueueStats:
        if not ref:
            return QueueStats()
        return self._stats.get(ref.name, QueueStats())


def make_controller(
    stage_configs: dict[str, StageQueueConfig] | None = None,
    stats: dict[str, QueueStats] | None = None,
    dag_edges: dict[str, list[str]] | None = None,
    **config_overrides,
) -> PipelineController:
    # Enable flow_control when stats are provided so _tick collects metrics
    if stats and "flow_control_enabled" not in config_overrides:
        config_overrides["flow_control_enabled"] = True
    cfg = ControllerConfig(**config_overrides)
    stats_client = FakeQueueStatsClient(stats or {})
    return PipelineController(
        config=cfg,
        queue_stats_client=stats_client,
        stage_queue_configs=stage_configs or {},
        dag_edges=dag_edges or {},
    )


# ============================================================================
# Config
# ============================================================================


class TestControllerConfig:
    def test_defaults(self):
        cfg = ControllerConfig()
        assert cfg.tick_interval_s == 10.0
        assert cfg.scale_up_threshold == 500
        assert cfg.scale_down_threshold == 100
        assert cfg.cooldown_up_s == 15.0
        assert cfg.cooldown_down_s == 60.0
        assert cfg.max_scale_step == 32
        assert cfg.output_saturation_ratio == 0.8
        assert cfg.liveness_timeout_s == 600.0


# ============================================================================
# Scaling decisions
# ============================================================================


class TestScaling:
    def _make_setup(self, input_pending=0, output_pending=0, max_pending=0):
        """Create controller + master + stage config for a non-source stage."""
        master = MockStageMaster(
            stage_id="s0", worker_count=2, max_workers=8, max_pending_total=max_pending
        )
        stats = {
            "input_q": QueueStats(pending_count=input_pending),
            "output_q": QueueStats(pending_count=output_pending),
        }
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(
            stage_configs=configs,
            stats=stats,
            scaling_enabled=True,
            scale_up_threshold=100,
            scale_down_threshold=10,
        )
        return ctrl, master

    async def test_scale_up_on_high_input(self):
        ctrl, master = self._make_setup(input_pending=200)
        await ctrl._tick({"s0": master})
        assert len(master._workers) > 2  # scale_up executed synchronously via await
        assert "s0" in ctrl._last_scale_up

    async def test_no_scale_up_when_output_saturated(self):
        ctrl, master = self._make_setup(input_pending=200, output_pending=90, max_pending=100)
        await ctrl._tick({"s0": master})
        assert "s0" not in ctrl._last_scale_up  # Should NOT scale up

    async def test_scale_down_on_low_input(self):
        ctrl, master = self._make_setup(input_pending=5)
        await ctrl._tick({"s0": master})
        assert "s0" in ctrl._last_scale_down

    async def test_no_scale_down_below_min(self):
        master = MockStageMaster(stage_id="s0", worker_count=1, min_workers=1, max_workers=8)
        stats = {
            "input_q": QueueStats(pending_count=0),
            "output_q": QueueStats(),
        }
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(
            stage_configs=configs, stats=stats, scaling_enabled=True, scale_down_threshold=10
        )
        await ctrl._tick({"s0": master})
        assert "s0" not in ctrl._last_scale_down

    async def test_no_scaling_when_disabled(self):
        """Scaling is opt-in — disabled by default."""
        master = MockStageMaster(stage_id="s0", worker_count=4, max_workers=8)
        stats = {
            "input_q": QueueStats(pending_count=9999),
            "output_q": QueueStats(),
        }
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(
            stage_configs=configs,
            stats=stats,
            # scaling_enabled defaults to False
        )
        await ctrl._tick({"s0": master})
        assert "s0" not in ctrl._last_scale_up
        assert "s0" not in ctrl._last_scale_down

    async def test_skip_source_stages(self):
        master = MockStageMaster(stage_id="s0", is_source=True)
        stats = {"input_q": QueueStats(pending_count=9999), "output_q": QueueStats()}
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats, scaling_enabled=True)
        await ctrl._tick({"s0": master})
        assert "s0" not in ctrl._last_scale_up


# ============================================================================
# Flow control
# ============================================================================


class TestFlowControl:
    async def test_source_paused_when_output_saturated(self):
        master = MockStageMaster(stage_id="src", is_source=True, max_pending_total=100)
        stats = {
            "input_q": QueueStats(),
            "output_q": QueueStats(pending_count=90),
        }
        configs = {
            "src": StageQueueConfig(
                stage_id="src",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats)
        await ctrl._tick({"src": master})
        assert master._source_paused is True

    async def test_source_resumed_when_output_drains(self):
        master = MockStageMaster(stage_id="src", is_source=True, max_pending_total=100)
        master._source_paused = True
        stats = {
            "input_q": QueueStats(),
            "output_q": QueueStats(pending_count=50),  # Below 80%
        }
        configs = {
            "src": StageQueueConfig(
                stage_id="src",
                input=QueueRef.group("input_q"),
                output=QueueRef.group("output_q"),
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats)
        await ctrl._tick({"src": master})
        assert master._source_paused is False

    async def test_source_paused_by_downstream_saturation(self):
        src = MockStageMaster(stage_id="src", is_source=True, max_pending_total=0)
        transform = MockStageMaster(stage_id="t1", max_pending_total=100)

        stats = {
            "src_in": QueueStats(),
            "src_out": QueueStats(pending_count=10),  # src output not full
            "t1_in": QueueStats(pending_count=50),
            "t1_out": QueueStats(pending_count=95),  # t1 output full
        }
        configs = {
            "src": StageQueueConfig(
                stage_id="src",
                input=QueueRef.group("src_in"),
                output=QueueRef.group("src_out"),
            ),
            "t1": StageQueueConfig(
                stage_id="t1",
                input=QueueRef.group("t1_in"),
                output=QueueRef.group("t1_out"),
            ),
        }
        ctrl = make_controller(
            stage_configs=configs,
            stats=stats,
            dag_edges={"src": ["t1"]},
        )
        await ctrl._tick({"src": src, "t1": transform})
        assert src._source_paused is True  # Paused because downstream (t1) output is full


# ============================================================================
# Liveness
# ============================================================================


class TestLiveness:
    async def test_no_liveness_failure_when_making_progress(self):
        master = MockStageMaster(stage_id="s0", worker_count=2)
        master._last_completion_time = time.monotonic()  # Just completed
        stats = {"in": QueueStats(pending_count=100), "out": QueueStats()}
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0", input=QueueRef.group("in"), output=QueueRef.group("out")
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats, liveness_timeout_s=10)
        await ctrl._tick({"s0": master})
        assert not master._failed

    async def test_liveness_failure_when_stuck(self):
        master = MockStageMaster(stage_id="s0", worker_count=2, max_pending_total=100)
        # Simulate old completion time (exceeded timeout)
        master._last_completion_time = time.monotonic() - 700
        stats = {
            "in": QueueStats(pending_count=100),
            "out": QueueStats(pending_count=10),  # Output NOT saturated
        }
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0", input=QueueRef.group("in"), output=QueueRef.group("out")
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats, liveness_timeout_s=600)
        await ctrl._tick({"s0": master})
        assert master._failed
        assert "No progress" in master._fail_reason

    async def test_no_liveness_failure_when_backpressured(self):
        """P0 bug fix: backpressure should NOT trigger liveness failure."""
        master = MockStageMaster(stage_id="s0", worker_count=2, max_pending_total=100)
        master._last_completion_time = time.monotonic() - 700  # Old
        stats = {
            "in": QueueStats(pending_count=100),
            "out": QueueStats(pending_count=90),  # Output SATURATED (90% of 100)
        }
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0", input=QueueRef.group("in"), output=QueueRef.group("out")
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats, liveness_timeout_s=600)
        await ctrl._tick({"s0": master})
        assert not master._failed  # P0: NOT stuck — just backpressured

    async def test_no_liveness_failure_when_unbounded_and_no_workers(self):
        master = MockStageMaster(stage_id="s0", worker_count=0)
        master._last_completion_time = time.monotonic() - 700
        stats = {"in": QueueStats(pending_count=100), "out": QueueStats()}
        configs = {
            "s0": StageQueueConfig(
                stage_id="s0", input=QueueRef.group("in"), output=QueueRef.group("out")
            ),
        }
        ctrl = make_controller(stage_configs=configs, stats=stats, liveness_timeout_s=600)
        await ctrl._tick({"s0": master})
        assert not master._failed  # No workers → not stuck (master will spawn)


# ============================================================================
# Output saturation helper
# ============================================================================


class TestOutputSaturation:
    def test_unbounded_never_saturated(self):
        m = StageMetrics(
            stage_id="s0",
            worker_count=1,
            min_workers=1,
            max_workers=1,
            is_source=False,
            is_finished=False,
            output_pending=99999,
            output_max_pending=0,
        )
        ctrl = make_controller()
        assert not ctrl._is_output_saturated(m)

    def test_bounded_below_threshold(self):
        m = StageMetrics(
            stage_id="s0",
            worker_count=1,
            min_workers=1,
            max_workers=1,
            is_source=False,
            is_finished=False,
            output_pending=70,
            output_max_pending=100,
        )
        ctrl = make_controller()
        assert not ctrl._is_output_saturated(m)

    def test_bounded_above_threshold(self):
        m = StageMetrics(
            stage_id="s0",
            worker_count=1,
            min_workers=1,
            max_workers=1,
            is_source=False,
            is_finished=False,
            output_pending=85,
            output_max_pending=100,
        )
        ctrl = make_controller()
        assert ctrl._is_output_saturated(m)
