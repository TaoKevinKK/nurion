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

"""Diagnostic helpers for data integrity tests.

When a data integrity assertion fails in CI (but not locally), these
helpers dump enough state to diagnose the root cause from CI logs alone.

Usage::

    sink_data = get_sink_records(collector_name)
    if len(sink_data) != expected:
        dump_data_loss_diagnostics(
            test_name="test_many_small_batches_stress",
            sink_data=sink_data,
            expected_count=expected,
            collector_name=collector_name,
            runner=runner,          # RayJobRunner (broker still alive)
            batch_size=BATCH_SIZE,
            id_field="id",
        )
    assert len(sink_data) == expected, ...
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import ray


def dump_data_loss_diagnostics(
    *,
    test_name: str,
    sink_data: List[Dict],
    expected_count: int,
    collector_name: str,
    runner: Any = None,
    batch_size: int = 100,
    id_field: str = "id",
    expected_ids: Optional[Set] = None,
    composite_key_fields: Optional[List[str]] = None,
) -> None:
    """Print comprehensive diagnostic info when data loss is detected.

    Call this BEFORE runner.stop() so the broker is still alive for
    queue stat queries.

    All output goes to stdout via print() — Ray worker logs go to
    stderr and may be deduped, but driver-side print() always appears
    in pytest CI output.

    Args:
        test_name: Test function name for the header.
        sink_data: Records collected by the sink.
        expected_count: Expected number of records.
        collector_name: Ray actor name of the RecordCollector.
        runner: RayJobRunner instance (optional, for queue stats).
        batch_size: Source batch size (for identifying affected splits).
        id_field: Field name for record ID.
        expected_ids: If provided, use this set for missing ID calculation.
            Otherwise computed from sink_data range.
        composite_key_fields: If provided (e.g. ["id", "copy_idx"]),
            use composite keys for missing record detection.
    """
    actual_count = len(sink_data)
    delta = expected_count - actual_count

    print(f"\n{'=' * 70}")
    print(f"DIAGNOSTIC: {test_name}")
    print(f"{'=' * 70}")
    print(f"Expected: {expected_count}, Got: {actual_count}, Delta: {delta}")

    # --- Missing IDs ---
    if composite_key_fields:
        actual_keys = {tuple(r.get(f) for f in composite_key_fields) for r in sink_data}
        if expected_ids:
            missing = sorted(expected_ids - actual_keys)
        else:
            missing = []
        print(f"Missing composite keys (first 30): {missing[:30]}")
        missing_source_ids = sorted({k[0] for k in missing}) if missing else []
    else:
        actual_ids = {r[id_field] for r in sink_data if id_field in r}
        if expected_ids:
            missing_ids = sorted(expected_ids - actual_ids)
        else:
            # Infer from max ID
            max_id = max(actual_ids) if actual_ids else 0
            all_possible = {i for i in range(max_id + 1)}
            missing_ids = sorted(all_possible - actual_ids)
        print(f"Missing IDs (first 30): {missing_ids[:30]}")
        missing_source_ids = missing_ids

    # --- Affected batches ---
    if missing_source_ids and batch_size > 0:
        affected_batches = sorted({mid // batch_size for mid in missing_source_ids})
        print(f"Affected batches (split indices): {affected_batches}")
        print(
            f"Batch ranges: {[(b * batch_size, (b + 1) * batch_size - 1) for b in affected_batches]}"
        )

    # --- Collector dedup stats ---
    try:
        collector = ray.get_actor(collector_name)
        dup_count = ray.get(collector.get_duplicate_count.remote())
        total_added = ray.get(collector.count.remote())
        print(f"Collector: {total_added} records stored, {dup_count} duplicates filtered")
    except Exception as e:
        print(f"Collector stats unavailable: {e}")

    # --- Broker queue stats (driver-side, before stop) ---
    if runner and hasattr(runner, "_shared_broker") and runner._shared_broker:
        _dump_broker_stats(runner)

    print(f"{'=' * 70}\n")


def _dump_broker_stats(runner: Any) -> None:
    """Query broker for all queue/group stats. Must be called before runner.stop()."""
    try:
        from _internal.queue.anvil import AnvilQueueClient

        broker_url = runner._shared_broker.get_broker_url()
        client = AnvilQueueClient(broker_url, worker_id="diagnostic")
        client.start()
        try:
            print("\n--- Broker Queue Stats ---")
            for stage_id, master in runner._masters.items():
                try:
                    output_group = master.get_output_group_name()
                    stats = client.get_group_stats(output_group)
                    partitions = stats.get("partitions", [])
                    total_pending = sum(p.get("pending_count", 0) for p in partitions)
                    total_claimed = sum(p.get("claimed_count", 0) for p in partitions)
                    total_acked = sum(p.get("acked_count", 0) for p in partitions)
                    print(
                        f"  {stage_id} output [{output_group}]: "
                        f"pending={total_pending}, claimed={total_claimed}, "
                        f"acked={total_acked}, partitions={len(partitions)}"
                    )
                    for p in partitions:
                        if p.get("pending_count", 0) > 0 or p.get("claimed_count", 0) > 0:
                            print(f"    partition {p.get('partition_id', '?')}: {p}")
                except Exception as e:
                    print(f"  {stage_id}: failed to get stats: {e}")
        finally:
            client.stop()
    except Exception as e:
        print(f"Broker stats unavailable: {e}")
