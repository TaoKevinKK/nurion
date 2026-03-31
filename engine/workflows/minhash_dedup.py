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

"""MinHash deduplication workflow - Union-Find Service architecture.

Design:
1. Union-Find Service for cluster management (O(n) vs O(n*k) CC iterations)
2. xxhash + numpy vectorization (~50x faster signature computation)
3. No signature data amplification (only band_hash in shuffle, not full signature)
4. 3 pipeline stages (Encode -> BucketUnion -> Filter)
5. Stateless operators -- UF state lives in independent service actors

Architecture:

    [Pre-pipeline] Deploy UnionFindService
         │
    ┌────▼──────────────────────────────────────────────────┐
    │  Stage 1: Source -> MinHashEncoder                     │
    │  (compute signatures, shuffle by bucket_id)            │
    └────┬──────────────────────────────────────────────────┘
         │ shuffle by bucket_id
    ┌────▼──────────────────────────────────────────────────┐
    │  Stage 2: BucketUnionOperator                          │
    │  (union same-hash docs via UFService RPC, no output)   │
    └────┬──────────────────────────────────────────────────┘
         │ [pipeline complete, orchestration step]
    ┌────▼──────────────────────────────────────────────────┐
    │  Cross-shard resolution (manager.resolve_cross_shard)  │
    │  Export clusters (manager.export_clusters)              │
    └────┬──────────────────────────────────────────────────┘
         │
    ┌────▼──────────────────────────────────────────────────┐
    │  Stage 3: Source -> DedupFilter -> Sink                │
    │  (re-read source, filter duplicates using cluster map)  │
    └───────────────────────────────────────────────────────┘

Note: This workflow creates two separate jobs:
  - Job 1: Encode + BucketUnion (builds the UF clusters)
  - Job 2: Filter (re-reads source, filters using exported clusters)

This is because the filter stage needs the complete cluster map,
which is only available after all buckets are processed and
cross-shard resolution is done.

Usage:
    nurion run \\
        --workflow workflows.minhash_dedup \\
        --job-id dedup_001 \\
        --input /data/documents \\
        --output /data/deduplicated \\
        --content-column text \\
        --id-column doc_id
"""

import logging
from typing import Any, Dict

import pyarrow as pa

from nurion import (
    FileSinkConfig,
    Job,
    JobConfig,
    LanceSinkConfig,
    LanceTableSourceConfig,
    Stage,
)
from _internal.operators.dedup.bucket_union import BucketUnionOperatorConfig
from _internal.operators.dedup.encoder import MinHashEncoderConfig
from _internal.operators.dedup.filter import DedupFilterOperatorConfig
from _internal.serve.union_find import UFClusterConfig, UnionFindServiceManager

# Default parameters (following datatrove conventions)
DEFAULT_NUM_BUCKETS = 14
DEFAULT_HASHES_PER_BUCKET = 8
DEFAULT_NGRAM_SIZE = 5
DEFAULT_NUM_SHARDS = 16

logger = logging.getLogger(__name__)


def create_union_job(
    job_id: str,
    config: Dict[str, Any],
    uf_manager: UnionFindServiceManager,
) -> Job:
    """Create the first job: Encode + BucketUnion.

    This job computes MinHash signatures, shuffles by bucket,
    and unions same-bucket documents via the UFService.

    Args:
        job_id: Unique job identifier
        config: Job configuration dictionary
        uf_manager: Deployed UnionFindServiceManager instance

    Returns:
        Configured Job instance for the union phase
    """
    input_path = config.get("input")
    content_column = config.get("content_column")
    id_column = config.get("id_column")

    if not input_path:
        raise ValueError("'input' parameter is required")
    if not content_column:
        raise ValueError("'content_column' parameter is required")
    if not id_column:
        raise ValueError("'id_column' parameter is required")

    num_buckets = int(config.get("num_buckets", DEFAULT_NUM_BUCKETS))
    hashes_per_bucket = int(config.get("hashes_per_bucket", DEFAULT_HASHES_PER_BUCKET))
    ngram_size = int(config.get("ngram_size", DEFAULT_NGRAM_SIZE))
    num_partitions = int(config.get("num_partitions", 32))
    anvil_db_path = config.get("anvil_db_path", "memory://")
    split_size = int(config.get("split_size", 1000))

    worker_resources = {
        "num_cpus": config.get("worker_num_cpus", 1.0),
        "num_gpus": config.get("worker_num_gpus", 0),
        "memory": int(config.get("worker_memory_mb", 2048)) * 1024**2,
    }

    encoder_parallelism = config.get("encoder_parallelism", (2, 8))
    union_parallelism = config.get("union_parallelism", (2, 8))

    job_config = JobConfig(anvil_db_path=anvil_db_path)
    job = Job(job_id=f"{job_id}_union", config=job_config)

    uf_client = uf_manager.create_client()

    # Stage 1: Source
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=split_size,
            columns=[id_column, content_column],
        ),
        parallelism=1,
        worker_resources=worker_resources,
    )

    # Stage 2: MinHash Encode + shuffle by bucket_id
    seed = int(config.get("seed", 1))  # Default seed=1 for reproducibility
    encoder_stage = Stage(
        stage_id="encoder",
        operator_config=MinHashEncoderConfig(
            content_column=content_column,
            id_column=id_column,
            num_buckets=num_buckets,
            hashes_per_bucket=hashes_per_bucket,
            ngram_size=ngram_size,
            seed=seed,
            partition_keys=["bucket_id"],
            num_partitions=num_partitions,
        ),
        parallelism=encoder_parallelism,
        worker_resources=worker_resources,
    )

    # Stage 3: Bucket Union (calls UFService, no output)
    union_stage = Stage(
        stage_id="bucket_union",
        operator_config=BucketUnionOperatorConfig(
            doc_id_column="doc_id",
            band_hash_column="band_hash",
            uf_client=uf_client,
        ),
        parallelism=union_parallelism,
        worker_resources=worker_resources,
    )

    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(encoder_stage, upstream_stages=["source"])
    job.add_stage(union_stage, upstream_stages=["encoder"])

    logger.info(
        f"Union job created: {len(job.stages)} stages, "
        f"buckets={num_buckets}, hashes_per_bucket={hashes_per_bucket}"
    )

    return job


def create_filter_job(
    job_id: str,
    config: Dict[str, Any],
    cluster_table: pa.Table,
) -> Job:
    """Create the second job: Filter duplicates.

    This job re-reads the source data and filters out duplicates
    using the pre-computed cluster membership table.

    Args:
        job_id: Unique job identifier
        config: Job configuration dictionary
        cluster_table: Pre-exported (doc_id, cluster_id) Arrow Table

    Returns:
        Configured Job instance for the filter phase
    """
    input_path = config["input"]
    output_path = config["output"]
    id_column = config["id_column"]
    anvil_db_path = config.get("anvil_db_path", "memory://")
    split_size = int(config.get("split_size", 1000))

    worker_resources = {
        "num_cpus": config.get("worker_num_cpus", 1.0),
        "num_gpus": config.get("worker_num_gpus", 0),
        "memory": int(config.get("worker_memory_mb", 2048)) * 1024**2,
    }

    filter_parallelism = config.get("filter_parallelism", (2, 4))

    job_config = JobConfig(anvil_db_path=anvil_db_path)
    job = Job(job_id=f"{job_id}_filter", config=job_config)

    # Stage 1: Re-read source (all columns this time)
    source_stage = Stage(
        stage_id="source",
        operator_config=LanceTableSourceConfig(
            dataset_uri=input_path,
            split_size=split_size,
        ),
        parallelism=1,
        worker_resources=worker_resources,
    )

    # Stage 2: Filter
    filter_stage = Stage(
        stage_id="filter",
        operator_config=DedupFilterOperatorConfig(
            id_column=id_column,
            cluster_table=cluster_table,
        ),
        parallelism=filter_parallelism,
        worker_resources=worker_resources,
    )

    # Stage 3: Sink
    output_format = config.get("output_format", "lance")
    if output_format == "lance":
        sink_config = LanceSinkConfig(
            table_path=output_path,
            mode="overwrite",
            merge_batch_size=config.get("sink_buffer_size", 1000),
        )
    else:
        sink_config = FileSinkConfig(
            output_path=output_path,
            format=output_format,
            buffer_size=config.get("sink_buffer_size", 1000),
        )

    sink_stage = Stage(
        stage_id="sink",
        operator_config=sink_config,
        parallelism=1,
        worker_resources=worker_resources,
    )

    # Build DAG
    job.add_stage(source_stage)
    job.add_stage(filter_stage, upstream_stages=["source"])
    job.add_stage(sink_stage, upstream_stages=["filter"])

    logger.info(
        f"Filter job created: {len(job.stages)} stages, cluster_table={cluster_table.num_rows} rows"
    )

    return job


async def run_dedup_pipeline(
    job_id: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Run the complete MinHash dedup pipeline.

    This orchestrates the full workflow:
    1. Deploy UnionFindService
    2. Run union job (encode + bucket union)
    3. Resolve cross-shard edges
    4. Export cluster table
    5. Run filter job (re-read source + filter duplicates)
    6. Shutdown UnionFindService

    Args:
        job_id: Unique job identifier
        config: Job configuration dictionary

    Returns:
        Dict with pipeline results
    """
    import time

    start_time = time.time()
    num_shards = int(config.get("num_shards", DEFAULT_NUM_SHARDS))
    shard_num_cpus = float(config.get("shard_num_cpus", 1.0))
    shard_memory_mb = int(config.get("shard_memory_mb", 4096))

    # Step 1: Deploy UnionFindService
    uf_manager = UnionFindServiceManager()
    await uf_manager.deploy(
        UFClusterConfig(
            cluster_id=job_id,
            num_shards=num_shards,
            shard_num_cpus=shard_num_cpus,
            shard_memory_mb=shard_memory_mb,
        )
    )

    try:
        # Step 2: Run union job
        union_job = create_union_job(job_id, config, uf_manager)
        union_runner = union_job.create_ray_runner()
        try:
            union_status = await union_runner.run(timeout=config.get("union_timeout", 3600))
            logger.info(f"Union job completed: {union_status}")
        finally:
            await union_runner.stop()

        # Step 3: Resolve cross-shard edges
        resolution = await uf_manager.resolve_cross_shard()
        logger.info(f"Cross-shard resolution: {resolution}")

        # Step 4: Export clusters
        cluster_table = await uf_manager.export_clusters()
        logger.info(f"Exported {cluster_table.num_rows} cluster mappings")

        # Step 5: Run filter job
        filter_job = create_filter_job(job_id, config, cluster_table)
        filter_runner = filter_job.create_ray_runner()
        try:
            filter_status = await filter_runner.run(timeout=config.get("filter_timeout", 3600))
            logger.info(f"Filter job completed: {filter_status}")
        finally:
            await filter_runner.stop()

    finally:
        # Step 6: Shutdown UF service
        await uf_manager.shutdown()

    duration = time.time() - start_time
    logger.info(f"Dedup pipeline complete in {duration:.1f}s")

    return {
        "job_id": job_id,
        "duration_s": duration,
        "cluster_mappings": cluster_table.num_rows,
        "cross_shard_resolution": resolution,
    }


def create_job(job_id: str, config: Dict[str, Any]) -> Job:
    """Create a union job for CLI usage.

    Note: This only creates the union phase. For the full pipeline
    (including filter), use run_dedup_pipeline() directly.
    """
    # Deploy UF service inline (will be managed externally in production)
    import asyncio

    uf_manager = UnionFindServiceManager()

    loop = asyncio.get_event_loop()
    if loop.is_running():
        # We're in an async context; deploy synchronously via ray
        import ray

        num_shards = int(config.get("num_shards", DEFAULT_NUM_SHARDS))

        @ray.remote
        def _deploy():
            import asyncio

            mgr = UnionFindServiceManager()
            asyncio.run(
                mgr.deploy(
                    UFClusterConfig(
                        cluster_id=job_id,
                        num_shards=num_shards,
                    )
                )
            )
            return mgr

        uf_manager = ray.get(_deploy.remote())
    else:
        num_shards = int(config.get("num_shards", DEFAULT_NUM_SHARDS))
        loop.run_until_complete(
            uf_manager.deploy(
                UFClusterConfig(
                    cluster_id=job_id,
                    num_shards=num_shards,
                )
            )
        )

    return create_union_job(job_id, config, uf_manager)
