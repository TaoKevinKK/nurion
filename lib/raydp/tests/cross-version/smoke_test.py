#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Version-agnostic smoke test for nurion-raydp wheels.

Exercised by run.sh under two venvs:
  * venv-spark3: pyspark 3.5.x + nurion-raydp-spark3 (Scala 2.12)
  * venv-spark4: pyspark 4.1.x + nurion-raydp-spark4 (Scala 2.13)

Stages:
  1. import              - libs importable
  2. flavor_match        - installed pyspark major matches the expected flavor
  3. jar_selection       - code_search_jars filters to the right Scala suffix
  4. init_spark          - raydp.init_spark returns a working SparkSession
  5. json_roundtrip      - write DataFrame to JSON, read back, verify
  6. parquet_roundtrip   - same for Parquet (columnar + schema preserved)
  7. teardown            - stop spark + ray, cleanup tempdir (always runs)

Stages 4..6 share a SparkSession via a module-level context object. Stage 7
runs unconditionally so Ray processes don't leak after a failure.

Exit code is 0 on full pass, 1 on any failure.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import traceback
from typing import Callable


class Ctx:
    """Shared state across the spark-needing stages."""
    spark = None
    tmpdir: str | None = None


ctx = Ctx()


class Stage:
    def __init__(self, name: str):
        self.name = name
        self.ok: bool | None = None
        self.detail: str = ""


def run(stage: Stage, fn: Callable[[], str | None]) -> None:
    print(f"[{stage.name}] running ...", flush=True)
    try:
        out = fn()
        stage.ok = True
        stage.detail = out or "ok"
        print(f"[{stage.name}] PASS — {stage.detail}", flush=True)
    except Exception as e:  # noqa: BLE001
        stage.ok = False
        stage.detail = f"{type(e).__name__}: {e}"
        print(f"[{stage.name}] FAIL — {stage.detail}", flush=True)
        traceback.print_exc()


# -------- 1. imports & metadata --------

def stage_import() -> str:
    import pyspark  # noqa: F401
    import ray  # noqa: F401
    import raydp  # noqa: F401

    return (
        f"raydp.__file__={raydp.__file__} "
        f"pyspark={pyspark.__version__} ray={ray.__version__}"
    )


def stage_expected_flavor() -> str:
    """The env tells us which wheel should be installed; confirm the install matches."""
    flavor = os.environ.get("NURION_RAYDP_EXPECTED_FLAVOR")
    assert flavor in {"spark3", "spark4"}, (
        f"NURION_RAYDP_EXPECTED_FLAVOR must be spark3 or spark4, got {flavor!r}"
    )

    import pyspark
    major = int(pyspark.__version__.split(".")[0])
    expected_major = 3 if flavor == "spark3" else 4
    assert major == expected_major, (
        f"flavor={flavor} expects pyspark major={expected_major} but got {major}"
    )
    return f"flavor={flavor}, pyspark major={major}"


# -------- 2. jar selection (utils.code_search_jars) --------

def stage_jar_selection() -> str:
    from raydp import utils

    jars = utils.code_search_jars()
    raydp_jars = [j for j in jars if "raydp" in os.path.basename(j)]
    assert raydp_jars, "code_search_jars() returned no raydp jars"

    flavor = os.environ["NURION_RAYDP_EXPECTED_FLAVOR"]
    expected_suffix = "_2.12-" if flavor == "spark3" else "_2.13-"
    bad = [j for j in raydp_jars if expected_suffix not in os.path.basename(j)]
    assert not bad, (
        f"found jars not matching expected suffix {expected_suffix}: "
        f"{[os.path.basename(j) for j in bad]}"
    )
    return f"{len(raydp_jars)} raydp jars, all carry {expected_suffix}"


# -------- 3. raydp.init_spark launches a Spark session --------

def stage_init_spark() -> str:
    import ray
    from ray.job_config import JobConfig
    import raydp
    from raydp.utils import code_search_path

    ray.shutdown()  # defensive against lingering Ray from a previous iteration

    # Pin pyspark Python worker to *this* interpreter. Without this, Spark picks
    # `python3` from PATH which on a dev laptop is often the system/miniconda
    # Python, producing PYTHON_VERSION_MISMATCH when the driver runs under a
    # venv. This only matters for operations that spawn a Python worker
    # (e.g. DataFrame write to file sources triggers PySpark's internal
    # Arrow/UDF plumbing); a pure `spark.range(n).count()` doesn't hit it.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Cross-language actors (RayDP's Java RayAppMaster + PyWorkerFactory) require
    # the jar classpath to be declared on the Ray driver's JobConfig, otherwise
    # Ray refuses with "Cross language feature needs --load-code-from-local".
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
        job_config=JobConfig(code_search_path=code_search_path()),
    )

    spark = raydp.init_spark(
        app_name="raydp-smoke",
        num_executors=1,
        executor_cores=1,
        executor_memory="512m",
    )

    n = spark.range(0, 10).count()
    assert n == 10, f"expected count=10, got {n}"
    ctx.spark = spark
    ctx.tmpdir = tempfile.mkdtemp(prefix="raydp-smoke-")
    return f"spark.range(10).count() = {n}, spark.version={spark.version}"


# -------- 4. JSON + 5. Parquet: shared helper --------

def _make_sample_df():
    """Small DataFrame with mixed types to exercise schema roundtripping."""
    spark = ctx.spark
    rows = [
        (1, "alice", 1.5, True),
        (2, "bob", 2.5, False),
        (3, "charlie", 3.5, True),
        (4, "dave", 4.5, True),
        (5, "eve", 5.5, False),
    ]
    # Explicit schema so JSON roundtrip (which infers types) has a clear target.
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("score", DoubleType(), nullable=False),
        StructField("active", BooleanType(), nullable=False),
    ])
    return spark.createDataFrame(rows, schema=schema)


def _assert_roundtrip_equal(original, roundtripped, fmt: str) -> None:
    """Verify rowcount + row-by-row content equality after a write/read cycle."""
    n_orig = original.count()
    n_round = roundtripped.count()
    assert n_orig == n_round, (
        f"[{fmt}] row count mismatch: original={n_orig} roundtripped={n_round}"
    )

    # Sort both sides by id and compare content. Using tuple-of-tuples for a
    # stable equality check that works the same across pyspark 3 and 4.
    orig_rows = [tuple(r) for r in original.orderBy("id").collect()]
    round_rows = [tuple(r) for r in roundtripped.orderBy("id").collect()]
    assert orig_rows == round_rows, (
        f"[{fmt}] content mismatch:\n  original={orig_rows}\n  roundtrip={round_rows}"
    )


def stage_json_roundtrip() -> str:
    spark = ctx.spark
    df = _make_sample_df()
    path = os.path.join(ctx.tmpdir, "data.json")
    df.write.mode("overwrite").json(path)

    # On read, re-declare the schema so `id`/`score` come back as LongType /
    # DoubleType. JSON doesn't preserve numeric width without an explicit schema.
    round_df = spark.read.schema(df.schema).json(path)
    _assert_roundtrip_equal(df, round_df, "json")

    # Sanity check: ensure at least one JSON data file actually hit disk.
    files = [
        f for f in os.listdir(path)
        if f.endswith(".json") and not f.startswith(".")
    ]
    assert files, f"no .json files written under {path}"
    return f"wrote {len(files)} file(s) to {path}, read back {round_df.count()} rows"


def stage_parquet_roundtrip() -> str:
    spark = ctx.spark
    df = _make_sample_df()
    path = os.path.join(ctx.tmpdir, "data.parquet")
    df.write.mode("overwrite").parquet(path)

    # Parquet preserves full schema — no explicit .schema() needed.
    round_df = spark.read.parquet(path)
    _assert_roundtrip_equal(df, round_df, "parquet")

    # Verify schema survived for name + type. Nullability deliberately ignored:
    # Parquet writers from Spark conservatively mark all columns as nullable on
    # read-back regardless of the original StructField.nullable flag.
    orig_name_type = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    round_name_type = [(f.name, f.dataType.simpleString()) for f in round_df.schema.fields]
    assert orig_name_type == round_name_type, (
        f"parquet schema mismatch (name,type):\n"
        f"  original={orig_name_type}\n  roundtrip={round_name_type}"
    )
    files = [
        f for f in os.listdir(path)
        if f.endswith(".parquet") and not f.startswith(".")
    ]
    assert files, f"no .parquet files written under {path}"
    return f"wrote {len(files)} file(s) to {path}, schema preserved, read {round_df.count()} rows"


# -------- 6. teardown (always runs) --------

def stage_teardown() -> str:
    messages = []
    try:
        import raydp
        raydp.stop_spark()
        messages.append("raydp.stop_spark() ok")
    except Exception as e:  # noqa: BLE001
        messages.append(f"raydp.stop_spark() raised {type(e).__name__}: {e}")

    try:
        import ray
        ray.shutdown()
        messages.append("ray.shutdown() ok")
    except Exception as e:  # noqa: BLE001
        messages.append(f"ray.shutdown() raised {type(e).__name__}: {e}")

    if ctx.tmpdir and os.path.isdir(ctx.tmpdir):
        try:
            shutil.rmtree(ctx.tmpdir)
            messages.append(f"removed tmpdir {ctx.tmpdir}")
        except Exception as e:  # noqa: BLE001
            messages.append(f"tmpdir cleanup raised {type(e).__name__}: {e}")

    return "; ".join(messages)


# -------- main --------

def main() -> int:
    stages = [
        Stage("1.import"),
        Stage("2.flavor_match"),
        Stage("3.jar_selection"),
        Stage("4.init_spark"),
        Stage("5.json_roundtrip"),
        Stage("6.parquet_roundtrip"),
        Stage("7.teardown"),
    ]

    run(stages[0], stage_import)
    if not stages[0].ok:
        # Without imports nothing else can work; skip the rest.
        pass
    else:
        run(stages[1], stage_expected_flavor)
        run(stages[2], stage_jar_selection)
        run(stages[3], stage_init_spark)
        if stages[3].ok:
            run(stages[4], stage_json_roundtrip)
            run(stages[5], stage_parquet_roundtrip)
        # Teardown always runs if init_spark was attempted, so we don't leak
        # Ray/JVM processes after a failure.
        run(stages[6], stage_teardown)

    print()
    print("=" * 64)
    attempted = [s for s in stages if s.ok is not None]
    passed = sum(1 for s in attempted if s.ok)
    total = len(attempted)
    print(f"SMOKE SUMMARY: {passed}/{total} stages passed")
    for s in stages:
        symbol = "PASS" if s.ok else ("FAIL" if s.ok is False else "SKIP")
        print(f"  [{symbol}] {s.name}: {s.detail}")
    print("=" * 64)
    # Teardown issues don't fail the smoke test; only the functional stages do.
    functional = [s for s in stages if s.name != "7.teardown" and s.ok is not None]
    functional_passed = sum(1 for s in functional if s.ok)
    return 0 if functional_passed == len(functional) else 1


if __name__ == "__main__":
    sys.exit(main())
