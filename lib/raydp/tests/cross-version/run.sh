#!/usr/bin/env bash
#
# Build both wheels and run smoke_test.py under two isolated venvs:
#   venv-spark3: pyspark 3.5.x + nurion-raydp-spark3 (Scala 2.12)
#   venv-spark4: pyspark 4.1.x + nurion-raydp-spark4 (Scala 2.13)
#
# Exits 0 only if both smoke suites pass. Prints a compact summary at the end.
#
# Usage:
#   ./run.sh                 # build wheels + test both flavors
#   ./run.sh --no-build      # skip the uv build step (expects dist/ already present)
#   ./run.sh spark3          # test only spark3 flavor
#   ./run.sh spark4          # test only spark4 flavor
#
set -u  # NOT -e -- we want to report both flavors even if one fails

cd "$(dirname "$0")"
CROSS_DIR="$(pwd)"
RAYDP_ROOT="$(cd ../.. && pwd)"
SCRIPT="$CROSS_DIR/smoke_test.py"

FLAVORS=(spark3 spark4)
BUILD=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) BUILD=0 ;;
        spark3) FLAVORS=(spark3) ;;
        spark4) FLAVORS=(spark4) ;;
        -h|--help)
            head -20 "$0" | tail -16
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

declare -A RESULTS

build_wheel () {
    local flavor="$1"
    echo "======== [build] $flavor ========"
    cd "$RAYDP_ROOT/packaging/$flavor"
    rm -rf dist build "nurion_raydp_${flavor}.egg-info"
    # Each flavor rebuilds raydp_*.jar from scratch; clear jars/ so stale files
    # from the other flavor can't leak in.
    rm -f "$RAYDP_ROOT/jars"/*.jar
    uv build --wheel >/dev/null 2>&1
    local whl
    whl="$(ls -1 dist/*.whl 2>/dev/null | head -1)"
    if [[ -z "$whl" ]]; then
        echo "BUILD FAILED for $flavor (no wheel under $PWD/dist/)"
        return 1
    fi
    echo "built: $whl"
    cd "$CROSS_DIR"
}

test_flavor () {
    local flavor="$1"
    local pyspark_pin
    case "$flavor" in
        spark3) pyspark_pin="pyspark>=3.5,<4" ;;
        spark4) pyspark_pin="pyspark>=4.1,<5" ;;
    esac

    local venv="$CROSS_DIR/venv-$flavor"
    echo
    echo "======== [test] $flavor ========"
    local whl
    whl="$(ls -1 "$RAYDP_ROOT/packaging/$flavor"/dist/*.whl 2>/dev/null | head -1)"
    if [[ -z "$whl" ]]; then
        echo "no wheel for $flavor at $RAYDP_ROOT/packaging/$flavor/dist/"
        RESULTS[$flavor]="BUILD_MISSING"
        return 1
    fi

    # Fresh venv each run to make results reproducible.
    rm -rf "$venv"
    uv venv --python 3.11 "$venv" >/dev/null 2>&1 || {
        echo "venv creation failed for $flavor"
        RESULTS[$flavor]="VENV_FAIL"
        return 1
    }

    echo "installing pyspark pin + wheel into $venv ..."
    VIRTUAL_ENV="$venv" uv pip install --quiet "$pyspark_pin" "$whl" || {
        echo "install failed for $flavor"
        RESULTS[$flavor]="INSTALL_FAIL"
        return 1
    }

    echo "running smoke_test.py under $flavor ..."
    NURION_RAYDP_EXPECTED_FLAVOR="$flavor" \
        "$venv/bin/python" "$SCRIPT"
    local rc=$?
    if [[ $rc -eq 0 ]]; then
        RESULTS[$flavor]="PASS"
    else
        RESULTS[$flavor]="SMOKE_FAIL(rc=$rc)"
    fi
}

if [[ $BUILD -eq 1 ]]; then
    for flavor in "${FLAVORS[@]}"; do
        build_wheel "$flavor" || { RESULTS[$flavor]="BUILD_FAIL"; }
    done
fi

for flavor in "${FLAVORS[@]}"; do
    if [[ "${RESULTS[$flavor]:-}" == "BUILD_FAIL" ]]; then
        continue
    fi
    test_flavor "$flavor"
done

echo
echo "===================================================="
echo "CROSS-VERSION SMOKE SUMMARY"
echo "===================================================="
overall=0
for flavor in "${FLAVORS[@]}"; do
    result="${RESULTS[$flavor]:-UNKNOWN}"
    printf "  %-8s  %s\n" "$flavor" "$result"
    if [[ "$result" != "PASS" ]]; then overall=1; fi
done
exit "$overall"
