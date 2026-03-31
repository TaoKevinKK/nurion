#!/usr/bin/env bash
#
# Build and optionally upload nurion wheels.
#
# Usage:
#   ./scripts/build_wheels.sh                  # Build only
#   ./scripts/build_wheels.sh --upload S3_PATH # Build + upload to S3
#   ./scripts/build_wheels.sh --upload-pyx     # Build + upload to pyx registry
#
# Outputs all wheels to dist/ in the repo root.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$REPO_ROOT/dist"

# Parse args
UPLOAD_S3=""
UPLOAD_PYX=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upload)
            UPLOAD_S3="$2"; shift 2 ;;
        --upload-pyx)
            UPLOAD_PYX=true; shift ;;
        *)
            echo "Unknown arg: $1"; exit 1 ;;
    esac
done

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

echo "=== Building nurion-workqueue (Rust + Python) ==="
cd "$REPO_ROOT/lib/workqueue-rs"
maturin build --release --out "$DIST_DIR"
echo "  -> $(ls "$DIST_DIR"/nurion_workqueue-*.whl)"

echo ""
echo "=== Building nurion engine ==="
cd "$REPO_ROOT/engine"
uv build --wheel --no-sources --out-dir "$DIST_DIR"
echo "  -> $(ls "$DIST_DIR"/engine-*.whl)"

echo ""
echo "=== Built wheels ==="
ls -lh "$DIST_DIR"/*.whl

# Upload to S3
if [[ -n "$UPLOAD_S3" ]]; then
    echo ""
    echo "=== Uploading to $UPLOAD_S3 ==="
    for whl in "$DIST_DIR"/*.whl; do
        aws s3 cp "$whl" "$UPLOAD_S3/$(basename "$whl")"
        echo "  -> $UPLOAD_S3/$(basename "$whl")"
    done
    echo ""
    echo "Done. Install with:"
    echo "  pip install $UPLOAD_S3/nurion_workqueue-*.whl $UPLOAD_S3/engine-*.whl"
fi

# Upload to private registry
if [[ "$UPLOAD_PYX" == "true" ]]; then
    echo ""
    echo "=== Uploading to private registry ==="
    if ! command -v twine &>/dev/null; then
        echo "Error: twine not found. Install with: pip install twine"
        exit 1
    fi
    if [[ -z "${PYX_API_KEY:-}" ]]; then
        echo "Error: PYX_API_KEY not set"
        exit 1
    fi
    if [[ -z "${PYX_REPOSITORY_URL:-}" ]]; then
        echo "Error: PYX_REPOSITORY_URL not set"
        exit 1
    fi
    twine upload \
        --repository-url "$PYX_REPOSITORY_URL" \
        --username __token__ \
        --password "$PYX_API_KEY" \
        "$DIST_DIR"/*.whl
    echo ""
    echo "Done. Install with:"
    echo "  pip install --index-url $PYX_REPOSITORY_URL nurion-workqueue engine"
fi

echo ""
echo "Build complete."
