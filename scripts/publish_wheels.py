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

#!/usr/bin/env python3
"""Build and publish all nurion wheels to S3.

Usage:
    python scripts/publish_wheels.py              # build + upload
    python scripts/publish_wheels.py --dry-run    # build only, show what would upload

Prereqs: uv, maturin, aws CLI configured with write access.
"""

import shutil
import subprocess
import sys
from pathlib import Path

S3_BUCKET = "s3://ai-lumalabs-datasets-ap-se-2/wheels/nurion"
ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT / "dist"


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print(f"  $ {' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=cwd)


def build_all() -> None:
    version = (ROOT / "VERSION").read_text().strip()
    print(f"Building nurion v{version} wheels...\n")

    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir()

    # 1. Engine (pure Python)
    print("==> nurion-engine")
    run(["uv", "build", "--out-dir", str(DIST_DIR)], cwd=ROOT / "engine")

    # 2. Control (pure Python)
    print("\n==> nurion-control")
    run(["uv", "build", "--out-dir", str(DIST_DIR)], cwd=ROOT / "control")

    # 3. Anvil (Rust + Python via maturin)
    print("\n==> nurion-anvil")
    run(["maturin", "build", "--release", "--out", str(DIST_DIR)], cwd=ROOT / "lib" / "anvil-rs")

    # 4. RayDP — skipped (has JAR deps, built separately in CI)
    # run(["uv", "build", "--out-dir", str(DIST_DIR)], cwd=ROOT / "lib" / "raydp")

    wheels = sorted(DIST_DIR.glob("*.whl"))
    print(f"\nBuilt {len(wheels)} wheels:")
    for w in wheels:
        print(f"  {w.name}")

    return version, wheels


def upload(version: str, wheels: list[Path]) -> None:
    dest = f"{S3_BUCKET}/v{version}/"
    print(f"\nUploading to {dest}...")
    run([
        "aws", "s3", "cp", str(DIST_DIR) + "/", dest,
        "--recursive", "--exclude", "*", "--include", "*.whl",
    ])
    print("\nDone. Install with:")
    print(f"  pip install nurion-engine --find-links {dest}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    version, wheels = build_all()

    if dry_run:
        print(f"\n[dry-run] Would upload {len(wheels)} wheels to {S3_BUCKET}/v{version}/")
    else:
        upload(version, wheels)


if __name__ == "__main__":
    main()
