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
"""Sync version across all modules from the VERSION file.

Usage:
    python scripts/sync_version.py          # show current versions
    python scripts/sync_version.py 0.3.0    # bump all to 0.3.0

Updates:
    VERSION                              (single source of truth)
    pyproject.toml                       (workspace root)
    engine/pyproject.toml                (engine package)
    engine/_internal/__init__.py         (__version__ string)
    control/pyproject.toml               (control plane)
    lib/anvil-rs/pyproject.toml          (Python binding)
    lib/anvil-rs/Cargo.toml              (Rust crate)

Does NOT touch lib/raydp/ (independent versioning).
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "VERSION"

TARGETS = [
    # (file, pattern, replacement_template)
    (ROOT / "pyproject.toml", r'^version = ".*"', 'version = "{v}"'),
    (ROOT / "engine/pyproject.toml", r'^version = ".*"', 'version = "{v}"'),
    (ROOT / "engine/_internal/__init__.py", r'^__version__ = ".*"', '__version__ = "{v}"'),
    (ROOT / "control/pyproject.toml", r'^version = ".*"', 'version = "{v}"'),
    (ROOT / "lib/anvil-rs/pyproject.toml", r'^version = ".*"', 'version = "{v}"'),
    (ROOT / "lib/anvil-rs/Cargo.toml", r'^version = ".*"', 'version = "{v}"'),
]


def read_version() -> str:
    return VERSION_FILE.read_text().strip()


def show_versions():
    print(f"VERSION file: {read_version()}")
    for path, pattern, _ in TARGETS:
        rel = path.relative_to(ROOT)
        text = path.read_text()
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            print(f"  {rel}: {match.group()}")
        else:
            print(f"  {rel}: NOT FOUND")


def sync_version(new_version: str):
    VERSION_FILE.write_text(new_version + "\n")
    print(f"VERSION → {new_version}")

    for path, pattern, template in TARGETS:
        rel = path.relative_to(ROOT)
        text = path.read_text()
        replacement = template.format(v=new_version)
        new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
        if count == 0:
            print(f"  {rel}: SKIPPED (pattern not found)")
        else:
            path.write_text(new_text)
            print(f"  {rel}: {replacement}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        sync_version(sys.argv[1])
    else:
        show_versions()
