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

"""
Custom build hooks for the raydp package.

Handles Maven invocation and JAR file staging during build. The build flavor
(NURION_RAYDP_FLAVOR) selects the Maven profile (scala-2.12 vs scala-2.13) and
the JAR suffix filter so only the matching Scala variant ends up in the wheel.
"""

import glob
import os
import re
import subprocess
import sys
from shutil import copy2

from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

# The shared Python source root (and the `jars/` staging dir) is the directory
# containing this file. We anchor all paths here so the hooks behave identically
# whether invoked from lib/raydp/ directly or from a packaging/spark{3,4}/ subdir.
_SHARED_ROOT = os.path.dirname(os.path.abspath(__file__))
JARS_TARGET = os.path.join(_SHARED_ROOT, "jars")

# Flavor -> (Maven profile id, Scala binary version suffix that JAR finalNames carry)
_FLAVOR_BUILD_MATRIX = {
    "spark3": ("scala-2.12", "2.12"),
    "spark4": ("scala-2.13", "2.13"),
}


def _resolve_flavor() -> tuple[str, str, str]:
    """Return (flavor, maven_profile, scala_binary) from the env."""
    flavor = os.environ.get("NURION_RAYDP_FLAVOR", "spark3").lower()
    if flavor not in _FLAVOR_BUILD_MATRIX:
        raise RuntimeError(
            f"NURION_RAYDP_FLAVOR={flavor!r} is not supported. "
            f"Valid values: {sorted(_FLAVOR_BUILD_MATRIX)}"
        )
    profile, scala_bin = _FLAVOR_BUILD_MATRIX[flavor]
    return flavor, profile, scala_bin


# JAR finalName pattern produced by Maven is `raydp[-shims-...]_<scala_bin>-<version>.jar`.
# Match on `_<scala_bin>-` so we only pick up the active flavor's jars.
def _scala_suffix_matcher(scala_bin: str) -> re.Pattern:
    return re.compile(rf"_{re.escape(scala_bin)}-[^/\\]+\.jar$")


class BuildWithJars(_build_py):
    """Custom build_py command that handles JAR files."""

    def run(self):
        # Setup JAR files before building
        self.setup_jars()

        # Run the normal build
        super().run()

    def setup_jars(self):
        """Set up JAR files for packaging."""
        flavor, maven_profile, scala_bin = _resolve_flavor()
        print(f"[raydp] flavor={flavor} maven_profile={maven_profile} scala_bin={scala_bin}")

        # Java directory is a subdirectory of the shared source root (alongside this file).
        CORE_DIR = os.path.join(_SHARED_ROOT, "java")

        # Build JAR files using Maven (pinned to this flavor's profile)
        self.build_jars(CORE_DIR, maven_profile)

        # Pick only the jars whose finalName carries the matching `_<scala_bin>-` suffix.
        suffix_pattern = _scala_suffix_matcher(scala_bin)
        all_jars = glob.glob(os.path.join(CORE_DIR, "**/target/raydp*.jar"), recursive=True)
        matched = [p for p in all_jars if suffix_pattern.search(p)]
        thirdparty = glob.glob(os.path.join(CORE_DIR, "thirdparty/*.jar"))
        JARS_PATH = matched + thirdparty

        if len(JARS_PATH) == 0:
            print(
                f"Can't find core module jars for flavor {flavor!r} (expected suffix "
                f"_{scala_bin}-) after Maven build. Available jars: {all_jars}",
                file=sys.stderr,
            )
            raise RuntimeError("JAR files not found after Maven build")

        # Clean stale jars from the staging dir before copying fresh ones.
        if os.path.exists(JARS_TARGET):
            for jar_file in glob.glob(os.path.join(JARS_TARGET, "*.jar")):
                try:
                    os.remove(jar_file)
                    print(f"Removed existing JAR file: {jar_file}")
                except OSError as e:
                    print(f"Failed to remove {jar_file}: {e}", file=sys.stderr)

        try:
            os.makedirs(JARS_TARGET, exist_ok=True)
        except Exception as e:
            print(f"Failed to create temp directories: {e}", file=sys.stderr)
            raise

        try:
            for jar_path in JARS_PATH:
                print(f"Copying {jar_path} to {JARS_TARGET}")
                copy2(jar_path, JARS_TARGET)
            print(f"Successfully copied {len(JARS_PATH)} JAR files")
        except Exception as e:
            print(f"Failed to copy JAR files: {e}", file=sys.stderr)
            raise

    def build_jars(self, core_dir, maven_profile):
        """Build JAR files using Maven under the given profile."""
        # Check if Maven is available
        try:
            subprocess.run(["mvn", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Maven (mvn) could not be found. Please install Maven first.", file=sys.stderr)
            raise RuntimeError("Maven not found") from None

        print(f"Building JAR files in {core_dir} (profile={maven_profile})")

        # Save current directory
        original_dir = os.getcwd()

        try:
            os.chdir(core_dir)
            cmd = ["mvn", "-P", maven_profile, "clean", "package", "-DskipTests"]
            print(f"Running: {' '.join(cmd)}")

            subprocess.run(
                cmd,
                check=True,
                capture_output=False,  # Let Maven output be visible
            )

            print("Maven build completed successfully")

        except subprocess.CalledProcessError as e:
            print(f"Maven build failed with exit code {e.returncode}", file=sys.stderr)
            raise RuntimeError(f"Maven build failed: {e}") from e
        except Exception as e:
            print(f"Failed to run Maven build: {e}", file=sys.stderr)
            raise
        finally:
            # Always restore original directory
            os.chdir(original_dir)


class SdistWithJars(_sdist):
    """Custom sdist command that handles JAR files."""

    def run(self):
        # Setup JAR files before creating source distribution
        build_cmd = BuildWithJars(self.distribution)
        build_cmd.setup_jars()

        # Run the normal sdist
        super().run()
