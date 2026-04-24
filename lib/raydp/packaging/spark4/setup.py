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

"""Build entrypoint for the Spark 4.x / Scala 2.13 wheel."""

import importlib.util
import os

from setuptools import setup

# Pin the flavor *before* importing _build_hooks so the Maven profile and the
# JAR suffix filter both resolve to Scala 2.13.
os.environ["NURION_RAYDP_FLAVOR"] = "spark4"

_SHARED_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
_build_hooks_path = os.path.join(_SHARED_ROOT, "_build_hooks.py")
spec = importlib.util.spec_from_file_location("_build_hooks", _build_hooks_path)
_build_hooks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_build_hooks)

setup(
    cmdclass={
        "build_py": _build_hooks.BuildWithJars,
        "sdist": _build_hooks.SdistWithJars,
    },
)
