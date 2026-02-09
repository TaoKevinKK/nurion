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

"""WorkQueue state key schema for WebUI metadata."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple


def job_namespace(job_id: str) -> str:
    return f"job:{job_id}"


def jobs_namespace() -> str:
    return "jobs"


def job_index_key(job_id: str) -> str:
    return f"job:{job_id}"


def job_key() -> str:
    return "job"


def config_key() -> str:
    return "config"


def stage_key(stage_id: str) -> str:
    return f"stage:{stage_id}"


def split_key(split_id: str) -> str:
    return f"split:{split_id}"


def event_key(stage_id: str, ts_ns: int, msg_id: str) -> str:
    return f"event:{stage_id}:{ts_ns}:{msg_id}"


def parse_event_key(key: str) -> Optional[Tuple[str, int, str]]:
    if not key.startswith("event:"):
        return None
    parts = key.split(":", 3)
    if len(parts) != 4:
        return None
    _, stage_id, ts_str, msg_id = parts
    try:
        ts_ns = int(ts_str)
    except ValueError:
        return None
    return stage_id, ts_ns, msg_id


def encode_json(data: Dict[str, Any]) -> bytes:
    return json.dumps(data, separators=(",", ":"), sort_keys=False).encode("utf-8")


def decode_json(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))
