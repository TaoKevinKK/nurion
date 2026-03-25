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

"""File sink implementations."""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TextIO

import pyarrow as pa
import pyarrow.parquet as pq

from _internal.core.models import Split, SplitPayload
from _internal.core.operator import OperatorConfig, OperatorRuntime, operator
from _internal.core.sink_operator import SinkOperator


@dataclass
class FileSinkConfig(OperatorConfig):
    """Configuration for FileSink operator."""

    output_path: str
    """Output file or directory path."""

    format: Literal["json", "parquet", "csv"] = "json"
    """Output format (json, parquet, or csv)."""

    buffer_size: int = 1000
    """Number of records to buffer before flushing."""


@operator(FileSinkConfig)
class FileSink(SinkOperator):
    """Sink that writes records to a local file."""

    def __init__(self, config: FileSinkConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        if not config.output_path:
            raise ValueError("output_path is required for FileSink")

        self.output_path = config.output_path
        self.format = config.format.lower()
        self.buffer_size = config.buffer_size

        self.logger = logging.getLogger(self.__class__.__name__)
        self.buffer: List[Dict[str, Any]] = []
        self.file_handle: Optional[TextIO] = None
        self._initialized = False
        self.output_file_path: Optional[Path] = None
        self._staging_file_path: Optional[Path] = None

        # Track written records for exactly-once
        self._records_written = 0
        self._commit_offset = {"records_committed": 0}

    def process_split(
        self, split: Split, payload: Optional[SplitPayload] = None
    ) -> Optional[SplitPayload]:
        if payload is None:
            raise ValueError("FileSink requires a payload")
        self.buffer.extend(payload.to_pylist())
        if len(self.buffer) >= self.buffer_size:
            self._flush()
        return None

    def close(self) -> None:
        self._flush()
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
        if self._initialized:
            target = self.output_file_path or Path(self.output_path)
            self.logger.info("Closed output file: %s", target)
        self._initialized = False

    def _ensure_output_dir(self) -> None:
        raw_path = Path(self.output_path)
        target_dir = raw_path.parent if raw_path.suffix else raw_path
        target_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        self._ensure_output_dir()
        if self.format == "json":
            self._initialize_json_writer()
        self._initialized = True
        self.logger.info("Opened output file: %s", self.output_file_path or self.output_path)

    def _initialize_json_writer(self) -> None:
        target_path = self._build_output_file_path()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_handle = open(target_path, "w")
        self.output_file_path = target_path

    def _build_output_file_path(self) -> Path:
        base_path = Path(self.output_path)
        if base_path.suffix:
            return base_path
        worker_label = self.worker_id or "default"
        return base_path / f"part-{worker_label}.{self.format}"

    def _flush(self) -> None:
        if not self.buffer:
            return

        self._ensure_initialized()

        records_to_write = len(self.buffer)

        if self.format == "json":
            self._flush_json()
        elif self.format == "parquet":
            self._flush_parquet()
        elif self.format == "csv":
            self._flush_csv()
        else:
            raise ValueError(f"Unsupported format: {self.format}")

        self._records_written += records_to_write
        self.buffer.clear()

    def _flush_json(self) -> None:
        if not self.file_handle:
            raise RuntimeError("JSON sink file handle unavailable")

        for record in self.buffer:
            payload = self._format_json_record(record)
            self.file_handle.write(json.dumps(payload) + "\n")

    def _format_json_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(record)
        key = row.pop(SplitPayload.NURION_KEY_COLUMN, None)
        timestamp = row.pop(SplitPayload.NURION_TS_COLUMN, None)
        return {
            "key": key,
            "timestamp": timestamp,
            "value": self._encode_bytes_fields(row),
        }

    def _encode_bytes_fields(self, obj: Any) -> Any:
        """Recursively encode bytes fields to base64 strings for JSON serialization."""
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("ascii")
        elif isinstance(obj, dict):
            return {k: self._encode_bytes_fields(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._encode_bytes_fields(item) for item in obj]
        return obj

    def _flush_parquet(self) -> None:
        self._ensure_output_dir()
        table = pa.Table.from_pylist(
            [
                {
                    "key": record.get("key"),
                    "value": record.get("value"),
                    "timestamp": record.get("timestamp"),
                }
                for record in self.buffer
            ]
        )

        if Path(self.output_path).exists():
            pq.write_table(table, self.output_path, append=True)
        else:
            pq.write_table(table, self.output_path)

    def _flush_csv(self) -> None:
        if not self.buffer:
            return

        import csv

        first_value = self.buffer[0].get("value")
        if isinstance(first_value, dict):
            fieldnames = ["key"] + list(first_value.keys())
        else:
            fieldnames = ["key", "value"]
        self._ensure_output_dir()
        file_exists = Path(self.output_path).exists()

        with open(self.output_path, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for record in self.buffer:
                row = {"key": record.get("key")}
                record_value = record.get("value")
                if isinstance(record_value, dict):
                    row.update(record_value)
                else:
                    row["value"] = record_value
                writer.writerow(row)
