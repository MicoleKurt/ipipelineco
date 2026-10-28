"""Warehouse interface plus the portable ops-table logic both adapters share."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

OPS_TABLES = {
    "ops.runs": "run_id VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR, trigger VARCHAR, engine VARCHAR",
    "ops.task_runs": "run_id VARCHAR, task VARCHAR, status VARCHAR, attempts BIGINT, started_at TIMESTAMP, duration_s DOUBLE, error VARCHAR, sla_breached BOOLEAN",
    "ops.source_runs": "run_id VARCHAR, source VARCHAR, mode VARCHAR, engine VARCHAR, extracted BIGINT, rejected BIGINT, duplicates BIGINT, inserted BIGINT, updated BIGINT, watermark VARCHAR",
    "ops.dlq": "run_id VARCHAR, source VARCHAR, reason VARCHAR, payload VARCHAR, ts TIMESTAMP",
    "ops.drift": "run_id VARCHAR, source VARCHAR, kind VARCHAR, detail VARCHAR, ts TIMESTAMP",
    "ops.dq_results": "run_id VARCHAR, check_name VARCHAR, target VARCHAR, status VARCHAR, detail VARCHAR, metric DOUBLE",
    "ops.alerts": "run_id VARCHAR, ts TIMESTAMP, level VARCHAR, message VARCHAR",
    "ops.run_meta": "run_id VARCHAR, k VARCHAR, v VARCHAR",
    "ops.state": "source VARCHAR, watermark VARCHAR, updated_at TIMESTAMP",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class MergeResult:
    inserted: int
    updated: int


class Warehouse(ABC):
    kind = "abstract"

    # --- engine-specific ---
    @abstractmethod
    def execute(self, sql: str, params: tuple | list = ()) -> None: ...

    @abstractmethod
    def query(self, sql: str, params: tuple | list = ()) -> list[dict]: ...

    @abstractmethod
    def transaction(self): ...

    @abstractmethod
    def ensure_table(self, target: str, columns: dict[str, str], key: list[str]) -> None: ...

    @abstractmethod
    def table_columns(self, target: str) -> list[str]: ...

    @abstractmethod
    def add_columns(self, target: str, columns: dict[str, str]) -> None: ...

    @abstractmethod
    def merge_parquet(
        self, glob: str, target: str, columns: dict[str, str], key: list[str], run_id: str
    ) -> MergeResult: ...

    @abstractmethod
    def fingerprint(self, target: str, exclude: tuple[str, ...] = ("_loaded_at", "_run_id")) -> dict: ...

    @abstractmethod
    def close(self) -> None: ...

    @contextmanager
    def released(self):
        """Give up the connection while an external tool (dbt) uses the database."""
        yield

    # --- portable ---
    def init_ops(self) -> None:
        for name, ddl in OPS_TABLES.items():
            schema = name.split(".")[0]
            self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            self.execute(f"CREATE TABLE IF NOT EXISTS {name} ({ddl})")

    def insert_row(self, table: str, row: dict) -> None:
        cols = list(row)
        marks = ", ".join("?" for _ in cols)
        self.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({marks})", [row[c] for c in cols])

    def get_watermark(self, source: str) -> str | None:
        rows = self.query("SELECT watermark FROM ops.state WHERE source = ?", [source])
        return rows[0]["watermark"] if rows else None

    def set_watermark(self, source: str, watermark: str | None) -> None:
        self.execute("DELETE FROM ops.state WHERE source = ?", [source])
        if watermark is not None:
            self.insert_row("ops.state", {"source": source, "watermark": watermark, "updated_at": utcnow()})

    def set_meta(self, run_id: str, key: str, value) -> None:
        self.execute("DELETE FROM ops.run_meta WHERE run_id = ? AND k = ?", [run_id, key])
        self.insert_row("ops.run_meta", {"run_id": run_id, "k": key, "v": json.dumps(value, default=str)})

    def get_meta(self, run_id: str, key: str):
        rows = self.query("SELECT v FROM ops.run_meta WHERE run_id = ? AND k = ?", [run_id, key])
        return json.loads(rows[0]["v"]) if rows else None

    def latest_run_id(self) -> str | None:
        rows = self.query("SELECT run_id FROM ops.runs ORDER BY started_at DESC LIMIT 1")
        return rows[0]["run_id"] if rows else None
