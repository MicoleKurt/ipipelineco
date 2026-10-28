"""DuckDB adapter: the free default. Same contract as the Snowflake adapter."""

from __future__ import annotations

from contextlib import contextmanager

import duckdb

from .base import MergeResult, Warehouse, utcnow


class DuckDBWarehouse(Warehouse):
    kind = "duckdb"

    def __init__(self, path: str):
        self.path = str(path)
        self._connect()

    def _connect(self) -> None:
        self.con = duckdb.connect(self.path)
        self.con.execute("SET TimeZone='UTC'")

    def close(self) -> None:
        if self.con is not None:
            self.con.close()
            self.con = None

    @contextmanager
    def released(self):
        self.close()
        try:
            yield
        finally:
            self._connect()

    def execute(self, sql, params=()):
        self.con.execute(sql, list(params))

    def query(self, sql, params=()):
        cur = self.con.execute(sql, list(params))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN TRANSACTION")
        try:
            yield
        except BaseException:
            self.con.execute("ROLLBACK")
            raise
        else:
            self.con.execute("COMMIT")

    def ensure_table(self, target, columns, key):
        schema = target.split(".")[0]
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cols = ", ".join(f'"{c}" {t}' for c, t in columns.items())
        pk = ", ".join(f'"{k}"' for k in key)
        self.execute(
            f"CREATE TABLE IF NOT EXISTS {target} ({cols}, _loaded_at TIMESTAMP, _run_id VARCHAR, PRIMARY KEY ({pk}))"
        )

    def table_columns(self, target):
        schema, name = target.split(".")
        rows = self.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, name],
        )
        return [r["column_name"] for r in rows]

    def add_columns(self, target, columns):
        for c, t in columns.items():
            self.execute(f'ALTER TABLE {target} ADD COLUMN IF NOT EXISTS "{c}" {t}')

    def merge_parquet(self, glob, target, columns, key, run_id):
        names = list(columns)
        select = ", ".join(f'"{c}"::{t} AS "{c}"' for c, t in columns.items())
        on = " AND ".join(f't."{k}" = s."{k}"' for k in key)
        counts = self.query(
            f"WITH s AS (SELECT {select} FROM read_parquet(?)) "
            f"SELECT count(*) AS total, count(t.\"{key[0]}\") AS existing FROM s LEFT JOIN {target} t ON {on}",
            [glob],
        )[0]
        total, existing = int(counts["total"]), int(counts["existing"])
        if total:
            col_list = ", ".join(f'"{c}"' for c in names) + ", _loaded_at, _run_id"
            self.execute(
                f"INSERT OR REPLACE INTO {target} ({col_list}) "
                f"SELECT {select}, ?::TIMESTAMP, ? FROM read_parquet(?)",
                [utcnow(), run_id, glob],
            )
        return MergeResult(inserted=total - existing, updated=existing)

    def fingerprint(self, target, exclude=("_loaded_at", "_run_id")):
        ex = ", ".join(exclude)
        row = self.query(
            f"SELECT count(*) AS n, coalesce(sum(hash(s)), 0)::VARCHAR AS h FROM (SELECT * EXCLUDE ({ex}) FROM {target}) s"
        )[0]
        return {"rows": int(row["n"]), "hash": row["h"]}
