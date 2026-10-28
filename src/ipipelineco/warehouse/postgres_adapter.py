"""Postgres adapter. Works with Supabase, Neon, RDS, Cloud SQL or any Postgres 12+.

    pip install "ipipelineco[postgres]"

Connection (first match wins):
  IPIPELINECO_PG_DSN   full connection string
  SUPABASE_DB_URL     the connection string from Supabase: Project Settings > Database
  DATABASE_URL        common on hosting platforms
  PGHOST/PGUSER/...   the standard libpq variables

Supabase: use the direct connection or the *session* pooler (port 5432). The transaction
pooler (port 6543) does not keep session state, and this adapter uses temp tables.
Tables go into the `raw`, `ops`, `staging` and `marts` schemas, which Supabase's Data API
does not expose by default, so loaded data is not reachable through the public REST API.
"""

from __future__ import annotations

import glob as globmod
import os
from contextlib import contextmanager

import pyarrow.parquet as pq

from . import postgres_sql as sql
from .base import MergeResult, Warehouse, utcnow

DSN_VARS = ("IPIPELINECO_PG_DSN", "SUPABASE_DB_URL", "DATABASE_URL")


def resolve_dsn(env=os.environ) -> str:
    for var in DSN_VARS:
        if env.get(var):
            return env[var]
    if env.get("PGHOST"):
        return ""  # psycopg reads the PG* variables itself
    raise RuntimeError(f"Set one of {', '.join(DSN_VARS)} (or the PGHOST/PGUSER/... variables)")


class PostgresWarehouse(Warehouse):
    kind = "postgres"
    ops_types = {"DOUBLE": "DOUBLE PRECISION", "VARCHAR": "TEXT"}

    def __init__(self, dsn: str | None = None):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Postgres support needs: pip install 'ipipelineco[postgres]'") from exc
        self.dsn = dsn if dsn is not None else resolve_dsn()
        self.con = psycopg.connect(self.dsn, autocommit=True)

    def close(self) -> None:
        if self.con is not None and not self.con.closed:
            self.con.close()

    @staticmethod
    def _p(statement: str) -> str:
        return statement.replace("?", "%s")

    def execute(self, statement, params=()):
        self.con.execute(self._p(statement), list(params) if params else None)

    def query(self, statement, params=()):
        cur = self.con.execute(self._p(statement), list(params) if params else None)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.con.execute("ROLLBACK")
            raise
        else:
            self.con.execute("COMMIT")

    def ensure_table(self, target, columns, key):
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
        self.execute(sql.create_table_sql(target, columns, key))

    def table_columns(self, target):
        schema, name = target.split(".")
        rows = self.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, name],
        )
        return [r["column_name"] for r in rows]

    def add_columns(self, target, columns):
        for c, t in columns.items():
            self.execute(sql.add_column_sql(target, c, t))

    def merge_parquet(self, glob, target, columns, key, run_id):
        files = sorted(globmod.glob(glob))
        names = list(columns)
        tmp = "_pc_tmp_" + target.replace(".", "_")
        self.execute(f"DROP TABLE IF EXISTS {tmp}")
        self.execute(sql.create_tmp_sql(tmp, target))
        try:
            with self.con.cursor() as cur, cur.copy(sql.copy_sql(tmp, names)) as cp:
                for f in files:
                    for batch in pq.ParquetFile(f).iter_batches(batch_size=5000):
                        for row in batch.to_pylist():
                            cp.write_row([row[c] for c in names])
            self.execute(sql.stamp_sql(tmp), [utcnow(), run_id])
            counts = self.query(sql.count_existing_sql(target, tmp, key))[0]
            total, existing = int(counts["total"]), int(counts["existing"])
            if total:
                self.execute(sql.upsert_sql(target, tmp, columns, key))
        finally:
            self.execute(f"DROP TABLE IF EXISTS {tmp}")
        return MergeResult(inserted=total - existing, updated=existing)

    def fingerprint(self, target, exclude=("_loaded_at", "_run_id")):
        cols = [c for c in self.table_columns(target) if c not in exclude]
        row = self.query(sql.fingerprint_sql(target, cols))[0]
        return {"rows": int(row["n"]), "hash": row["h"]}
