"""Snowflake adapter. Needs `pip install ipipelineco[snowflake]` and your own account.

Credentials come from environment variables, never from the config file:
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or SNOWFLAKE_PRIVATE_KEY_PATH),
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_ROLE (optional).

This build only unit-tests the SQL it generates (tests/test_snowflake_sql.py). It has not
been run against a live account.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

from . import snowflake_sql as sql
from .base import MergeResult, Warehouse, utcnow


class SnowflakeWarehouse(Warehouse):
    kind = "snowflake"

    def __init__(self, cluster_by: dict[str, list[str]] | None = None, stage: str = "ops.ipipelineco_stage"):
        try:
            import snowflake.connector as sf
        except ImportError as exc:  # pragma: no cover - exercised only with the extra installed
            raise RuntimeError("Snowflake support needs: pip install 'ipipelineco[snowflake]'") from exc
        missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE") if v not in os.environ]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
        sf.paramstyle = "qmark"
        args = dict(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            paramstyle="qmark",
        )
        if os.environ.get("SNOWFLAKE_ROLE"):
            args["role"] = os.environ["SNOWFLAKE_ROLE"]
        if os.environ.get("SNOWFLAKE_PASSWORD"):
            args["password"] = os.environ["SNOWFLAKE_PASSWORD"]
        elif os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
            args["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        self.con = sf.connect(**args)
        self.cluster_by = cluster_by or {}
        self.stage = stage

    def close(self):
        self.con.close()

    def execute(self, statement, params=()):
        cur = self.con.cursor()
        try:
            cur.execute(statement, list(params) if params else None)
        finally:
            cur.close()

    def query(self, statement, params=()):
        cur = self.con.cursor()
        try:
            cur.execute(statement, list(params) if params else None)
            cols = [d[0].lower() for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            cur.close()

    @contextmanager
    def transaction(self):
        self.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.execute("ROLLBACK")
            raise
        else:
            self.execute("COMMIT")

    def ensure_table(self, target, columns, key):
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
        self.execute(sql.create_table_sql(target, columns, key, self.cluster_by.get(target)))

    def table_columns(self, target):
        schema, name = target.split(".")
        rows = self.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = UPPER(?) AND table_name = UPPER(?) ORDER BY ordinal_position",
            [schema, name],
        )
        return [r["column_name"].lower() for r in rows]

    def add_columns(self, target, columns):
        for c, t in columns.items():
            self.execute(sql.add_column_sql(target, c, t))

    def merge_parquet(self, glob, target, columns, key, run_id):
        tmp = f"{target.split('.')[0]}._ipipelineco_tmp_{target.split('.')[1]}"
        self.execute(sql.create_stage_sql(self.stage))
        self.execute(sql.put_sql(glob, self.stage, run_id))
        self.execute(sql.create_temp_sql(tmp, target))
        self.execute(sql.copy_sql(tmp, self.stage, run_id))
        self.execute(sql.stamp_sql(tmp), [utcnow(), run_id])
        counts = self.query(sql.count_existing_sql(target, tmp, key))[0]
        total, existing = int(counts["total"]), int(counts["existing"])
        self.execute(sql.merge_sql(target, tmp, columns, key))
        return MergeResult(inserted=total - existing, updated=existing)

    def fingerprint(self, target, exclude=("_loaded_at", "_run_id")):
        cols = [c for c in self.table_columns(target) if c not in {e.lower() for e in exclude}]
        expr = ", ".join(sql.qident(c.upper()) for c in cols)
        row = self.query(f"SELECT count(*) AS n, coalesce(sum(hash({expr})), 0)::VARCHAR AS h FROM {target}")[0]
        return {"rows": int(row["n"]), "hash": row["h"]}


def local_glob(path: Path) -> str:
    return str(path).replace("\\", "/")
