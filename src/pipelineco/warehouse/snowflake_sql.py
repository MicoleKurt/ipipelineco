"""Pure SQL builders for the Snowflake adapter.

They return strings and touch no network, so they are unit-tested without an account.
The flow per load: PUT parquet to a stage, COPY into a temp table, MERGE into the target.
"""

from __future__ import annotations

from ..types import SNOWFLAKE


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_table_sql(target: str, columns: dict[str, str], key: list[str], cluster_by: list[str] | None = None) -> str:
    cols = ", ".join(f"{qident(c)} {SNOWFLAKE[t]}" for c, t in columns.items())
    pk = ", ".join(qident(k) for k in key)
    sql = (
        f"CREATE TABLE IF NOT EXISTS {target} ({cols}, _LOADED_AT TIMESTAMP_NTZ, _RUN_ID VARCHAR, "
        f"PRIMARY KEY ({pk}))"
    )
    if cluster_by:
        sql += " CLUSTER BY (" + ", ".join(qident(c) for c in cluster_by) + ")"
    return sql


def create_stage_sql(stage: str) -> str:
    return f"CREATE STAGE IF NOT EXISTS {stage} FILE_FORMAT = (TYPE = PARQUET)"


def put_sql(local_glob: str, stage: str, run_id: str) -> str:
    path = local_glob.replace("\\", "/")
    return f"PUT 'file://{path}' @{stage}/{run_id}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE"


def create_temp_sql(tmp: str, target: str) -> str:
    return f"CREATE OR REPLACE TEMPORARY TABLE {tmp} LIKE {target}"


def copy_sql(tmp: str, stage: str, run_id: str) -> str:
    return (
        f"COPY INTO {tmp} FROM @{stage}/{run_id}/ FILE_FORMAT = (TYPE = PARQUET) "
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PURGE = TRUE"
    )


def merge_sql(target: str, tmp: str, columns: dict[str, str], key: list[str]) -> str:
    names = list(columns)
    on = " AND ".join(f"t.{qident(k)} = s.{qident(k)}" for k in key)
    non_key = [c for c in names if c not in key]
    sets = ", ".join([f"t.{qident(c)} = s.{qident(c)}" for c in non_key] + ["t._LOADED_AT = s._LOADED_AT", "t._RUN_ID = s._RUN_ID"])
    cols = ", ".join(qident(c) for c in names) + ", _LOADED_AT, _RUN_ID"
    vals = ", ".join(f"s.{qident(c)}" for c in names) + ", s._LOADED_AT, s._RUN_ID"
    return (
        f"MERGE INTO {target} t USING {tmp} s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )


def stamp_sql(tmp: str) -> str:
    return f"UPDATE {tmp} SET _LOADED_AT = ?, _RUN_ID = ? WHERE TRUE"


def count_existing_sql(target: str, tmp: str, key: list[str]) -> str:
    on = " AND ".join(f"t.{qident(k)} = s.{qident(k)}" for k in key)
    return (
        f"SELECT count(*) AS total, count(t.{qident(key[0])}) AS existing "
        f"FROM {tmp} s LEFT JOIN {target} t ON {on}"
    )


def add_column_sql(target: str, column: str, sql_type: str) -> str:
    return f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {qident(column)} {SNOWFLAKE[sql_type]}"
