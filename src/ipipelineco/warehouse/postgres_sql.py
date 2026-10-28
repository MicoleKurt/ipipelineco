"""Pure SQL builders for the Postgres adapter (Supabase, RDS, Neon, self-hosted: anything that speaks Postgres)."""

from __future__ import annotations

POSTGRES = {
    "BIGINT": "BIGINT",
    "VARCHAR": "TEXT",
    "DOUBLE": "DOUBLE PRECISION",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "BOOLEAN": "BOOLEAN",
}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_table_sql(target: str, columns: dict[str, str], key: list[str]) -> str:
    cols = ", ".join(f"{qident(c)} {POSTGRES[t]}" for c, t in columns.items())
    pk = ", ".join(qident(k) for k in key)
    return f"CREATE TABLE IF NOT EXISTS {target} ({cols}, _loaded_at TIMESTAMP, _run_id TEXT, PRIMARY KEY ({pk}))"


def add_column_sql(target: str, column: str, sql_type: str) -> str:
    return f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {qident(column)} {POSTGRES[sql_type]}"


def create_tmp_sql(tmp: str, target: str) -> str:
    # Columns and types only: no primary key, so the bulk COPY has no index to maintain.
    return f"CREATE TEMP TABLE {tmp} (LIKE {target} INCLUDING DEFAULTS)"


def copy_sql(tmp: str, columns: list[str]) -> str:
    return f"COPY {tmp} ({', '.join(qident(c) for c in columns)}) FROM STDIN"


def stamp_sql(tmp: str) -> str:
    return f"UPDATE {tmp} SET _loaded_at = %s, _run_id = %s"


def count_existing_sql(target: str, tmp: str, key: list[str]) -> str:
    on = " AND ".join(f"t.{qident(k)} = s.{qident(k)}" for k in key)
    return f"SELECT count(*) AS total, count(t.{qident(key[0])}) AS existing FROM {tmp} s LEFT JOIN {target} t ON {on}"


def upsert_sql(target: str, tmp: str, columns: dict[str, str], key: list[str]) -> str:
    names = list(columns)
    cols = ", ".join(qident(c) for c in names) + ", _loaded_at, _run_id"
    conflict = ", ".join(qident(k) for k in key)
    non_key = [c for c in names if c not in key]
    sets = ", ".join([f"{qident(c)} = EXCLUDED.{qident(c)}" for c in non_key] + ["_loaded_at = EXCLUDED._loaded_at", "_run_id = EXCLUDED._run_id"])
    return f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {tmp} ON CONFLICT ({conflict}) DO UPDATE SET {sets}"


def fingerprint_sql(target: str, columns: list[str]) -> str:
    """Order-independent content hash: sum of the first 60 bits of each row's md5."""
    row = ", ".join(qident(c) for c in columns)
    return (
        f"SELECT count(*) AS n, coalesce(sum(('x' || left(md5(ROW({row})::text), 15))::bit(60)::bigint), 0)::text AS h FROM {target}"
    )
