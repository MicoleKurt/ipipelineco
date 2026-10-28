import json

import duckdb
import pytest

from ipipelineco.engines import process_python, process_spark, spark_available

pytestmark = pytest.mark.skipif(not spark_available(), reason="needs Java and pyspark")

COLS = {"id": "BIGINT", "name": "VARCHAR", "price": "DOUBLE", "updated_at": "TIMESTAMP"}


def test_spark_matches_python(tmp_path):
    lines = [
        json.dumps({"id": 1, "name": "a", "price": "1.0", "updated_at": "2026-10-01T10:00:00"}),
        json.dumps({"id": 1, "name": "a2", "price": "2.0", "updated_at": "2026-10-02T10:00:00"}),
        json.dumps({"id": 2, "name": "b", "price": "N/A", "updated_at": "2026-10-01T10:00:00"}),
        json.dumps({"id": None, "name": "c", "price": "1", "updated_at": "2026-10-01T10:00:00"}),
        '{"id": 3, "name": ',
        json.dumps({"id": 4, "name": "d", "price": "3", "updated_at": "2026-10-03T00:00:00"}),
    ]
    p = tmp_path / "raw.jsonl"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    a = process_python(p, COLS, ["id"], "updated_at", tmp_path / "py")
    b = process_spark(p, COLS, ["id"], "updated_at", tmp_path / "sp")
    assert (a.read, a.kept, a.duplicates, len(a.rejected)) == (b.read, b.kept, b.duplicates, len(b.rejected))
    assert sorted(r["reason"] for r in a.rejected) == sorted(r["reason"] for r in b.rejected)
    q = "select id, name, price, updated_at from read_parquet('{}') order by id"
    assert duckdb.sql(q.format(a.parquet_glob)).fetchall() == duckdb.sql(q.format(b.parquet_glob)).fetchall()
