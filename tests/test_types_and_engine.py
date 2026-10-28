import json

import duckdb
import pytest

from pipelineco.engines import process_python, scan_columns
from pipelineco.types import CastError, cast_value

COLS = {"id": "BIGINT", "name": "VARCHAR", "price": "DOUBLE", "updated_at": "TIMESTAMP"}


def stage(tmp_path, lines):
    p = tmp_path / "raw.jsonl"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def test_cast_basics():
    assert cast_value("12", "BIGINT") == 12
    assert cast_value(" ", "VARCHAR") is None
    assert cast_value("2026-10-01T10:00:00Z", "TIMESTAMP").isoformat() == "2026-10-01T10:00:00"
    assert cast_value("yes", "BOOLEAN") is True
    for bad, typ in (("12,50 EUR", "DOUBLE"), ("1.5", "BIGINT"), ("maybe", "BOOLEAN"), ("nan", "DOUBLE")):
        with pytest.raises(CastError):
            cast_value(bad, typ)


def test_engine_rejects_bad_rows_and_keeps_latest(tmp_path):
    p = stage(
        tmp_path,
        [
            json.dumps({"id": 1, "name": "a", "price": "1.0", "updated_at": "2026-10-01T10:00:00"}),
            json.dumps({"id": 1, "name": "a2", "price": "2.0", "updated_at": "2026-10-02T10:00:00"}),
            json.dumps({"id": 1, "name": "old", "price": "9.0", "updated_at": "2026-09-01T10:00:00"}),
            json.dumps({"id": 2, "name": "b", "price": "N/A", "updated_at": "2026-10-01T10:00:00"}),
            json.dumps({"id": None, "name": "c", "price": "1", "updated_at": "2026-10-01T10:00:00"}),
            '{"id": 3, "name": ',
            json.dumps({"id": 4, "name": "d", "price": "3", "updated_at": "2026-10-03T00:00:00"}),
        ],
    )
    res = process_python(p, COLS, ["id"], "updated_at", tmp_path / "out")
    reasons = sorted(r["reason"] for r in res.rejected)
    assert reasons == ["cast_error:price:DOUBLE", "malformed_json", "null_key:id"]
    assert res.read == 7 and res.kept == 2 and res.duplicates == 2
    assert res.watermark == "2026-10-03T00:00:00"
    rows = duckdb.sql(f"select id, name from read_parquet('{res.parquet_glob}') order by id").fetchall()
    assert rows == [(1, "a2"), (4, "d")]


def test_scan_columns(tmp_path):
    p = stage(tmp_path, ['{"a":1}', "oops", '{"b":2}'])
    assert scan_columns(p) == {"a", "b"}
