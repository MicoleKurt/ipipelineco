import json

from pipelineco.engines import process_python

COLS = {"id": "BIGINT", "v": "VARCHAR", "updated_at": "TIMESTAMP"}


def parquet(tmp_path, name, rows):
    p = tmp_path / f"{name}.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return process_python(p, COLS, ["id"], "updated_at", tmp_path / name).parquet_glob


def test_merge_is_idempotent(wh, tmp_path):
    wh.ensure_table("raw.t", COLS, ["id"])
    g1 = parquet(
        tmp_path,
        "a",
        [{"id": 1, "v": "x", "updated_at": "2026-10-01T00:00:00"}, {"id": 2, "v": "y", "updated_at": "2026-10-01T00:00:00"}],
    )
    m = wh.merge_parquet(g1, "raw.t", COLS, ["id"], "r1")
    assert (m.inserted, m.updated) == (2, 0)
    before = wh.fingerprint("raw.t")
    m = wh.merge_parquet(g1, "raw.t", COLS, ["id"], "r2")
    assert (m.inserted, m.updated) == (0, 2)
    assert wh.fingerprint("raw.t") == before
    g2 = parquet(
        tmp_path,
        "b",
        [{"id": 2, "v": "changed", "updated_at": "2026-10-02T00:00:00"}, {"id": 3, "v": "z", "updated_at": "2026-10-02T00:00:00"}],
    )
    m = wh.merge_parquet(g2, "raw.t", COLS, ["id"], "r3")
    assert (m.inserted, m.updated) == (1, 1)
    assert wh.query("select v from raw.t where id = 2")[0]["v"] == "changed"


def test_add_columns_and_transaction_rollback(wh):
    wh.ensure_table("raw.t", COLS, ["id"])
    wh.add_columns("raw.t", {"extra": "VARCHAR"})
    assert "extra" in wh.table_columns("raw.t")
    try:
        with wh.transaction():
            wh.set_watermark("s", "w1")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert wh.get_watermark("s") is None
    wh.set_watermark("s", "w2")
    assert wh.get_watermark("s") == "w2"
