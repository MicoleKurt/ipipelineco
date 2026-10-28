"""Postgres / Supabase adapter. The SQL builders are always tested; the live tests need a database.

    IPIPELINECO_TEST_PG_DSN=postgresql://postgres:postgres@localhost:5432/postgres pytest tests/test_postgres.py

CI runs them against a Postgres 16 service container.
"""

import os

import pytest

from ipipelineco.config import load_config
from ipipelineco.warehouse import REGISTRY, open_warehouse, register_warehouse
from ipipelineco.warehouse import postgres_sql as s

COLS = {"id": "BIGINT", "name": "VARCHAR", "amount": "DOUBLE", "updated_at": "TIMESTAMP"}


def test_create_and_upsert_sql():
    assert s.create_table_sql("raw.t", COLS, ["id"]) == (
        'CREATE TABLE IF NOT EXISTS raw.t ("id" BIGINT, "name" TEXT, "amount" DOUBLE PRECISION, "updated_at" TIMESTAMP, '
        '_loaded_at TIMESTAMP, _run_id TEXT, PRIMARY KEY ("id"))'
    )
    up = s.upsert_sql("raw.t", "tmp", COLS, ["id"])
    assert 'ON CONFLICT ("id") DO UPDATE SET' in up
    assert '"name" = EXCLUDED."name"' in up and '"id" = EXCLUDED."id"' not in up
    assert "_run_id = EXCLUDED._run_id" in up


def test_composite_key_count_and_fingerprint():
    assert 't."a" = s."a" AND t."b" = s."b"' in s.count_existing_sql("raw.x", "tmp", ["a", "b"])
    assert s.copy_sql("tmp", ["a", "b"]) == 'COPY tmp ("a", "b") FROM STDIN'
    assert "md5(ROW(" in s.fingerprint_sql("raw.x", ["a", "b"])


def test_registry_accepts_custom_warehouses(tmp_path):
    cfg = load_config(None, tmp_path)
    cfg.warehouse = {"type": "mydb"}
    with pytest.raises(ValueError, match="known:"):
        open_warehouse(cfg)
    assert {"duckdb", "snowflake", "postgres", "supabase"} <= set(REGISTRY)
    calls = []
    register_warehouse("mydb", lambda c: calls.append(c) or (_ for _ in ()).throw(RuntimeError("factory reached")))
    with pytest.raises(RuntimeError, match="factory reached"):
        open_warehouse(cfg)
    assert calls == [cfg]
    REGISTRY.pop("mydb")


DSN = os.environ.get("IPIPELINECO_TEST_PG_DSN")
live = pytest.mark.skipif(not DSN, reason="set IPIPELINECO_TEST_PG_DSN to run against a Postgres")


@pytest.fixture
def pg(tmp_path):
    from ipipelineco.sources.generate import generate

    generate(tmp_path, tick=0)
    cfg = load_config(None, tmp_path, api_url="http://127.0.0.1:0")
    cfg.warehouse = {"type": "postgres", "dsn": DSN}
    cfg.retry.delay_s = 0.0
    wh = open_warehouse(cfg)
    for schema in ("raw", "ops", "staging", "marts"):
        wh.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    wh.init_ops()
    yield cfg, wh
    wh.close()


@live
def test_postgres_merge_is_idempotent(pg, tmp_path):
    import json

    from ipipelineco.engines import process_python

    _, wh = pg
    p = tmp_path / "a.jsonl"
    rows = [{"id": i, "name": f"n{i}", "amount": i * 1.5, "updated_at": "2026-10-01T00:00:00"} for i in range(1, 51)]
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    glob = process_python(p, COLS, ["id"], "updated_at", tmp_path / "out").parquet_glob
    wh.ensure_table("raw.t", COLS, ["id"])
    m = wh.merge_parquet(glob, "raw.t", COLS, ["id"], "r1")
    assert (m.inserted, m.updated) == (50, 0)
    before = wh.fingerprint("raw.t")
    m = wh.merge_parquet(glob, "raw.t", COLS, ["id"], "r2")
    assert (m.inserted, m.updated) == (0, 50)
    assert wh.fingerprint("raw.t") == before
    wh.add_columns("raw.t", {"extra": "VARCHAR"})
    assert "extra" in wh.table_columns("raw.t")
    with pytest.raises(RuntimeError):
        with wh.transaction():
            wh.set_watermark("s", "w1")
            raise RuntimeError("boom")
    assert wh.get_watermark("s") is None


@live
def test_postgres_ingest_dlq_replay(pg):
    from ipipelineco.ingest import ingest_source

    cfg, wh = pg
    for name in ("customers", "events", "products"):
        ingest_source(cfg, wh, cfg.source(name), "run1", "python")
    r = ingest_source(cfg, wh, cfg.source("events"), "run1b", "python")  # nothing new
    assert r.extracted == 0
    assert wh.query("select count(*) n from ops.dlq where source = 'events'")[0]["n"] == 6
    fp = wh.fingerprint("raw.events")
    wh.set_watermark("events", None)
    r = ingest_source(cfg, wh, cfg.source("events"), "run2", "python")
    assert r.inserted == 0 and wh.fingerprint("raw.events") == fp


@live
def test_postgres_full_pipeline_with_dbt(pg):
    pytest.importorskip("dbt.adapters.postgres")
    from ipipelineco.pipeline import run_pipeline
    from ipipelineco.sources.generate import paths
    from ipipelineco.sources.mockapi import ApiServer

    cfg, wh = pg
    with ApiServer(paths(cfg.workdir)["payments"], fail_first=1) as api:
        cfg.vars["api_url"] = api.url
        s = run_pipeline(cfg, wh=wh)
    assert s.status == "success", [(t.task, t.error) for t in s.tasks if t.status != "success"]
    assert wh.query("select count(*) n from marts.fct_orders")[0]["n"] == 1200
