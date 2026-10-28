import json
import sqlite3

import pytest

from ipipelineco.errors import DriftError, PermanentError, TransientError
from ipipelineco.extract import extract
from ipipelineco.ingest import ingest_source
from ipipelineco.sources.generate import generate, paths
from ipipelineco.sources.mockapi import ApiServer


def test_sqlite_watermark_is_inclusive(cfg, tmp_path):
    src = cfg.source("orders")
    full = extract(cfg, src, None, tmp_path / "a.jsonl")
    again = extract(cfg, src, full.new_watermark, tmp_path / "b.jsonl")
    assert full.count == 1200 and again.count >= 1  # boundary rows come back on purpose


def test_jsonl_offset_catches_late_events(cfg, tmp_path):
    src = cfg.source("events")
    first = extract(cfg, src, None, tmp_path / "a.jsonl")
    generate(cfg.workdir, tick=1)
    second = extract(cfg, src, first.new_watermark, tmp_path / "b.jsonl")
    late = [json.loads(line) for line in open(second.stage_file) if '"late-' in line]
    assert len(late) == 5 and second.count == 300 + 5 + 3


def test_csv_each_drop_once(cfg, tmp_path):
    src = cfg.source("products")
    a = extract(cfg, src, None, tmp_path / "a.jsonl")
    assert len(a.files) == 1 and a.count == 120
    assert extract(cfg, src, a.new_watermark, tmp_path / "b.jsonl").count == 0


def test_api_retry_signal_and_pagination(cfg, tmp_path):
    src = cfg.source("payments")
    with ApiServer(paths(cfg.workdir)["payments"], fail_first=1) as api:
        cfg.vars["api_url"] = api.url
        with pytest.raises(TransientError):
            extract(cfg, src, None, tmp_path / "a.jsonl")
        ok = extract(cfg, src, None, tmp_path / "b.jsonl")
    assert ok.count == 1500 and api.requests >= 1 + 15


def test_api_auth_failure_is_permanent(cfg, tmp_path, monkeypatch):
    monkeypatch.setenv("IPIPELINECO_API_KEY", "wrong")
    with ApiServer(paths(cfg.workdir)["payments"]) as api:
        cfg.vars["api_url"] = api.url
        with pytest.raises(PermanentError):
            extract(cfg, cfg.source("payments"), None, tmp_path / "a.jsonl")


def test_ingest_reconciles_and_is_replayable(cfg, wh):
    src = cfg.source("events")
    r1 = ingest_source(cfg, wh, src, "run1", "python")
    assert r1.extracted == r1.inserted + r1.updated + r1.duplicates + r1.rejected
    assert r1.rejected == 6 and r1.duplicates == 24  # 3 truncated + 3 bad types; 24 at-least-once duplicates
    fp = wh.fingerprint("raw.events")
    wh.set_watermark("events", None)
    r2 = ingest_source(cfg, wh, src, "run2", "python")
    assert r2.inserted == 0 and r2.updated == r1.inserted
    assert wh.fingerprint("raw.events") == fp
    assert wh.query("select count(*) n from ops.dlq where run_id = 'run1'")[0]["n"] == 6


def test_schema_drift_adds_column(cfg, wh):
    src = cfg.source("products")
    ingest_source(cfg, wh, src, "run1", "python")
    generate(cfg.workdir, tick=1)
    r = ingest_source(cfg, wh, src, "run2", "python")
    assert "supplier" in wh.table_columns("raw.products")
    assert r.inserted == 36 and r.rejected == 4  # 40 rows: 3 bad prices + 1 blank sku
    assert wh.query("select kind, detail from ops.drift")[0] == {"kind": "new_column_added", "detail": "supplier:VARCHAR"}


def test_drift_fail_policy_keeps_watermark(cfg, wh):
    src = cfg.source("products")
    ingest_source(cfg, wh, src, "run1", "python")
    wm = wh.get_watermark("products")
    generate(cfg.workdir, tick=1)
    src.on_new_column = "fail"
    with pytest.raises(DriftError):
        ingest_source(cfg, wh, src, "run2", "python")
    assert wh.get_watermark("products") == wm


def test_failure_leaves_watermark_untouched(cfg, wh):
    src = cfg.source("orders")
    ingest_source(cfg, wh, src, "run1", "python")
    wm = wh.get_watermark("orders")
    con = sqlite3.connect(paths(cfg.workdir)["db"])
    con.execute("ALTER TABLE orders RENAME TO orders_x")
    con.commit()
    con.close()
    with pytest.raises(TransientError):
        ingest_source(cfg, wh, src, "run2", "python")
    assert wh.get_watermark("orders") == wm
