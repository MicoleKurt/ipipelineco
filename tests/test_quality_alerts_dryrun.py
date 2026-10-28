import json
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from pipelineco.alerts import AlertSink
from pipelineco.dryrun import Assumptions, dry_run
from pipelineco.ingest import SourceResult, ingest_source
from pipelineco.quality import freshness, not_null_and_unique, reconcile, volume_anomaly
from pipelineco.sources.generate import paths
from pipelineco.sources.mockapi import ApiServer
from pipelineco.warehouse import utcnow


def result(**kw):
    base = dict(source="s", mode="incremental", engine="python", extracted=10, rejected=1, duplicates=2, inserted=5, updated=2, watermark=None)
    base.update(kw)
    return SourceResult(**base)


def test_reconciliation_detects_lost_rows():
    assert reconcile(result()).status == "pass"
    bad = reconcile(result(inserted=4))
    assert bad.status == "fail" and bad.metric == 1.0


def test_volume_anomaly_needs_baseline_then_flags():
    assert volume_anomaly([100, 110], 5, 0.5, 3, "t").status == "skip"
    assert volume_anomaly([100, 110, 90], 100, 0.5, 3, "t").status == "pass"
    assert volume_anomaly([100, 110, 90], 10, 0.5, 3, "t").status == "warn"
    assert volume_anomaly([100, 110, 90], 400, 0.5, 3, "t").status == "warn"


def test_freshness_and_uniqueness(cfg, wh):
    ingest_source(cfg, wh, cfg.source("customers"), "r1", "python")
    assert freshness(wh, "raw.customers", 24).status == "pass"
    assert freshness(wh, "raw.customers", 24, now=utcnow() + timedelta(hours=30)).status == "fail"
    assert all(c.status == "pass" for c in not_null_and_unique(wh, "raw.customers", ["id"]))


def test_alert_sink_writes_table_and_webhook(wh):
    got = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            got.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    Thread(target=srv.serve_forever, daemon=True).start()
    AlertSink(wh, "r1", webhook=f"http://127.0.0.1:{srv.server_port}/").send("error", "boom")
    AlertSink(wh, "r1", webhook="http://127.0.0.1:1/").send("warning", "webhook down must not raise")
    srv.shutdown()
    assert "boom" in got[0]["text"]
    assert wh.query("select count(*) n from ops.alerts")[0]["n"] == 2


def test_dry_run_loads_nothing_and_estimates(cfg, wh):
    with ApiServer(paths(cfg.workdir)["payments"]) as api:
        cfg.vars["api_url"] = api.url
        out = dry_run(cfg, wh, Assumptions(rows_per_sec=1000, credits_per_hour=2))
    assert out["rows"] > 5000 and {p["source"] for p in out["plan"]} == {s.name for s in cfg.sources}
    assert out["est_credits"] == round(max(out["rows"] / 1000, 60) / 3600 * 2, 4)
    assert wh.table_columns("raw.orders") == []  # nothing created
    assert wh.get_watermark("orders") is None
