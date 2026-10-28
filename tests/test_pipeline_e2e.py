"""Full pipeline on generated sources: ingest, dbt build, quality checks, report."""

import pytest

pytest.importorskip("dbt.cli.main")

from ipipelineco.pipeline import run_pipeline  # noqa: E402
from ipipelineco.sources.generate import generate, paths  # noqa: E402
from ipipelineco.sources.mockapi import ApiServer  # noqa: E402


def run(cfg, wh, **kw):
    with ApiServer(paths(cfg.workdir)["payments"], fail_first=1) as api:
        cfg.vars["api_url"] = api.url
        return run_pipeline(cfg, wh=wh, **kw)


def test_two_runs_and_replay(cfg, wh):
    s1 = run(cfg, wh)
    assert s1.status == "success", [(t.task, t.error) for t in s1.tasks if t.status != "success"]
    pay = next(t for t in s1.tasks if t.task == "ingest_payments")
    assert pay.attempts == 2  # the API's first response was a 503
    assert s1.ctx.dbt["models"]["fail"] == 0 and s1.ctx.dbt["tests"]["fail"] == 0
    assert s1.ctx.dbt["models"]["pass"] == 9
    assert wh.query("select count(*) n from marts.fct_orders")[0]["n"] == 1200

    generate(cfg.workdir, tick=1)
    s2 = run(cfg, wh)
    assert s2.status == "success", [(t.task, t.error) for t in s2.tasks if t.status != "success"]
    assert wh.query("select count(*) n from marts.fct_orders")[0]["n"] == 1350
    assert wh.query("select count(*) n from ops.drift")[0]["n"] == 1
    for r in s2.ctx.results.values():
        assert r.extracted == r.inserted + r.updated + r.duplicates + r.rejected

    before = wh.fingerprint("raw.orders")
    wh.set_watermark("orders", None)
    s3 = run(cfg, wh)
    assert s3.status == "success"
    assert s3.ctx.results["orders"].inserted == 0
    assert wh.fingerprint("raw.orders") == before
    assert s3.ctx.report_path.exists()
    html = s3.ctx.report_path.read_text(encoding="utf-8")
    assert "<script" not in html and "http://" not in html.replace("http://www.w3.org", "")
