"""The pipeline definition: one task graph, used by the local runner and the Airflow DAG."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .alerts import AlertSink
from .config import Config
from .dbtrun import run_dbt
from .errors import PermanentError
from .ingest import SourceResult, cleanup_stage, ingest_source
from .orchestrator import Task, TaskResult, run_tasks
from .quality import Check, run_checks
from .warehouse import Warehouse, open_warehouse, utcnow


@dataclass
class Context:
    cfg: Config
    wh: Warehouse
    run_id: str
    engine: str
    alerts: AlertSink
    results: dict[str, SourceResult] = field(default_factory=dict)
    checks: list[Check] = field(default_factory=list)
    dbt: dict | None = None
    report_path: Path | None = None


def new_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:4]


def make_context(cfg: Config, run_id: str | None = None, engine: str | None = None, console=None, wh: Warehouse | None = None) -> Context:
    wh = wh or open_warehouse(cfg)
    run_id = run_id or new_run_id()
    return Context(cfg, wh, run_id, engine or cfg.engine, AlertSink(wh, run_id, console))


def task_ingest(src_name: str):
    def fn(ctx: Context) -> None:
        src = ctx.cfg.source(src_name)
        ctx.results[src_name] = ingest_source(ctx.cfg, ctx.wh, src, ctx.run_id, ctx.engine)

    fn.__name__ = f"ingest_{src_name}"
    return fn


def task_dbt(ctx: Context) -> None:
    from .warehouse import DBT_TARGET

    kind = ctx.cfg.warehouse.get("type", "duckdb")
    target = DBT_TARGET.get(kind)
    if target is None:
        raise PermanentError(f"no dbt target mapped for warehouse {kind!r}; run dbt yourself or add it to DBT_TARGET")
    db = ctx.cfg.fmt(ctx.cfg.warehouse.get("database", "{workdir}/warehouse.duckdb"))
    with ctx.wh.released():
        summary = run_dbt(ctx.cfg.workdir, db, target=target)
    ctx.dbt = summary
    ctx.wh.set_meta(ctx.run_id, "dbt", summary)
    if not summary["success"]:
        failed = [n["name"] for n in summary["nodes"] if n["status"] not in ("success", "pass", "warn")]
        raise PermanentError(f"dbt build failed: {', '.join(failed[:5])}")


def task_quality(ctx: Context) -> None:
    ctx.checks = run_checks(ctx.cfg, ctx.wh, ctx.run_id, ctx.results)
    for c in ctx.checks:
        if c.status == "warn":
            ctx.alerts.send("warning", f"data quality warning on {c.target}: {c.name} - {c.detail}")
    bad = [c for c in ctx.checks if c.status == "fail"]
    if bad:
        raise PermanentError("; ".join(f"{c.name} {c.target}: {c.detail}" for c in bad[:3]))


def task_report(ctx: Context) -> None:
    from .report import write_report

    ctx.report_path = write_report(ctx.cfg, ctx.wh, ctx.run_id)


def build_tasks(cfg: Config) -> list[Task]:
    ingests = [Task(f"ingest_{s.name}", task_ingest(s.name)) for s in cfg.sources]
    names = [t.name for t in ingests]
    return ingests + [
        Task("quality_checks", task_quality, deps=names),
        Task("dbt_build", task_dbt, deps=names, retries=0),
        Task("publish_report", task_report, deps=["quality_checks", "dbt_build"], trigger_rule="all_done", retries=0),
    ]


@dataclass
class RunSummary:
    run_id: str
    status: str
    tasks: list[TaskResult]
    ctx: Context


def run_pipeline(
    cfg: Config,
    wh: Warehouse | None = None,
    engine: str | None = None,
    console=None,
    emit=lambda ev: None,
    trigger: str = "manual",
    sleep=None,
) -> RunSummary:
    import time

    ctx = make_context(cfg, engine=engine, console=console, wh=wh)
    started = utcnow()
    ctx.wh.insert_row("ops.runs", {"run_id": ctx.run_id, "started_at": started, "finished_at": None, "status": "running", "trigger": trigger, "engine": ctx.engine})

    def record(r: TaskResult) -> None:
        ctx.wh.insert_row(
            "ops.task_runs",
            {
                "run_id": ctx.run_id, "task": r.task, "status": r.status, "attempts": r.attempts,
                "started_at": utcnow(), "duration_s": round(r.duration_s, 3), "error": r.error, "sla_breached": r.sla_breached,
            },
        )

    t0 = time.monotonic()
    results = run_tasks(
        build_tasks(cfg), ctx,
        attempts=cfg.retry.attempts, delay_s=cfg.retry.delay_s, backoff=cfg.retry.backoff,
        default_sla_s=cfg.sla_s, alert=ctx.alerts.send, emit=emit, record=record, sleep=sleep or time.sleep,
    )
    total = time.monotonic() - t0
    if total > cfg.sla_s:
        ctx.alerts.send("warning", f"pipeline SLA breached: {total:.0f}s (limit {cfg.sla_s:.0f}s)")
    status = "success" if all(r.status == "success" for r in results) else "failed"
    ctx.wh.execute("UPDATE ops.runs SET finished_at = ?, status = ? WHERE run_id = ?", [utcnow(), status, ctx.run_id])
    cleanup_stage(cfg.workdir, ctx.run_id)
    return RunSummary(ctx.run_id, status, results, ctx)
