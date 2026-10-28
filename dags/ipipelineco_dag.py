"""Airflow binding for the ipipelineco task graph.

The graph itself lives in `ipipelineco.pipeline.build_tasks`; this file only maps each task
to an Airflow operator and carries the reliability settings: retries with exponential
backoff, a per-task SLA, and a failure callback that alerts.

Environment: IPIPELINECO_WORKDIR (default /opt/airflow/ipipelineco) and optionally
IPIPELINECO_CONFIG, IPIPELINECO_ENGINE, IPIPELINECO_WEBHOOK_URL.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 2.x
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - Airflow 3 moved it
    from airflow.providers.standard.operators.python import PythonOperator

from ipipelineco.config import load_config
from ipipelineco.pipeline import build_tasks, make_context

WORKDIR = os.environ.get("IPIPELINECO_WORKDIR", "/opt/airflow/ipipelineco")
CONFIG = os.environ.get("IPIPELINECO_CONFIG") or None
ENGINE = os.environ.get("IPIPELINECO_ENGINE") or None
cfg = load_config(CONFIG, WORKDIR, engine=ENGINE)


def _alert_on_failure(context) -> None:
    ti = context["task_instance"]
    ctx = make_context(cfg, run_id=context["run_id"])
    try:
        ctx.alerts.send("error", f"Airflow task {ti.task_id} failed (try {ti.try_number}): {context.get('exception')}")
    finally:
        ctx.wh.close()


def _run_task(task_name: str, **airflow_ctx) -> None:
    """Each Airflow task opens its own context, so tasks can run on different workers."""
    task = next(t for t in build_tasks(cfg) if t.name == task_name)
    ctx = make_context(cfg, run_id=airflow_ctx["run_id"])
    try:
        task.fn(ctx)
    finally:
        ctx.wh.close()


default_args = {
    "owner": "data-eng",
    "retries": cfg.retry.attempts - 1,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(seconds=cfg.sla_s),
    "on_failure_callback": _alert_on_failure,
}

with DAG(
    dag_id="ipipelineco",
    description="Ingest sources into the warehouse, run dbt, check quality, publish the run report.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "dbt"],
) as dag:
    ops = {}
    for t in build_tasks(cfg):
        ops[t.name] = PythonOperator(
            task_id=t.name,
            python_callable=_run_task,
            op_kwargs={"task_name": t.name},
            retries=t.retries if t.retries is not None else default_args["retries"],
            trigger_rule="all_done" if t.trigger_rule == "all_done" else "all_success",
        )
    for t in build_tasks(cfg):
        for dep in t.deps:
            ops[dep] >> ops[t.name]
