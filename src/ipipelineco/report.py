"""Self-contained HTML run report: one file, inline SVG, no network."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

from jinja2 import Environment, select_autoescape
from markupsafe import Markup

from .config import Config
from .lineage import build_lineage
from .viz import Node, render_svg
from .warehouse import Warehouse


def _tasks_graph(cfg: Config, tasks: list[dict]) -> str:
    from .pipeline import build_tasks

    by = {t["task"]: t for t in tasks}
    nodes, edges = [], []
    for t in build_tasks(cfg):
        row = by.get(t.name)
        status = row["status"] if row else "skipped"
        sub = f"{row['duration_s']:.1f}s - {row['attempts']} attempt(s)" if row and row["status"] != "skipped" else "skipped"
        nodes.append(Node(t.name, t.name, status, sub))
        edges.extend((d, t.name) for d in t.deps)
    return render_svg(nodes, edges)


def collect(cfg: Config, wh: Warehouse, run_id: str) -> dict:
    q = wh.query
    run = q("SELECT * FROM ops.runs WHERE run_id = ?", [run_id])[0]
    tasks = q("SELECT * FROM ops.task_runs WHERE run_id = ? ORDER BY started_at", [run_id])
    sources = q("SELECT * FROM ops.source_runs WHERE run_id = ? ORDER BY source", [run_id])
    dq = q("SELECT * FROM ops.dq_results WHERE run_id = ?", [run_id])
    alerts = q("SELECT * FROM ops.alerts WHERE run_id = ? ORDER BY ts", [run_id])
    drift = q("SELECT * FROM ops.drift WHERE run_id = ? ORDER BY ts", [run_id])
    dlq = q("SELECT source, reason, count(*) AS n FROM ops.dlq WHERE run_id = ? GROUP BY source, reason ORDER BY source, reason", [run_id])
    dlq_samples = q("SELECT source, reason, payload FROM ops.dlq WHERE run_id = ? LIMIT 6", [run_id])
    history = q("SELECT run_id, status, started_at, finished_at FROM ops.runs ORDER BY started_at DESC LIMIT 8")
    dbt = wh.get_meta(run_id, "dbt")
    proof = wh.get_meta(run_id, "proof")
    manifest = cfg.workdir / "dbt_project" / "target" / "manifest.json"
    nodes, edges = build_lineage(cfg, manifest)
    retries = sum(max(t["attempts"] - 1, 0) for t in tasks)
    return {
        "run": run, "tasks": tasks, "sources": sources, "dq": dq, "alerts": alerts, "drift": drift,
        "dlq": dlq, "dlq_samples": dlq_samples, "history": history, "dbt": dbt, "proof": proof,
        "retries": retries,
        "merged": sum(s["inserted"] + s["updated"] for s in sources),
        "rejected": sum(s["rejected"] for s in sources),
        "dq_pass": sum(1 for c in dq if c["status"] == "pass"),
        "dq_total": len([c for c in dq if c["status"] != "skip"]),
        "dag_svg": Markup(_tasks_graph(cfg, tasks)),
        "lineage_svg": Markup(render_svg(nodes, edges, node_w=150, node_h=40, gap_x=40, gap_y=10)),
        "engine": run["engine"],
        "warehouse": cfg.warehouse.get("type", "duckdb"),
    }


def render(data: dict) -> str:
    env = Environment(autoescape=select_autoescape(default=True))
    tpl = env.from_string(resources.files("ipipelineco").joinpath("templates/report.html.j2").read_text(encoding="utf-8"))
    return tpl.render(**data)


def write_report(cfg: Config, wh: Warehouse, run_id: str, name: str = "index.html") -> Path:
    out_dir = cfg.workdir / "report"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(render(collect(cfg, wh, run_id)), encoding="utf-8")
    return path
