"""Dry run: read the sources (never write the warehouse) and print what a real run would do."""

from __future__ import annotations

import shutil
from dataclasses import dataclass

from .config import Config
from .extract import extract
from .pipeline import build_tasks
from .warehouse import Warehouse


@dataclass
class Assumptions:
    rows_per_sec: float = 20_000.0  # MERGE throughput on the chosen warehouse size
    credits_per_hour: float = 1.0  # X-Small = 1 credit/hour
    min_billed_s: float = 60.0  # Snowflake bills at least 60s each time a warehouse resumes


def dry_run(cfg: Config, wh: Warehouse, a: Assumptions | None = None) -> dict:
    a = a or Assumptions()
    tmp = cfg.workdir / "stage" / "dryrun"
    plan = []
    try:
        for src in cfg.sources:
            since = wh.get_watermark(src.name)
            ex = extract(cfg, src, since, tmp / src.name / "raw.jsonl")
            plan.append(
                {
                    "source": src.name, "type": src.type, "since": since, "rows": ex.count,
                    "mb": round(ex.bytes / 1_048_576, 3), "target": src.target,
                    "mode": "full" if since is None else "incremental",
                }
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    rows = sum(p["rows"] for p in plan)
    seconds = rows / a.rows_per_sec
    billed = max(seconds, a.min_billed_s) if rows else 0.0
    return {
        "plan": plan,
        "rows": rows,
        "mb": round(sum(p["mb"] for p in plan), 3),
        "tasks": [(t.name, t.deps) for t in build_tasks(cfg)],
        "est_seconds": round(seconds, 2),
        "est_credits": round(billed / 3600 * a.credits_per_hour, 4),
        "assumptions": a,
    }
