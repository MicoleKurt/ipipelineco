"""Data quality checks that run after ingestion, before anything downstream trusts the data.

Statuses: pass | warn | fail | skip. A `fail` fails the task (and alerts); `warn` only alerts.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import timedelta

from .config import Config
from .ingest import SourceResult
from .warehouse import Warehouse, utcnow


@dataclass
class Check:
    name: str
    target: str
    status: str
    detail: str
    metric: float | None = None


def reconcile(r: SourceResult) -> Check:
    """Every extracted row must be accounted for: merged, collapsed as a duplicate, or rejected."""
    accounted = r.inserted + r.updated + r.duplicates + r.rejected
    ok = accounted == r.extracted
    detail = (
        f"extracted {r.extracted} = inserted {r.inserted} + updated {r.updated} + duplicates {r.duplicates} + rejected {r.rejected}"
        if ok
        else f"extracted {r.extracted} but only {accounted} accounted for"
    )
    return Check("reconciliation", f"raw.{r.source}", "pass" if ok else "fail", detail, float(r.extracted - accounted))


def not_null_and_unique(wh: Warehouse, target: str, key: list[str]) -> list[Check]:
    out = []
    for k in key:
        n = wh.query(f'SELECT count(*) AS n FROM {target} WHERE "{k}" IS NULL')[0]["n"]
        out.append(Check("not_null", f"{target}.{k}", "pass" if n == 0 else "fail", f"{n} null value(s)", float(n)))
    cols = ", ".join(f'"{k}"' for k in key)
    d = wh.query(f"SELECT count(*) AS n FROM (SELECT {cols} FROM {target} GROUP BY {cols} HAVING count(*) > 1) x")[0]["n"]
    out.append(Check("unique_key", target, "pass" if d == 0 else "fail", f"{d} duplicated key(s)", float(d)))
    return out


def freshness(wh: Warehouse, target: str, max_hours: float, now=None) -> Check:
    now = now or utcnow()
    row = wh.query(f"SELECT max(_loaded_at) AS m FROM {target}")[0]["m"]
    if row is None:
        return Check("freshness", target, "fail", "table is empty", None)
    age_h = (now - row).total_seconds() / 3600
    ok = age_h <= max_hours
    return Check("freshness", target, "pass" if ok else "fail", f"last load {age_h:.2f}h ago (limit {max_hours:g}h)", round(age_h, 3))


def volume_anomaly(history: list[int], current: int, ratio: float, min_history: int, target: str) -> Check:
    if len(history) < min_history:
        return Check("volume", target, "skip", f"building baseline ({len(history)}/{min_history} incremental runs)", float(current))
    med = statistics.median(history)
    if med == 0:
        return Check("volume", target, "pass", "baseline median is 0", float(current))
    r = current / med
    ok = ratio <= r <= 1 / ratio
    return Check("volume", target, "pass" if ok else "warn", f"{current} rows vs median {med:g} ({r:.2f}x)", round(r, 3))


def run_checks(cfg: Config, wh: Warehouse, run_id: str, results: dict[str, SourceResult]) -> list[Check]:
    checks: list[Check] = []
    max_h = float(cfg.dq.get("freshness_hours", 24))
    ratio = float(cfg.dq.get("volume_ratio", 0.5))
    min_hist = int(cfg.dq.get("volume_min_history", 3))
    for src in cfg.sources:
        r = results.get(src.name)
        if r is None:
            continue
        checks.append(reconcile(r))
        checks.extend(not_null_and_unique(wh, src.target, src.key))
        checks.append(freshness(wh, src.target, max_h))
        hist = wh.query(
            "SELECT extracted FROM ops.source_runs WHERE source = ? AND mode = 'incremental' AND run_id <> ? ORDER BY run_id DESC LIMIT 10",
            [src.name, run_id],
        )
        if r.mode == "incremental":
            checks.append(volume_anomaly([int(h["extracted"]) for h in hist], r.extracted, ratio, min_hist, src.target))
    for c in checks:
        wh.insert_row(
            "ops.dq_results",
            {"run_id": run_id, "check_name": c.name, "target": c.target, "status": c.status, "detail": c.detail, "metric": c.metric},
        )
    return checks


__all__ = ["Check", "run_checks", "reconcile", "freshness", "volume_anomaly", "not_null_and_unique", "timedelta"]
