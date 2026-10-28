"""Ingest one source: extract, detect drift, process, MERGE, advance the watermark.

The watermark moves in the same transaction as the MERGE and the dead-letter rows, so a
crash at any point leaves the previous watermark in place and the next run re-reads the
same window. Together with the keyed MERGE that is what makes a run safe to repeat.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from .config import Config, SourceCfg
from .engines import process, scan_columns
from .errors import DriftError
from .extract import extract
from .warehouse import Warehouse, utcnow


@dataclass
class SourceResult:
    source: str
    mode: str
    engine: str
    extracted: int
    rejected: int
    duplicates: int
    inserted: int
    updated: int
    watermark: str | None

    def as_dict(self) -> dict:
        return asdict(self)


def ingest_source(cfg: Config, wh: Warehouse, src: SourceCfg, run_id: str, engine: str) -> SourceResult:
    since = wh.get_watermark(src.name)
    mode = "full" if since is None else "incremental"
    stage_dir = cfg.workdir / "stage" / run_id / src.name
    stage_dir.mkdir(parents=True, exist_ok=True)
    ex = extract(cfg, src, since, stage_dir / "raw.jsonl")

    columns = dict(src.columns)
    seen = scan_columns(ex.stage_file) if ex.count else set()
    new_cols = sorted(seen - set(columns))
    missing_keys = [k for k in src.key if ex.count and k not in seen]
    drift: list[tuple[str, str]] = []
    if missing_keys:
        wh.insert_row("ops.drift", {"run_id": run_id, "source": src.name, "kind": "key_missing", "detail": ",".join(missing_keys), "ts": utcnow()})
        raise DriftError(f"{src.name}: key column(s) {missing_keys} missing from source")
    if new_cols:
        if src.on_new_column == "fail":
            wh.insert_row("ops.drift", {"run_id": run_id, "source": src.name, "kind": "new_column_rejected", "detail": ",".join(new_cols), "ts": utcnow()})
            raise DriftError(f"{src.name}: unexpected new column(s) {new_cols}")
        if src.on_new_column == "add":
            for c in new_cols:
                columns[c] = "VARCHAR"
            drift.append(("new_column_added", ",".join(f"{c}:VARCHAR" for c in new_cols)))
        else:
            drift.append(("new_column_ignored", ",".join(new_cols)))

    order_by = src.watermark
    res = process(engine, ex.stage_file, columns, src.key, order_by, stage_dir / "typed")

    wh.ensure_table(src.target, src.columns, src.key)
    now = utcnow()
    with wh.transaction():
        if "new_column_added" in dict(drift):
            wh.add_columns(src.target, {c: columns[c] for c in new_cols})
        merged = wh.merge_parquet(res.parquet_glob, src.target, columns, src.key, run_id) if res.parquet_glob else None
        for kind, detail in drift:
            wh.insert_row("ops.drift", {"run_id": run_id, "source": src.name, "kind": kind, "detail": detail, "ts": now})
        for r in res.rejected:
            wh.insert_row("ops.dlq", {"run_id": run_id, "source": src.name, "reason": r["reason"], "payload": r["payload"][:2000], "ts": now})
        new_wm = ex.new_watermark
        if new_wm is not None:
            wh.set_watermark(src.name, new_wm)
        out = SourceResult(
            source=src.name,
            mode=mode,
            engine=engine,
            extracted=ex.count,
            rejected=len(res.rejected),
            duplicates=res.duplicates,
            inserted=merged.inserted if merged else 0,
            updated=merged.updated if merged else 0,
            watermark=new_wm,
        )
        wh.insert_row("ops.source_runs", {"run_id": run_id, **out.as_dict()})
    return out


def cleanup_stage(workdir: Path, run_id: str) -> None:
    import shutil

    shutil.rmtree(workdir / "stage" / run_id, ignore_errors=True)
