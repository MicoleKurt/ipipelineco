"""`ipipelineco demo`: three runs on generated sources, offline and free.

1. initial load        every source, with a flaky API, bad rows and duplicates in the data
2. incremental load    new and updated rows, late events, a CSV drop with a new column
3. replay proof        re-ingest `orders` from scratch and show the table did not change
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import load_config
from .pipeline import RunSummary, run_pipeline
from .report import write_report
from .sources.generate import generate, paths
from .sources.mockapi import ApiServer
from .warehouse import open_warehouse

MARKER = ".ipipelineco-demo"


def _prepare(workdir: Path) -> None:
    if workdir.exists() and any(workdir.iterdir()):
        if not (workdir / MARKER).exists():
            raise SystemExit(f"{workdir} exists and is not a ipipelineco demo folder. Pick another --workdir or empty it.")
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / MARKER).write_text("safe to delete\n")


def _event_printer(console: Console):
    def emit(ev: dict) -> None:
        t, name = ev["type"], ev["task"]
        if t == "retry":
            console.print(f"  [yellow]retry[/]   {name}  attempt {ev['attempt']} failed ({ev['error']}); waiting {ev['wait']:.1f}s")
        elif t == "permanent":
            console.print(f"  [red]stop[/]    {name}  {ev['error']} (not retryable)")
        elif t == "success":
            extra = f"  after {ev['attempts']} attempts" if ev["attempts"] > 1 else ""
            console.print(f"  [green]ok[/]      {name:<20} {ev['duration']:.1f}s{extra}")
        elif t == "failed":
            console.print(f"  [red]failed[/]  {name}")
        elif t == "skipped":
            console.print(f"  [dim]skipped[/] {name}")

    return emit


def _sources_table(summary: RunSummary) -> Table:
    t = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
    for col in ("source", "mode", "extracted", "rejected", "dupes", "inserted", "updated"):
        t.add_column(col, justify="left" if col in ("source", "mode") else "right")
    for r in summary.ctx.results.values():
        t.add_row(r.source, r.mode, str(r.extracted), str(r.rejected), str(r.duplicates), str(r.inserted), str(r.updated))
    return t


def _run(console: Console, cfg, wh, engine: str, title: str) -> RunSummary:
    console.rule(f"[bold]{title}")
    with ApiServer(paths(cfg.workdir)["payments"], fail_first=1) as api:
        cfg.vars["api_url"] = api.url
        summary = run_pipeline(cfg, wh=wh, engine=engine, console=console, emit=_event_printer(console), trigger="demo")
    console.print(_sources_table(summary))
    return summary


def run_demo(workdir: Path, engine: str = "python", console: Console | None = None) -> Path:
    console = console or Console()
    workdir = workdir.resolve()
    _prepare(workdir)
    cfg = load_config(None, workdir, engine=engine, api_url="http://127.0.0.1:0")
    cfg.retry.delay_s = 0.3
    console.print(Panel.fit("[bold]ipipelineco demo[/]  sources -> warehouse -> dbt -> report\nOffline, free, DuckDB. Engine: " + engine, border_style="magenta"))

    s = generate(workdir, tick=0)
    console.print(
        f"generated sources: {s['oltp']['customers']} customers, {s['oltp']['orders']} orders, {s['payments']} payments (API), "
        f"{s['events']} stream lines, 1 CSV drop"
    )
    wh = open_warehouse(cfg)
    first = _run(console, cfg, wh, engine, "Run 1: initial load (API fails once, data has bad rows and duplicates)")

    s = generate(workdir, tick=1)
    console.print("\nupstream changed: new + updated rows, 5 late events, replayed events, a CSV drop with a [bold]new column[/]")
    second = _run(console, cfg, wh, engine, "Run 2: incremental load")

    console.print()
    before = wh.fingerprint("raw.orders")
    wh.set_watermark("orders", None)
    third = _run(console, cfg, wh, engine, "Run 3: replay `orders` from scratch (idempotency check)")
    after = wh.fingerprint("raw.orders")
    r = third.ctx.results["orders"]
    proof = {"source": "orders", "before": before, "after": after, "reread": r.extracted, "inserted": r.inserted, "updated": r.updated}
    wh.set_meta(third.run_id, "proof", proof)
    same = before == after
    console.print(
        Panel(
            f"{'[green]IDEMPOTENT[/]' if same else '[red]DIFFERENT[/]'}  re-read {r.extracted} orders: {r.inserted} inserted, {r.updated} rewritten\n"
            f"fingerprint before {before['hash']} ({before['rows']} rows)\nfingerprint after  {after['hash']} ({after['rows']} rows)",
            title="replay proof",
            border_style="green" if same else "red",
        )
    )
    # The incremental run tells the fuller story (retry, drift, dead letters), so it is the main
    # report; the replay run keeps its own page.
    wh.set_meta(second.run_id, "proof", proof)
    write_report(cfg, wh, third.run_id, name="replay.html")
    path = write_report(cfg, wh, second.run_id)
    wh.close()
    ok = all(x.status == "success" for x in (first, second, third)) and same
    console.print(f"\n[bold]{'All 3 runs succeeded.' if ok else 'A run failed.'}[/]  Report: {path}")
    console.print(f"Open it:  [cyan]{path.as_uri()}[/]")
    time.sleep(0)
    return path
