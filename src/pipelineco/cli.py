"""pipelineco command line."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import default_config_text, load_config

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Reliability-first ingestion: sources to warehouse to dbt to report.")
console = Console()

WORKDIR = typer.Option(Path(".pipelineco"), "--workdir", "-w", help="Folder for state, staging, dbt project and reports.")
CONFIG = typer.Option(None, "--config", "-c", help="pipelineco.yml. Omit to use the bundled demo config.")
ENGINE = typer.Option(None, "--engine", "-e", help="python | spark")


@app.callback()
def _root(version: bool = typer.Option(False, "--version", is_eager=True)):
    if version:
        console.print(__version__)
        raise typer.Exit()


@app.command()
def demo(
    workdir: Path = typer.Option(Path("pipelineco-demo"), "--workdir", "-w"),
    engine: str = typer.Option("python", "--engine", "-e", help="python | spark"),
    save_svg: Path = typer.Option(None, "--save-svg", help="Also save the terminal output as an SVG."),
):
    """Three runs on generated sources: initial load, incremental load, idempotent replay."""
    from .demo import run_demo

    rec = Console(record=bool(save_svg), width=100)
    path = run_demo(workdir, engine=engine, console=rec)
    if save_svg:
        rec.save_svg(str(save_svg), title="pipelineco demo")
    raise typer.Exit(0 if path.exists() else 1)


@app.command()
def init(path: Path = typer.Argument(Path("pipelineco.yml"))):
    """Write the default config to edit."""
    if path.exists():
        console.print(f"[red]{path} already exists[/]")
        raise typer.Exit(1)
    path.write_text(default_config_text(), encoding="utf-8")
    console.print(f"wrote {path}")


@app.command()
def generate(workdir: Path = WORKDIR, tick: int = typer.Option(0, help="0 = initial sources, 1 = what changed since")):
    """Generate the demo sources into <workdir>/sources."""
    from .sources.generate import generate as gen

    console.print(gen(workdir, tick=tick))


@app.command("serve-api")
def serve_api(workdir: Path = WORKDIR, port: int = 8765, fail_first: int = 0):
    """Serve the generated payments file as a paginated REST API (for trying `type: api`)."""
    import time

    from .sources.generate import paths
    from .sources.mockapi import ApiServer

    with ApiServer(paths(workdir)["payments"], port=port, fail_first=fail_first) as srv:
        console.print(f"serving {srv.url}/payments  (X-Api-Key: demo-key). Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


@app.command()
def run(
    workdir: Path = WORKDIR,
    config: Path = CONFIG,
    engine: str = ENGINE,
    dry_run: bool = typer.Option(False, "--dry-run", help="Read sources and print the plan, row estimates and Snowflake credits. Loads nothing."),
    reset: list[str] = typer.Option([], "--reset", help="Reset a source's watermark to backfill it from scratch."),
    rows_per_sec: float = typer.Option(20000, help="Dry-run assumption: warehouse MERGE throughput."),
    credits_per_hour: float = typer.Option(1.0, help="Dry-run assumption: X-Small = 1."),
):
    """Run the pipeline once."""
    from .dryrun import Assumptions
    from .dryrun import dry_run as plan_run
    from .pipeline import run_pipeline
    from .warehouse import open_warehouse

    cfg = load_config(config, workdir, engine=engine)
    wh = open_warehouse(cfg)
    if dry_run:
        out = plan_run(cfg, wh, Assumptions(rows_per_sec=rows_per_sec, credits_per_hour=credits_per_hour))
        t = Table(title="dry run: nothing was loaded")
        for c in ("source", "type", "mode", "since", "rows", "MB", "target"):
            t.add_column(c)
        for p in out["plan"]:
            t.add_row(p["source"], p["type"], p["mode"], str(p["since"] or "-"), str(p["rows"]), str(p["mb"]), p["target"])
        console.print(t)
        a = out["assumptions"]
        console.print("task order: " + " -> ".join(n for n, _ in out["tasks"]))
        console.print(
            f"estimate: {out['rows']} rows, {out['mb']} MB, ~{out['est_seconds']}s of merge time, "
            f"~[bold]{out['est_credits']}[/] Snowflake credits\n"
            f"[dim]assumes {a.rows_per_sec:,.0f} rows/s, {a.credits_per_hour} credit/h, {a.min_billed_s:.0f}s minimum billing. These are estimates; override with the flags.[/]"
        )
        wh.close()
        return
    for s in reset:
        wh.set_watermark(s, None)
    from .demo import _event_printer

    summary = run_pipeline(cfg, wh=wh, engine=engine, console=console, emit=_event_printer(console))
    console.print(f"run {summary.run_id}: [bold]{summary.status}[/]")
    if summary.ctx.report_path:
        console.print(f"report: {summary.ctx.report_path}")
    wh.close()
    raise typer.Exit(0 if summary.status == "success" else 1)


@app.command()
def report(workdir: Path = WORKDIR, config: Path = CONFIG, run_id: str = typer.Option(None)):
    """Re-render the HTML report for a run (default: the latest)."""
    from .report import write_report
    from .warehouse import open_warehouse

    cfg = load_config(config, workdir)
    wh = open_warehouse(cfg)
    rid = run_id or wh.latest_run_id()
    if not rid:
        console.print("[red]no runs found[/]")
        raise typer.Exit(1)
    console.print(write_report(cfg, wh, rid))
    wh.close()


@app.command()
def lineage(workdir: Path = WORKDIR, config: Path = CONFIG):
    """Print the lineage graph as Mermaid (needs a prior run so dbt's manifest exists)."""
    from .lineage import build_lineage, mermaid

    cfg = load_config(config, workdir)
    nodes, edges = build_lineage(cfg, workdir / "dbt_project" / "target" / "manifest.json")
    console.print(mermaid(nodes, edges), markup=False)


@app.command()
def doctor():
    """Check which optional pieces are available on this machine."""
    from .engines import spark_available

    def has(mod: str) -> bool:
        try:
            __import__(mod)
            return True
        except ImportError:
            return False

    rows = [
        ("python", sys.version.split()[0], True),
        ("duckdb (default warehouse)", "", has("duckdb")),
        ("dbt-duckdb", "", has("dbt.adapters.duckdb")),
        ("pyspark + java (spark engine)", "java on PATH" if shutil.which("java") else "no java", spark_available()),
        ("snowflake-connector (snowflake adapter)", "", has("snowflake.connector")),
        ("apache-airflow (DAG)", "", has("airflow")),
    ]
    t = Table()
    for c in ("component", "note", "available"):
        t.add_column(c)
    for name, note, ok in rows:
        t.add_row(name, note, "[green]yes[/]" if ok else "[yellow]no[/]")
    console.print(t)


if __name__ == "__main__":  # pragma: no cover
    app()
