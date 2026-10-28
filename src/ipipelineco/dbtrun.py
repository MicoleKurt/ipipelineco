"""Run the bundled dbt project programmatically and summarise the result."""

from __future__ import annotations

import os
import shutil
from importlib import resources
from pathlib import Path

from .errors import PermanentError


def project_dir(workdir: Path) -> Path:
    """Copy the packaged dbt project into the workdir so dbt can write target/ there."""
    dest = workdir / "dbt_project"
    src = Path(str(resources.files("ipipelineco").joinpath("dbt_project")))
    dest.mkdir(parents=True, exist_ok=True)
    for item in src.rglob("*"):
        rel = item.relative_to(src)
        if "target" in rel.parts or "logs" in rel.parts:
            continue
        if item.is_dir():
            (dest / rel).mkdir(parents=True, exist_ok=True)
        else:
            shutil.copy2(item, dest / rel)
    return dest


def run_dbt(workdir: Path, db_path: str, command: str = "build", target: str = "duckdb") -> dict:
    try:
        from dbt.cli.main import dbtRunner
    except ImportError as exc:  # pragma: no cover
        raise PermanentError("dbt is not installed: pip install dbt-duckdb") from exc
    proj = project_dir(workdir)
    os.environ["IPIPELINECO_DB"] = db_path
    args = [
        command,
        "--project-dir", str(proj),
        "--profiles-dir", str(proj),
        "--target", target,
        "--target-path", str(proj / "target"),
        "--log-path", str(proj / "logs"),
        "--quiet",
    ]
    res = dbtRunner().invoke(args)
    summary = {"models": {"pass": 0, "fail": 0}, "tests": {"pass": 0, "fail": 0, "warn": 0}, "nodes": [], "success": bool(res.success)}
    if res.exception and not res.result:
        raise PermanentError(f"dbt could not run: {res.exception}")
    for r in (res.result.results if res.result else []):
        node = r.node
        kind = node.resource_type.value
        status = str(r.status.value if hasattr(r.status, "value") else r.status)
        summary["nodes"].append({"name": node.name, "kind": kind, "status": status})
        bucket = "models" if kind == "model" else "tests" if kind == "test" else None
        if bucket is None:
            continue
        if status in ("success", "pass"):
            summary[bucket]["pass"] += 1
        elif status == "warn" and bucket == "tests":
            summary[bucket]["warn"] += 1
        else:
            summary[bucket]["fail"] += 1
    summary["manifest"] = str(proj / "target" / "manifest.json")
    return summary
