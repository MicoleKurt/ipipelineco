"""A small task-graph runner with the reliability features the Airflow DAG also uses:
dependencies, retries with exponential backoff, per-task and pipeline SLAs, upstream-failure
skipping, and failure alerts. `pipeline.py` defines the tasks once; this module runs them
locally, and `dags/pipelineco_dag.py` maps the same tasks onto Airflow operators.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .errors import PermanentError


@dataclass
class Task:
    name: str
    fn: Callable[[Any], Any]
    deps: list[str] = field(default_factory=list)
    retries: int | None = None  # extra attempts after the first; None = pipeline default
    sla_s: float | None = None
    trigger_rule: str = "all_success"  # all_success | all_done


@dataclass
class TaskResult:
    task: str
    status: str  # success | failed | skipped
    attempts: int = 0
    duration_s: float = 0.0
    error: str | None = None
    sla_breached: bool = False
    started_at: float = 0.0


def topo_order(tasks: list[Task]) -> list[Task]:
    by_name = {t.name: t for t in tasks}
    for t in tasks:
        for d in t.deps:
            if d not in by_name:
                raise ValueError(f"task {t.name!r} depends on unknown task {d!r}")
    order: list[Task] = []
    done: set[str] = set()
    remaining = list(tasks)
    while remaining:
        ready = [t for t in remaining if all(d in done for d in t.deps)]
        if not ready:
            raise ValueError("dependency cycle in task graph")
        for t in ready:
            order.append(t)
            done.add(t.name)
            remaining.remove(t)
    return order


def run_tasks(
    tasks: list[Task],
    ctx: Any,
    attempts: int = 3,
    delay_s: float = 0.2,
    backoff: float = 2.0,
    default_sla_s: float | None = None,
    alert: Callable[[str, str], None] = lambda level, msg: None,
    emit: Callable[[dict], None] = lambda ev: None,
    record: Callable[[TaskResult], None] = lambda r: None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[TaskResult]:
    results: dict[str, TaskResult] = {}
    for task in topo_order(tasks):
        up = [results[d] for d in task.deps]
        if task.trigger_rule == "all_success" and any(r.status != "success" for r in up):
            res = TaskResult(task.name, "skipped", error="upstream failed")
            results[task.name] = res
            emit({"type": "skipped", "task": task.name})
            record(res)
            continue
        max_attempts = (task.retries + 1) if task.retries is not None else attempts
        started = clock()
        res = TaskResult(task.name, "failed", started_at=time.time())
        emit({"type": "start", "task": task.name})
        for attempt in range(1, max_attempts + 1):
            res.attempts = attempt
            try:
                task.fn(ctx)
                res.status, res.error = "success", None
                break
            except PermanentError as exc:
                res.error = f"{type(exc).__name__}: {exc}"
                emit({"type": "permanent", "task": task.name, "error": res.error})
                break
            except Exception as exc:  # noqa: BLE001 - any other failure is retried
                res.error = f"{type(exc).__name__}: {exc}"
                if attempt < max_attempts:
                    wait = delay_s * (backoff ** (attempt - 1))
                    emit({"type": "retry", "task": task.name, "attempt": attempt, "error": res.error, "wait": wait})
                    sleep(wait)
        res.duration_s = clock() - started
        sla = task.sla_s if task.sla_s is not None else default_sla_s
        if sla is not None and res.duration_s > sla:
            res.sla_breached = True
            alert("warning", f"SLA breached: {task.name} took {res.duration_s:.1f}s (limit {sla:.0f}s)")
        if res.status == "failed":
            alert("error", f"{task.name} failed after {res.attempts} attempt(s): {res.error}")
        emit({"type": res.status, "task": task.name, "attempts": res.attempts, "duration": res.duration_s})
        results[task.name] = res
        record(res)
    return list(results.values())
