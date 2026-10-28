import pytest

from pipelineco.errors import PermanentError
from pipelineco.orchestrator import Task, run_tasks, topo_order


def runner(tasks, **kw):
    alerts, events = [], []
    res = run_tasks(
        tasks, None, sleep=lambda s: events.append(("sleep", s)), alert=lambda lvl, m: alerts.append((lvl, m)), emit=events.append, **kw
    )
    return {r.task: r for r in res}, alerts, events


def test_retries_with_backoff_then_succeeds():
    calls = {"n": 0}

    def flaky(_):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("503")

    res, alerts, events = runner([Task("a", flaky)], attempts=3, delay_s=1.0, backoff=2.0)
    assert res["a"].status == "success" and res["a"].attempts == 3
    assert [e[1] for e in events if isinstance(e, tuple)] == [1.0, 2.0]
    assert alerts == []


def test_permanent_error_is_not_retried_and_downstream_skips():
    def bad(_):
        raise PermanentError("contract broken")

    seen = []
    tasks = [
        Task("a", bad),
        Task("b", lambda _: seen.append("b"), deps=["a"]),
        Task("c", lambda _: seen.append("c"), deps=["b"], trigger_rule="all_done"),
        Task("d", lambda _: seen.append("d"), deps=["a"], trigger_rule="all_done"),
    ]
    res, alerts, _ = runner(tasks, attempts=5)
    assert res["a"].status == "failed" and res["a"].attempts == 1
    assert res["b"].status == "skipped" and "b" not in seen
    assert res["d"].status == "success" and res["c"].status == "success"
    assert any(lvl == "error" and "a failed" in m for lvl, m in alerts)


def test_exhausted_retries_fail_and_alert():
    res, alerts, _ = runner([Task("a", lambda _: 1 / 0)], attempts=2)
    assert res["a"].status == "failed" and res["a"].attempts == 2
    assert "ZeroDivisionError" in res["a"].error


def test_sla_breach_alerts():
    t = iter([0.0, 10.0])
    res, alerts, _ = runner([Task("a", lambda _: None, sla_s=5)], clock=lambda: next(t))
    assert res["a"].sla_breached and any("SLA breached" in m for _, m in alerts)


def test_topology_errors():
    with pytest.raises(ValueError):
        topo_order([Task("a", print, deps=["b"]), Task("b", print, deps=["a"])])
    with pytest.raises(ValueError):
        topo_order([Task("a", print, deps=["zzz"])])
