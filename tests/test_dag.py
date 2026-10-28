"""Parses the Airflow DAG and checks it mirrors the task graph. Runs where apache-airflow is installed (CI)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

from pipelineco.config import load_config  # noqa: E402
from pipelineco.pipeline import build_tasks  # noqa: E402


def test_dag_imports_and_matches_task_graph(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINECO_WORKDIR", str(tmp_path))
    bag = DagBag(dag_folder=str(Path(__file__).parent.parent / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["pipelineco"]
    expected = {t.name: set(t.deps) for t in build_tasks(load_config(None, tmp_path))}
    actual = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
    assert actual == expected
    assert dag.default_args["retry_exponential_backoff"] is True
    assert dag.get_task("publish_report").trigger_rule == "all_done"
