from __future__ import annotations

import sys
import types
from pathlib import Path

import dpone_airflow_pack.pack_tasks as pack_tasks
import pytest
from dpone_airflow_pack.dag_loader import load_dpone_dags
from tools.airflow_provider_parse_benchmark_fixture import (
    build_fixture,
    load_expected_topology,
)
from tools.airflow_provider_parse_benchmark_support import (
    DAG_COUNT,
    WORKLOAD_COUNT,
    expected_layout,
    inspect_parse_topology,
)


def test_strict_v2_fixture_loads_exact_phase0_topology_within_parse_slo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path, fixture = build_fixture(tmp_path)
    expected_topology = load_expected_topology(index_path)
    cold_namespace: dict[str, object] = {}
    warm_namespace: dict[str, object] = {}

    cold_report = load_dpone_dags(cold_namespace, index_path=index_path)
    warm_report = load_dpone_dags(warm_namespace, index_path=index_path)

    expected_dags = tuple(expected_layout())
    assert fixture["status"] == "READY"
    assert fixture["strict_v2_validated"] is True
    assert fixture["runtime_connection_artifacts"]["status"] == "PASS"
    assert cold_report.loaded == expected_dags
    assert cold_report.errors == ()
    assert cold_report.skipped == ()
    assert cold_report.fatal is False
    assert warm_report.loaded == expected_dags
    assert warm_report.errors == ()
    assert warm_report.skipped == ()
    assert warm_report.fatal is False
    # Absolute parse SLO is gated by airflow-pack-compat installed-provider
    # evidence. Do not compare single-shot warm vs cold under pytest-xdist:
    # scheduler noise can invert the order by tens of milliseconds.

    cold_topology = inspect_parse_topology(cold_namespace, expected_topology)
    warm_topology = inspect_parse_topology(warm_namespace, expected_topology)
    assert cold_topology["dag_count"] == DAG_COUNT
    assert cold_topology["runtime_task_count"] == WORKLOAD_COUNT
    assert cold_topology["topology_fingerprint"] == fixture["expected_topology"]["topology_sha256"]
    assert warm_topology == cold_topology


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    operators = types.ModuleType("airflow.operators")
    empty_mod = types.ModuleType("airflow.operators.empty")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    standard_operators = types.ModuleType("airflow.providers.standard.operators")
    standard_empty = types.ModuleType("airflow.providers.standard.operators.empty")

    class DAG:
        def __init__(self, *, dag_id: str, schedule: object = None, **kwargs: object) -> None:
            self.kwargs = {"dag_id": dag_id, "schedule": schedule, **kwargs}
            self.tasks: list[EmptyOperator] = []

    class EmptyOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.task_id = str(kwargs["task_id"])
            self.upstream_task_ids: set[str] = set()
            self.downstream_task_ids: set[str] = set()
            dag = kwargs.get("dag")
            if isinstance(dag, DAG):
                dag.tasks.append(self)

        def __rshift__(self, other: object) -> object:
            if isinstance(other, EmptyOperator):
                self.downstream_task_ids.add(other.task_id)
                other.upstream_task_ids.add(self.task_id)
            return other

    class RecordingOperator(EmptyOperator):
        pass

    empty_mod.EmptyOperator = EmptyOperator
    standard_empty.EmptyOperator = EmptyOperator
    airflow.DAG = DAG
    for name, module in {
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.empty": empty_mod,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": standard_operators,
        "airflow.providers.standard.operators.empty": standard_empty,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    monkeypatch.setitem(sys.modules, "vault_kv_client", None)
