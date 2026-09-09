from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import dpone_airflow_pack.dag_materializer as dag_materializer
import pytest
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint
from dpone_airflow_pack.deployment_index import AirflowDeploymentIndexError

from tests.airflow_dag_loader_test_support import (
    install_fake_airflow as _install_fake_airflow,
)
from tests.airflow_dag_loader_test_support import (
    legacy_dag_spec_path as _legacy_dag_spec_path,
)
from tests.airflow_dag_loader_test_support import (
    legacy_dag_spec_payload as _legacy_dag_spec_payload,
)
from tests.airflow_dag_loader_test_support import minimal_pack as _minimal_pack

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64


def test_legacy_loader_reports_invalid_json_and_schema_without_creating_diagnostics(tmp_path: Path) -> None:
    _legacy_dag_spec_path(tmp_path, "invalid_json").write_text("{not-json", encoding="utf-8")
    invalid_schema = _legacy_dag_spec_payload("invalid_schema")
    invalid_schema["nodes"] = []
    invalid_schema["topological_order"] = []
    invalid_schema["spec_fingerprint"] = compute_dag_spec_fingerprint(invalid_schema)
    _legacy_dag_spec_path(tmp_path, "invalid_schema").write_text(
        json.dumps(invalid_schema),
        encoding="utf-8",
    )
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, tmp_path)

    assert report.loaded == ()
    assert report.skipped == ()
    assert globals_dict == {}
    errors_by_dag = {error["dag_id"]: error for error in report.errors}
    assert errors_by_dag["invalid_json"]["code"] == "DPONE_AIRFLOW_DAG_SPEC_JSON_INVALID"
    assert errors_by_dag["invalid_schema"]["code"] == "DPONE_AIRFLOW_DAG_SPEC_NODES_MISSING"
    for error in report.errors:
        assert error["schema"] == "dpone.error.v1"
        assert error["stage"] == "airflow_parse"
        assert error["severity"] == "error"


def test_legacy_loader_fail_all_raises_and_leaves_namespace_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _legacy_dag_spec_path(tmp_path, "a_valid").write_text(
        json.dumps(_legacy_dag_spec_payload("a_valid")),
        encoding="utf-8",
    )
    _legacy_dag_spec_path(tmp_path, "z_invalid").write_text("{not-json", encoding="utf-8")
    staged_dag = object()

    def materialize(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return staged_dag

    monkeypatch.setattr("dpone_airflow_pack.dag_loader._materialize_dag_spec", materialize)
    sentinel = object()
    globals_dict: dict[str, object] = {"sentinel": sentinel}

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags(globals_dict, tmp_path, invalid_dag_policy="fail_all")

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_JSON_INVALID"
    assert globals_dict == {"sentinel": sentinel}
    assert globals_dict["sentinel"] is sentinel


def test_legacy_loader_retries_diagnostic_and_loads_repaired_dag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    spec_path = _legacy_dag_spec_path(tmp_path, "repairable")
    spec_path.write_text("{not-json", encoding="utf-8")
    globals_dict: dict[str, object] = {}

    first_report = load_dpone_dags(
        globals_dict,
        tmp_path,
        invalid_dag_policy="create_diagnostic_dag",
    )
    diagnostic = globals_dict["repairable"]
    retry_report = load_dpone_dags(
        globals_dict,
        tmp_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert first_report.errors
    assert retry_report.errors
    assert globals_dict["repairable"] is diagnostic
    assert getattr(diagnostic, "_dpone_diagnostic_error") is True

    repaired_dag = object()

    def materialize(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return repaired_dag

    monkeypatch.setattr("dpone_airflow_pack.dag_loader._materialize_dag_spec", materialize)
    spec_path.write_text(
        json.dumps(_legacy_dag_spec_payload("repairable")),
        encoding="utf-8",
    )

    repaired_report = load_dpone_dags(
        globals_dict,
        tmp_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert repaired_report.loaded == ("repairable",)
    assert repaired_report.errors == ()
    assert globals_dict["repairable"] is repaired_dag


def test_load_dpone_dags_materializes_dag_and_wires_edges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_airflow(monkeypatch)
    repo = tmp_path
    pack_dir = repo / ".dpone/gitops/airflow"
    (pack_dir / "app").mkdir(parents=True)
    (pack_dir / "web").mkdir(parents=True)
    (pack_dir / "app/airflow-pack.json").write_text(
        json.dumps(_minimal_pack(task_id="app__dpone_runtime")), encoding="utf-8"
    )
    (pack_dir / "web/airflow-pack.json").write_text(
        json.dumps(_minimal_pack(task_id="web__dpone_runtime")), encoding="utf-8"
    )
    payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "DAG__demo",
        "domain": "marketing",
        "description": "demo",
        "schedule": None,
        "start_date": "2026-07-07",
        "catchup": False,
        "tags": ["dpone"],
        "default_args": {"retries": 0},
        "operator_overrides": {},
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "pack_ref": "cached://app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            },
            {
                "node_id": "web",
                "workload_id": "web",
                "pack_ref": "cached://web",
                "pack_path": ".dpone/gitops/airflow/web/airflow-pack.json",
            },
        ],
        "edges": [{"upstream": "app", "downstream": "web", "reason": "curated", "origin": "wiring.dependencies"}],
        "topological_order": ["app", "web"],
        "warnings": [],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    spec_dir = pack_dir / "_dags"
    spec_dir.mkdir()
    (spec_dir / "DAG__demo.dag-spec.json").write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(
        "dpone_airflow_pack.pack_wiring.load_dpone_airflow_pack_with_provenance",
        lambda ref: (
            json.loads(
                (repo / ".dpone/gitops/airflow" / str(ref).removeprefix("cached://") / "airflow-pack.json").read_text()
            ),
            {"source": "local_path", "path": str(ref)},
        ),
    )
    monkeypatch.setattr(
        "dpone_airflow_pack.pack_tasks.load_dpone_airflow_pack",
        lambda ref: json.loads(
            (repo / ".dpone/gitops/airflow" / str(ref).removeprefix("cached://") / "airflow-pack.json").read_text()
        ),
    )

    globals_dict: dict[str, object] = {}
    report = load_dpone_dags(globals_dict, repo)

    assert report.errors == ()
    assert "DAG__demo" in globals_dict
    assert "DAG__demo" not in globals_dict or globals_dict["DAG__demo"].kwargs["dag_id"] == "DAG__demo"


def test_materializer_fails_closed_when_validated_edge_cannot_be_wired(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    monkeypatch.setattr(dag_materializer, "_wire_nodes", lambda *_args, **_kwargs: {})
    spec = {
        "dag_id": "DAG__demo",
        "schedule": None,
        "start_date": "2026-07-07",
        "catchup": False,
        "nodes": [
            {"node_id": "app", "workload_id": "app"},
            {"node_id": "web", "workload_id": "web"},
        ],
        "edges": [{"upstream": "app", "downstream": "web"}],
    }

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        dag_materializer._materialize_dag_spec(
            spec,
            repo_root=tmp_path,
            operator_overrides={},
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_EDGE_WIRING_FAILED"


def test_materializer_serializes_exact_activation_identity_in_dag_tags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    monkeypatch.setattr(dag_materializer, "_wire_nodes", lambda *_args, **_kwargs: {})
    activation_id = "12345678-1234-4234-9234-123456789abc"
    release_id = "sha256:" + ("a" * 64)
    deployment_id = "sha256:" + ("b" * 64)

    dag = dag_materializer._materialize_dag_spec(
        {
            "dag_id": "DAG__demo",
            "schedule": None,
            "start_date": "2026-07-07",
            "catchup": False,
            "tags": [
                "dpone",
                "dpone_activation:untrusted",
                "dpone_release:deadbeef",
                "dpone_deployment:cafebabe",
                "demo",
            ],
            "nodes": [],
            "edges": [],
        },
        repo_root=tmp_path,
        operator_overrides={},
        run_identity_context={
            "_activation_id": activation_id,
            "release_id": release_id,
            "deployment_id": deployment_id,
        },
    )

    assert dag.kwargs["tags"] == [
        "dpone",
        "demo",
        f"dpone_release:{'a' * 64}",
        f"dpone_deployment:{'b' * 64}",
        f"dpone_activation:{activation_id}",
    ]
    assert dag._dpone_run_identity_context["_activation_id"] == activation_id


def test_load_dpone_dags_materializes_cron_timezone_without_dag_timezone_kwarg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    timetables = types.ModuleType("airflow.timetables")
    trigger = types.ModuleType("airflow.timetables.trigger")

    class CronTriggerTimetable:
        def __init__(self, cron: str, *, timezone: object) -> None:
            self.cron = cron
            self.timezone = timezone

    trigger.CronTriggerTimetable = CronTriggerTimetable
    monkeypatch.setitem(sys.modules, "airflow.timetables", timetables)
    monkeypatch.setitem(sys.modules, "airflow.timetables.trigger", trigger)

    repo = tmp_path
    pack_dir = repo / ".dpone/gitops/airflow/app"
    pack_dir.mkdir(parents=True)
    (pack_dir / "airflow-pack.json").write_text(
        json.dumps(_minimal_pack(task_id="app__dpone_runtime")),
        encoding="utf-8",
    )
    payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "DAG__demo_tz",
        "schedule": "0 7 * * *",
        "timezone": "Europe/Moscow",
        "start_date": "2026-06-25",
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            }
        ],
        "edges": [],
        "topological_order": ["app"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    spec_dir = repo / ".dpone/gitops/airflow/_dags"
    spec_dir.mkdir(parents=True)
    (spec_dir / "DAG__demo_tz.dag-spec.json").write_text(json.dumps(payload), encoding="utf-8")

    pack_payload = json.loads((pack_dir / "airflow-pack.json").read_text())
    monkeypatch.setattr(
        "dpone_airflow_pack.pack_provenance.load_dpone_airflow_pack_with_provenance",
        lambda ref: (pack_payload, {"source": "local_path", "path": str(ref)}),
    )

    globals_dict: dict[str, object] = {}
    load_dpone_dags(globals_dict, repo)

    dag = globals_dict["DAG__demo_tz"]
    assert "dpone_spec_error" not in dag.kwargs.get("tags", [])
    assert "timezone" not in dag.kwargs
    assert dag.kwargs["schedule"].cron == "0 7 * * *"


def test_load_dpone_dags_skips_existing_dag_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_airflow(monkeypatch)
    payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "DAG__existing",
        "schedule": None,
        "start_date": "2026-07-07",
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "pack_ref": "cached://app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            }
        ],
        "edges": [],
        "topological_order": ["app"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    spec_dir = tmp_path / ".dpone/gitops/airflow/_dags"
    spec_dir.mkdir(parents=True)
    (spec_dir / "DAG__existing.dag-spec.json").write_text(json.dumps(payload), encoding="utf-8")

    sentinel = object()
    globals_dict = {"DAG__existing": sentinel}
    load_dpone_dags(globals_dict, tmp_path)
    assert globals_dict["DAG__existing"] is sentinel
