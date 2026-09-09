from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagProjectionIdentity,
)
from dpone_airflow_pack.semantic_refresh_index_artifacts import (
    SemanticRefreshDagProjectionArtifact,
)

from dpone.app.semantic_refresh_airflow_composition import (
    SemanticRefreshAirflowRuntime,
    build_semantic_refresh_airflow_callables,
    load_verified_semantic_refresh_airflow_dags,
    materialize_verified_semantic_refresh_dag,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


@dataclass(frozen=True)
class _Record:
    operation_id: str = _digest("1")
    operation_plan_sha256: str = _digest("2")
    workflow_execution_binding_sha256: str = _digest("3")
    attempt_binding_sha256: str = _digest("4")
    status: str = "COMPLETE"
    artifact_manifest_sha256: str = _digest("5")
    clickhouse_terminal_receipt_sha256: str = _digest("6")
    terminal_receipt_sha256: str = _digest("7")
    target_generation: int = 2
    scope_revision: int = 1


class _Reader:
    def read(self, **_: object) -> tuple[_Record, ...]:
        return (_Record(),)


class _Summary:
    def persist(self, summary: Any) -> Any:
        return summary


class _Models:
    def prepare(self, **_: object) -> str:
        return "prepared"

    def commit(self, **_: object) -> str:
        return "complete"


class _Publication:
    models = _Models()


class _Worker:
    def dbt_build_test(self, **_: object) -> None:
        return None

    def resolve_execution_binding(self, **_: object) -> str:
        return _digest("3")


def _runtime() -> SemanticRefreshAirflowRuntime:
    return SemanticRefreshAirflowRuntime(
        publication=_Publication(),  # type: ignore[arg-type]
        workflow_publications=_Reader(),  # type: ignore[arg-type]
        workflow_summary=_Summary(),  # type: ignore[arg-type]
    )


def test_airflow_runtime_maps_full_canonical_publication() -> None:
    publications = _runtime().read_publications(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("3"),
        topology_sha256=_digest("8"),
        expected_operation_ids=(_digest("1"),),
    )

    assert publications[0].to_mapping() == {
        "operation_id": _digest("1"),
        "operation_plan_sha256": _digest("2"),
        "workflow_execution_binding_sha256": _digest("3"),
        "attempt_binding_sha256": _digest("4"),
        "artifact_manifest_sha256": _digest("5"),
        "clickhouse_terminal_receipt_sha256": _digest("6"),
        "terminal_receipt_sha256": _digest("7"),
        "target_generation": 2,
        "scope_revision": 1,
        "status": "COMPLETE",
    }


def test_airflow_runtime_rejects_free_topology_claim() -> None:
    with pytest.raises(ValueError, match="topology_sha256"):
        _runtime().read_publications(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=_digest("3"),
            topology_sha256="free-text",
            expected_operation_ids=(_digest("1"),),
        )


def test_airflow_callable_factory_is_parse_inert() -> None:
    callables = build_semantic_refresh_airflow_callables(
        runtime=_runtime(),
        worker=_Worker(),
    )

    assert callables.prepare(operation_id=_digest("1"), workflow_execution_binding_sha256=_digest("3")) == ("prepared")


def test_production_materializer_uses_verified_index_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    observed: dict[str, object] = {}

    def _materialize(**kwargs: object) -> str:
        observed.update(kwargs)
        return "dag"

    monkeypatch.setattr(
        "dpone_airflow_pack.semantic_refresh_airflow.materialize_semantic_refresh_dag",
        _materialize,
    )
    callables = build_semantic_refresh_airflow_callables(
        runtime=_runtime(),
        worker=_Worker(),
    )

    identity = SemanticRefreshDagProjectionIdentity(
        dag_projection_sha256=_digest("9"),
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        topology_sha256=_digest("c"),
        template_pack_fingerprint=_digest("d"),
        plan_bundle_sha256=_digest("e"),
        workflow_plan_sha256=_digest("c"),
        pre_release_bundle_sha256=_digest("f"),
        package_artifacts_sha256=_digest("f"),
    )
    authority = LocalSemanticRefreshDagProjectionAuthority.build(identity)
    artifact = SemanticRefreshDagProjectionArtifact(
        projection_id="semantic_refresh_v2::daily_events",
        workflow_name="daily_events",
        dag_id="daily_events",
        dag_projection_sha256=identity.dag_projection_sha256,
        artifact_ref="cache://deployments/prod/sidecar.json",
        artifact_sha256=_digest("1"),
        artifact_bytes=1,
        path=tmp_path / "sidecar.json",
        cache_root=tmp_path,
        authority=authority,
    )
    projection = type(
        "Projection",
        (),
        {"to_mapping": lambda self: {"dag_projection_sha256": _digest("9")}},
    )()
    monkeypatch.setattr(
        "dpone_airflow_pack.semantic_refresh_index_artifacts.load_semantic_refresh_dag_projection_artifact",
        lambda *_args, **_kwargs: projection,
    )
    result = materialize_verified_semantic_refresh_dag(
        artifact=artifact,
        callables=callables,
    )

    assert result == "dag"
    assert observed["dag_projection_authority"] is authority
    assert set(observed) == {
        "callables",
        "dag_projection",
        "dag_projection_authority",
    }


def test_production_loader_injects_correlated_worker_into_strict_index_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    observed: dict[str, object] = {}

    def _load(namespace, **kwargs: object) -> str:
        observed.update(kwargs)
        observed["namespace"] = namespace
        return "loaded"

    monkeypatch.setattr("dpone_airflow_pack.provider.load_and_acknowledge_dpone_dags", _load)
    namespace: dict[str, object] = {}
    ack_path = tmp_path / "ack/loader-ack.json"
    ack_root = tmp_path / "ack"

    result = load_verified_semantic_refresh_airflow_dags(
        namespace,
        index_path=tmp_path / "current/airflow-index.json",
        ack_path=ack_path,
        ack_root=ack_root,
        runtime=_runtime(),
        worker=_Worker(),
    )

    assert result == "loaded"
    assert observed["namespace"] is namespace
    assert observed["duplicate_policy"] == "fail_all"
    assert observed["invalid_dag_policy"] == "fail_all"
    assert observed["ack_path"] == ack_path
    assert observed["ack_root"] == ack_root
    callables = observed["semantic_refresh_callables"]
    assert getattr(getattr(callables, "dbt_build_test"), "__self__").__class__ is _Worker
    assert getattr(getattr(callables, "resolve_execution_binding"), "__self__").__class__ is _Worker
