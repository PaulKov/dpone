"""Ordinary pack-exec plan reopen and typed dispatch; not live route certification."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.app.composition_clickhouse_execution import CompositionClickHouseResult
from dpone.app.composition_execution_cells import (
    MSSQL_CLICKHOUSE_FULL_REFRESH_V1,
    POSTGRES_MSSQL_FULL_REFRESH_V1,
)
from dpone.app.composition_pack_execution_dispatcher import (
    CACHE_ROOT_ENV,
    CompositionPackExecutionDispatcher,
    cache_root_from_environment,
    ordinary_cell,
    reopen_composition_plan,
    transfer_execution_request,
    verify_transfer_pack_operation,
)
from dpone.app.composition_transfer_execution import (
    CompositionTransferExecutionRequest,
    CompositionTransferResult,
)
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_execution_authority import CompositionSupervisorProjection
from dpone.contracts.dbt_runtime import (
    AIRFLOW_RUN_IDENTITY_ENV,
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    TRY_NUMBER_ENV,
)
from dpone.runtime.composition_native_dbt_dispatch import ORDINARY_WORKER_UNAVAILABLE
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionDispatchRequest,
    CompositionRunVolume,
)

_SUPERVISOR = CompositionSupervisorProjection.from_mapping(
    {
        "schema": "dpone.composition-supervisor.v1",
        "persistent_volume_claim": "dpone-composition-supervisor",
        "child_uid_start": 1_000_000_000,
        "child_gid_start": 1_000_000_000,
        "child_identity_count": 1_000_000,
    }
)
_RELEASE = "sha256:" + "a" * 64
_DEPLOYMENT = "sha256:" + "b" * 64
_PACK = "sha256:" + "d" * 64
_ORDINARY_MANIFEST = {
    "name": "b_ordinary",
    "source": {
        "type": "postgres",
        "connection_ref": "pg-source",
        "table": {"database": "sales", "schema": "public", "name": "orders"},
    },
    "sink": {
        "type": "mssql",
        "connection_ref": "mssql-sink",
        "table": {"database": "warehouse", "schema": "dbo", "name": "orders"},
        "strategy": {"mode": "full_refresh"},
    },
    "state": {
        "type": "mssql",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "connection_ref": "mssql-state",
    },
}


def _run_identity_json() -> str:
    return AirflowRunIdentity(
        _RELEASE,
        _DEPLOYMENT,
        AirflowArtifactIdentity("b_ordinary", _PACK),
    ).to_json()


def _dispatch(tmp_path: Path) -> CompositionDispatchRequest:
    worktree = tmp_path / "worktree"
    worktree.mkdir(exist_ok=True)
    (worktree / "manifest.json").write_text(json.dumps(_ORDINARY_MANIFEST), encoding="utf-8")
    run = tmp_path / "run"
    run.mkdir(exist_ok=True)
    return CompositionDispatchRequest(
        kind="ordinary_transfer",
        argv=("dpone", "run", "manifest.json", "--format", "json"),
        verified_input="manifest.json",
        process_selector=None,
        working_directory=worktree,
        supervisor=_SUPERVISOR,
        run_volume=CompositionRunVolume(run / "runtime-evidence.json", run / "stderr.log"),
        env={
            AIRFLOW_RUN_IDENTITY_ENV: _run_identity_json(),
            DAG_ID_ENV: "b_ordinary",
            DAG_RUN_ID_ENV: "scheduled__2026-09-12T00:00:00+00:00",
            TRY_NUMBER_ENV: "1",
            CACHE_ROOT_ENV: str(tmp_path / "cache"),
        },
    )


def test_cache_root_from_environment_requires_existing_directory(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    assert cache_root_from_environment({CACHE_ROOT_ENV: str(cache)}) == cache.resolve()
    with pytest.raises(CompositionAdmissionError, match="transfer_source_plan"):
        cache_root_from_environment({})
    with pytest.raises(CompositionAdmissionError, match="transfer_source_plan"):
        cache_root_from_environment({CACHE_ROOT_ENV: str(tmp_path / "missing")})


def test_reopen_composition_plan_rejects_missing_release_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(CompositionAdmissionError, match="transfer_source_plan"):
        reopen_composition_plan(cache, _RELEASE)


def _transfer_operation_case(*, map_index: int = -1):
    from dpone.contracts.mssql_transaction_governance import InvocationIdentity
    from tests.test_mssql_composition_transaction_fence import binding

    bound = binding()
    attempt = replace(bound.attempt, map_index=map_index)
    task = attempt.task_id + (f"[{map_index}]" if map_index >= 0 else "")
    invocation = InvocationIdentity(attempt.dag_run_id, bound.write.workflow_id, f"{bound.write.workflow_id}:{task}")
    request = replace(bound.operation.attempt.request, invocation=invocation)
    operation = replace(bound.operation, attempt=replace(bound.operation.attempt, request=request))
    plan = SimpleNamespace(writes=(bound.write,), sources=SimpleNamespace(subject_sha256=attempt.plan_sha256))
    return attempt, operation, bound.write, bound.mutation_plan_sha256, plan


def test_verify_operation_requires_write_in_plan() -> None:
    attempt, operation, write, mutation, plan = _transfer_operation_case()
    verify_transfer_pack_operation(attempt, operation, write, mutation, plan)
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        verify_transfer_pack_operation(attempt, operation, write, mutation, SimpleNamespace(writes=()))
    foreign = replace(
        operation, attempt=replace(operation.attempt, request=replace(operation.attempt.request, target_schema="other"))
    )
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        verify_transfer_pack_operation(attempt, foreign, write, mutation, plan)


@pytest.mark.parametrize("field", ["run_id", "process", "task_partition"])
def test_verify_operation_rejects_foreign_invocation(field: str) -> None:
    attempt, operation, write, mutation, plan = _transfer_operation_case()
    request = operation.attempt.request
    foreign = replace(
        operation,
        attempt=replace(
            operation.attempt, request=replace(request, invocation=replace(request.invocation, **{field: "foreign"}))
        ),
    )
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        verify_transfer_pack_operation(attempt, foreign, write, mutation, plan)


def test_verify_operation_rejects_other_source_plan() -> None:
    attempt, operation, write, mutation, plan = _transfer_operation_case()
    plan.sources.subject_sha256 = "sha256:" + "e" * 64
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        verify_transfer_pack_operation(attempt, operation, write, mutation, plan)


@pytest.mark.parametrize("map_index", [-1, 0, 2])
def test_verify_operation_preserves_mapped_task_identity(map_index: int) -> None:
    attempt, operation, write, mutation, plan = _transfer_operation_case(map_index=map_index)
    verify_transfer_pack_operation(attempt, operation, write, mutation, plan)
    foreign = replace(attempt, map_index=map_index + 1)
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        verify_transfer_pack_operation(foreign, operation, write, mutation, plan)


def test_transfer_request_binds_run_identity_and_load_config(tmp_path: Path) -> None:
    request = transfer_execution_request(_dispatch(tmp_path), _ORDINARY_MANIFEST, plan_sha256=_RELEASE)

    assert type(request) is CompositionTransferExecutionRequest
    assert request.plan_sha256 == _RELEASE
    assert request.selector == "b_ordinary"
    assert request.run_identity.workload_pack.id == "b_ordinary"
    assert request.airflow_attempt.dag_id == "b_ordinary"
    assert request.airflow_attempt.task_id == "b_ordinary__dpone_runtime"
    assert request.load_config.source_table == "orders"
    assert request.load_config.target_table == "orders"
    assert ordinary_cell(_ORDINARY_MANIFEST) == POSTGRES_MSSQL_FULL_REFRESH_V1


def test_run_ordinary_executes_typed_transfer_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []

    class Root:
        def execute(self, request: object) -> CompositionTransferResult:
            seen.append(request)
            return CompositionTransferResult(rows_written=3)

    monkeypatch.setattr(
        "dpone.app.composition_pack_execution_dispatcher.reopen_composition_plan",
        lambda *_args, **_kwargs: SimpleNamespace(sources=SimpleNamespace(subject_sha256=_RELEASE)),
    )
    (tmp_path / "cache").mkdir()
    request = _dispatch(tmp_path)
    dispatcher = CompositionPackExecutionDispatcher(
        supervisor=_SUPERVISOR,
        capabilities=SimpleNamespace(factory=lambda _cell: object),
        native_executor=None,
        ordinary_root=lambda *_args, **_kwargs: Root(),
    )

    assert dispatcher.run(request) == 0
    assert len(seen) == 1
    assert type(seen[0]) is CompositionTransferExecutionRequest
    assert seen[0].plan_sha256 == _RELEASE
    payload = json.loads(request.run_volume.evidence_path.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["cell"] == POSTGRES_MSSQL_FULL_REFRESH_V1
    assert payload["rows_written"] == 3


def test_run_ordinary_refuses_root_that_cannot_prove_outcome(tmp_path: Path) -> None:
    class Root:
        def can_execute_attempt(self) -> bool:
            return False

        def execute(self, request: object) -> CompositionTransferResult:
            raise AssertionError(f"must not execute {request}")

    dispatcher = CompositionPackExecutionDispatcher(
        supervisor=_SUPERVISOR,
        capabilities=SimpleNamespace(factory=lambda _cell: object),
        native_executor=None,
        ordinary_root=lambda *_args, **_kwargs: Root(),
    )
    with pytest.raises(CompositionDispatchRejection, match=ORDINARY_WORKER_UNAVAILABLE):
        dispatcher.run(_dispatch(tmp_path))


def test_run_ordinary_missing_root_is_unavailable(tmp_path: Path) -> None:
    dispatcher = CompositionPackExecutionDispatcher(
        supervisor=_SUPERVISOR,
        capabilities=SimpleNamespace(factory=lambda _cell: object),
        native_executor=None,
        ordinary_root=lambda *_args, **_kwargs: None,
    )
    with pytest.raises(CompositionDispatchRejection, match=ORDINARY_WORKER_UNAVAILABLE):
        dispatcher.run(_dispatch(tmp_path))


_CH_MANIFEST = {
    "name": "a_native",
    "source": {
        "type": "mssql",
        "connection_ref": "mssql-source",
        "table": {"database": "sales", "schema": "dbo", "name": "orders"},
    },
    "sink": {
        "type": "clickhouse",
        "connection_ref": "ch-sink",
        "table": {"database": "analytics", "schema": "default", "name": "orders"},
        "mode": "replace",
        "strategy": {"mode": "full_refresh", "max_source_bytes": 1_000_000},
    },
    "runtime": {},
    "quality": {},
    "gitops": {},
}


def test_run_ordinary_executes_typed_clickhouse_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRequest

    seen: list[object] = []

    class Root:
        def execute(self, request: object) -> CompositionClickHouseResult:
            seen.append(request)
            return CompositionClickHouseResult(rows=((1,),), publication_state="PUBLISHED")

    cache = tmp_path / "cache"
    cache.mkdir()  # Runtime capture no longer requires a release-side snapshot.
    monkeypatch.setattr(
        "dpone.app.composition_pack_execution_dispatcher.reopen_composition_plan",
        lambda *_args, **_kwargs: SimpleNamespace(sources=SimpleNamespace(subject_sha256=_RELEASE)),
    )
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "manifest.json").write_text(json.dumps(_CH_MANIFEST), encoding="utf-8")
    run = tmp_path / "run"
    run.mkdir()
    request = CompositionDispatchRequest(
        kind="ordinary_transfer",
        argv=("dpone", "run", "manifest.json", "--format", "json"),
        verified_input="manifest.json",
        process_selector=None,
        working_directory=worktree,
        supervisor=_SUPERVISOR,
        run_volume=CompositionRunVolume(run / "runtime-evidence.json", run / "stderr.log"),
        env={
            AIRFLOW_RUN_IDENTITY_ENV: AirflowRunIdentity(
                _RELEASE, _DEPLOYMENT, AirflowArtifactIdentity("a_native", _PACK)
            ).to_json(),
            DAG_ID_ENV: "a_native",
            DAG_RUN_ID_ENV: "scheduled__2026-09-12T00:00:00+00:00",
            TRY_NUMBER_ENV: "1",
            CACHE_ROOT_ENV: str(cache),
        },
    )
    dispatcher = CompositionPackExecutionDispatcher(
        supervisor=_SUPERVISOR,
        capabilities=SimpleNamespace(factory=lambda _cell: object),
        native_executor=None,
        ordinary_root=lambda *_args, **_kwargs: Root(),
    )

    assert dispatcher.run(request) == 0
    assert len(seen) == 1
    assert type(seen[0]) is CompositionClickHouseExecutionRequest
    assert seen[0].columns == ()
    assert seen[0].generation_ref == ""
    assert seen[0].generation_uuid == ""
    payload = json.loads(request.run_volume.evidence_path.read_text(encoding="utf-8"))
    assert payload["cell"] == MSSQL_CLICKHOUSE_FULL_REFRESH_V1
    assert payload["publication_state"] == "PUBLISHED"
    assert payload["status"] == "passed"


def test_run_ordinary_rejects_clickhouse_commit_unknown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.app.composition_clickhouse_execution import CompositionClickHouseResult
    from dpone.runtime.composition_native_dbt_dispatch import EVIDENCE_DISAGREEMENT
    from tests.composition_snapshot_helpers import digest

    class Root:
        def execute(self, request: object) -> CompositionClickHouseResult:
            del request
            return CompositionClickHouseResult(rows=((1,),), publication_state="COMMIT_UNKNOWN")

    monkeypatch.setattr(
        "dpone.app.composition_pack_execution_dispatcher.reopen_composition_plan",
        lambda *_args, **_kwargs: SimpleNamespace(sources=SimpleNamespace(subject_sha256=_RELEASE)),
    )
    cache = tmp_path / "cache"
    snap = cache / "releases" / _RELEASE.replace(":", "-") / "composition-snapshots"
    snap.mkdir(parents=True)
    (snap / "a_native.json").write_text(
        json.dumps(
            {
                "generation_ref": digest("generation record"),
                "generation_uuid": "10000000-0000-4000-8000-000000000013",
                "columns": [{"name": "id", "type_name": "Int32"}],
            }
        ),
        encoding="utf-8",
    )
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "manifest.json").write_text(json.dumps(_CH_MANIFEST), encoding="utf-8")
    run = tmp_path / "run"
    run.mkdir()
    request = CompositionDispatchRequest(
        kind="ordinary_transfer",
        argv=("dpone", "run", "manifest.json", "--format", "json"),
        verified_input="manifest.json",
        process_selector=None,
        working_directory=worktree,
        supervisor=_SUPERVISOR,
        run_volume=CompositionRunVolume(run / "runtime-evidence.json", run / "stderr.log"),
        env={
            AIRFLOW_RUN_IDENTITY_ENV: AirflowRunIdentity(
                _RELEASE, _DEPLOYMENT, AirflowArtifactIdentity("a_native", _PACK)
            ).to_json(),
            DAG_ID_ENV: "a_native",
            DAG_RUN_ID_ENV: "scheduled__2026-09-12T00:00:00+00:00",
            TRY_NUMBER_ENV: "1",
            CACHE_ROOT_ENV: str(cache),
        },
    )
    dispatcher = CompositionPackExecutionDispatcher(
        supervisor=_SUPERVISOR,
        capabilities=SimpleNamespace(factory=lambda _cell: object),
        native_executor=None,
        ordinary_root=lambda *_args, **_kwargs: Root(),
    )
    with pytest.raises(CompositionDispatchRejection, match=EVIDENCE_DISAGREEMENT):
        dispatcher.run(request)


def test_compose_pack_execution_root_installs_clickhouse_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.app.composition_clickhouse_execution import (
        CompositionClickHouseExecutionDependencies,
        CompositionClickHouseExecutionRoot,
    )
    from dpone.app.composition_pack_execution_dispatcher import compose_pack_execution_root
    from tests.composition_snapshot_helpers import SERVICE, target

    (tmp_path / "cache").mkdir()
    request = _dispatch(tmp_path)
    deps = CompositionClickHouseExecutionDependencies(
        read_active=lambda: None,
        attempts=object(),
        gate=object(),
        publisher_gate=object(),
        publisher=object(),
        bind_transport=lambda *_args, **_kwargs: None,
        read_source=lambda *_args, **_kwargs: (),
        require_enrollment=lambda *_args, **_kwargs: b"{}",
        outcome_observer=object(),
        target=target(),
        expected_service_id=SERVICE,
    )
    constructed: list[object] = []

    def factory(*, dependencies: object) -> CompositionClickHouseExecutionRoot:
        constructed.append(dependencies)
        return CompositionClickHouseExecutionRoot(dependencies)  # type: ignore[arg-type]

    monkeypatch.setattr(
        "dpone.app.composition_pack_execution_dispatcher.reopen_composition_plan",
        lambda *_args, **_kwargs: SimpleNamespace(sources=SimpleNamespace(subject_sha256=_RELEASE)),
    )
    monkeypatch.setattr(
        "dpone.app.composition_clickhouse_execution_factory.compose_clickhouse_pack_dependencies",
        lambda **_kwargs: deps,
    )
    root = compose_pack_execution_root(
        request=request,
        cell=MSSQL_CLICKHOUSE_FULL_REFRESH_V1,
        manifest=_CH_MANIFEST,
        capabilities=SimpleNamespace(factory=lambda _cell: factory),
        parent={"context": SimpleNamespace(release_id=_RELEASE)},
    )
    assert type(root) is CompositionClickHouseExecutionRoot
    assert constructed == [deps]


def test_compose_pack_execution_root_omits_clickhouse_without_collaborators(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dpone.app.composition_pack_execution_dispatcher import compose_pack_execution_root

    (tmp_path / "cache").mkdir()
    constructed: list[str] = []
    monkeypatch.setattr(
        "dpone.app.composition_pack_execution_dispatcher.reopen_composition_plan",
        lambda *_args, **_kwargs: SimpleNamespace(sources=SimpleNamespace(subject_sha256=_RELEASE)),
    )
    root = compose_pack_execution_root(
        request=_dispatch(tmp_path),
        cell=MSSQL_CLICKHOUSE_FULL_REFRESH_V1,
        manifest=_CH_MANIFEST,
        capabilities=SimpleNamespace(factory=lambda _cell: constructed.append),
        parent={"context": SimpleNamespace(release_id=_RELEASE)},
    )
    assert root is None
    assert constructed == []
