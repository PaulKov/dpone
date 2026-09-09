from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from dpone.adapters.dbt_artifacts import LocalDbtRunResultsReader
from dpone.adapters.dbt_semantic_refresh_project import (
    semantic_refresh_package_sha256,
)
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import (
    DbtExecutionPack,
    DbtProfileSpec,
    DbtPublishingError,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)
from dpone.contracts.dbt_semantic_refresh_project_overlay import semantic_refresh_project_overlay
from dpone.contracts.dbt_semantic_refresh_selection import prove_mutation_closure
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.ports.dbt_publishing import DbtCommandResult
from dpone.runtime import dbt_execution_bootstrap
from dpone.runtime.dbt_execution_policy import prepare_dbt_output_paths
from dpone.runtime.dbt_semantic_refresh_execution import (
    SemanticRefreshDbtCommandRunner,
    SemanticRefreshDbtExecutionVariables,
    SemanticRefreshDbtRuntimePreflight,
    load_semantic_refresh_scope_map,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtAdmittedRun,
    SemanticRefreshDbtProofObservation,
    SemanticRefreshDbtStaticProjectionIdentity,
    semantic_refresh_worker_pack_fingerprint,
)

DIGEST = "sha256:" + "a" * 64
_PACKAGE_FILES = (
    "dbt_project.yml",
    "macros/dpone_publish.sql",
    "macros/semantic_refresh_restore.sql",
    "macros/semantic_refresh_scope_merge.sql",
)


class _ScopeMapLoader:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        model_unique_ids: tuple[str, ...],
    ) -> dict[str, object]:
        self.calls.append((workflow_execution_binding_sha256, model_unique_ids))
        return self.value


class _RunAdmission:
    def __init__(self, *, package_artifacts_sha256: str, workflow_execution_id: str | None = None) -> None:
        self._package_artifacts_sha256 = package_artifacts_sha256
        self._workflow_execution_id = workflow_execution_id
        self.calls: list[tuple[dict[str, object], str, SemanticRefreshDbtStaticProjectionIdentity]] = []

    def admit(
        self,
        *,
        plan_bundle: Mapping[str, object],
        workflow_execution_id: str,
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
    ) -> SemanticRefreshDbtAdmittedRun:
        self.calls.append((dict(plan_bundle), workflow_execution_id, projection_identity))
        return SemanticRefreshDbtAdmittedRun(
            workflow_execution_id=self._workflow_execution_id or workflow_execution_id,
            workflow_execution_binding_sha256=DIGEST,
            plan_bundle_sha256="sha256:" + "c" * 64,
            pre_release_bundle_sha256="sha256:" + "d" * 64,
            package_artifacts_sha256=self._package_artifacts_sha256,
            model_unique_ids=("model.analytics.events",),
            protected_state_receipt_sha256="sha256:" + "e" * 64,
        )


class _ProofRechecker:
    def __init__(self, *, drift: bool = False, source_drift: bool = False) -> None:
        self._drift = drift
        self._source_drift = source_drift
        self.source_calls: list[tuple[tuple[str, ...], str]] = []
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def recheck_sources(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> None:
        assert manifest["nodes"]
        assert plan_bundle["plan_bundle_sha256"]
        self.source_calls.append((selected_model_unique_ids, projection_identity.dag_projection_sha256))
        if self._source_drift:
            raise DbtPublishingError("DPONE_DBT_V2_PROOF_DRIFT", "raw source drift")

    def recheck(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> SemanticRefreshDbtProofObservation:
        assert manifest["nodes"]
        self.calls.append((selected_model_unique_ids, projection_identity.dag_projection_sha256))
        operations = cast(list[Mapping[str, object]], plan_bundle["operation_plans"])
        digests = tuple(
            str(operation[field])
            for operation in operations
            for field in (
                "model_definition_proof_sha256",
                "read_dependency_proof_sha256",
                "mutation_closure_sha256",
                "sqlserver_lifecycle_policy_sha256",
            )
        )
        if self._drift:
            digests = ("sha256:" + "0" * 64, *digests[1:])
        return SemanticRefreshDbtProofObservation(
            observed_selected_unique_ids=selected_model_unique_ids,
            observed_proof_digests=digests,
            proof_statuses=tuple("PROVEN" for _ in digests),
        )


def test_semantic_runtime_injects_exact_overlay_and_platform_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)
    package_sha256 = semantic_refresh_package_sha256(package_root)
    captured: dict[str, object] = {}

    def execute(pack: DbtExecutionPack, **kwargs: object) -> str:
        materialized_root = Path(str(kwargs["runtime_root"]))
        project = materialized_root / "dbt-project"
        captured["pack"] = pack.to_dict()
        captured["project"] = yaml.safe_load((project / "dbt_project.yml").read_text(encoding="utf-8"))
        captured["macro"] = (project / "dbt_packages/dbt_dpone/macros/semantic_refresh_scope_merge.sql").read_bytes()
        captured["interval"] = cast(Any, kwargs["interval"]).dbt_vars_json()
        captured["preflight"] = kwargs["preflight"]
        return "executed"

    monkeypatch.setattr(dbt_execution_bootstrap, "_execute_loaded_pack", execute)
    overlay = semantic_refresh_project_overlay((("analytics", "events"),))
    scope_map = _scope_map(_execution_pack())
    loader = _ScopeMapLoader(scope_map)
    admission = _RunAdmission(package_artifacts_sha256=package_sha256)
    plan_bundle = _plan_bundle(package_sha256)

    outcome = dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
        dbt_execution_pack=_execution_pack().to_dict(),
        project_config_overlay=overlay,
        profile_sha256=DIGEST,
        topology_sha256="sha256:" + "b" * 64,
        plan_bundle=plan_bundle,
        projection_identity=_projection_identity(plan_bundle),
        workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
        package_source_root=package_root,
        run_admission=admission,
        scope_map_loader=loader,
        immutable_proof_rechecker=_ProofRechecker(),
        environ={
            "DPONE_INTERVAL_START": "2026-08-08T00:00:00Z",
            "DPONE_INTERVAL_END": "2026-08-09T00:00:00Z",
        },
        runtime_root=runtime_root,
    )

    assert outcome == "executed"
    assert captured["pack"] == _execution_pack().to_dict()
    project = captured["project"]
    assert isinstance(project, dict)
    assert project["models"]["analytics"]["events"] == {
        "+contract": {"enforced": True},
        "+incremental_strategy": "dpone_scope_merge",
        "+materialized": "incremental",
        "+on_schema_change": "fail",
    }
    assert captured["macro"] == (package_root / "macros/semantic_refresh_scope_merge.sql").read_bytes()
    assert json.loads(str(captured["interval"]))["dpone_semantic_refresh_scope_map"] == scope_map
    assert isinstance(captured["preflight"], SemanticRefreshDbtRuntimePreflight)
    assert loader.calls == [(DIGEST, ("model.analytics.events",))]
    assert admission.calls == [
        (
            plan_bundle,
            "scheduled__2026-08-08T00:00:00+00:00",
            SemanticRefreshDbtStaticProjectionIdentity.from_mapping(_projection_identity(plan_bundle)),
        )
    ]
    assert "models" not in yaml.safe_load((runtime_root / "dbt-project/dbt_project.yml").read_text(encoding="utf-8"))


def test_semantic_runtime_rejects_tampered_package_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)
    expected = semantic_refresh_package_sha256(package_root)
    (package_root / "macros/semantic_refresh_scope_merge.sql").write_text("{% macro changed() %}{% endmacro %}")
    monkeypatch.setattr(
        dbt_execution_bootstrap,
        "_execute_loaded_pack",
        lambda *_args, **_kwargs: pytest.fail("tampered package reached dbt"),
    )
    with pytest.raises(DbtPublishingError, match="package authority differs") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=semantic_refresh_project_overlay((("analytics", "events"),)),
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=_plan_bundle(expected),
            projection_identity=_projection_identity(_plan_bundle(expected)),
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(package_artifacts_sha256=expected),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )
    assert raised.value.code == "DPONE_DBT_V2_PACKAGE_INVALID"


def test_semantic_runtime_rejects_tampered_overlay_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)
    package_sha256 = semantic_refresh_package_sha256(package_root)
    overlay = semantic_refresh_project_overlay((("analytics", "events"),))
    overlay["models"]["analytics"]["events"]["+incremental_strategy"] = "merge"
    monkeypatch.setattr(
        dbt_execution_bootstrap,
        "_execute_loaded_pack",
        lambda *_args, **_kwargs: pytest.fail("tampered overlay reached dbt"),
    )

    with pytest.raises(DbtPublishingError, match="differs from activated topology") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=overlay,
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=_plan_bundle(package_sha256),
            projection_identity=_projection_identity(_plan_bundle(package_sha256)),
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(package_artifacts_sha256=package_sha256),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )
    assert raised.value.code == "DPONE_DBT_V2_OVERLAY_INVALID"


def test_semantic_runtime_rejects_run_admission_for_another_logical_dagrun(tmp_path: Path) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)

    with pytest.raises(DbtPublishingError, match="actual DagRun") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=semantic_refresh_project_overlay((("analytics", "events"),)),
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=_plan_bundle(semantic_refresh_package_sha256(package_root)),
            projection_identity=_projection_identity(_plan_bundle(semantic_refresh_package_sha256(package_root))),
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(
                package_artifacts_sha256=semantic_refresh_package_sha256(package_root),
                workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
            ),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )

    assert raised.value.code == "DPONE_DBT_V2_RUN_AUTHORITY_INVALID"


def test_semantic_runtime_rejects_admission_for_another_plan(tmp_path: Path) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)
    package_sha256 = semantic_refresh_package_sha256(package_root)
    plan = _plan_bundle(package_sha256)
    plan["pre_release_bundle_sha256"] = "sha256:" + "f" * 64

    with pytest.raises(DbtPublishingError, match="static projection authority") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=semantic_refresh_project_overlay((("analytics", "events"),)),
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=plan,
            projection_identity=_projection_identity(_plan_bundle(package_sha256)),
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(package_artifacts_sha256=package_sha256),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )

    assert raised.value.code == "DPONE_DBT_V2_RUN_AUTHORITY_INVALID"


def test_semantic_runtime_rejects_unclosed_or_mismatched_projection_identity(
    tmp_path: Path,
) -> None:
    runtime_root, package_root = _runtime_inputs(tmp_path)
    package_sha256 = semantic_refresh_package_sha256(package_root)
    plan = _plan_bundle(package_sha256)
    projection = _projection_identity(plan)
    projection["caller_override"] = DIGEST

    with pytest.raises(DbtPublishingError, match="static projection authority") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=semantic_refresh_project_overlay((("analytics", "events"),)),
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=plan,
            projection_identity=projection,
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(package_artifacts_sha256=package_sha256),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )
    assert raised.value.code == "DPONE_DBT_V2_RUN_AUTHORITY_INVALID"

    projection = _projection_identity(plan)
    projection["template_pack_fingerprint"] = "sha256:" + "9" * 64
    projection["topology_sha256"] = "sha256:" + "8" * 64
    with pytest.raises(DbtPublishingError, match="static projection") as raised:
        dbt_execution_bootstrap.execute_semantic_refresh_dbt_pack(
            dbt_execution_pack=_execution_pack().to_dict(),
            project_config_overlay=semantic_refresh_project_overlay((("analytics", "events"),)),
            profile_sha256=DIGEST,
            topology_sha256="sha256:" + "b" * 64,
            plan_bundle=plan,
            projection_identity=projection,
            workflow_execution_id="scheduled__2026-08-08T00:00:00+00:00",
            package_source_root=package_root,
            run_admission=_RunAdmission(package_artifacts_sha256=package_sha256),
            scope_map_loader=_ScopeMapLoader(_scope_map(_execution_pack())),
            immutable_proof_rechecker=_ProofRechecker(),
            runtime_root=runtime_root,
        )
    assert raised.value.code == "DPONE_DBT_V2_RUN_AUTHORITY_INVALID"


def test_worker_pack_fingerprint_binds_projection_run_and_deployment_receipt() -> None:
    plan = _plan_bundle(DIGEST)
    identity = SemanticRefreshDbtStaticProjectionIdentity.from_mapping(_projection_identity(plan))

    def fingerprint(
        *,
        projection: SemanticRefreshDbtStaticProjectionIdentity = identity,
        run_sha256: str = "sha256:" + "5" * 64,
    ) -> str:
        return semantic_refresh_worker_pack_fingerprint(
            projection_identity=projection,
            run_execution_bundle_sha256=run_sha256,
            activation_authority_receipt_sha256="sha256:" + "6" * 64,
            authority_store_ref="mssql-control://prod/dpone_control",
            run_guard_closure_sha256="sha256:" + "7" * 64,
        )

    expected = fingerprint()

    assert expected == fingerprint()
    assert expected != fingerprint(run_sha256="sha256:" + "8" * 64)
    assert expected != fingerprint(
        projection=SemanticRefreshDbtStaticProjectionIdentity.from_mapping(
            {
                **_projection_identity(plan),
                "template_pack_fingerprint": "sha256:" + "9" * 64,
            }
        )
    )


def test_semantic_runtime_scope_map_and_argv_are_closed() -> None:
    pack = _execution_pack()
    scope_map = _scope_map(pack)
    loader = _ScopeMapLoader(scope_map)
    loaded = load_semantic_refresh_scope_map(
        loader,
        workflow_execution_binding_sha256=DIGEST,
        model_unique_ids=pack.selection_lock.publish_model_unique_ids,
    )
    variables = SemanticRefreshDbtExecutionVariables(
        start="2026-08-08T00:00:00Z",
        end="2026-08-09T00:00:00Z",
        scope_map=loaded,
        statement_timeout_seconds=600,
    )
    delegate = _RecordingRunner()
    runner = SemanticRefreshDbtCommandRunner(delegate, expected_vars_json=variables.dbt_vars_json())
    result = runner.run(
        (
            "dbt",
            "build",
            "--indirect-selection",
            "eager",
            "--vars",
            variables.dbt_vars_json(),
        ),
        cwd=Path("."),
        timeout_seconds=10,
        redactions=(),
    )

    assert result.exit_code == 0
    assert delegate.calls[0][delegate.calls[0].index("--indirect-selection") + 1] == "empty"
    assert (
        json.loads(delegate.calls[0][delegate.calls[0].index("--vars") + 1])["dpone_semantic_refresh_scope_map"]
        == scope_map
    )


def test_semantic_runtime_rejects_tampered_scope_map() -> None:
    pack = _execution_pack()
    scope_map = _scope_map(pack)
    scope_map["scope_map_sha256"] = "sha256:" + "f" * 64

    with pytest.raises(ValueError, match="scope map digest differs"):
        load_semantic_refresh_scope_map(
            _ScopeMapLoader(scope_map),
            workflow_execution_binding_sha256=DIGEST,
            model_unique_ids=pack.selection_lock.publish_model_unique_ids,
        )


def test_semantic_runtime_preflight_proves_exact_mutation_closure(tmp_path: Path) -> None:
    pack = _execution_pack()
    variables = SemanticRefreshDbtExecutionVariables(
        start="2026-08-08T00:00:00Z",
        end="2026-08-09T00:00:00Z",
        scope_map=_scope_map(pack),
        statement_timeout_seconds=600,
    )
    delegate = _PreflightRunner(_semantic_manifest())
    runner = SemanticRefreshDbtCommandRunner(delegate, expected_vars_json=variables.dbt_vars_json())
    profile_path = tmp_path / "profiles/profiles.yml"
    profile_path.parent.mkdir()
    profile_path.write_text("safe: true\n", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()

    proof = SemanticRefreshDbtRuntimePreflight(
        command_runner=runner,
        artifact_reader=LocalDbtRunResultsReader(),
        manifest_validator=_ManifestValidator(),
        mutation_prover=prove_mutation_closure,
        immutable_proof_rechecker=(rechecker := _ProofRechecker()),
        plan_bundle=(plan := _plan_bundle(DIGEST)),
        projection_identity=SemanticRefreshDbtStaticProjectionIdentity.from_mapping(_projection_identity(plan)),
        error_factory=lambda code, message: DbtPublishingError(code, message),
    ).verify(
        pack,
        project_dir=tmp_path,
        profile_path=profile_path,
        output_paths=prepare_dbt_output_paths(output, pack.target_path, attempt_id="a" * 32),
        interval_vars_json=variables.dbt_vars_json(),
        redactions=(),
    )

    assert proof.selected_graph_unique_ids == ("model.analytics.events",)
    assert tuple(
        "parse" if "parse" in call else ("compile" if "compile" in call else "ls") for call in delegate.calls
    ) == ("parse", "compile", "ls")
    assert rechecker.calls == [(("model.analytics.events",), "sha256:" + "3" * 64)]
    assert rechecker.source_calls == [(("model.analytics.events",), "sha256:" + "3" * 64)]
    assert all(
        call[call.index("--indirect-selection") + 1] == "empty"
        for call in delegate.calls
        if "--indirect-selection" in call
    )


def test_semantic_runtime_preflight_rejects_current_proof_drift(tmp_path: Path) -> None:
    pack = _execution_pack()
    plan = _plan_bundle(DIGEST)
    runner = SemanticRefreshDbtCommandRunner(
        _PreflightRunner(_semantic_manifest()),
        expected_vars_json=(
            variables := SemanticRefreshDbtExecutionVariables(
                start="2026-08-08T00:00:00Z",
                end="2026-08-09T00:00:00Z",
                scope_map=_scope_map(pack),
                statement_timeout_seconds=600,
            )
        ).dbt_vars_json(),
    )
    profile_path = tmp_path / "profiles/profiles.yml"
    profile_path.parent.mkdir()
    profile_path.write_text("safe: true\n", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()

    with pytest.raises(DbtPublishingError, match="immutable proof") as raised:
        SemanticRefreshDbtRuntimePreflight(
            command_runner=runner,
            artifact_reader=LocalDbtRunResultsReader(),
            manifest_validator=_ManifestValidator(),
            mutation_prover=prove_mutation_closure,
            immutable_proof_rechecker=_ProofRechecker(drift=True),
            plan_bundle=plan,
            projection_identity=SemanticRefreshDbtStaticProjectionIdentity.from_mapping(_projection_identity(plan)),
            error_factory=lambda code, message: DbtPublishingError(code, message),
        ).verify(
            pack,
            project_dir=tmp_path,
            profile_path=profile_path,
            output_paths=prepare_dbt_output_paths(
                output,
                pack.target_path,
                attempt_id="b" * 32,
            ),
            interval_vars_json=variables.dbt_vars_json(),
            redactions=(),
        )
    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"


def test_semantic_runtime_preflight_rejects_source_drift_before_compile(tmp_path: Path) -> None:
    pack = _execution_pack()
    plan = _plan_bundle(DIGEST)
    delegate = _PreflightRunner(_semantic_manifest())
    variables = SemanticRefreshDbtExecutionVariables(
        start="2026-08-08T00:00:00Z",
        end="2026-08-09T00:00:00Z",
        scope_map=_scope_map(pack),
        statement_timeout_seconds=600,
    )
    runner = SemanticRefreshDbtCommandRunner(delegate, expected_vars_json=variables.dbt_vars_json())
    profile_path = tmp_path / "profiles/profiles.yml"
    profile_path.parent.mkdir()
    profile_path.write_text("safe: true\n", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()

    with pytest.raises(DbtPublishingError, match="raw source drift") as raised:
        SemanticRefreshDbtRuntimePreflight(
            command_runner=runner,
            artifact_reader=LocalDbtRunResultsReader(),
            manifest_validator=_ManifestValidator(),
            mutation_prover=prove_mutation_closure,
            immutable_proof_rechecker=_ProofRechecker(source_drift=True),
            plan_bundle=plan,
            projection_identity=SemanticRefreshDbtStaticProjectionIdentity.from_mapping(_projection_identity(plan)),
            error_factory=lambda code, message: DbtPublishingError(code, message),
        ).verify(
            pack,
            project_dir=tmp_path,
            profile_path=profile_path,
            output_paths=prepare_dbt_output_paths(output, pack.target_path, attempt_id="c" * 32),
            interval_vars_json=variables.dbt_vars_json(),
            redactions=(),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"
    assert tuple("parse" if "parse" in call else "other" for call in delegate.calls) == ("parse",)


def _plan_bundle(package_artifacts_sha256: str) -> dict[str, object]:
    return {
        "operation_plans": [
            {
                "model_definition_proof_sha256": "sha256:" + "a" * 64,
                "model_unique_id": "model.analytics.events",
                "mutation_closure_sha256": "sha256:" + "b" * 64,
                "read_dependency_proof_sha256": "sha256:" + "c" * 64,
                "sqlserver_lifecycle_policy_sha256": "sha256:" + "d" * 64,
            }
        ],
        "package_artifacts_sha256": package_artifacts_sha256,
        "plan_bundle_sha256": "sha256:" + "c" * 64,
        "pre_release_bundle_sha256": "sha256:" + "d" * 64,
        "release_deployment_authority": {
            "deployment_id": "sha256:" + "1" * 64,
            "release_id": "sha256:" + "f" * 64,
        },
        "schema": "dpone.dbt-semantic-refresh-plan-bundle.v1",
        "workflow_plan": {"workflow_plan_sha256": "sha256:" + "2" * 64},
    }


def _projection_identity(plan: Mapping[str, object]) -> dict[str, object]:
    release = cast(Mapping[str, object], plan["release_deployment_authority"])
    workflow = cast(Mapping[str, object], plan["workflow_plan"])
    return {
        "dag_projection_sha256": "sha256:" + "3" * 64,
        "deployment_id": release["deployment_id"],
        "package_artifacts_sha256": plan["package_artifacts_sha256"],
        "plan_bundle_sha256": plan["plan_bundle_sha256"],
        "pre_release_bundle_sha256": plan["pre_release_bundle_sha256"],
        "release_id": release["release_id"],
        "template_pack_fingerprint": "sha256:" + "4" * 64,
        "topology_sha256": "sha256:" + "b" * 64,
        "workflow_plan_sha256": workflow["workflow_plan_sha256"],
    }


def _runtime_inputs(tmp_path: Path) -> tuple[Path, Path]:
    runtime_root = tmp_path / "runtime-root"
    project = runtime_root / "dbt-project"
    project.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        yaml.safe_dump({"name": "analytics", "version": "1.0", "config-version": 2}),
        encoding="utf-8",
    )
    (project / "models").mkdir()
    (project / "models/events.sql").write_text("select 1 as event_id", encoding="utf-8")
    package_root = tmp_path / "platform-package"
    repository_package = Path("packages/dbt-dpone")
    for relative in _PACKAGE_FILES:
        target = package_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repository_package / relative).read_bytes())
    return runtime_root, package_root


class _RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        self.calls.append(args)
        return DbtCommandResult(0)


class _PreflightRunner(_RecordingRunner):
    def __init__(self, manifest: dict[str, object]) -> None:
        super().__init__()
        self.manifest = manifest

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        self.calls.append(args)
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True, exist_ok=True)
            (target / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")
            return DbtCommandResult(0)
        return DbtCommandResult(0, stdout=json.dumps({"unique_id": "model.analytics.events"}))


class _ManifestValidator:
    def validate(self, payload: object, *, version: int) -> tuple[object, ...]:
        assert version == 12
        assert isinstance(payload, dict)
        return ()


def _semantic_manifest() -> dict[str, object]:
    return {
        "nodes": {
            "model.analytics.events": {
                "alias": "events",
                "columns": {},
                "config": {
                    "contract": {"enforced": True},
                    "enabled": True,
                    "incremental_strategy": "dpone_scope_merge",
                    "materialized": "incremental",
                    "on_schema_change": "fail",
                    "post-hook": [],
                    "pre-hook": [],
                },
                "database": "DWH",
                "depends_on": {"macros": [], "nodes": []},
                "fqn": ["analytics", "events"],
                "language": "sql",
                "meta": {"dpone": {"publish": {"enabled": True}}},
                "name": "events",
                "relation_name": "[DWH].[mart].[events]",
                "resource_type": "model",
                "schema": "mart",
                "unique_id": "model.analytics.events",
            }
        },
        "unit_tests": {},
    }


def _scope_map(pack: DbtExecutionPack) -> dict[str, object]:
    model_id = pack.selection_lock.publish_model_unique_ids[0]
    strategy = {
        "model_unique_id": model_id,
        "workflow_execution_binding_sha256": DIGEST,
    }
    authority = json.dumps(strategy, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    unsigned = {
        "authorities": {model_id: authority},
        "models": {model_id: strategy},
        "schema": "dpone.semantic-refresh-mssql-scope-map.v1",
    }
    raw = json.dumps(unsigned, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return {
        **unsigned,
        "scope_map_sha256": "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
        "signature_sha256": "sha256:" + "c" * 64,
        "verification_status": "VERIFIED",
    }


def _execution_pack() -> DbtExecutionPack:
    invocation = DbtInvocationContext.canonical()
    lock = DbtSelectionLock.build(
        manifest_sha256=DIGEST,
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256="sha256:" + "b" * 64,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("fqn:analytics.events",),
        selected_graph_unique_ids=("model.analytics.events",),
        expected_run_result_unique_ids=("model.analytics.events",),
        publish_model_unique_ids=("model.analytics.events",),
    )
    return DbtExecutionPack.build(
        workflow_id="daily_events",
        project_bundle_sha256=DIGEST,
        project_subdir="dbt-project",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="dpone_runtime",
            target_name="runtime",
            connection_ref="mssql_prod",
            adapter_type="sqlserver",
            database="DWH",
            schema="mart",
            threads=4,
        ),
        selection_lock=lock,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(900),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=900,
    )
