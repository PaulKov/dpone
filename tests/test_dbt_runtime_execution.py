from __future__ import annotations

import io
import json
import stat
import subprocess
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dpone.adapters.dbt_artifacts import (
    LocalDbtExecutionEvidenceWriter,
    LocalDbtRunResultsReader,
)
from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer, TemporaryDbtProfileStore
from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.adapters.dbt_workspace_attempt_request import DbtWorkspaceAttemptRequestFactory
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    AirflowArtifactIdentity,
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
)
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import (
    DbtCredentialVersion,
    DbtExecutionPack,
    DbtProfileSpec,
    DbtPublishingError,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
    dbt_target_identity_sha256,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
)
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    dbt_sqlserver_graph_contract_sha256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import (
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    DbtWorkspaceAttemptReceipt,
    DbtWorkspaceAttemptRequest,
)
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.ports.dbt_publishing import (
    DbtCommandResult,
    DbtInstalledToolchain,
    RenderedDbtProfile,
)
from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.dbt_execution_bootstrap import _workspace_attempt_dependencies, execute_dbt_pack
from dpone.runtime.dbt_execution_policy import prepare_dbt_output_paths
from dpone.runtime.dbt_execution_service import DbtExecutionInterval, DbtExecutionService
from dpone.runtime.dbt_preflight import DbtRuntimePreflight

_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64
_DIGEST_C = "sha256:" + "c" * 64
_TOOLCHAIN_DIGEST = DBT_SQLSERVER_1_12_CERTIFIED.sha256
_UTC = timezone.utc  # noqa: UP017 - mypy baseline targets pre-3.11 datetime stubs.
_PROJECT_YAML = """\
name: analytics
flags:
  dbt_sqlserver_enable_safe_type_expansion: false
  dbt_sqlserver_use_dbt_transactions: true
  dbt_sqlserver_use_default_schema_concat: true
  dbt_sqlserver_use_native_string_types: true
"""
_AUTHORITY_MANIFEST = json.loads(
    (Path(__file__).parents[1] / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(
        encoding="utf-8"
    )
)


def _pack(*, warning_policy: str = "fail") -> DbtExecutionPack:
    invocation = DbtInvocationContext.canonical()
    manifest = _preflight_manifest()
    selection = DbtSelectionLock.build(
        manifest_sha256=_DIGEST_A,
        toolchain_sha256=_TOOLCHAIN_DIGEST,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(
            manifest,
            (
                "model.analytics.orders",
                "test.analytics.orders_not_null",
            ),
        ),
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("orders",),
        selected_graph_unique_ids=(
            "model.analytics.orders",
            "test.analytics.orders_not_null",
        ),
        expected_run_result_unique_ids=(
            "model.analytics.orders",
            "test.analytics.orders_not_null",
        ),
        publish_model_unique_ids=("model.analytics.orders",),
    )
    return DbtExecutionPack.build(
        workflow_id="orders_publish",
        project_bundle_sha256=_DIGEST_C,
        project_subdir=".",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="analytics",
            target_name="prod",
            connection_ref="warehouse",
            adapter_type="sqlserver",
            database="DWH",
            schema="mart",
            threads=2,
        ),
        selection_lock=selection,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(600),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy=warning_policy,
        timeout_seconds=600,
    )


def _v2_pack() -> DbtExecutionPack:
    from dpone.contracts.dbt_contract_validation import canonical_fingerprint

    raw = _pack().to_dict()
    raw["schema"] = "dpone.dbt-execution-pack.v2"
    raw["invocation_target"] = {"database": "DWH", "schema": "base"}
    raw["pack_sha256"] = canonical_fingerprint({key: value for key, value in raw.items() if key != "pack_sha256"})
    return DbtExecutionPack.from_mapping(raw)


def _real_sqlserver_fixture_pack(manifest: dict[str, object]) -> DbtExecutionPack:
    unique_id = "model.dpone_dbt_demo.competitive_pricing"
    invocation = DbtInvocationContext.canonical()
    selection = DbtSelectionLock.build(
        manifest_sha256=_DIGEST_A,
        toolchain_sha256=_TOOLCHAIN_DIGEST,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(
            manifest,
            (unique_id,),
        ),
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=(unique_id,),
        selected_graph_unique_ids=(unique_id,),
        expected_run_result_unique_ids=(unique_id,),
        publish_model_unique_ids=(unique_id,),
    )
    return DbtExecutionPack.build(
        workflow_id="pricing_publish",
        project_bundle_sha256=_DIGEST_C,
        project_subdir=".",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="analytics",
            target_name="prod",
            connection_ref="warehouse",
            adapter_type="sqlserver",
            database="DWH_Stage",
            schema="pricing_pricing",
            threads=2,
        ),
        selection_lock=selection,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(600),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=600,
    )


def _interval() -> DbtExecutionInterval:
    return DbtExecutionInterval(
        start="2026-07-27T00:00:00Z",
        end="2026-07-28T00:00:00Z",
    )


@dataclass
class _ToolchainInspector:
    core_version: str = "1.12.3"
    adapter_version: str = "1.11.1"
    inspected_adapter: str | None = None

    def inspect(self, adapter_name: str) -> DbtInstalledToolchain:
        self.inspected_adapter = adapter_name
        return DbtInstalledToolchain(
            dbt_core_version=self.core_version,
            adapter_name=adapter_name,
            adapter_version=self.adapter_version,
        )


def _run_identity(pack: DbtExecutionPack) -> AirflowRunIdentity:
    return AirflowRunIdentity(
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        workload_pack=AirflowArtifactIdentity(id="dbt__orders_publish", sha256=pack.pack_sha256),
    )


def _attempt() -> AirflowAttemptCorrelation:
    return AirflowAttemptCorrelation(
        dag_id="orders_publish",
        task_id="dbt__orders_publish",
        run_id="scheduled__2026-07-27T00:00:00+00:00",
        try_number=1,
        map_index=-1,
    )


class _WorkspaceAttemptFactory:
    def build(self, *, pack, manifest, run_identity, airflow_attempt):
        assert "model.analytics.orders" in manifest["nodes"]
        return DbtWorkspaceAttemptRequest.build(
            activation_id="164a3c74-cf85-4a4a-a087-07c9b07050ff",
            attempt_id=_DIGEST_C,
            workflow_id=pack.workflow_id,
            write_subjects=(_DIGEST_A,),
        )


class _WorkspaceAttemptAdmission:
    def __init__(self) -> None:
        self.states: list[str] = []

    def _receipt(self, request, state):
        self.states.append(state)
        return DbtWorkspaceAttemptReceipt.build(
            request=request,
            state=state,
            guard_epochs=(DbtWorkspaceGuardEpoch("mssql://warehouse/DWH/mart/orders", 7),),
        )

    def admit(self, request):
        return self._receipt(request, "RUNNING")

    def terminalize(self, request, *, state):
        return self._receipt(request, state)


@dataclass
class _ProfileRenderer:
    secret: str = "runtime-only-password"
    expected_schema: str | None = None

    def render(
        self,
        profile: DbtProfileSpec,
        adapter_runtime: DbtSqlServerRuntimePolicy,
    ) -> RenderedDbtProfile:
        assert profile.connection_ref == "warehouse"
        if self.expected_schema is not None:
            assert profile.schema == self.expected_schema
        assert adapter_runtime.retries == 1
        return RenderedDbtProfile(
            content=f"password: {self.secret}\n".encode(),
            credential_versions=(
                DbtCredentialVersion(
                    connection_ref="warehouse",
                    resolver="vault_kv",
                    resolved_version="7",
                ),
            ),
            logical_target_sha256=dbt_target_identity_sha256(profile),
            redaction_values=(self.secret,),
        )


class _Runner:
    def __init__(
        self,
        *,
        exit_code: int = 0,
        result_statuses: tuple[str, ...] = ("success", "pass"),
        dbt_version: str = "1.12.3",
        schema_version: str = "https://schemas.getdbt.com/dbt/run-results/v6.json",
        write_results: bool = True,
        result_extra_fields: dict[str, object] | None = None,
        preflight_manifest: dict[str, object] | None = None,
        preflight_selection: tuple[str, ...] | None = None,
        build_error: Exception | None = None,
    ) -> None:
        self.exit_code = exit_code
        self.result_statuses = result_statuses
        self.dbt_version = dbt_version
        self.schema_version = schema_version
        self.write_results = write_results
        self.result_extra_fields = result_extra_fields or {}
        self.preflight_manifest = preflight_manifest or _preflight_manifest()
        self.preflight_selection = preflight_selection
        self.build_error = build_error
        self.args: tuple[str, ...] | None = None
        self.calls: list[tuple[str, ...]] = []
        self.profile_path: Path | None = None
        self.redactions: tuple[str, ...] = ()

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        self.args = args
        self.calls.append(args)
        self.redactions = redactions
        assert timeout_seconds in {120, 600}
        profiles_dir = Path(args[args.index("--profiles-dir") + 1])
        self.profile_path = profiles_dir / "profiles.yml"
        assert stat.S_IMODE(self.profile_path.stat().st_mode) == 0o600
        assert self.profile_path.read_text(encoding="utf-8") == "password: runtime-only-password\n"
        target_path = Path(args[args.index("--target-path") + 1])
        if "parse" in args:
            target_path.mkdir(parents=True, exist_ok=True)
            (target_path / "manifest.json").write_text(
                json.dumps(self.preflight_manifest),
                encoding="utf-8",
            )
            return DbtCommandResult(exit_code=0)
        if "ls" in args:
            return DbtCommandResult(
                exit_code=0,
                stdout="\n".join(
                    json.dumps({"unique_id": unique_id})
                    for unique_id in (self.preflight_selection or _pack().selection_lock.selected_graph_unique_ids)
                ),
            )
        if self.build_error is not None:
            raise self.build_error
        results_path = cwd / target_path / "run_results.json"
        if not self.write_results:
            return DbtCommandResult(exit_code=self.exit_code)
        results_path.parent.mkdir(parents=True, exist_ok=True)
        results_path.write_text(
            json.dumps(
                {
                    "metadata": {
                        "dbt_schema_version": self.schema_version,
                        "dbt_version": self.dbt_version,
                        "generated_at": "2026-07-27T00:00:01Z",
                        "invocation_id": "invocation-01",
                        "invocation_started_at": "2026-07-27T00:00:00Z",
                        "env": {},
                    },
                    "results": [
                        {
                            "status": status,
                            "timing": [],
                            "thread_id": "Thread-1",
                            "execution_time": 0.25,
                            "adapter_response": {},
                            "message": "must not enter evidence",
                            "failures": 0,
                            "unique_id": unique_id,
                            "compiled": True,
                            "compiled_code": (
                                "select order_id, amount from mart.orders" if unique_id.startswith("model.") else None
                            ),
                            "relation_name": "[DWH].[mart].[orders]",
                            "batch_results": None,
                            **self.result_extra_fields,
                        }
                        for unique_id, status in zip(
                            _pack().selection_lock.expected_run_result_unique_ids,
                            self.result_statuses,
                            strict=True,
                        )
                    ],
                    "elapsed_time": 0.5,
                    "args": {},
                }
            ),
            encoding="utf-8",
        )
        return DbtCommandResult(exit_code=self.exit_code)


class _ManifestValidator:
    def validate(
        self,
        payload: object,
        *,
        version: int,
    ) -> tuple[object, ...]:
        assert isinstance(payload, dict)
        assert version == 12
        return ()


def _preflight(runner: object) -> DbtRuntimePreflight:
    return DbtRuntimePreflight(
        command_runner=runner,  # type: ignore[arg-type]
        artifact_reader=LocalDbtRunResultsReader(),
        manifest_validator=_ManifestValidator(),
    )


def _preflight_manifest() -> dict[str, object]:
    return {
        "metadata": deepcopy(_AUTHORITY_MANIFEST["metadata"]),
        "macros": deepcopy(_AUTHORITY_MANIFEST["macros"]),
        "nodes": {
            "model.analytics.ephemeral_orders": {
                "unique_id": "model.analytics.ephemeral_orders",
                "resource_type": "model",
                "fqn": ["analytics", "ephemeral_orders"],
                "depends_on": {"nodes": [], "macros": []},
                "config": {"materialized": "ephemeral"},
                "database": "DWH",
                "schema": "mart",
                "alias": "ephemeral_orders",
                "relation_name": None,
                "columns": {},
            },
            "model.analytics.orders": {
                "unique_id": "model.analytics.orders",
                "resource_type": "model",
                "language": "sql",
                "fqn": ["analytics", "orders"],
                "depends_on": {"nodes": [], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "as_columnstore": False,
                    "indexes": [],
                    "drop_unmanaged_indexes": False,
                    "prefer_single_alter_column": False,
                },
                "database": "DWH",
                "schema": "mart",
                "alias": "orders",
                "relation_name": "[DWH].[mart].[orders]",
                "columns": {},
            },
            "test.analytics.orders_not_null": {
                "unique_id": "test.analytics.orders_not_null",
                "resource_type": "test",
                "language": "sql",
                "fqn": ["analytics", "orders_not_null"],
                "depends_on": {
                    "nodes": ["model.analytics.orders"],
                    "macros": [],
                },
                "config": {
                    "enabled": True,
                    "materialized": "test",
                },
                "database": None,
                "schema": None,
                "alias": "orders_not_null",
                "relation_name": None,
                "columns": {},
            },
        },
        "unit_tests": {},
    }


def test_preflight_allows_known_sqlserver_manifest_warnings_and_reaches_selection(
    tmp_path: Path,
) -> None:
    manifest = json.loads(
        (Path(__file__).parents[1] / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(
            encoding="utf-8"
        )
    )
    diagnostics = OfficialDbtManifestValidator().validate(manifest, version=12)
    assert len(diagnostics) == 20
    assert {diagnostic.severity for diagnostic in diagnostics} == {"warning"}
    assert {diagnostic.rule for diagnostic in diagnostics} == {"additionalProperties"}
    assert all(diagnostic.path.startswith("$.macros.") for diagnostic in diagnostics)
    pack = _real_sqlserver_fixture_pack(manifest)
    runner = _Runner(
        preflight_manifest=manifest,
        preflight_selection=pack.selection_lock.selected_graph_unique_ids,
    )
    profile_path = tmp_path / "profiles" / "profiles.yml"
    profile_path.parent.mkdir()
    profile_path.write_text("password: runtime-only-password\n", encoding="utf-8")
    profile_path.chmod(0o600)
    output_paths = prepare_dbt_output_paths(
        _run_output_root(tmp_path),
        pack.target_path,
        attempt_id="a" * 32,
    )

    result = DbtRuntimePreflight(
        command_runner=runner,
        artifact_reader=LocalDbtRunResultsReader(),
        manifest_validator=OfficialDbtManifestValidator(),
    ).verify(
        pack,
        project_dir=tmp_path,
        profile_path=profile_path,
        output_paths=output_paths,
        interval_vars_json=_interval().dbt_vars_json(),
        redactions=("runtime-only-password",),
    )

    assert result.selected_graph_unique_ids == pack.selection_lock.selected_graph_unique_ids
    assert tuple("parse" if "parse" in call else "ls" for call in runner.calls) == (
        "parse",
        "ls",
    )


def test_preflight_blocks_official_manifest_errors_before_selection(
    tmp_path: Path,
) -> None:
    manifest = json.loads(
        (Path(__file__).parents[1] / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(
            encoding="utf-8"
        )
    )
    pack = _real_sqlserver_fixture_pack(manifest)
    manifest["macros"]["macro.dbt_sqlserver.materialization_table_sqlserver"]["unsupported_official_field"] = True
    runner = _Runner(
        preflight_manifest=manifest,
        preflight_selection=pack.selection_lock.selected_graph_unique_ids,
    )
    profile_path = tmp_path / "profiles" / "profiles.yml"
    profile_path.parent.mkdir()
    profile_path.write_text("password: runtime-only-password\n", encoding="utf-8")
    profile_path.chmod(0o600)
    output_paths = prepare_dbt_output_paths(
        _run_output_root(tmp_path),
        pack.target_path,
        attempt_id="b" * 32,
    )

    with pytest.raises(DbtPublishingError) as raised:
        DbtRuntimePreflight(
            command_runner=runner,
            artifact_reader=LocalDbtRunResultsReader(),
            manifest_validator=OfficialDbtManifestValidator(),
        ).verify(
            pack,
            project_dir=tmp_path,
            profile_path=profile_path,
            output_paths=output_paths,
            interval_vars_json=_interval().dbt_vars_json(),
            redactions=("runtime-only-password",),
        )

    assert raised.value.code == "DPONE_DBT_SELECTION_DRIFT"
    assert len(runner.calls) == 1
    assert "parse" in runner.calls[0]


def _clock() -> Callable[[], datetime]:
    moments = iter(
        (
            datetime(2026, 7, 27, 0, 0, tzinfo=_UTC),
            datetime(2026, 7, 27, 0, 1, tzinfo=_UTC),
        )
    )
    return lambda: next(moments)


def _run_output_root(tmp_path: Path) -> Path:
    for project_root in (tmp_path, tmp_path / "project"):
        if project_root.is_dir():
            project_file = project_root / "dbt_project.yml"
            if not project_file.exists():
                project_file.write_text(_PROJECT_YAML, encoding="utf-8")
    path = tmp_path / "run-output"
    path.mkdir(exist_ok=True)
    return path


@pytest.mark.parametrize("version", [1, 2])
def test_execution_uses_locked_argv_private_profile_and_secret_free_evidence(tmp_path: Path, version: int) -> None:
    project = tmp_path / "project"
    project.mkdir()
    source_path = project / "dbt_project.yml"
    source_path.write_text(_PROJECT_YAML, encoding="utf-8")
    source_before = source_path.read_bytes()
    run_output_root = tmp_path / "run-output"
    run_output_root.mkdir()
    evidence_path = tmp_path / "evidence" / "dbt-execution.json"
    runner = _Runner()
    workspace_admission = _WorkspaceAttemptAdmission() if version == 2 else None
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(expected_schema="base" if version == 2 else "mart"),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        workspace_attempt_factory=_WorkspaceAttemptFactory() if version == 2 else None,
        workspace_attempt_admission=workspace_admission,
        clock=_clock(),
    )
    pack = _pack()
    if version == 2:
        pack = _v2_pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=run_output_root,
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert outcome.passed
    assert workspace_admission is None or workspace_admission.states == ["RUNNING", "SUCCEEDED"]
    assert outcome.exit_code == 0
    assert runner.profile_path is not None
    assert runner.args is not None
    target_path = Path(runner.args[runner.args.index("--target-path") + 1])
    log_path = Path(runner.args[runner.args.index("--log-path") + 1])
    assert target_path.parent.parent == run_output_root / "attempts"
    assert log_path.parent == target_path.parent
    assert runner.args == (
        "dbt",
        "--warn-error",
        "build",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(runner.profile_path.parent),
        "--profile",
        "analytics",
        "--target",
        "prod",
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--indirect-selection",
        "eager",
        "--select",
        "orders",
        "--vars",
        '{"dpone_data_interval_end":"2026-07-28T00:00:00Z","dpone_data_interval_start":"2026-07-27T00:00:00Z"}',
    )
    assert runner.redactions == ("runtime-only-password",)
    assert not runner.profile_path.exists()
    evidence_text = evidence_path.read_text(encoding="utf-8")
    assert "runtime-only-password" not in evidence_text
    assert "must not enter evidence" not in evidence_text
    evidence = json.loads(evidence_text)
    assert evidence["schema"] == "dpone.dbt-execution-evidence.v1"
    assert evidence["status"] == "passed"
    assert evidence["logical_target_sha256"] == dbt_target_identity_sha256(pack.profile)
    assert evidence["credential_versions"] == [
        {"connection_ref": "warehouse", "resolved_version": "7", "resolver": "vault_kv"}
    ]
    assert evidence["dbt_warning_policy"] == "fail"
    assert evidence["dbt_warning_count"] == 0
    assert [item["unique_id"] for item in evidence["nodes"]] == list(pack.selection_lock.expected_run_result_unique_ids)
    assert source_path.read_bytes() == source_before
    assert not (project / "target").exists()
    assert not (project / "logs").exists()
    assert (target_path / "run_results.json").is_file()


def test_workspace_attempt_factory_binds_activation_attempt_and_complete_relation_subset() -> None:
    pack = _v2_pack()
    deployment = AirflowDeploymentIdentity(
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        activation_id="164a3c74-cf85-4a4a-a087-07c9b07050ff",
    )

    request = DbtWorkspaceAttemptRequestFactory(deployment).build(
        pack=pack,
        manifest=_preflight_manifest(),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
    )

    assert request.activation_id == deployment.activation_id
    assert request.workflow_id == pack.workflow_id
    assert len(request.write_subjects) == 4  # target, intermediate, backup and helper


def test_workspace_v2_without_attempt_admission_fails_before_build(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(_PROJECT_YAML, encoding="utf-8")
    runner = _Runner()
    pack = _v2_pack()
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(expected_schema="base"),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert outcome.evidence.code == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
    assert not outcome.evidence.build_started
    assert all("build" not in call for call in runner.calls)


def test_workspace_composition_resolves_dedicated_authority_binding_not_target() -> None:
    pack = _v2_pack()
    deployment = AirflowDeploymentIdentity(
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        activation_id="164a3c74-cf85-4a4a-a087-07c9b07050ff",
    )

    class _Resolver:
        def __init__(self) -> None:
            self.refs: list[str] = []

        def resolve(self, connection_ref: str):
            self.refs.append(connection_ref)
            return object()

    resolver = _Resolver()
    factory, admission = _workspace_attempt_dependencies(
        pack,
        environment={
            AIRFLOW_DEPLOYMENT_IDENTITY_ENV: deployment.to_json(),
            DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: "dpone_control",
        },
        run_identity=_run_identity(pack),
        resolver=resolver,
    )

    assert factory is not None and admission is not None
    assert resolver.refs == ["dpone_control"]


def test_workspace_composition_rejects_target_binding_as_control_authority() -> None:
    pack = _v2_pack()
    deployment = AirflowDeploymentIdentity(
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        activation_id="164a3c74-cf85-4a4a-a087-07c9b07050ff",
    )

    with pytest.raises(DbtPublishingError, match="must be distinct"):
        _workspace_attempt_dependencies(
            pack,
            environment={
                AIRFLOW_DEPLOYMENT_IDENTITY_ENV: deployment.to_json(),
                DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: pack.profile.connection_ref,
            },
            run_identity=_run_identity(pack),
            resolver=object(),
        )


def test_execution_succeeds_with_read_only_extracted_project(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    source_path = project / "dbt_project.yml"
    source_path.write_text(_PROJECT_YAML, encoding="utf-8")
    source_path.chmod(0o444)
    project.chmod(0o555)
    run_output_root = tmp_path / "run-output"
    run_output_root.mkdir()
    runner = _Runner()
    pack = _pack()
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(run_output_root / "evidence.json"),
        clock=_clock(),
    )

    try:
        outcome = service.execute(
            pack,
            runtime_root=project,
            run_output_root=run_output_root,
            run_identity=_run_identity(pack),
            airflow_attempt=_attempt(),
            interval=_interval(),
        )
    finally:
        project.chmod(0o755)
        source_path.chmod(0o644)

    assert outcome.passed
    assert (run_output_root / "evidence.json").is_file()
    assert source_path.read_text(encoding="utf-8") == _PROJECT_YAML
    assert not (project / "target").exists()
    assert not (project / "logs").exists()
    assert runner.args is not None
    assert Path(runner.args[runner.args.index("--target-path") + 1]).is_absolute()
    assert Path(runner.args[runner.args.index("--log-path") + 1]).is_absolute()


@pytest.mark.parametrize(
    ("drift", "expected_code", "expected_commands"),
    (
        (
            "logical_target",
            "DPONE_DBT_TARGET_IDENTITY_MISMATCH",
            ("parse",),
        ),
        (
            "graph_contract",
            "DPONE_DBT_SELECTION_DRIFT",
            ("parse", "ls"),
        ),
        (
            "selected_graph",
            "DPONE_DBT_SELECTION_DRIFT",
            ("parse", "ls"),
        ),
    ),
)
def test_preflight_drift_blocks_build_and_records_non_mutating_evidence(
    drift: str,
    expected_code: str,
    expected_commands: tuple[str, ...],
    tmp_path: Path,
) -> None:
    manifest = json.loads(json.dumps(_preflight_manifest()))
    selected_graph: tuple[str, ...] | None = None
    if drift == "logical_target":
        manifest["nodes"]["model.analytics.orders"]["database"] = "OTHER"
    elif drift == "graph_contract":
        manifest["nodes"]["model.analytics.orders"]["alias"] = "orders_changed"
    else:
        selected_graph = ("model.analytics.orders",)
    runner = _Runner(
        preflight_manifest=manifest,
        preflight_selection=selected_graph,
    )
    output_root = _run_output_root(tmp_path)
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=tmp_path,
        run_output_root=output_root,
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == expected_code
    assert outcome.evidence.preflight_status == "failed"
    assert not outcome.evidence.build_started
    assert outcome.evidence.dbt_exit_code is None
    assert (
        tuple(next(command for command in ("parse", "ls", "build") if command in call) for call in runner.calls)
        == expected_commands
    )
    assert not tuple((output_root / "attempts").glob("*/target"))
    assert evidence_path.is_file()


@pytest.mark.parametrize(
    ("unsafe_case", "expected_code"),
    [
        ("adapter_config", "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED"),
        ("macro_shadow", "DPONE_DBT_SQLSERVER_MACRO_AUTHORITY_INVALID"),
        ("physical_constraint", DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED),
        ("unique_key_expression", "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED"),
        ("foreign_test_dependency", "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW"),
    ],
)
def test_graph_policy_rejection_blocks_build_before_mutation(
    tmp_path: Path,
    unsafe_case: str,
    expected_code: str,
) -> None:
    manifest = json.loads(json.dumps(_preflight_manifest()))
    model = manifest["nodes"]["model.analytics.orders"]
    if unsafe_case == "adapter_config":
        model["config"]["query_options_raw"] = ["RECOMPILE"]
    elif unsafe_case == "physical_constraint":
        model["constraints"] = [{"type": "primary_key", "columns": ["order_id"]}]
    elif unsafe_case == "unique_key_expression":
        model["config"].update(
            {
                "materialized": "incremental",
                "incremental_strategy": "merge",
                "unique_key": "lower(order_id)",
                "on_schema_change": "fail",
                "contract": {"enforced": True},
            }
        )
        model["columns"] = {
            "order_id": {
                "name": "order_id",
                "data_type": "bigint",
                "constraints": [{"type": "not_null"}],
            }
        }
    elif unsafe_case == "foreign_test_dependency":
        manifest["nodes"]["test.analytics.orders_not_null"]["depends_on"]["nodes"].append("model.shared.customers")
    else:
        manifest["macros"]["macro.analytics.get_query_options"] = {
            "unique_id": "macro.analytics.get_query_options",
            "name": "get_query_options",
            "resource_type": "macro",
            "package_name": "analytics",
            "original_file_path": "macros/get_query_options.sql",
            "macro_sql": "{% macro get_query_options() %}{% endmacro %}",
            "depends_on": {"macros": []},
        }
    runner = _Runner(preflight_manifest=manifest)
    output_root = _run_output_root(tmp_path)
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=tmp_path,
        run_output_root=output_root,
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == expected_code
    assert not outcome.evidence.build_started
    assert outcome.evidence.preflight_status == "failed"
    assert tuple(command for call in runner.calls for command in ("parse", "ls", "build") if command in call) == (
        "parse",
    )
    assert evidence_path.is_file()


def test_resolved_profile_target_mismatch_blocks_preflight_and_build(
    tmp_path: Path,
) -> None:
    class MismatchedProfileRenderer:
        def render(
            self,
            profile: DbtProfileSpec,
            adapter_runtime: DbtSqlServerRuntimePolicy,
        ) -> RenderedDbtProfile:
            rendered = _ProfileRenderer().render(profile, adapter_runtime)
            return RenderedDbtProfile(
                content=rendered.content,
                credential_versions=rendered.credential_versions,
                logical_target_sha256=_DIGEST_A,
                redaction_values=rendered.redaction_values,
            )

    runner = _Runner()
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=MismatchedProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=tmp_path,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert outcome.evidence.code == "DPONE_DBT_TARGET_IDENTITY_MISMATCH"
    assert outcome.evidence.preflight_status == "not_started"
    assert not outcome.evidence.build_started
    assert outcome.evidence.dbt_exit_code is None
    assert not runner.calls


def test_execution_preserves_real_nonzero_dbt_exit_code(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(exit_code=17, result_statuses=("error", "skipped"))
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 17
    assert outcome.evidence.code == "DPONE_DBT_EXECUTION_FAILED"


def test_build_timeout_is_commit_unknown_and_never_reported_as_retryable_failure(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(build_error=TimeoutError("private timeout detail"))
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 1
    assert outcome.evidence.status == "failed"
    assert outcome.evidence.code == "COMMIT_UNKNOWN"
    assert outcome.evidence.preflight_status == "passed"
    assert outcome.evidence.build_started
    assert outcome.evidence.dbt_exit_code is None
    assert outcome.evidence.recovery == {
        "status": "COMMIT_UNKNOWN",
        "failure_boundary": "target_invocation",
        "target_state": "unknown",
        "checkpoint_state": "not_advanced",
        "source_state": "not_advanced",
        "safe_to_retry": False,
        "operator_verification_required": True,
        "recovery_action": "operator_verification_required",
    }
    assert "private timeout detail" not in evidence_path.read_text(encoding="utf-8")


def test_unexpected_post_build_result_validator_failure_is_commit_unknown(
    tmp_path: Path,
) -> None:
    class ExplodingValidator:
        def validate(self, _payload: object, *, version: int) -> tuple[object, ...]:
            assert version == 6
            raise RuntimeError("private validator detail")

    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner()
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=ExplodingValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == "COMMIT_UNKNOWN"
    assert outcome.evidence.dbt_exit_code == 0
    assert outcome.evidence.build_started
    assert outcome.evidence.recovery is not None
    assert outcome.evidence.recovery["safe_to_retry"] is False
    assert "private validator detail" not in evidence_path.read_text(encoding="utf-8")


def test_post_build_evidence_write_failure_raises_non_retryable_commit_unknown(
    tmp_path: Path,
) -> None:
    class FailingEvidenceWriter:
        def write(self, _evidence: object) -> Path:
            raise OSError("private storage detail")

    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner()
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=FailingEvidenceWriter(),
        clock=_clock(),
    )
    pack = _pack()

    with pytest.raises(CommitUnknownError) as raised:
        service.execute(
            pack,
            runtime_root=project,
            run_output_root=_run_output_root(tmp_path),
            run_identity=_run_identity(pack),
            airflow_attempt=_attempt(),
            interval=_interval(),
        )

    assert raised.value.code == "COMMIT_UNKNOWN"
    assert not raised.value.safe_to_retry
    assert raised.value.operator_verification_required
    assert "private storage detail" not in str(raised.value)


def test_execution_returns_nonzero_when_dbt_exit_zero_but_results_fail(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(exit_code=0, result_statuses=("error", "skipped"))
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 1
    assert outcome.evidence.dbt_exit_code == 0


@pytest.mark.parametrize(
    ("warning_policy", "statuses", "expected_passed", "expected_warnings"),
    [
        ("fail", ("no-op", "pass"), True, 0),
        ("fail", ("success", "warn"), False, 1),
        ("allow", ("success", "warn"), True, 1),
    ],
)
def test_execution_applies_platform_warning_policy(
    warning_policy: str,
    statuses: tuple[str, str],
    expected_passed: bool,
    expected_warnings: int,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(result_statuses=statuses)
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack(warning_policy=warning_policy)

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert outcome.passed is expected_passed
    assert outcome.evidence.dbt_warning_policy == warning_policy
    assert outcome.evidence.dbt_warning_count == expected_warnings
    assert runner.args is not None
    assert ("--warn-error" in runner.args) is (warning_policy == "fail")


def test_runtime_profile_renderer_uses_resolved_connection_without_leaking_secret() -> None:
    class Resolver:
        def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
            assert connection_ref == "warehouse"
            return ResolvedBindingConnection(
                credentials=CredentialsConfig(
                    host="sql.internal",
                    port=1433,
                    database="DWH",
                    schema="mart",
                    username="dpone",
                    password="private-password",
                    encrypt=True,
                    trust_server_certificate=False,
                ),
                safe_metadata={
                    "resolver": "vault_kv",
                    "resolved_version": 9,
                },
                descriptor=ResolvedConnectionDescriptor(
                    connection_type="mssql",
                    properties={},
                ),
            )

    pack = _pack()
    rendered = RuntimeDbtProfileRenderer(Resolver()).render(
        pack.profile,
        pack.adapter_runtime,
    )

    profile = rendered.content.decode("utf-8")
    assert "sql.internal" in profile
    assert "private-password" in profile
    assert "backend: pyodbc" in profile
    assert "retries: 1" in profile
    assert "login_timeout: 15" in profile
    assert "query_timeout: 300" in profile
    assert repr(rendered).find("private-password") == -1
    assert rendered.credential_versions[0].to_dict() == {
        "connection_ref": "warehouse",
        "resolver": "vault_kv",
        "resolved_version": "9",
    }
    assert rendered.redaction_values == ("private-password",)


@pytest.mark.parametrize(
    "project_yaml",
    [
        "name: analytics\nflags:\n  dbt_sqlserver_use_dbt_transactions: false\n",
        _PROJECT_YAML + "dispatch:\n  - macro_namespace: dbt\n    search_order: [analytics, dbt]\n",
    ],
)
def test_execution_rejects_project_policy_before_toolchain_or_profile(
    tmp_path: Path,
    project_yaml: str,
) -> None:
    class MustNotRender:
        def render(
            self,
            profile: DbtProfileSpec,
            adapter_runtime: DbtSqlServerRuntimePolicy,
        ) -> RenderedDbtProfile:
            raise AssertionError("invalid project policy must block profile resolution")

    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(project_yaml, encoding="utf-8")
    runner = _Runner()
    inspector = _ToolchainInspector()
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=inspector,
        profile_renderer=MustNotRender(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == "DPONE_DBT_SQLSERVER_PROJECT_POLICY_INVALID"
    assert not outcome.evidence.build_started
    assert outcome.evidence.preflight_status == "not_started"
    assert inspector.inspected_adapter is None
    assert not runner.calls
    assert evidence_path.is_file()


@pytest.mark.parametrize(
    ("core_version", "adapter_version"),
    (("1.11.12", "1.11.1"), ("1.12.3", "1.10.0")),
)
def test_execution_rejects_toolchain_mismatch_before_profile_resolution(
    core_version: str,
    adapter_version: str,
    tmp_path: Path,
) -> None:
    class MustNotRender:
        def render(
            self,
            profile: DbtProfileSpec,
            adapter_runtime: DbtSqlServerRuntimePolicy,
        ) -> RenderedDbtProfile:
            raise AssertionError("profile resolution must happen after toolchain validation")

    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner()
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(
            core_version=core_version,
            adapter_version=adapter_version,
        ),
        profile_renderer=MustNotRender(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 1
    assert outcome.evidence.code == "DPONE_DBT_EXECUTION_FAILED"
    assert (tmp_path / "evidence.json").exists()


def test_execution_rejects_workload_identity_mismatch_before_runtime_side_effects(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    pack = _pack()
    runner = _Runner()
    inspector = _ToolchainInspector()
    identity = AirflowRunIdentity(
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        workload_pack=AirflowArtifactIdentity(
            id="dbt__other__password=must-not-leak",
            sha256=pack.pack_sha256,
        ),
    )
    evidence_path = tmp_path / "evidence.json"
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=inspector,
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(evidence_path),
        clock=_clock(),
    )

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=identity,
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 1
    assert outcome.evidence.code == "DPONE_DBT_SELECTION_DRIFT"
    assert inspector.inspected_adapter is None
    assert runner.args is None
    assert "must-not-leak" not in evidence_path.read_text(encoding="utf-8")


def test_execution_never_accepts_stale_run_results(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    run_output_root = _run_output_root(tmp_path)
    pack = _pack()
    first_runner = _Runner()
    first = DbtExecutionService(
        command_runner=first_runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-a"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(first_runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence-a.json"),
        clock=_clock(),
    )
    assert first.execute(
        pack,
        runtime_root=project,
        run_output_root=run_output_root,
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    ).passed

    second_runner = _Runner(write_results=False)
    second = DbtExecutionService(
        command_runner=second_runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-b"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(second_runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence-b.json"),
        clock=_clock(),
    )
    outcome = second.execute(
        pack,
        runtime_root=project,
        run_output_root=run_output_root,
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == "COMMIT_UNKNOWN"
    assert second_runner.args is not None
    second_target = Path(second_runner.args[second_runner.args.index("--target-path") + 1])
    assert not (second_target / "run_results.json").exists()


def test_execution_rejects_run_results_contract_mismatch_with_stable_code(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(dbt_version="1.11.12")
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.exit_code == 1
    assert outcome.evidence.code == "COMMIT_UNKNOWN"


def test_execution_accepts_official_run_results_v6_optional_shape(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner()
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert outcome.passed


def test_execution_treats_partial_success_as_non_passing(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(result_statuses=("partial success", "pass"))
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )

    outcome = service.execute(
        _pack(),
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(_pack()),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == "DPONE_DBT_EXECUTION_FAILED"


@pytest.mark.parametrize(
    "result_extra_fields",
    (
        {"compiled_code": ["select 1"]},
        {"unexpected_adapter_field": "not part of run-results v6"},
    ),
)
def test_execution_rejects_invalid_or_unknown_run_result_fields(
    result_extra_fields: dict[str, object],
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    runner = _Runner(result_extra_fields=result_extra_fields)
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles-tmpfs"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json"),
        clock=_clock(),
    )
    pack = _pack()

    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=_run_output_root(tmp_path),
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )

    assert not outcome.passed
    assert outcome.evidence.code == "COMMIT_UNKNOWN"


def test_execution_pack_is_closed_strict_and_fingerprint_protected() -> None:
    payload = _pack().to_dict()

    with pytest.raises(DbtPublishingError) as unknown_exc:
        DbtExecutionPack.from_mapping({**payload, "runtime_command": "dbt build; curl example"})
    assert unknown_exc.value.code == "DPONE_DBT_PACK_INVALID"

    payload["timeout_seconds"] = True
    with pytest.raises(DbtPublishingError) as type_exc:
        DbtExecutionPack.from_mapping(payload)
    assert type_exc.value.code == "DPONE_DBT_PACK_INVALID"

    with pytest.raises(DbtPublishingError) as selection_exc:
        DbtSelectionLock.build(
            manifest_sha256=_DIGEST_A,
            toolchain_sha256=_DIGEST_B,
            invocation_context_sha256=(DbtInvocationContext.canonical().invocation_context_sha256),
            graph_contract_sha256=_DIGEST_C,
            graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
            graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
            selectors=("--profiles-dir=/stolen",),
            selected_graph_unique_ids=("model.analytics.orders",),
            expected_run_result_unique_ids=("model.analytics.orders",),
            publish_model_unique_ids=("model.analytics.orders",),
        )
    assert selection_exc.value.code == "DPONE_DBT_SELECTION_INVALID"


def test_execute_pack_rejects_malformed_query_timeout_with_stable_public_error(
    tmp_path: Path,
) -> None:
    payload = _pack().to_dict()
    payload["adapter_runtime"]["query_timeout_seconds"] = "3300"
    (tmp_path / "pack.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(DbtPublishingError) as exc_info:
        execute_dbt_pack(
            "pack.json",
            environ={},
            runtime_root=tmp_path,
            run_output_root=_run_output_root(tmp_path),
        )

    assert exc_info.value.code == "DPONE_DBT_SQLSERVER_RUNTIME_POLICY_INVALID"


def test_subprocess_runner_never_invokes_a_shell_and_redacts_bounded_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    secret = "sentinel-runtime-secret"

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = io.BytesIO(f"stdout {secret}".encode())
            self.stderr = io.BytesIO((f"stderr {secret} " + "x" * 128).encode())
            self.returncode = 23

        def wait(self, timeout: int | float | None = None) -> int:
            observed["timeout"] = timeout
            return self.returncode

        def terminate(self) -> None:
            observed["terminated"] = True

        def kill(self) -> None:
            observed["killed"] = True

    def fake_popen(args: tuple[str, ...], **kwargs: object) -> FakeProcess:
        observed["args"] = args
        observed.update(kwargs)
        return FakeProcess()

    monkeypatch.setenv("DBT_TARGET", "ambient-target-must-not-leak")
    monkeypatch.setenv("DPONE_DBT_CONTEXT", "ambient-context-must-not-leak")
    result = SubprocessDbtCommandRunner(
        max_output_bytes=64,
        popen_factory=fake_popen,
        dbt_executable="/locked/python-environment/bin/dbt",
    ).run(
        (
            "dbt",
            "build",
            "--target-path",
            str(tmp_path / "attempt" / "target"),
            "--select",
            "orders",
        ),
        cwd=tmp_path,
        timeout_seconds=10,
        redactions=(secret,),
    )

    assert result.exit_code == 23
    assert result.stdout == "stdout [REDACTED]"
    assert result.stderr.startswith("stderr [REDACTED] ")
    assert len(result.stderr.encode("utf-8")) == 64
    assert not result.stdout_truncated
    assert result.stderr_truncated
    assert observed["args"] == (
        "/locked/python-environment/bin/dbt",
        "build",
        "--target-path",
        str(tmp_path / "attempt" / "target"),
        "--select",
        "orders",
    )
    assert observed["shell"] is False
    assert observed["stdin"] is subprocess.DEVNULL
    assert observed["stdout"] is subprocess.PIPE
    assert observed["stderr"] is subprocess.PIPE
    process_environment = observed["env"]
    assert isinstance(process_environment, dict)
    assert "DBT_TARGET" not in process_environment
    assert "DPONE_DBT_CONTEXT" not in process_environment
    assert secret not in json.dumps((observed, result), default=str)


def test_execution_pack_contains_exact_supported_toolchain_contract() -> None:
    pack = _pack()

    assert pack.dbt_core_version == "1.12.3"
    assert pack.dbt_adapter_version == "1.11.1"
    assert pack.manifest_schema_version == "v12"
    assert pack.run_results_schema_version == "v6"
