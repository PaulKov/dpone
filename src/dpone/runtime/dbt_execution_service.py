"""Application service for one locked dbt invocation and its evidence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.contracts.dbt_runtime import (
    MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
    AirflowAttemptCorrelation,
    AirflowRunIdentity,
    DbtCredentialVersion,
    DbtExecutionEvidence,
    DbtExecutionInterval,
    DbtExecutionPack,
    DbtNodeOutcome,
    DbtPublishingError,
    dbt_attempt_id,
    dbt_target_binding_sha256,
    dbt_target_identity_sha256,
    validate_dbt_runtime_release_identity,
    validate_dbt_workload_identity,
)
from dpone.ports.dbt_publishing import (
    DbtCommandRunner,
    DbtExecutionEvidenceWriter,
    DbtExecutionOutcome,
    DbtProfileRenderer,
    DbtProfileStore,
    DbtRunResultsReader,
    DbtRunResultsSchemaValidator,
    DbtToolchainInspector,
)
from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.dbt_execution_policy import (
    DEFAULT_DBT_RUN_OUTPUT_ROOT,
    aware_timestamp,
    build_dbt_command,
    confined_dbt_project_directory,
    dbt_failure_code,
    discard_previous_dbt_run_results,
    prepare_dbt_build_output_paths,
    prepare_dbt_output_paths,
    runtime_failure_code,
    validate_dbt_toolchain,
)
from dpone.runtime.dbt_preflight import DbtRuntimePreflight
from dpone.runtime.dbt_run_results import (
    MAX_DBT_RUN_RESULTS_BYTES,
    ParsedDbtRunResults,
    dbt_node_outcomes,
    read_dbt_run_results,
)
from dpone.runtime.dbt_sqlserver_project_policy import (
    validate_runtime_sqlserver_project_policy,
)
from dpone.runtime.dbt_workspace_attempt_lifecycle import DbtWorkspaceAttemptLifecycle

if TYPE_CHECKING:
    from dpone.ports.composition_dbt import CompositionDbtBuildAuthority
    from dpone.ports.dbt_workspace_attempt import (
        DbtWorkspaceAttemptAdmissionPort,
        DbtWorkspaceAttemptRequest,
        DbtWorkspaceAttemptRequestFactoryPort,
        DbtWorkspaceAttemptTerminalState,
    )


class DbtExecutionService:
    """Resolve profile material, invoke dbt once, validate results and persist evidence."""

    def __init__(
        self,
        *,
        command_runner: DbtCommandRunner,
        toolchain_inspector: DbtToolchainInspector,
        profile_renderer: DbtProfileRenderer,
        profile_store: DbtProfileStore,
        run_results_reader: DbtRunResultsReader,
        run_results_validator: DbtRunResultsSchemaValidator,
        preflight: DbtRuntimePreflight,
        evidence_writer: DbtExecutionEvidenceWriter,
        workspace_attempt_factory: DbtWorkspaceAttemptRequestFactoryPort | None = None,
        workspace_attempt_admission: DbtWorkspaceAttemptAdmissionPort | None = None,
        composition_attempt: CompositionDbtBuildAuthority | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._command_runner = command_runner
        self._toolchain_inspector = toolchain_inspector
        self._profile_renderer = profile_renderer
        self._profile_store = profile_store
        self._run_results_reader = run_results_reader
        self._run_results_validator = run_results_validator
        self._preflight = preflight
        self._evidence_writer = evidence_writer
        self._workspace_attempts = DbtWorkspaceAttemptLifecycle(
            run_results_reader=run_results_reader,
            request_factory=workspace_attempt_factory,
            admission=workspace_attempt_admission,
            composition_attempt=composition_attempt,
        )
        self._clock = clock or (lambda: datetime.now(UTC))

    def execute(
        self,
        pack: DbtExecutionPack,
        *,
        runtime_root: Path,
        run_output_root: Path = DEFAULT_DBT_RUN_OUTPUT_ROOT,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        interval: DbtExecutionInterval,
    ) -> DbtExecutionOutcome:
        """Execute exact argv and return the real dbt process exit code."""

        validated_pack = DbtExecutionPack.from_mapping(pack.to_dict())
        validated_identity = AirflowRunIdentity.from_mapping(run_identity.to_dict())
        validated_attempt = AirflowAttemptCorrelation.from_mapping(airflow_attempt.to_dict())
        started_at = aware_timestamp(self._clock())
        credential_versions: tuple[DbtCredentialVersion, ...] = ()
        preflight_status = "not_started"
        build_started = False
        dbt_exit_code: int | None = None
        workspace_attempt: DbtWorkspaceAttemptRequest | None = None
        try:
            validate_dbt_workload_identity(validated_pack, validated_identity)
            project_dir = confined_dbt_project_directory(
                Path(runtime_root),
                validated_pack.project_subdir,
            )
            project_policy_issue = validate_runtime_sqlserver_project_policy(
                project_dir,
                expected_flags=(validated_pack.adapter_policy.required_project_flags),
                maximum=MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
            )
            if project_policy_issue is not None:
                raise DbtPublishingError(*project_policy_issue)
            validate_dbt_toolchain(
                validated_pack,
                self._toolchain_inspector.inspect(validated_pack.profile.adapter_type),
            )
            invocation_profile = validated_pack.invocation_profile()
            rendered = self._profile_renderer.render(
                invocation_profile,
                validated_pack.adapter_runtime,
            )
            credential_versions = rendered.credential_versions
            if rendered.logical_target_sha256 != dbt_target_identity_sha256(invocation_profile):
                raise DbtPublishingError(
                    "DPONE_DBT_TARGET_IDENTITY_MISMATCH",
                    "Rendered dbt target identity differs from the release",
                )
            output_paths = prepare_dbt_output_paths(
                Path(run_output_root),
                validated_pack.target_path,
                attempt_id=dbt_attempt_id(validated_identity, validated_attempt),
            )
            with self._profile_store.materialize(rendered.content) as profile_path:
                try:
                    self._preflight.verify(
                        validated_pack,
                        project_dir=project_dir,
                        profile_path=profile_path,
                        output_paths=output_paths,
                        interval_vars_json=interval.dbt_vars_json(),
                        redactions=rendered.redaction_values,
                    )
                except Exception:
                    preflight_status = "failed"
                    raise
                preflight_status = "passed"
                workspace_attempt = self._workspace_attempts.admit(
                    validated_pack,
                    output_paths=output_paths,
                    run_identity=validated_identity,
                    airflow_attempt=validated_attempt,
                )
                prepare_dbt_build_output_paths(output_paths)
                discard_previous_dbt_run_results(
                    output_paths.attempt,
                    validated_pack.target_path,
                )
                build_started = True
                result = self._command_runner.run(
                    build_dbt_command(
                        validated_pack,
                        project_dir=project_dir,
                        profile_path=profile_path,
                        target_path=output_paths.target,
                        log_path=output_paths.logs,
                        interval_vars_json=interval.dbt_vars_json(),
                    ),
                    cwd=project_dir,
                    timeout_seconds=validated_pack.timeout_seconds,
                    redactions=rendered.redaction_values,
                )
                dbt_exit_code = result.exit_code
        except Exception as exc:  # noqa: BLE001 - failures require durable evidence
            code = "COMMIT_UNKNOWN" if build_started else runtime_failure_code(exc)
            code = self._workspace_attempts.terminalize(
                workspace_attempt,
                state="COMMIT_UNKNOWN" if build_started else "FAILED",
                fallback_code=code,
                build_started=build_started,
            )
            return self._outcome(
                pack=validated_pack,
                run_identity=validated_identity,
                airflow_attempt=validated_attempt,
                started_at=started_at,
                dbt_exit_code=dbt_exit_code,
                code=code,
                parsed=None,
                credential_versions=credential_versions,
                preflight_status=preflight_status,
                build_started=build_started,
            )
        try:
            parsed, results_error = read_dbt_run_results(
                validated_pack,
                output_paths,
                reader=self._run_results_reader,
                validator=self._run_results_validator,
            )
        except Exception:  # noqa: BLE001 - post-build proof failures are ambiguous
            self._workspace_attempts.terminalize(
                workspace_attempt,
                state="COMMIT_UNKNOWN",
                fallback_code="COMMIT_UNKNOWN",
                build_started=True,
            )
            return self._outcome(
                pack=validated_pack,
                run_identity=validated_identity,
                airflow_attempt=validated_attempt,
                started_at=started_at,
                dbt_exit_code=result.exit_code,
                code="COMMIT_UNKNOWN",
                parsed=None,
                credential_versions=credential_versions,
                preflight_status=preflight_status,
                build_started=build_started,
            )
        passed = (
            result.exit_code == 0
            and results_error is None
            and parsed is not None
            and parsed.passes(validated_pack.dbt_warning_policy)
        )
        code = (
            "DPONE_DBT_EXECUTION_PASSED"
            if passed
            else ("COMMIT_UNKNOWN" if results_error is not None else dbt_failure_code(result.exit_code, None))
        )
        terminal_state: DbtWorkspaceAttemptTerminalState = (
            "SUCCEEDED" if passed else ("COMMIT_UNKNOWN" if code == "COMMIT_UNKNOWN" else "FAILED")
        )
        code = self._workspace_attempts.terminalize(
            workspace_attempt,
            state=terminal_state,
            fallback_code=code,
            build_started=True,
        )
        if code == "COMMIT_UNKNOWN":
            passed = False
        return self._outcome(
            pack=validated_pack,
            run_identity=validated_identity,
            airflow_attempt=validated_attempt,
            started_at=started_at,
            dbt_exit_code=result.exit_code,
            code=code,
            parsed=parsed,
            credential_versions=credential_versions,
            preflight_status=preflight_status,
            build_started=build_started,
            passed=passed,
        )

    def _outcome(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        started_at: str,
        dbt_exit_code: int | None,
        code: str,
        parsed: ParsedDbtRunResults | None,
        credential_versions: tuple[DbtCredentialVersion, ...],
        preflight_status: str,
        build_started: bool,
        passed: bool = False,
    ) -> DbtExecutionOutcome:
        evidence = DbtExecutionEvidence(
            status="passed" if passed else "failed",
            code=code,
            workflow_id=pack.workflow_id,
            release_id=run_identity.release_id,
            deployment_id=run_identity.deployment_id,
            workload_pack_sha256=run_identity.workload_pack.sha256,
            project_bundle_sha256=pack.project_bundle_sha256,
            manifest_sha256=pack.selection_lock.manifest_sha256,
            selection_sha256=pack.selection_lock.selection_sha256,
            toolchain_sha256=pack.selection_lock.toolchain_sha256,
            invocation_context_sha256=(pack.invocation_context.invocation_context_sha256),
            logical_target_sha256=dbt_target_identity_sha256(pack.profile),
            target_binding_sha256=dbt_target_binding_sha256(
                pack,
                run_identity,
            ),
            adapter_runtime=pack.adapter_runtime,
            adapter_policy_sha256=pack.adapter_policy.adapter_policy_sha256,
            graph_policy_sha256=pack.selection_lock.graph_policy_sha256,
            preflight_status=preflight_status,
            build_started=build_started,
            dbt_exit_code=dbt_exit_code,
            dbt_warning_policy=pack.dbt_warning_policy,
            dbt_warning_count=(parsed.warning_count if parsed is not None else 0),
            dbt_schema_version=parsed.schema_version if parsed is not None else None,
            dbt_version=parsed.dbt_version if parsed is not None else None,
            invocation_id=parsed.invocation_id if parsed is not None else None,
            started_at=started_at,
            finished_at=aware_timestamp(self._clock()),
            airflow=airflow_attempt.to_dict(),
            credential_versions=credential_versions,
            nodes=dbt_node_outcomes(parsed, factory=DbtNodeOutcome),
            recovery=(
                CommitUnknownOutcome(
                    failure_boundary="target_invocation",
                    checkpoint_state="not_advanced",
                ).to_jsonable()
                if code == "COMMIT_UNKNOWN"
                else None
            ),
        )
        try:
            self._evidence_writer.write(evidence)
        except Exception as exc:
            if build_started:
                raise CommitUnknownError(
                    CommitUnknownOutcome(
                        failure_boundary="target_invocation",
                        checkpoint_state="not_advanced",
                    )
                ) from exc
            raise
        outcome_exit_code = dbt_exit_code if dbt_exit_code is not None and dbt_exit_code != 0 else (0 if passed else 1)
        return DbtExecutionOutcome(
            exit_code=outcome_exit_code,
            evidence=evidence,
        )


__all__ = [
    "DEFAULT_DBT_RUN_OUTPUT_ROOT",
    "DbtExecutionInterval",
    "DbtExecutionOutcome",
    "DbtExecutionService",
    "MAX_DBT_RUN_RESULTS_BYTES",
    "validate_dbt_runtime_release_identity",
]
