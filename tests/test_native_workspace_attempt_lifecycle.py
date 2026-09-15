"""Native execution retains an existing physical attempt through evidence writing."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.adapters.dbt_runtime import (
    LocalDbtExecutionEvidenceWriter,
    LocalDbtRunResultsReader,
    OfficialDbtRunResultsValidator,
    TemporaryDbtProfileStore,
)
from dpone.adapters.dbt_workspace_attempt_request import DbtWorkspaceAttemptRequestFactory
from dpone.contracts.dbt_runtime import AirflowDeploymentIdentity
from dpone.runtime.dbt_execution_service import DbtExecutionService
from dpone.runtime.native_workspace_attempt_lifecycle import NativeWorkspaceAttemptLifecycle
from tests.test_dbt_runtime_execution import (
    _PROJECT_YAML,
    _attempt,
    _interval,
    _preflight,
    _preflight_manifest,
    _ProfileRenderer,
    _run_identity,
    _Runner,
    _ToolchainInspector,
    _v2_pack,
    _WorkspaceAttemptAdmission,
)


def _factory(pack):
    identity = _run_identity(pack)
    return DbtWorkspaceAttemptRequestFactory(
        AirflowDeploymentIdentity(
            release_id=identity.release_id,
            deployment_id=identity.deployment_id,
            activation_id="164a3c74-cf85-4a4a-a087-07c9b07050ff",
        )
    )


def _fixture(tmp_path: Path):
    pack = _v2_pack()
    factory = _factory(pack)
    request = factory.build(
        pack=pack, manifest=_preflight_manifest(), run_identity=_run_identity(pack), airflow_attempt=_attempt()
    )
    admission = _WorkspaceAttemptAdmission()
    receipt = admission.admit(request)
    reads = []

    def current(value):
        reads.append(value)
        assert value == request
        return receipt

    lifecycle = NativeWorkspaceAttemptLifecycle(
        request=request,
        receipt=receipt,
        request_factory=factory,
        run_results_reader=LocalDbtRunResultsReader(),
        require_current_running=current,
    )
    return pack, request, admission, reads, lifecycle


def _service(tmp_path: Path, lifecycle, admission, *, fail_evidence=False):
    project = tmp_path / "project"
    project.mkdir()
    (tmp_path / "output").mkdir()
    (project / "dbt_project.yml").write_text(_PROJECT_YAML)
    runner = _Runner()
    delegate = LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json")

    class Writer:
        def write(self, evidence):
            assert admission.states == ["RUNNING"]
            if fail_evidence:
                raise OSError("evidence unavailable")
            return delegate.write(evidence)

    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(expected_schema="base"),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=Writer(),
        workspace_attempt_lifecycle=lifecycle,
        clock=lambda: datetime.now(UTC),
    )
    return service, project, runner


def _execute(service, project, pack, tmp_path):
    return service.execute(
        pack,
        runtime_root=project,
        run_output_root=tmp_path / "output",
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )


def test_native_success_retains_running_owner_during_evidence(tmp_path: Path):
    pack, _, admission, reads, lifecycle = _fixture(tmp_path)
    service, project, _ = _service(tmp_path, lifecycle, admission)
    assert _execute(service, project, pack, tmp_path).passed
    assert len(reads) == 1
    assert admission.states == ["RUNNING"]


def test_native_second_execution_cannot_dispatch_build(tmp_path: Path):
    pack, _, admission, _, lifecycle = _fixture(tmp_path)
    service, project, runner = _service(tmp_path, lifecycle, admission)
    assert _execute(service, project, pack, tmp_path).passed
    from dpone.contracts.dbt_runtime import DbtPublishingError

    with pytest.raises(DbtPublishingError, match="evidence already exists"):
        _execute(service, project, pack, tmp_path)
    assert admission.states == ["RUNNING"]
    assert sum("build" in args for args in runner.calls) == 1


def test_native_evidence_failure_never_terminalizes_owner(tmp_path: Path):
    from dpone.runtime.commit_unknown import CommitUnknownError

    pack, _, admission, _, lifecycle = _fixture(tmp_path)
    service, project, _ = _service(tmp_path, lifecycle, admission, fail_evidence=True)
    with pytest.raises(CommitUnknownError):
        _execute(service, project, pack, tmp_path)
    assert admission.states == ["RUNNING"]


def test_recording_exception_after_build_preserves_unknown_evidence(tmp_path: Path):
    import json

    pack, _, admission, _, lifecycle = _fixture(tmp_path)

    class BrokenRecorder:
        admit = lifecycle.admit

        def record_execution_outcome(self, *args, **kwargs):
            raise OSError("record failed")

    service, project, _ = _service(tmp_path, BrokenRecorder(), admission)
    result = _execute(service, project, pack, tmp_path)
    assert not result.passed
    assert json.loads((tmp_path / "evidence.json").read_text())["code"] == "COMMIT_UNKNOWN"
    assert admission.states == ["RUNNING"]


def test_conflicting_native_outcome_cannot_replace_first_outcome(tmp_path: Path):
    from dpone.contracts.dbt_runtime import DbtPublishingError

    pack, request, admission, _, lifecycle = _fixture(tmp_path)
    service, project, _ = _service(tmp_path, lifecycle, admission)
    assert _execute(service, project, pack, tmp_path).passed
    assert (
        lifecycle.record_execution_outcome(
            request, state="SUCCEEDED", fallback_code="DPONE_DBT_EXECUTION_PASSED", build_started=True
        )
        == "DPONE_DBT_EXECUTION_PASSED"
    )
    with pytest.raises(DbtPublishingError):
        lifecycle.record_execution_outcome(request, state="FAILED", fallback_code="failed", build_started=True)


def test_explicit_lifecycle_rejects_ambiguous_admission_configuration():
    with pytest.raises(ValueError, match="cannot be combined"):
        DbtExecutionService(
            command_runner=None,
            toolchain_inspector=None,
            profile_renderer=None,
            profile_store=None,
            run_results_reader=None,
            run_results_validator=None,
            preflight=None,
            evidence_writer=None,
            workspace_attempt_lifecycle=object(),
            workspace_attempt_factory=object(),
        )


def test_failed_build_retains_physical_owner(tmp_path: Path):
    pack, _, admission, _, lifecycle = _fixture(tmp_path)
    service, project, runner = _service(tmp_path, lifecycle, admission)
    runner.exit_code = 1
    assert not _execute(service, project, pack, tmp_path).passed
    assert admission.states == ["RUNNING"]


def test_current_receipt_mismatch_blocks_build(tmp_path: Path):
    from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
    from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptReceipt

    pack, request, admission, _, _ = _fixture(tmp_path)
    retained = admission._receipt(request, "RUNNING")
    admission.states[:] = ["RUNNING"]
    changed = DbtWorkspaceAttemptReceipt.build(
        request=request,
        state="RUNNING",
        guard_epochs=(DbtWorkspaceGuardEpoch("mssql://warehouse/DWH/mart/orders", 8),),
    )
    lifecycle = NativeWorkspaceAttemptLifecycle(
        request=request,
        receipt=retained,
        request_factory=_factory(pack),
        run_results_reader=LocalDbtRunResultsReader(),
        require_current_running=lambda _: changed,
    )
    service, project, runner = _service(tmp_path, lifecycle, admission)
    assert not _execute(service, project, pack, tmp_path).passed
    assert not any("build" in args for args in runner.calls)
    assert admission.states == ["RUNNING"]


def test_changed_airflow_attempt_cannot_reuse_retained_physical_owner(tmp_path: Path):
    from dataclasses import replace

    pack, _, admission, reads, lifecycle = _fixture(tmp_path)
    service, project, runner = _service(tmp_path, lifecycle, admission)
    result = service.execute(
        pack,
        runtime_root=project,
        run_output_root=tmp_path / "output",
        run_identity=_run_identity(pack),
        airflow_attempt=replace(_attempt(), try_number=2),
        interval=_interval(),
    )
    assert not result.passed
    assert reads == []
    assert not any("build" in args for args in runner.calls)


def test_native_v1_is_rejected_before_manifest_read(tmp_path: Path):
    from dpone.contracts.dbt_runtime import DbtPublishingError
    from tests.test_dbt_runtime_execution import _pack

    _, _, _, reads, lifecycle = _fixture(tmp_path)
    pack = _pack()
    with pytest.raises(DbtPublishingError, match="V2"):
        lifecycle.admit(pack, output_paths=None, run_identity=_run_identity(pack), airflow_attempt=_attempt())
    assert reads == []


def test_retained_write_subset_mismatch_blocks_build_before_owner_read(tmp_path: Path):
    from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest

    pack, original, admission, _, _ = _fixture(tmp_path)
    request = DbtWorkspaceAttemptRequest.build(
        activation_id=original.activation_id,
        attempt_id=original.attempt_id,
        workflow_id=original.workflow_id,
        write_subjects=original.write_subjects[:-1],
    )
    receipt = admission._receipt(request, "RUNNING")
    admission.states[:] = ["RUNNING"]

    def forbidden_read(_):
        pytest.fail("request mismatch must fail before current-owner read")

    lifecycle = NativeWorkspaceAttemptLifecycle(
        request=request,
        receipt=receipt,
        request_factory=_factory(pack),
        run_results_reader=LocalDbtRunResultsReader(),
        require_current_running=forbidden_read,
    )
    service, project, runner = _service(tmp_path, lifecycle, admission)
    assert not _execute(service, project, pack, tmp_path).passed
    assert not any("build" in args for args in runner.calls)
