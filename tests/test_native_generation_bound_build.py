"""Contract tests for the bound native build's explicit execution dependencies."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from dpone.runtime.native_generation_bound_build import BoundNativeGenerationBuild
from tests.test_dbt_runtime_execution import _attempt, _interval, _pack, _run_identity


def _inputs(tmp_path: Path) -> dict[str, object]:
    pack = _pack()
    return {
        "pack": pack,
        "runtime_root": tmp_path / "project",
        "run_output_root": tmp_path / "output",
        "run_identity": _run_identity(pack),
        "airflow_attempt": _attempt(),
        "interval": _interval(),
        "toolchain_inspector": Mock(),
        "profile_store": Mock(),
        "run_results_reader": Mock(),
        "run_results_validator": Mock(),
        "manifest_validator": Mock(),
        "workspace_attempt_lifecycle": Mock(),
        "clock": Mock(),
    }


def test_bound_build_forwards_actual_dependencies_to_existing_engine(tmp_path, monkeypatch):
    inputs = _inputs(tmp_path)
    service = Mock()
    preflight = Mock()
    monkeypatch.setattr("dpone.runtime.native_generation_bound_build.DbtExecutionService", service)
    monkeypatch.setattr("dpone.runtime.native_generation_bound_build.DbtRuntimePreflight", preflight)
    build = BoundNativeGenerationBuild(**inputs)
    service.assert_not_called()
    preflight.assert_not_called()
    assert not (tmp_path / "project").exists()
    assert not (tmp_path / "output").exists()

    runner, renderer, writer = Mock(), Mock(), Mock()
    result = build.execute(command_runner=runner, profile_renderer=renderer, evidence_writer=writer)

    assert result is service.return_value.execute.return_value
    preflight.assert_called_once_with(
        command_runner=runner,
        artifact_reader=inputs["run_results_reader"],
        manifest_validator=inputs["manifest_validator"],
    )
    service.assert_called_once_with(
        command_runner=runner,
        toolchain_inspector=inputs["toolchain_inspector"],
        profile_renderer=renderer,
        profile_store=inputs["profile_store"],
        run_results_reader=inputs["run_results_reader"],
        run_results_validator=inputs["run_results_validator"],
        preflight=preflight.return_value,
        evidence_writer=writer,
        workspace_attempt_lifecycle=inputs["workspace_attempt_lifecycle"],
        clock=inputs["clock"],
    )
    service.return_value.execute.assert_called_once_with(
        inputs["pack"],
        runtime_root=inputs["runtime_root"],
        run_output_root=inputs["run_output_root"],
        run_identity=inputs["run_identity"],
        airflow_attempt=inputs["airflow_attempt"],
        interval=inputs["interval"],
    )


def test_bound_build_requires_native_lifecycle_instead_of_legacy_fallback(tmp_path):
    inputs = _inputs(tmp_path)
    inputs["workspace_attempt_lifecycle"] = None
    with pytest.raises(ValueError, match="lifecycle"):
        BoundNativeGenerationBuild(**inputs)


def test_bound_build_snapshots_values_before_caller_mutation(tmp_path, monkeypatch):
    inputs = _inputs(tmp_path)
    original_identity = inputs["run_identity"]
    expected = original_identity.to_dict()
    build = BoundNativeGenerationBuild(**inputs)
    # Frozen DTOs protect normal callers; this also checks there is no retained
    # caller object alias at this execution boundary.
    object.__setattr__(original_identity, "release_id", "sha256:" + "e" * 64)
    service = Mock()
    monkeypatch.setattr("dpone.runtime.native_generation_bound_build.DbtExecutionService", service)
    build.execute(command_runner=Mock(), profile_renderer=Mock(), evidence_writer=Mock())
    observed = service.return_value.execute.call_args.kwargs["run_identity"]
    assert observed.to_dict() == expected
    assert observed is not original_identity


@pytest.mark.parametrize("exit_code,expected_passed", [(0, True), (7, False)])
def test_bound_build_runs_existing_engine_and_retains_native_owner(tmp_path, exit_code, expected_passed):
    from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter, LocalDbtRunResultsReader
    from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
    from dpone.adapters.dbt_runtime_profile import TemporaryDbtProfileStore
    from tests.test_dbt_runtime_execution import (
        _PROJECT_YAML,
        _clock,
        _ManifestValidator,
        _ProfileRenderer,
        _Runner,
        _ToolchainInspector,
    )
    from tests.test_native_workspace_attempt_lifecycle import _fixture

    pack, _, admission, reads, lifecycle = _fixture(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (tmp_path / "output").mkdir()
    (project / "dbt_project.yml").write_text(_PROJECT_YAML)
    inputs = _inputs(tmp_path)
    inputs.update(
        pack=pack,
        run_identity=_run_identity(pack),
        toolchain_inspector=_ToolchainInspector(),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        manifest_validator=_ManifestValidator(),
        workspace_attempt_lifecycle=lifecycle,
        clock=_clock(),
    )
    build = BoundNativeGenerationBuild(**inputs)
    runner = _Runner(exit_code=exit_code)
    delegate = LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json")
    captured = []

    class Writer:
        def write(self, evidence):
            assert admission.states == ["RUNNING"]
            captured.append(evidence)
            return delegate.write(evidence)

    outcome = build.execute(
        command_runner=runner,
        profile_renderer=_ProfileRenderer(expected_schema="base"),
        evidence_writer=Writer(),
    )
    assert outcome.passed is expected_passed
    assert outcome.exit_code == exit_code
    assert captured == [outcome.evidence]
    assert admission.states == ["RUNNING"]
    assert len(reads) == 1
    assert len(runner.calls) == 3
    assert [next(verb for verb in ("parse", "ls", "build") if verb in args) for args in runner.calls] == [
        "parse",
        "ls",
        "build",
    ]


def test_bound_build_propagates_engine_exception_without_retry(tmp_path, monkeypatch):
    from dpone.contracts.commit_unknown import CommitUnknownOutcome
    from dpone.runtime.commit_unknown import CommitUnknownError

    service = Mock()
    failure = CommitUnknownError(
        CommitUnknownOutcome(failure_boundary="target_invocation", checkpoint_state="not_advanced")
    )
    service.return_value.execute.side_effect = failure
    monkeypatch.setattr("dpone.runtime.native_generation_bound_build.DbtExecutionService", service)
    build = BoundNativeGenerationBuild(**_inputs(tmp_path))
    with pytest.raises(CommitUnknownError) as observed:
        build.execute(command_runner=Mock(), profile_renderer=Mock(), evidence_writer=Mock())
    assert observed.value is failure
    assert service.return_value.execute.call_count == 1
