"""The real dbt engine chooses one authority and never downgrades a parent."""

from types import SimpleNamespace

import pytest

from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter, LocalDbtRunResultsReader
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.adapters.dbt_runtime_profile import TemporaryDbtProfileStore
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.runtime.dbt_execution_service import DbtExecutionService
from tests.test_dbt_runtime_execution import (
    _PROJECT_YAML,
    _attempt,
    _clock,
    _interval,
    _preflight,
    _ProfileRenderer,
    _run_identity,
    _Runner,
    _ToolchainInspector,
    _v2_pack,
)


def _service(tmp_path, authority, **extra):
    runner = _Runner()
    return runner, DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=_ToolchainInspector(),
        profile_renderer=_ProfileRenderer(expected_schema="base"),
        profile_store=TemporaryDbtProfileStore(tmp_path / "profiles"),
        run_results_reader=LocalDbtRunResultsReader(),
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=_preflight(runner),
        evidence_writer=LocalDbtExecutionEvidenceWriter(tmp_path / "execution.json"),
        composition_attempt=authority,
        clock=_clock(),
        **extra,
    )


@pytest.mark.parametrize("failure", [False, True])
def test_parent_verification_runs_after_preflight_before_build(tmp_path, failure):
    events = []

    def verify_before_build(**kwargs):
        assert kwargs["manifest"]["nodes"]
        assert kwargs["pack"].schema == "dpone.dbt-execution-pack.v2"
        events.append("parent")
        if failure:
            raise CompositionAdmissionError("attempt_guard_epochs")

    runner, service = _service(tmp_path, SimpleNamespace(verify_before_build=verify_before_build))
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(_PROJECT_YAML)
    (tmp_path / "output").mkdir()
    pack = _v2_pack()
    outcome = service.execute(
        pack,
        runtime_root=project,
        run_output_root=tmp_path / "output",
        run_identity=_run_identity(pack),
        airflow_attempt=_attempt(),
        interval=_interval(),
    )
    assert events == ["parent"], outcome
    assert outcome.passed is (not failure)
    if failure:
        assert runner.args is not None and "build" not in runner.args


@pytest.mark.parametrize("native_arg", ["workspace_attempt_factory", "workspace_attempt_admission"])
def test_parent_and_native_authorities_cannot_be_mixed(tmp_path, native_arg):
    with pytest.raises(ValueError, match="authority"):
        _service(tmp_path, SimpleNamespace(verify_before_build=lambda **kwargs: None), **{native_arg: object()})
