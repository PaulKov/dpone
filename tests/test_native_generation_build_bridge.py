"""Reserved build identity, actual writer provenance and explicit failure paths."""

from dataclasses import replace
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.contracts.native_delivery import GenerationReservation
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.ports.dbt_publishing import DbtExecutionOutcome
from dpone.runtime.native_generation_build_bridge import NativeGenerationBuildRejected, ReservedDbtBuildBridge
from tests.test_native_generation_build_evidence import writer_fixture


def _fixture(tmp_path):
    # Reuse actual writer/artifacts but run the synthetic invocation only when
    # the bridge dispatches the bound build, not during fixture construction.
    with patch("tests.native_trusted_dbt_fixtures.InvocationFixture.complete"):
        writer, invocation, evidence = writer_fixture(tmp_path)
    reservation = GenerationReservation(
        invocation.executor.generation_id, invocation.executor.guard_epoch, 1, invocation.executor.reservation
    )
    build = Mock()

    def execute(*, command_runner, profile_renderer, evidence_writer):
        assert command_runner is invocation.recorder
        assert evidence_writer is writer
        invocation.complete()
        evidence_writer.write(evidence)
        return DbtExecutionOutcome(0, evidence)

    build.execute.side_effect = execute
    inputs = dict(
        build=build,
        command_runner=invocation.recorder,
        profile_renderer=Mock(),
        evidence_writer=writer,
        originals=invocation.store,
        bindings=invocation.store,
        subject=invocation.store.subject,
        publish_original=invocation.store.publish,
        invocation=invocation.recorder,
    )
    return inputs, build, writer, invocation, evidence, reservation


def test_bridge_requires_identical_actual_runner_before_execution(tmp_path):
    inputs, build, _, _, _, _ = _fixture(tmp_path)
    inputs["command_runner"] = Mock()
    with pytest.raises(NativeSourceCustodyError, match="runner"):
        ReservedDbtBuildBridge(**inputs)
    build.execute.assert_not_called()
    inputs["profile_renderer"].render.assert_not_called()


def test_bridge_returns_only_authenticated_actual_writer_cohort(tmp_path):
    inputs, build, writer, invocation, _, reservation = _fixture(tmp_path)
    bridge = ReservedDbtBuildBridge(**inputs)
    receipt = bridge(reservation, invocation.executor)
    completion = writer.require_build_completion()
    assert receipt.outcome == "SUCCEEDED"
    assert receipt.executor_invocation_id == invocation.executor.invocation_id
    assert receipt.build_evidence == completion.build_evidence
    assert receipt.artifact_inventory == completion.artifact_inventory
    assert receipt.termination == completion.termination
    build.execute.assert_called_once()
    with pytest.raises(NativeSourceCustodyError, match="repeat"):
        bridge(reservation, invocation.executor)
    build.execute.assert_called_once()


@pytest.mark.parametrize("wrong", ["reservation", "executor"])
def test_bridge_rejects_wrong_owner_before_credentials_or_build(tmp_path, wrong):
    inputs, build, _, invocation, _, reservation = _fixture(tmp_path)
    executor = invocation.executor
    if wrong == "reservation":
        reservation = replace(reservation, guard_epoch=reservation.guard_epoch + 1)
    else:
        executor = replace(executor, invocation_id=uuid4())
    before = len(invocation.delegate.calls)
    with pytest.raises(NativeSourceCustodyError):
        ReservedDbtBuildBridge(**inputs)(reservation, executor)
    assert len(invocation.delegate.calls) == before
    build.execute.assert_not_called()
    inputs["profile_renderer"].render.assert_not_called()


@pytest.mark.parametrize("code,state", [("DPONE_DBT_EXECUTION_FAILED", "FAILED"), ("COMMIT_UNKNOWN", "UNKNOWN")])
def test_bridge_reports_failed_outcome_without_positive_receipt(tmp_path, code, state):
    inputs, build, _, invocation, evidence, reservation = _fixture(tmp_path)
    recovery = (
        CommitUnknownOutcome(failure_boundary="target_invocation", checkpoint_state="not_advanced").to_jsonable()
        if state == "UNKNOWN"
        else None
    )
    failed = DbtExecutionOutcome(1, replace(evidence, status="failed", code=code, dbt_exit_code=1, recovery=recovery))
    build.execute.side_effect = None
    build.execute.return_value = failed
    with pytest.raises(NativeGenerationBuildRejected) as observed:
        ReservedDbtBuildBridge(**inputs)(reservation, invocation.executor)
    assert observed.value.outcome is failed
    assert observed.value.state == state
    build.execute.assert_called_once()


def test_bridge_rejects_substituted_successful_evidence(tmp_path):
    inputs, build, writer, invocation, evidence, reservation = _fixture(tmp_path)

    def execute(**kwargs):
        invocation.complete()
        writer.write(evidence)
        return DbtExecutionOutcome(0, replace(evidence, finished_at="2026-09-15T00:00:01Z"))

    build.execute.side_effect = execute
    with pytest.raises(NativeSourceCustodyError, match="evidence|bytes differ"):
        ReservedDbtBuildBridge(**inputs)(reservation, invocation.executor)


@pytest.mark.parametrize("owner", ["recorder", "writer"])
def test_owner_identity_check_is_pure_and_compares_full_descriptor(tmp_path, owner):
    _, _, writer, invocation, _, _ = _fixture(tmp_path)
    actual = invocation.recorder if owner == "recorder" else writer
    invocation.store.fail_reads = True
    actual.require_executor(invocation.executor)
    with pytest.raises(NativeSourceCustodyError):
        actual.require_executor(replace(invocation.executor, invocation_id=uuid4()))


@pytest.mark.parametrize("exit_code", [False, 1, "0"])
def test_bridge_rejects_non_exact_positive_outcome_code(tmp_path, exit_code):
    inputs, build, _, invocation, evidence, reservation = _fixture(tmp_path)
    build.execute.side_effect = None
    build.execute.return_value = DbtExecutionOutcome(exit_code, evidence)
    with pytest.raises(NativeSourceCustodyError, match="inconsistent positive"):
        ReservedDbtBuildBridge(**inputs)(reservation, invocation.executor)


def test_bridge_requires_actual_positive_writer_not_successful_dto_only(tmp_path):
    inputs, build, _, invocation, evidence, reservation = _fixture(tmp_path)
    build.execute.side_effect = None
    build.execute.return_value = DbtExecutionOutcome(0, evidence)
    with pytest.raises(NativeSourceCustodyError, match="no complete positive cohort"):
        ReservedDbtBuildBridge(**inputs)(reservation, invocation.executor)


def test_bridge_rejects_unavailable_originals_before_build(tmp_path):
    inputs, build, _, invocation, _, reservation = _fixture(tmp_path)
    invocation.store.fail_reads = True
    with pytest.raises(OSError, match="unavailable original"):
        ReservedDbtBuildBridge(**inputs)(reservation, invocation.executor)
    build.execute.assert_not_called()
    inputs["profile_renderer"].render.assert_not_called()
    assert invocation.delegate.calls == []


@pytest.mark.parametrize("failure", [OSError("lost acknowledgement"), KeyboardInterrupt()])
def test_bridge_preserves_exception_and_blocks_local_redispatch(tmp_path, failure):
    inputs, build, _, invocation, _, reservation = _fixture(tmp_path)
    build.execute.side_effect = failure
    bridge = ReservedDbtBuildBridge(**inputs)
    with pytest.raises(type(failure)) as observed:
        bridge(reservation, invocation.executor)
    assert observed.value is failure
    with pytest.raises(NativeSourceCustodyError, match="repeat"):
        bridge(reservation, invocation.executor)
    build.execute.assert_called_once()
