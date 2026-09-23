"""Phase obligations are independent of record identity and external effects."""

import pytest

from dpone.contracts.mssql_tds_phase_obligations import validate_phase_obligations


def evidence(**changes):
    values = dict(
        phase=0,
        sequence=0,
        object_present=False,
        process_present=False,
        exit_code=None,
        result_present=False,
        verification_present=False,
        error_present=False,
        observation_present=False,
        parent_present=False,
    )
    return values | changes


def test_initial_intent_requires_no_external_proof():
    validate_phase_obligations(**evidence())


@pytest.mark.parametrize(
    "code,changes",
    [
        ("tds_invalid_phase_evidence", {"error_present": True}),
        ("tds_invalid_phase_identity", {"process_present": True}),
        ("tds_invalid_phase_result", {"result_present": True}),
        ("tds_invalid_phase_observation", {"observation_present": True}),
    ],
)
def test_initial_intent_cannot_claim_later_effects(code, changes):
    with pytest.raises(ValueError, match=code):
        validate_phase_obligations(**evidence(**changes))


@pytest.mark.parametrize("exit_code", [None, -1, 1, 255])
def test_verified_requires_successful_exit(exit_code):
    with pytest.raises(ValueError):
        validate_phase_obligations(
            **evidence(
                phase=6,
                sequence=6,
                object_present=True,
                process_present=True,
                exit_code=exit_code,
                result_present=True,
                verification_present=True,
                observation_present=True,
            )
        )


def test_containment_preserves_parent_requirement_after_verification():
    values = evidence(
        phase=7,
        sequence=7,
        object_present=True,
        process_present=True,
        exit_code=0,
        result_present=True,
        verification_present=True,
        observation_present=True,
        error_present=True,
    )
    with pytest.raises(ValueError, match="tds_parent_unsettled"):
        validate_phase_obligations(**values)
    validate_phase_obligations(**(values | {"parent_present": True}))


def test_record_classes_keep_their_original_defining_module():
    from dpone.contracts import mssql_tds_worker as worker

    for cls in (
        worker.TdsAttemptIdentity,
        worker.TdsAttemptState,
        worker.TdsChildExit,
        worker.Prepared,
        worker.TdsAttemptSnapshot,
    ):
        assert cls.__module__ == worker.__name__
