from dpone.runtime.commit_unknown import (
    CommitUnknownError,
    classify_commit_unknown,
    classify_runtime_commit_unknown,
)


def test_failure_before_target_invocation_is_not_commit_unknown() -> None:
    failure = classify_commit_unknown(
        target_invocation_started=False,
        target_returned_success=False,
        expected_checkpoint_ids=frozenset({"partition-a"}),
        committed_checkpoint_ids=frozenset(),
    )

    assert failure is None


def test_target_failure_without_checkpoint_proof_is_commit_unknown() -> None:
    failure = classify_commit_unknown(
        target_invocation_started=True,
        target_returned_success=False,
        expected_checkpoint_ids=frozenset({"partition-a"}),
        committed_checkpoint_ids=frozenset(),
    )

    assert isinstance(failure, CommitUnknownError)
    assert failure.outcome.failure_boundary == "target_invocation"
    assert failure.outcome.checkpoint_state == "not_advanced"
    assert failure.safe_to_retry is False
    assert failure.operator_verification_required is True


def test_target_success_before_checkpoint_persistence_is_commit_unknown() -> None:
    failure = classify_commit_unknown(
        target_invocation_started=True,
        target_returned_success=True,
        expected_checkpoint_ids=frozenset({"partition-a"}),
        committed_checkpoint_ids=frozenset(),
    )

    assert isinstance(failure, CommitUnknownError)
    assert failure.outcome.failure_boundary == "checkpoint_persistence"
    assert failure.outcome.checkpoint_state == "not_advanced"


def test_partial_checkpoint_persistence_is_commit_unknown() -> None:
    failure = classify_commit_unknown(
        target_invocation_started=True,
        target_returned_success=True,
        expected_checkpoint_ids=frozenset({"partition-a", "partition-b"}),
        committed_checkpoint_ids=frozenset({"partition-a"}),
    )

    assert isinstance(failure, CommitUnknownError)
    assert failure.outcome.failure_boundary == "checkpoint_persistence"
    assert failure.outcome.checkpoint_state == "incomplete"


def test_complete_checkpoint_proof_is_not_commit_unknown() -> None:
    failure = classify_commit_unknown(
        target_invocation_started=True,
        target_returned_success=True,
        expected_checkpoint_ids=frozenset({"partition-a", "partition-b"}),
        committed_checkpoint_ids=frozenset({"partition-a", "partition-b"}),
    )

    assert failure is None


def test_runtime_capability_is_adapted_without_reclassifying_existing_error() -> None:
    expected = classify_commit_unknown(
        target_invocation_started=True,
        target_returned_success=False,
        expected_checkpoint_ids=frozenset({"partition-a"}),
        committed_checkpoint_ids=frozenset(),
    )
    assert expected is not None

    class Runtime:
        def commit_unknown_error(self, _context: object) -> CommitUnknownError:
            return expected

    assert (
        classify_runtime_commit_unknown(
            Runtime(),
            object(),
            RuntimeError("redacted"),
        )
        is expected
    )
    assert (
        classify_runtime_commit_unknown(
            object(),
            object(),
            expected,
        )
        is expected
    )
