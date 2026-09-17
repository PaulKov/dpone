"""Pure development delivery authority; fixtures are synthetic and offline."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.contracts.development_delivery_authority import (
    DevelopmentAuthorityError,
    DevelopmentAuthorityReceipt,
    DevelopmentExecutionSubject,
)

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
COMMIT = "1" * 40
NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


def authority(*, execution_subjects: tuple[DevelopmentExecutionSubject, ...] = ()) -> DevelopmentAuthorityReceipt:
    return DevelopmentAuthorityReceipt(
        policy_sha256=DIGEST,
        grant_sha256=OTHER_DIGEST,
        signature_subject_sha256="sha256:" + "c" * 64,
        environment="development",
        source_repository_sha256="sha256:" + "d" * 64,
        source_commit=COMMIT,
        not_before="2026-09-17T11:00:00Z",
        expires_at="2026-09-18T11:00:00Z",
        revocation_epoch=7,
        max_workloads=4,
        max_source_bytes=1024,
        execution_subjects=execution_subjects,
    )


def test_delivery_binds_exact_source_limits_clock_and_revocation() -> None:
    receipt = authority()

    receipt.require_delivery(
        source_repository_sha256="sha256:" + "d" * 64,
        source_commit=COMMIT,
        workload_ids=("alpha", "beta"),
        source_bytes=1000,
        now=NOW,
        current_revocation_epoch=7,
    )

    for changes in (
        {"source_commit": "2" * 40},
        {"workload_ids": ("alpha", "beta", "gamma", "delta", "epsilon")},
        {"source_bytes": 1025},
        {"current_revocation_epoch": 8},
        {"now": datetime(2026, 9, 18, 11, 0, tzinfo=UTC)},
    ):
        arguments = {
            "source_repository_sha256": "sha256:" + "d" * 64,
            "source_commit": COMMIT,
            "workload_ids": ("alpha", "beta"),
            "source_bytes": 1000,
            "now": NOW,
            "current_revocation_epoch": 7,
        }
        arguments.update(changes)
        with pytest.raises(DevelopmentAuthorityError):
            receipt.require_delivery(**arguments)


def test_current_authority_rechecks_revocation_and_time() -> None:
    receipt = authority()
    receipt.require_current(now=NOW, current_revocation_epoch=7)
    with pytest.raises(DevelopmentAuthorityError, match="revoked"):
        receipt.require_current(now=NOW, current_revocation_epoch=8)
    with pytest.raises(DevelopmentAuthorityError, match="expired"):
        receipt.require_current(now=datetime(2026, 9, 18, 11, 0, tzinfo=UTC), current_revocation_epoch=7)


def test_delivery_does_not_implicitly_authorize_execution() -> None:
    receipt = authority()
    subject = DevelopmentExecutionSubject("alpha", "runtime")

    assert receipt.allows_execution(subject) is False
    assert replace(receipt, execution_subjects=(subject,)).allows_execution(subject) is True
    assert (
        replace(receipt, execution_subjects=(subject,)).allows_execution(
            DevelopmentExecutionSubject("alpha", "pre_hook", "prepare_source")
        )
        is False
    )


@pytest.mark.parametrize(
    "subject",
    [
        DevelopmentExecutionSubject("alpha", "runtime"),
        DevelopmentExecutionSubject("alpha", "pre_hook", "prepare_source"),
    ],
)
def test_execution_subject_round_trip_is_closed(subject: DevelopmentExecutionSubject) -> None:
    assert DevelopmentExecutionSubject.from_dict(subject.to_dict()) == subject
    with pytest.raises(DevelopmentAuthorityError):
        DevelopmentExecutionSubject.from_dict({**subject.to_dict(), "credential": "secret"})
