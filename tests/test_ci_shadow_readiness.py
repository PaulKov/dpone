from __future__ import annotations

import pytest

from dpone.contracts.ci_shadow_readiness import (
    ProvenanceFacts,
    ReadinessCode,
    ReadinessStatus,
    ReadinessSubject,
    evaluate_readiness,
)


@pytest.mark.parametrize(
    "authenticated,terminal,status,code",
    [
        (False, False, ReadinessStatus.UNVERIFIED, ReadinessCode.PROVENANCE_UNVERIFIED),
        (False, True, ReadinessStatus.UNVERIFIED, ReadinessCode.PROVENANCE_UNVERIFIED),
        (True, True, ReadinessStatus.FAIL, ReadinessCode.SOURCE_FAILURE),
        (True, False, ReadinessStatus.PASS, ReadinessCode.AUTHENTICATED),
    ],
)
def test_default_deny_decision_table(
    authenticated: bool, terminal: bool, status: ReadinessStatus, code: ReadinessCode
) -> None:
    decision = evaluate_readiness(ReadinessSubject("a" * 40), ProvenanceFacts(authenticated, terminal))
    assert (decision.status, decision.code) == (status, code)


def test_subject_rejects_noncanonical_sha() -> None:
    with pytest.raises(ValueError):
        ReadinessSubject("A" * 40)
