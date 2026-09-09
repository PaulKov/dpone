from __future__ import annotations

import pytest
from tests.agent_policy._runtime_image_promotion_helpers import (
    DIGEST,
    NEWER_DIGEST,
    VERSION,
    FakeRegistry,
    Lookup,
    RecordingJournal,
    certification_payload,
)
from tools.agent_policy import runtime_image_promotion as promotion
from tools.agent_policy import runtime_image_publication_journal as publication_journal


def test_missing_durable_journal_blocks_before_alias_mutation() -> None:
    registry = FakeRegistry([Lookup(404, {}, True)])
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
    )

    assert registry.created == []
    assert receipt.status == "FAIL"
    assert receipt.outcome == "BLOCKED"
    assert receipt.to_payload()["transitions"][0]["blocker_code"] == "PUBLICATION_JOURNAL_REQUIRED"


def test_postcondition_digest_mismatch_stops_before_next_alias() -> None:
    journal = RecordingJournal()
    registry = FakeRegistry(
        [
            Lookup(404, {}, True),
            Lookup(404, {}, True),
            Lookup(200, {"docker-content-digest": NEWER_DIGEST}, True),
        ]
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=journal,
    )

    assert receipt.status == "FAIL"
    assert receipt.outcome == "PARTIAL_ALIAS_PENDING"
    assert journal.transitions[0]["blocker_code"] == "ALIAS_POSTCONDITION_FAILED"
    assert registry.lookups == []


def test_tag_movement_between_decision_and_write_blocks_without_mutation() -> None:
    registry = FakeRegistry(
        [
            Lookup(404, {}, True),
            Lookup(200, {"docker-content-digest": NEWER_DIGEST}, True),
        ]
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    transition = receipt.to_payload()["transitions"][0]
    assert registry.created == []
    assert transition["decision"] == "BLOCKED"
    assert transition["blocker_code"] == "ALIAS_PRECONDITION_CHANGED"
    assert receipt.outcome == "BLOCKED"


def test_noop_alias_is_reobserved_before_it_can_pass() -> None:
    registry = FakeRegistry(
        [
            Lookup(200, {"docker-content-digest": DIGEST}, True),
            Lookup(200, {"docker-content-digest": NEWER_DIGEST}, True),
        ]
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    transitions = receipt.to_payload()["transitions"]
    assert [item["role"] for item in transitions] == ["version"]
    assert transitions[0]["decision"] == "BLOCKED"
    assert transitions[0]["blocker_code"] == "ALIAS_POSTCONDITION_FAILED"
    assert receipt.status == "FAIL"


def test_terminal_journal_failure_prevents_returning_a_pass_receipt() -> None:
    class FailingTerminalJournal(RecordingJournal):
        def complete(self, receipt: promotion.PublicationReceipt) -> None:
            del receipt
            raise publication_journal.PublicationJournalError("PUBLICATION_JOURNAL_WRITE_FAILED")

    candidate = Lookup(200, {"docker-content-digest": DIGEST}, True)
    latest = Lookup(200, {"docker-content-digest": DIGEST}, True, version=VERSION)
    registry = FakeRegistry([candidate, candidate, candidate, candidate, latest, latest])
    certification = promotion.validate_certification(certification_payload())

    with pytest.raises(
        publication_journal.PublicationJournalError,
        match="PUBLICATION_JOURNAL_WRITE_FAILED",
    ):
        promotion.promote_certified_image(
            certification=certification,
            certification_sha256=f"sha256:{'1' * 64}",
            registry=registry,
            journal=FailingTerminalJournal(),
        )
