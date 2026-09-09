from __future__ import annotations

import pytest
from tests.agent_policy._runtime_image_promotion_helpers import (
    COMMIT_SHA,
    DIGEST,
    IMAGE,
    NEWER_DIGEST,
    VERSION,
    Command,
    FakeRegistry,
    Lookup,
    RecordingJournal,
    certification_identity,
    certification_payload,
)
from tools.agent_policy import runtime_image_promotion as promotion


def test_promotion_preserves_only_the_digest_bound_certified_newer_release() -> None:
    newer_version = "0.74.0"
    trusted = promotion.validate_certification(
        certification_payload(
            version=newer_version,
            digest=NEWER_DIGEST,
            commit_sha="d" * 40,
        )
    )
    candidate = Lookup(200, {"docker-content-digest": DIGEST}, True)
    newer = Lookup(
        200,
        {"docker-content-digest": NEWER_DIGEST},
        True,
        version=newer_version,
    )
    registry = FakeRegistry([candidate, candidate, candidate, candidate, newer, newer])
    certification = promotion.validate_certification(certification_payload(), expected=certification_identity())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
        trusted_latest_certification=trusted,
    )

    assert receipt.status == "PASS"
    assert receipt.to_payload()["transitions"][-1]["decision"] == "PRESERVED_NEWER"
    assert registry.created == []


def test_promotion_repairs_aliases_and_verifies_each_write() -> None:
    absent = Lookup(404, {}, True)
    present = Lookup(200, {"docker-content-digest": DIGEST}, True)
    latest = Lookup(200, {"docker-content-digest": DIGEST}, True, version=VERSION)
    registry = FakeRegistry(
        [
            absent,
            absent,
            present,
            absent,
            absent,
            present,
            absent,
            absent,
            latest,
        ]
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    assert receipt.status == "PASS"
    assert [transition["decision"] for transition in receipt.to_payload()["transitions"]] == [
        "CREATED",
        "CREATED",
        "CREATED",
    ]
    assert registry.created == [
        (f"{IMAGE}:{VERSION}", DIGEST),
        (f"{IMAGE}:sha-{COMMIT_SHA[:12]}", DIGEST),
        (f"{IMAGE}:latest", DIGEST),
    ]


def test_promotion_writes_truthful_partial_failure_receipt() -> None:
    present = Lookup(200, {"docker-content-digest": DIGEST}, True)
    conflict = Lookup(200, {"docker-content-digest": NEWER_DIGEST}, True)
    registry = FakeRegistry([present, present, conflict])
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    assert receipt.status == "FAIL"
    assert receipt.outcome == "PARTIAL_ALIAS_PENDING"
    assert receipt.to_payload()["transitions"][-1]["decision"] == "BLOCKED"

    false_partial = receipt.to_payload()
    false_partial["transitions"][0]["after"]["digest"] = NEWER_DIGEST
    with pytest.raises(promotion.ContractError):
        promotion.validate_publication(false_partial)


def test_failed_first_write_is_truthfully_partial_pending() -> None:
    registry = FakeRegistry(
        [
            Lookup(404, {}, True),
            Lookup(404, {}, True),
            Lookup(404, {}, True),
        ],
        [Command(ok=False, error_code="IMAGETOOLS_CREATE_FAILED")],
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    assert receipt.status == "FAIL"
    assert receipt.outcome == "PARTIAL_ALIAS_PENDING"


def test_mutation_is_journaled_before_next_alias_observation() -> None:
    events: list[str] = []
    absent = Lookup(404, {}, True)
    present = Lookup(200, {"docker-content-digest": DIGEST}, True)
    latest = Lookup(200, {"docker-content-digest": DIGEST}, True, version=VERSION)
    registry = FakeRegistry(
        [
            absent,
            absent,
            present,
            absent,
            absent,
            present,
            absent,
            absent,
            latest,
        ],
        events=events,
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(events),
    )

    assert receipt.status == "PASS"
    assert events.index("journal:version") < events.index(f"lookup:{IMAGE}:sha-{COMMIT_SHA[:12]}")
    assert events.index("journal:sha") < events.index(f"lookup:{IMAGE}:latest")
    assert events[-1] == "journal:complete"
