from __future__ import annotations

import json

import jsonschema
import pytest
from tests.agent_policy._runtime_image_promotion_helpers import (
    DIGEST,
    IMAGE,
    NEWER_DIGEST,
    ROOT,
    VERSION,
    certification_identity,
    certification_payload,
    publication_payload,
)
from tools.agent_policy import runtime_image_promotion as promotion


def test_certification_accepts_only_complete_digest_bound_pass() -> None:
    certification = promotion.validate_certification(certification_payload(), expected=certification_identity())

    assert certification.digest == DIGEST
    assert certification.version == VERSION
    assert tuple(item["id"] for item in certification.to_payload()["checks"]) == promotion.REQUIRED_CHECK_IDS


def test_unknown_certification_schema_has_stable_failure_code() -> None:
    payload = certification_payload()
    payload["schema"] = "dpone.runtime-image-certification.v999"

    with pytest.raises(promotion.ContractError, match="CERTIFICATION_SCHEMA_UNSUPPORTED"):
        promotion.validate_certification(payload)


def test_historical_certification_is_valid_but_not_promotable() -> None:
    payload = certification_payload()
    payload["schema"] = promotion.CERTIFICATION_SCHEMA_V1
    payload["checks"] = [item for item in payload["checks"] if item["id"] in promotion.LEGACY_REQUIRED_CHECK_IDS]
    certification = promotion.validate_certification(payload)

    with pytest.raises(promotion.ContractError, match="CERTIFICATION_SCHEMA_NOT_PROMOTABLE"):
        promotion.require_promotable_certification(certification)


@pytest.mark.parametrize("mutation", ["missing", "skip", "mismatch", "partial", "extra"])
def test_certification_rejects_false_pass_claims(mutation: str) -> None:
    payload = certification_payload()
    if mutation == "missing":
        payload["checks"].pop()
    elif mutation == "skip":
        payload["checks"][0]["status"] = "SKIP"
    elif mutation == "mismatch":
        payload["checks"][0]["subject_digest"] = NEWER_DIGEST
    elif mutation == "partial":
        del payload["attestations"]["sbom"]
    else:
        payload["unexpected"] = True

    with pytest.raises(promotion.ContractError):
        promotion.validate_certification(payload, expected=certification_identity())


def test_publication_pass_requires_all_aliases_and_matching_postconditions() -> None:
    receipt = promotion.validate_publication(publication_payload())
    assert receipt.status == "PASS"

    for mutation in ("missing", "blocked", "mismatch", "partial"):
        payload = publication_payload()
        if mutation == "missing":
            payload["transitions"].pop()
        elif mutation == "blocked":
            payload["transitions"][1].update(decision="BLOCKED", status="FAIL")
        elif mutation == "mismatch":
            payload["transitions"][0]["after"]["digest"] = NEWER_DIGEST
        else:
            payload.update(status="PASS", outcome="PARTIAL_ALIAS_PENDING")
        with pytest.raises(promotion.ContractError):
            promotion.validate_publication(payload)


def test_preserved_latest_fails_closed_without_trusted_certification_proof() -> None:
    payload = publication_payload()
    latest = payload["transitions"][2]
    latest.update(
        before={"state": "PRESENT", "digest": NEWER_DIGEST, "version": "999.0.0"},
        decision="PRESERVED_NEWER",
        after={"state": "PRESENT", "digest": NEWER_DIGEST, "version": "999.0.0"},
    )

    with pytest.raises(promotion.ContractError, match="LATEST_CERTIFICATION_UNPROVEN"):
        promotion.validate_publication(payload)


def test_preserved_latest_passes_with_digest_bound_validated_certification() -> None:
    newer_version = "0.74.0"
    trusted = promotion.validate_certification(
        certification_payload(
            version=newer_version,
            digest=NEWER_DIGEST,
            commit_sha="d" * 40,
        )
    )
    payload = publication_payload()
    latest = payload["transitions"][2]
    latest.update(
        before={"state": "PRESENT", "digest": NEWER_DIGEST, "version": newer_version},
        decision="PRESERVED_NEWER",
        after={"state": "PRESENT", "digest": NEWER_DIGEST, "version": newer_version},
    )
    publication_schema = json.loads(
        (ROOT / "docs/schemas/release/runtime-image-publication.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, publication_schema)

    receipt = promotion.validate_publication(
        payload,
        trusted_latest_certification=trusted,
    )

    assert receipt.status == "PASS"


def test_newer_certification_must_bind_the_exact_observed_digest_and_version() -> None:
    trusted_for_other_digest = promotion.validate_certification(
        certification_payload(
            version="0.74.0",
            digest=f"sha256:{'e' * 64}",
            commit_sha="d" * 40,
        )
    )
    observed = promotion.ManifestObservation(
        promotion.ManifestState.PRESENT,
        digest=NEWER_DIGEST,
        version="0.74.0",
    )

    decision = promotion.decide_latest(
        candidate_version=VERSION,
        candidate_digest=DIGEST,
        candidate_image=IMAGE,
        observed=observed,
        observed_version=observed.version,
        trusted_latest_certification=trusted_for_other_digest,
    )

    assert decision.decision is promotion.AliasDecision.BLOCKED
    assert decision.blocker_code == "LATEST_CERTIFICATION_UNPROVEN"
