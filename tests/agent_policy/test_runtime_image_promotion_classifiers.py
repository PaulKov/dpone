from __future__ import annotations

import pytest
from tests.agent_policy._runtime_image_promotion_helpers import (
    DIGEST,
    NEWER_DIGEST,
    VERSION,
    Lookup,
)
from tools.agent_policy import runtime_image_promotion as promotion


@pytest.mark.parametrize(
    ("lookup", "state", "code"),
    [
        (Lookup(200, {"Docker-Content-Digest": DIGEST}, True), "present", None),
        (Lookup(404, {}, True), "absent", None),
        (Lookup(404, {}, False), "error", "LOOKUP_UNAUTHENTICATED"),
        (Lookup(401, {}, True), "error", "LOOKUP_HTTP_401"),
        (Lookup(403, {}, True), "error", "LOOKUP_HTTP_403"),
        (Lookup(429, {}, True), "error", "LOOKUP_HTTP_429"),
        (Lookup(500, {}, True), "error", "LOOKUP_HTTP_500"),
        (Lookup(200, {"Docker-Content-Digest": "not-a-digest"}, True), "error", "LOOKUP_DIGEST_INVALID"),
    ],
)
def test_lookup_classification_is_fail_closed(lookup: Lookup, state: str, code: str | None) -> None:
    observation = promotion.classify_manifest_lookup(lookup)

    assert observation.state.value == state
    assert observation.error_code == code


@pytest.mark.parametrize(
    ("observed", "decision", "write_required"),
    [
        (promotion.ManifestObservation(promotion.ManifestState.ABSENT), "CREATED", True),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, DIGEST), "NOOP_SAME", False),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST), "BLOCKED", False),
        (
            promotion.ManifestObservation(promotion.ManifestState.ERROR, error_code="LOOKUP_HTTP_500"),
            "BLOCKED",
            False,
        ),
    ],
)
def test_fixed_alias_create_or_compare_matrix(
    observed: promotion.ManifestObservation,
    decision: str,
    write_required: bool,
) -> None:
    result = promotion.decide_fixed_tag(expected_digest=DIGEST, observed=observed)

    assert result.decision.value == decision
    assert result.write_required is write_required


@pytest.mark.parametrize(
    ("observed", "observed_version", "decision", "write_required"),
    [
        (promotion.ManifestObservation(promotion.ManifestState.ABSENT), None, "CREATED", True),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, DIGEST), VERSION, "NOOP_SAME", False),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST), "0.73.1", "ADVANCED", True),
        (
            promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST),
            "0.74.0",
            "BLOCKED",
            False,
        ),
        (
            promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST),
            "999.0.0",
            "BLOCKED",
            False,
        ),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST), VERSION, "BLOCKED", False),
        (promotion.ManifestObservation(promotion.ManifestState.PRESENT, NEWER_DIGEST), None, "BLOCKED", False),
        (
            promotion.ManifestObservation(promotion.ManifestState.ERROR, error_code="LOOKUP_HTTP_500"),
            None,
            "BLOCKED",
            False,
        ),
    ],
)
def test_latest_alias_is_monotonic(
    observed: promotion.ManifestObservation,
    observed_version: str | None,
    decision: str,
    write_required: bool,
) -> None:
    result = promotion.decide_latest(
        candidate_version=VERSION,
        candidate_digest=DIGEST,
        observed=observed,
        observed_version=observed_version,
    )

    assert result.decision.value == decision
    assert result.write_required is write_required
    if observed_version in {"0.74.0", "999.0.0"}:
        assert result.blocker_code == "LATEST_CERTIFICATION_UNPROVEN"
