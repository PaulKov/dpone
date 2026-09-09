from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import pytest
from tests.agent_policy import _release_commit_gate_helpers as producer
from tests.agent_policy._release_candidate_evidence_helpers import (
    COMMIT_SHA,
    canonical,
    load_module,
    policy,
    valid_source_payloads,
)

validation = load_module(
    "dpone_release_candidate_exact_checks_validation_test",
    "tools/agent_policy/release_candidate_evidence_validation.py",
)


def _payload() -> dict[str, Any]:
    return copy.deepcopy(valid_source_payloads()["exact_commit_checks"])


def _validate(payload: dict[str, Any]) -> dict[str, Any]:
    return validation.validate_source(
        "exact_commit_checks",
        payload,
        raw=canonical(payload),
        commit_sha=COMMIT_SHA,
    )


def _observation(payload: dict[str, Any]) -> dict[str, Any]:
    return payload["contexts"][0]["current_observations"][0]


def test_accepts_exact_provider_bound_observation_inventory() -> None:
    payload = _payload()
    second = copy.deepcopy(_observation(payload))
    second["evidence_id"] = 2
    payload["contexts"][0]["current_observations"].append(second)
    payload["contexts"][0]["observed_count"] = 2

    assert _validate(payload) == {
        "status": "PASS",
        "required_contexts": 1,
        "repository": payload["repository"],
    }


def test_mixed_legacy_status_and_check_run_produces_valid_exact_inventory() -> None:
    report = producer.evaluate(
        producer.snapshot(
            observations=(
                producer.observation(source="status", state="success", evidence_id=1),
                producer.observation(source="status", state="failure", evidence_id=2),
                producer.observation(evidence_id=3),
            )
        )
    )._replace(policy_sha256=producer.POLICY_DIGEST)
    payload = report.to_payload()

    assert payload["contexts"][0]["observed_count"] == 1
    assert payload["contexts"][0]["current_observations"] == [dict(producer.observation(evidence_id=3)._asdict())]
    assert _validate(payload)["status"] == "PASS"


@pytest.mark.parametrize(
    "policy_sha256",
    [
        "sha256:" + "a" * 64,
        "A" * 64,
        "g" * 64,
        "a" * 63,
        "a" * 65,
        "",
        None,
    ],
    ids=["tagged", "uppercase", "nonhex", "short", "long", "empty", "null"],
)
def test_rejects_noncanonical_bare_policy_sha256(policy_sha256: object) -> None:
    payload = _payload()
    payload["policy_sha256"] = policy_sha256

    with pytest.raises(ValueError, match="bare lowercase SHA-256"):
        _validate(payload)


@pytest.mark.parametrize("field", ["context", "source", "evidence_id", "state", "integration_id", "commit_sha"])
def test_rejects_observation_with_missing_closed_schema_field(field: str) -> None:
    payload = _payload()
    del _observation(payload)[field]

    with pytest.raises(ValueError, match="field set is invalid"):
        _validate(payload)


def test_rejects_observation_with_unknown_closed_schema_field() -> None:
    payload = _payload()
    _observation(payload)["attacker_asserted"] = True

    with pytest.raises(ValueError, match="field set is invalid"):
        _validate(payload)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda item: item.update(context="attacker/context"), "identity or state"),
        (lambda item: item.update(source="status"), "identity or state"),
        (lambda item: item.update(state="failure"), "identity or state"),
        (lambda item: item.update(integration_id=policy.APP_ID + 1), "identity or state"),
        (lambda item: item.update(integration_id=True), "positive integer"),
        (lambda item: item.update(commit_sha="b" * 40), "identity or state"),
        (lambda item: item.update(evidence_id=0), "positive integer"),
        (lambda item: item.update(evidence_id=-1), "positive integer"),
        (lambda item: item.update(evidence_id=True), "positive integer"),
        (lambda item: item.update(evidence_id="1"), "positive integer"),
        (lambda item: item.update(evidence_id=None), "positive integer"),
        (lambda item: item.update(evidence_id=1.0), "positive integer"),
    ],
    ids=[
        "different-context",
        "commit-status-source",
        "failed-state",
        "different-integration",
        "boolean-integration",
        "different-commit",
        "zero-evidence-id",
        "negative-evidence-id",
        "boolean-evidence-id",
        "string-evidence-id",
        "null-evidence-id",
        "float-evidence-id",
    ],
)
def test_rejects_observation_not_bound_to_exact_provider(
    mutate: Callable[[dict[str, Any]], None],
    message: str,
) -> None:
    payload = _payload()
    mutate(_observation(payload))

    with pytest.raises(ValueError, match=message):
        _validate(payload)


@pytest.mark.parametrize("observed_count", [2, True])
def test_rejects_observed_count_not_closing_current_provider_inventory(observed_count: object) -> None:
    payload = _payload()
    payload["contexts"][0]["observed_count"] = observed_count

    with pytest.raises(ValueError, match="observed_count|positive integer"):
        _validate(payload)


def test_rejects_observed_count_smaller_than_current_provider_inventory() -> None:
    payload = _payload()
    second = copy.deepcopy(_observation(payload))
    second["evidence_id"] = 2
    payload["contexts"][0]["current_observations"].append(second)

    with pytest.raises(ValueError, match="observed_count"):
        _validate(payload)
