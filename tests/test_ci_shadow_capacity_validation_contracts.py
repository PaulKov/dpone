from __future__ import annotations

import copy
from typing import Any

import pytest
from jsonschema import ValidationError

from tests.test_ci_shadow_capacity_contracts import (
    CAPACITY_ARTIFACT_MAX_BYTES,
    _capacity_bytes,
    _capacity_evidence,
    _capacity_validator,
    _provider_transport_for,
)
from tests.test_ci_shadow_capacity_contracts import (
    _validate_capacity_evidence as _validate_raw_capacity_evidence,
)


def _validate_capacity_evidence(
    payload: dict[str, Any],
    *,
    provider_transport: dict[str, Any] | None = None,
    raw_downloaded_bytes: bytes | None = None,
    **kwargs: Any,
) -> None:
    raw_bytes = _capacity_bytes(payload) if raw_downloaded_bytes is None else raw_downloaded_bytes
    transport = _provider_transport_for(raw_bytes) if provider_transport is None else provider_transport
    _validate_raw_capacity_evidence(raw_bytes, provider_transport=transport, **kwargs)


def _transport_for(payload: dict[str, Any], raw_bytes: bytes | None = None) -> dict[str, Any]:
    return _provider_transport_for(_capacity_bytes(payload) if raw_bytes is None else raw_bytes)


def test_capacity_evidence_rejects_invalid_source_transport_and_freshness() -> None:
    evidence = _capacity_evidence()
    _validate_capacity_evidence(evidence)

    foreign_source = copy.deepcopy(evidence)
    foreign_source["source"]["workflow_path"] = ".github/workflows/foreign.yml"
    with pytest.raises(ValidationError):
        _validate_capacity_evidence(foreign_source)

    wrong_source_sha = copy.deepcopy(evidence)
    wrong_source_sha["source"]["workflow_sha"] = "d" * 40
    with pytest.raises(ValueError, match="approved workflow identity"):
        _validate_capacity_evidence(wrong_source_sha)

    wrong_source_run = copy.deepcopy(evidence)
    wrong_source_run["source"]["run_id"] = 999999
    wrong_source_run["source"]["run_attempt"] = 77
    with pytest.raises(ValueError, match="approved workflow identity"):
        _validate_capacity_evidence(wrong_source_run)

    wrong_transport = _transport_for(evidence)
    wrong_transport["artifact_id"] = 0
    with pytest.raises(ValueError, match="transport identity is incomplete"):
        _validate_capacity_evidence(evidence, provider_transport=wrong_transport)

    foreign_transport = _transport_for(evidence)
    foreign_transport["workflow_run_id"] = 999999
    with pytest.raises(ValueError, match="source run and attempt"):
        _validate_capacity_evidence(evidence, provider_transport=foreign_transport)

    wrong_configuration = copy.deepcopy(evidence)
    wrong_configuration["execution_configuration"]["runner_arch"] = "ARM64"
    with pytest.raises(ValueError, match="execution configuration"):
        _validate_capacity_evidence(wrong_configuration)

    wrong_digest = _transport_for(evidence)
    wrong_digest["provider_digest"] = "sha256:" + "d" * 64
    with pytest.raises(ValueError, match="digest does not match"):
        _validate_capacity_evidence(evidence, provider_transport=wrong_digest)

    wrong_size = _transport_for(evidence)
    wrong_size["size_in_bytes"] -= 1
    with pytest.raises(ValueError, match="size does not match"):
        _validate_capacity_evidence(evidence, provider_transport=wrong_size)

    unrelated_bytes = _capacity_bytes({**evidence, "complete": False})
    matching_unrelated_transport = _transport_for(evidence, unrelated_bytes)
    with pytest.raises(ValidationError):
        _validate_capacity_evidence(
            evidence,
            provider_transport=matching_unrelated_transport,
            raw_downloaded_bytes=unrelated_bytes,
        )

    wrong_interval = copy.deepcopy(evidence)
    wrong_interval["interval"]["scan_from"] = "2026-07-26T02:45:00Z"
    with pytest.raises(ValueError, match="exactly 14 days"):
        _validate_capacity_evidence(wrong_interval)

    wrong_grace = copy.deepcopy(evidence)
    wrong_grace["interval"]["observation_started_at"] = "2026-08-08T03:14:59Z"
    with pytest.raises(ValueError, match="30-minute grace"):
        _validate_capacity_evidence(wrong_grace)

    wrong_observation_order = copy.deepcopy(evidence)
    wrong_observation_order["evidence_observed_through"] = "2026-08-08T03:21:00Z"
    with pytest.raises(ValueError, match="not monotonic"):
        _validate_capacity_evidence(wrong_observation_order)

    old_observation_with_fresh_stamp = copy.deepcopy(evidence)
    old_observation_with_fresh_stamp["interval"] = {
        "scan_from": "2020-07-25T02:45:00Z",
        "safe_scan_through": "2020-08-08T02:45:00Z",
        "observation_started_at": "2020-08-08T03:15:00Z",
    }
    old_observation_with_fresh_stamp["evidence_observed_through"] = "2026-08-08T03:20:00Z"
    with pytest.raises(ValueError, match="wall-time budget"):
        _validate_capacity_evidence(old_observation_with_fresh_stamp)

    with pytest.raises(ValueError, match="not current"):
        _validate_capacity_evidence(evidence, approval_time="2026-08-10T00:00:00Z")
    with pytest.raises(ValueError, match="not current"):
        _validate_capacity_evidence(evidence, approval_time=evidence["valid_until"])


def test_capacity_evidence_rejects_invalid_bundle_and_accounting() -> None:
    evidence = _capacity_evidence()

    wrong_total = copy.deepcopy(evidence)
    wrong_total["counters"]["total_response_body_bytes"] += 1
    with pytest.raises(ValueError, match="API plus artifact"):
        _validate_capacity_evidence(wrong_total)

    stale = copy.deepcopy(evidence)
    stale["valid_until"] = "2026-08-10T03:20:00Z"
    with pytest.raises(ValueError, match="exactly 24 hours"):
        _validate_capacity_evidence(stale)

    incomplete = copy.deepcopy(evidence)
    incomplete["complete"] = False
    incomplete["two_observations_match"] = False
    incomplete["within_thresholds"] = False
    incomplete["code"] = "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
    _capacity_validator().validate(incomplete)
    with pytest.raises(ValueError, match="diagnostic only"):
        _validate_capacity_evidence(incomplete)

    saturated_overflow = copy.deepcopy(incomplete)
    saturated_overflow["limits_crossed"] = ["total_http_requests"]
    saturated_overflow["counters"]["producer_list_page_requests"] += 640
    saturated_overflow["counters"]["total_http_requests"] = 3000
    _capacity_validator().validate(saturated_overflow)

    wrong_bundle = copy.deepcopy(evidence)
    wrong_bundle["observation_bundle_sha256"] = "sha256:" + "d" * 64
    with pytest.raises(ValueError, match="canonical manifest"):
        _validate_capacity_evidence(wrong_bundle)

    changed_bundle_entry = copy.deepcopy(evidence)
    changed_bundle_entry["observation_bundle_manifest"]["entries"][1]["blob_sha256"] = "sha256:" + "3" * 64
    with pytest.raises(ValueError, match="canonical manifest"):
        _validate_capacity_evidence(changed_bundle_entry)

    changed_manifest_identity = copy.deepcopy(evidence)
    changed_manifest_identity["observation_bundle_manifest"]["manifest_sha256"] = "sha256:" + "3" * 64
    with pytest.raises(ValueError, match="canonical manifest"):
        _validate_capacity_evidence(changed_manifest_identity)

    unsorted_bundle = copy.deepcopy(evidence)
    unsorted_bundle["observation_bundle_manifest"]["entries"].reverse()
    with pytest.raises(ValueError, match="strictly sorted"):
        _validate_capacity_evidence(unsorted_bundle)

    oversized_path = copy.deepcopy(evidence)
    oversized_path["observation_bundle_manifest"]["entries"][1]["path"] = "é" * 600
    with pytest.raises(ValueError, match="1024 UTF-8 bytes"):
        _validate_capacity_evidence(oversized_path)

    wrong_policy = copy.deepcopy(evidence)
    wrong_policy["policy_sha256"] = "sha256:" + "d" * 64
    with pytest.raises(ValueError, match="approved policy"):
        _validate_capacity_evidence(wrong_policy)

    wrong_request_total = copy.deepcopy(evidence)
    wrong_request_total["counters"]["total_http_requests"] = 1
    with pytest.raises(ValueError, match="request-class sum"):
        _validate_capacity_evidence(wrong_request_total)

    impossible_retry_subset = copy.deepcopy(evidence)
    impossible_retry_subset["counters"]["retry_requests"] = 999
    with pytest.raises(ValueError, match="subset"):
        _validate_capacity_evidence(impossible_retry_subset)

    unsaturated_crossed_limit = copy.deepcopy(evidence)
    unsaturated_crossed_limit["complete"] = False
    unsaturated_crossed_limit["two_observations_match"] = False
    unsaturated_crossed_limit["within_thresholds"] = False
    unsaturated_crossed_limit["code"] = "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
    unsaturated_crossed_limit["limits_crossed"] = ["total_http_requests"]
    with pytest.raises(ValidationError):
        _capacity_validator().validate(unsaturated_crossed_limit)


def test_capacity_raw_json_and_size_boundaries_fail_closed() -> None:
    payload = _capacity_evidence()
    raw_bytes = _capacity_bytes(payload)

    duplicate_root = b'{"complete":false,' + raw_bytes[1:]
    with pytest.raises(ValueError, match="not strict JSON"):
        _validate_raw_capacity_evidence(
            duplicate_root,
            provider_transport=_provider_transport_for(duplicate_root),
        )

    duplicate_nested = raw_bytes.replace(b'"source":{', b'"source":{"run_id":999,', 1)
    with pytest.raises(ValueError, match="not strict JSON"):
        _validate_raw_capacity_evidence(
            duplicate_nested,
            provider_transport=_provider_transport_for(duplicate_nested),
        )

    nonfinite = b'{"probe":NaN,' + raw_bytes[1:]
    with pytest.raises(ValueError, match="not strict JSON"):
        _validate_raw_capacity_evidence(nonfinite, provider_transport=_provider_transport_for(nonfinite))

    deep = b'{"nested":' + (b"[" * 33) + b"0" + (b"]" * 33) + b"}"
    with pytest.raises(ValueError, match="depth limit"):
        _validate_raw_capacity_evidence(deep, provider_transport=_provider_transport_for(deep))

    exact_limit = raw_bytes + (b" " * (CAPACITY_ARTIFACT_MAX_BYTES - len(raw_bytes)))
    _validate_raw_capacity_evidence(exact_limit, provider_transport=_provider_transport_for(exact_limit))

    over_limit = exact_limit + b" "
    with pytest.raises(ValueError, match="provider-size limit"):
        _validate_raw_capacity_evidence(over_limit, provider_transport=_provider_transport_for(over_limit))


@pytest.mark.parametrize(
    ("field", "threshold", "exceeded"),
    [
        ("total_http_requests", 1500, 1501),
        ("total_response_body_bytes", 67108864, 67108865),
        ("execution_wall_seconds", 450, 450.001),
    ],
)
def test_capacity_threshold_boundaries_fail_closed(field: str, threshold: int, exceeded: float) -> None:
    at_threshold = _capacity_evidence()
    at_threshold["counters"][field] = threshold
    if field == "total_http_requests":
        at_threshold["counters"]["producer_list_page_requests"] += threshold - 160
    if field == "total_response_body_bytes":
        at_threshold["counters"]["api_response_body_bytes"] = threshold
        at_threshold["counters"]["artifact_download_body_bytes"] = 0
    _validate_capacity_evidence(at_threshold)

    blocked = copy.deepcopy(at_threshold)
    blocked["counters"][field] = exceeded
    if field == "total_http_requests":
        blocked["counters"]["producer_list_page_requests"] += exceeded - threshold
    if field == "total_response_body_bytes":
        blocked["counters"]["api_response_body_bytes"] = exceeded
    blocked["within_thresholds"] = False
    blocked["code"] = "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
    _capacity_validator().validate(blocked)
    with pytest.raises(ValueError, match="exceeds approval threshold"):
        _validate_capacity_evidence(blocked)

    false_qualifying_claim = copy.deepcopy(blocked)
    false_qualifying_claim["within_thresholds"] = True
    false_qualifying_claim["code"] = "RECONCILIATION_CAPACITY_CALIBRATION_ONLY"
    with pytest.raises(ValidationError):
        _capacity_validator().validate(false_qualifying_claim)
