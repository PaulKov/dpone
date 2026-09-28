"""Pre-publication capacity reservation for the complete v2 proof and wire frame."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from dpone.runtime.governance.quality_target_plan import target_request
from dpone.runtime.quality_replay_contracts import MAX_FRAME_BYTES, MAX_UINT64, contracts, unavailable_observation


def seal_target_plan(core: dict[str, Any], hosts: tuple[str, ...]) -> None:
    """Bind the original admitted replicas before the immutable core is sealed."""
    core["target_plan"]["replicas"] = list(sorted(hosts))


def reserve_target_completion(capsule: contracts.QualityReplayCapsule, hosts: tuple[str, ...]) -> None:
    """Fit both transitions, longest replica and all worst-case UInt64 counts.

    The fixed framing allowance includes nonce, envelope keys, transport status,
    and bound lifecycle metadata. Every variable request/observation string is
    included in full; no truncation or selector reduction is allowed.
    """
    request = target_request(capsule.core, capsule.core_digest, "f" * 32)
    replica = max(hosts, key=lambda host: len(json.dumps(host).encode("utf-8")))
    warning = unavailable_observation(request, replica=replica, attempt_id="f" * 32)
    successful = {
        **warning,
        "warnings": [],
        "row_count": MAX_UINT64 if request.row_count else None,
        "null_counts": dict.fromkeys(request.null_columns, MAX_UINT64),
        "distinct_counts": dict.fromkeys(request.distinct_columns, MAX_UINT64),
    }
    variants = (warning, successful) if capsule.core["target_plan"]["mode"] == "warn_only" else (successful,)
    for observation in variants:
        pending = capsule.advance("TARGET_PENDING", authority_version=MAX_UINT64 - 1)
        pending.advance("COMPLETE", authority_version=MAX_UINT64, target=observation)
        encoded = contracts.canonical_quality_json(observation).encode("utf-8")
        if len(encoded) + 4096 > MAX_FRAME_BYTES:
            raise contracts.ReplayQualityEvidenceError("INVALID")
    # The worker receives its own framed request as well as returning evidence.
    request_payload = replace(request, binding=dict(request.binding))
    from dataclasses import asdict

    if len(contracts.canonical_quality_json(asdict(request_payload)).encode("utf-8")) + 4096 > MAX_FRAME_BYTES:
        raise contracts.ReplayQualityEvidenceError("INVALID")
