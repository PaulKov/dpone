"""Safe runtime evidence boundary for verified route attestations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.services.safe_sample_execution_common import redact_mapping


def safe_verified_route_receipt(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return redacted evidence only for a complete verified receipt."""

    if value is None:
        return None
    safe = redact_mapping(value)
    if safe.get("schema") != "dpone.route-attestation-verification.v1" or safe.get("decision") != "verified":
        raise ValueError("DPONE_ROUTE_ATTESTATION_VERIFIED receipt is required for runtime evidence")
    return safe


__all__ = ["safe_verified_route_receipt"]
