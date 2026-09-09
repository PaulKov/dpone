"""Secret-free bounded source request for safe sample execution.

The request is a runtime contract, not a source connector implementation. It
turns the already evaluated policy and declared capabilities into a bounded
copy intent that a certified copier can execute later.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.services.safe_sample_policy import SafeSamplePolicyResult, TemporaryTargetPlan


@dataclass(frozen=True, slots=True)
class SafeSampleSourceRequest:
    status: str
    mode: str
    sample_rows: int
    max_bytes: int
    timeout_seconds: int
    source_read_only: bool
    full_scan_allowed: bool
    estimated_read_bytes: int | None
    proof: str | None
    pii_policy: str
    target: dict[str, Any]
    errors: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-source-request.v1",
            "status": self.status,
            "mode": self.mode,
            "sample_rows": self.sample_rows,
            "max_bytes": self.max_bytes,
            "timeout_seconds": self.timeout_seconds,
            "source_read_only": self.source_read_only,
            "full_scan_allowed": self.full_scan_allowed,
            "estimated_read_bytes": self.estimated_read_bytes,
            "proof": self.proof,
            "pii_policy": self.pii_policy,
            "target": dict(self.target),
            "errors": [dict(error) for error in self.errors],
        }


class SafeSampleSourceRequestBuilder:
    """Build the bounded, credential-free source request used by data copiers."""

    def build(
        self,
        policy_result: SafeSamplePolicyResult,
        target_plan: TemporaryTargetPlan,
    ) -> SafeSampleSourceRequest:
        capabilities = policy_result.capabilities
        errors = list(policy_result.errors)
        mode = _mode(policy_result)
        if mode is None:
            if not errors:
                errors.append(
                    _error(
                        "DPONE_SAFE_SAMPLE_SOURCE_CAPABILITY_UNPROVEN",
                        "Safe sample source request requires proven pushdown or policy-allowed bounded full-scan.",
                    )
                )
            mode = "blocked"
        elif errors:
            mode = "blocked"
        return SafeSampleSourceRequest(
            status="blocked" if errors else "planned",
            mode=mode,
            sample_rows=policy_result.request.sample_rows,
            max_bytes=policy_result.policy.max_bytes,
            timeout_seconds=policy_result.policy.timeout_seconds,
            source_read_only=True,
            full_scan_allowed=policy_result.policy.allow_full_scan,
            estimated_read_bytes=capabilities.estimated_read_bytes,
            proof=capabilities.proof,
            pii_policy=target_plan.pii_policy,
            target={
                "mode": target_plan.mode,
                "connection_ref": target_plan.connection_ref,
                "temporary_table": dict(target_plan.temporary_table),
                "ttl_seconds": target_plan.ttl_seconds,
            },
            errors=tuple(errors),
        )


def _mode(policy_result: SafeSamplePolicyResult) -> str | None:
    capabilities = policy_result.capabilities
    if capabilities.supports_pushdown_sampling is True:
        return "pushdown"
    if capabilities.full_scan_required is True and policy_result.policy.allow_full_scan:
        return "full_scan"
    return None


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_source_request",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = ["SafeSampleSourceRequest", "SafeSampleSourceRequestBuilder"]
