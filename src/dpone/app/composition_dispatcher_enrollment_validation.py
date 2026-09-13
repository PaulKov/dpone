"""Shared exact configuration/custody checks over a protected enrollment original.

This comparison performs no SQL or host observation. Callers must supply the
original from authenticated SQL and separately verify current host custody.
"""

from __future__ import annotations

from dataclasses import asdict

from dpone.adapters.composition_clickhouse_supervisor_enrollment import ClickHouseSupervisorEnrollment
from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes


def require_dispatcher_enrollment(
    config: DispatcherServiceConfig,
    enrollment: ClickHouseSupervisorEnrollment,
    service_id: str,
) -> None:
    """Require the configured original and exact profile, principal and root pins."""
    try:
        enrollment.__post_init__()
        body = enrollment.body
        if (
            enrollment.enrollment_sha256 != config.supervisor_enrollment_sha256
            or body["schema"] != "dpone.composition-clickhouse-supervisor-enrollment.v2"
            or body["service_id"] != service_id
        ):
            raise ValueError
        policy = body["policy"]["capture_custody"]
        if canonical_json_bytes(
            {name: policy[name] for name in ("profile", "uid", "gid", "destination")}
        ) != canonical_json_bytes(
            {
                "profile": config.capture_custody,
                "uid": config.dispatcher_uid,
                "gid": config.dispatcher_gid,
                "destination": str(config.capture_root),
            }
        ) or canonical_json_bytes(body["facts"]["linux"]["capture_custody"]["root_identity"]) != canonical_json_bytes(
            asdict(config.capture_root_identity)
        ):
            raise ValueError
    except Exception:
        raise CompositionAdmissionError("dispatcher_enrollment_unverified") from None
