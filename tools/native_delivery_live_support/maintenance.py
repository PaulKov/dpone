"""Explicit operations on an existing factory-owned durable invocation inventory."""

from __future__ import annotations

import uuid

from .artifacts import ArtifactStore
from .execution import SHA, RouteFactory, RouteSession
from .profiles import exact_multiset


def invocation_id(value: str) -> str:
    """Only UUID identities can enter retained diagnostics; object names stay private."""
    parsed = str(uuid.UUID(value))
    if parsed != value:
        raise ValueError("invalid_invocation_id")
    return parsed


def record_owner(store: ArtifactStore, session: RouteSession, case: str) -> None:
    """Record how to reattach; this diagnostic never authorizes object deletion."""
    store.write(
        f"{case}-owner",
        {
            "schema_version": 1,
            "kind": "native-delivery-owned-invocation",
            "status": "UNVERIFIED",
            "invocation_id": invocation_id(session.invocation_id),
            "case": case,
            "reason": "Reattach through the factory durable owner inventory; reports are not ownership authority.",
        },
    )


def maintain(factory: RouteFactory, *, action: str, owner: str, store: ArtifactStore) -> dict:
    """Source-free recovery or known-outcome cleanup; attach never creates objects."""
    owner = invocation_id(owner)
    store.preflight()
    result = {
        "schema_version": 1,
        "kind": "native-delivery-maintenance",
        "invocation_id": owner,
        "action": action,
        "status": "UNVERIFIED",
        "reason": None,
    }
    session = factory.attach(owner)
    try:
        if invocation_id(session.invocation_id) != owner:
            raise ValueError("attached_owner_mismatch")
        if action == "recover":
            before = session.snapshot()
            before_rows, before_outside = exact_multiset(before.rows), exact_multiset(before.outside_rows)
            session.recover(source_allowed=False)
            after = session.snapshot()
            if (
                after.source_queries != before.source_queries
                or not after.commit_known
                or not after.pipeline_complete
                or after.publications != 1
                or before.publications > 1
                or not isinstance(after.receipt_expected, str)
                or not SHA.fullmatch(after.receipt_expected)
                or after.receipt_observed != after.receipt_expected
            ):
                raise ValueError("unresolved_recovery")
            if before.publications == 1 and (
                exact_multiset(after.rows) != before_rows or exact_multiset(after.outside_rows) != before_outside
            ):
                raise ValueError("committed_recovery_mutated_target")
        elif action == "cleanup":
            # cleanup owns the required identity/outcome checks. A business
            # snapshot is unavailable after an acknowledged or partially saved DROP.
            session.cleanup()
        else:
            raise ValueError("invalid_maintenance_action")
        result["status"] = "PASS"
    except Exception:
        result["status"], result["reason"] = "UNVERIFIED", "operation_unresolved_resources_retained"
    finally:
        try:
            session.close()
        except Exception:
            result["status"], result["reason"] = "FAIL", "session_close_failed"
    store.publish(result)
    return result
