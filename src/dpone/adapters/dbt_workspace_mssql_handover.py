"""Typed exact readback of the protected SQL workspace execution witness.

The database procedures, not this adapter, enforce mutation permission and state
predicates. Connections are injected and independently owned. No local pointer,
cache, current catalog observation or remote desired publication supplies missing
historical authority. Runtime composition must not enable this partial backend
until the complete gateway and its live certification are available.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dpone.adapters.dbapi_lifecycle import close, row
from dpone.adapters.dbt_workspace_mssql_gateway_security import workspace_gateway_identifier
from dpone.contracts.dbt_workspace_channel import WorkspaceChannel, WorkspaceHandoverError, decode_workspace_document
from dpone.contracts.dbt_workspace_handover import WorkspaceHandoverClaim
from dpone.contracts.dbt_workspace_lifecycle import (
    DbtWorkspaceHistoricalGuard,
    DbtWorkspaceLifecycleIdentity,
    DbtWorkspaceLifecycleReadback,
    parse_workspace_request,
)
from dpone.contracts.dbt_workspace_registration import WorkspaceRegistrationReceipt
from dpone.contracts.dbt_workspace_registration_baseline import WorkspaceAdoptedCurrent
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback, WorkspaceStoredOccurrence

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlConnection

_SNAPSHOT_FIELDS = {
    "channel_json",
    "revision",
    "current_activation_id",
    "pending_activation_id",
    "registration_json",
    "current_claim_json",
    "current_request_json",
    "current_completed",
    "pending_claim_json",
    "pending_request_json",
    "pending_completed",
    "current_lifecycle",
    "pending_lifecycle",
}
_LIFECYCLE_FIELDS = {
    "activation_id",
    "request_sha256",
    "environment",
    "release_id",
    "deployment_id",
    "previous_deployment_id",
    "source_inventory_sha256",
    "runtime_context_sha256",
    "state",
    "guards",
}
_GUARD_FIELDS = {
    "guard_id",
    "resource_sha256",
    "fencing_epoch",
    "live_epoch",
    "owner_id",
    "workflow_id",
    "operation_id",
    "status",
    "write_subjects",
}


class MssqlWorkspaceHandoverStore:
    """Recover exact immutable witness snapshots through fixed SQL procedures."""

    def __init__(
        self, connection_factory: Callable[[], SqlControlConnection], *, control_schema: str = "dpone_control"
    ) -> None:
        self._connection_factory = connection_factory
        self._schema = workspace_gateway_identifier(control_schema)

    def read_channel(self, channel: WorkspaceChannel) -> WorkspaceChannelReadback:
        """Never mutate a lifecycle while observing the protected current state."""
        channel.__post_init__()
        connection = None
        cursor = None
        try:
            connection = self._connection_factory()
            # The fixed server procedure owns the complete transaction, including
            # commit before returning a snapshot. Ambient client transactions are
            # rejected by the gateway, not silently nested or independently ended.
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(
                f"EXEC [{self._schema}].[workspace_channel_read] ?;",
                json.dumps(channel.to_dict(), sort_keys=True, separators=(",", ":")),
            )
            observed = row(cursor)
            if observed is None or len(observed) != 1 or cursor.fetchone() is not None:
                raise WorkspaceHandoverError("gateway_snapshot_rows")
            return _channel_readback(observed[0], channel)
        except WorkspaceHandoverError:
            raise
        except Exception as error:
            raise _gateway_error(error) from None
        finally:
            close(cursor)
            close(connection)


def _gateway_error(error: Exception) -> WorkspaceHandoverError:
    """Translate fixed native numbers without retaining raw driver diagnostics."""
    codes = {
        "51000": "DPONE_WORKSPACE_CHANNEL_AUTHORITY_MISMATCH",
        "51001": "DPONE_WORKSPACE_HANDOVER_WAITING_ATTEMPTS",
        "51002": "DPONE_WORKSPACE_HANDOVER_COMMIT_UNKNOWN",
        "51004": "DPONE_WORKSPACE_CHANNEL_CAS_CONFLICT",
        "51005": "DPONE_WORKSPACE_CHANNEL_UNREGISTERED",
        "51006": "DPONE_WORKSPACE_REGISTRATION_PROOF_INVALID",
    }
    for argument in error.args:
        if isinstance(argument, str) and len(argument) <= 16384:
            match = re.search(r"\((5100[0-6])\)(?:\s|$)", argument)
            if match and match[1] in codes:
                return WorkspaceHandoverError("gateway_rejected", code=codes[match[1]])
    return WorkspaceHandoverError("gateway_read_unavailable")


def _closed(value: object, names: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != names:
        raise WorkspaceHandoverError("gateway_snapshot_shape")
    return value


def _channel_readback(raw: bytes | str, expected: WorkspaceChannel) -> WorkspaceChannelReadback:
    try:
        # SQL bounds every nested document and row collection. FOR JSON doubles
        # embedded JSON escapes; this transport cap covers both retained requests,
        # full registration and both bounded guard snapshots without truncation.
        value = _closed(decode_workspace_document(raw, 256 * 1024 * 1024), _SNAPSHOT_FIELDS)
        channel = WorkspaceChannel.from_json(value["channel_json"])
        registration = WorkspaceRegistrationReceipt.from_json(value["registration_json"])
        if channel != expected or registration.input.channel != expected:
            raise ValueError
        current = _current(value, registration.input.adopted_current)
        pending = None
        prepared = None
        if value["pending_activation_id"] is None:
            if any(
                value[name] is not None
                for name in ("pending_claim_json", "pending_request_json", "pending_completed", "pending_lifecycle")
            ):
                raise ValueError
        else:
            pending = WorkspaceHandoverClaim.from_json(value["pending_claim_json"])
            if (
                pending.successor_activation_id != value["pending_activation_id"]
                or value["pending_completed"] is not False
            ):
                raise ValueError
            if value["pending_request_json"] is None:
                if value["pending_lifecycle"] is not None:
                    raise ValueError
            else:
                prepared = _stored(pending, value["pending_request_json"], value["pending_lifecycle"])
        return WorkspaceChannelReadback(channel, value["revision"], current, pending, prepared)
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError):
        raise WorkspaceHandoverError("gateway_snapshot_authority") from None


def _current(value: dict[str, Any], baseline: WorkspaceAdoptedCurrent | None) -> WorkspaceStoredOccurrence | None:
    if value["current_activation_id"] is None:
        if baseline is not None or any(
            value[name] is not None
            for name in ("current_claim_json", "current_request_json", "current_completed", "current_lifecycle")
        ):
            raise WorkspaceHandoverError("gateway_current_missing")
        return None
    if value["current_claim_json"] is not None:
        snapshot: WorkspaceHandoverClaim | WorkspaceAdoptedCurrent = WorkspaceHandoverClaim.from_json(
            value["current_claim_json"]
        )
        if value["current_completed"] is not True:
            raise WorkspaceHandoverError("gateway_current_uncompleted")
        request_json = value["current_request_json"]
    else:
        if baseline is None or value["current_completed"] is not None or value["current_request_json"] is not None:
            raise WorkspaceHandoverError("gateway_current_baseline")
        snapshot = baseline
        request_json = baseline.request_json
    stored = _stored(snapshot, request_json, value["current_lifecycle"])
    if stored.request.activation_id != value["current_activation_id"]:
        raise WorkspaceHandoverError("gateway_current_identity")
    return stored


def _stored(
    snapshot: WorkspaceHandoverClaim | WorkspaceAdoptedCurrent, request_json: bytes | str, raw: object
) -> WorkspaceStoredOccurrence:
    request = parse_workspace_request(decode_workspace_document(request_json, 16 * 1024 * 1024))
    value = _closed(raw, _LIFECYCLE_FIELDS)
    if not isinstance(value["guards"], list) or not 1 <= len(value["guards"]) <= 8192:
        raise WorkspaceHandoverError("gateway_guard_bound")
    guards = tuple(sorted(_guard(item, value) for item in value["guards"]))
    coordinates = {name: value[name] for name in _LIFECYCLE_FIELDS - {"request_sha256", "state", "guards"}}
    identity = DbtWorkspaceLifecycleIdentity(
        **coordinates, write_subjects=tuple(sorted(subject for guard in guards for subject in guard.write_subjects))
    )
    lifecycle = DbtWorkspaceLifecycleReadback(identity, value["request_sha256"], value["state"], guards)
    return WorkspaceStoredOccurrence(snapshot, request, lifecycle)


def _guard(raw: object, lifecycle: dict[str, Any]) -> DbtWorkspaceHistoricalGuard:
    value = _closed(raw, _GUARD_FIELDS)
    if not isinstance(value["write_subjects"], list) or not 1 <= len(value["write_subjects"]) <= 8192:
        raise WorkspaceHandoverError("gateway_subject_bound")
    subjects = tuple(
        sorted(_closed(item, {"write_subject_sha256"})["write_subject_sha256"] for item in value["write_subjects"])
    )
    if lifecycle["state"] != "RETIRED":
        observed = tuple(value[name] for name in ("live_epoch", "owner_id", "workflow_id", "operation_id", "status"))
        expected = (
            value["fencing_epoch"],
            f"dbt-workspace:{lifecycle['activation_id']}",
            lifecycle["activation_id"],
            None,
            "HELD",
        )
        if type(value["live_epoch"]) is not int or observed != expected:
            raise WorkspaceHandoverError("gateway_live_guard")
    return DbtWorkspaceHistoricalGuard(value["guard_id"], value["resource_sha256"], value["fencing_epoch"], subjects)
