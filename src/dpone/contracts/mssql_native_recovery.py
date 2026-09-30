"""Closed public projection for MSSQL native recovery state."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_native_custody import NativeTargetCustodyRecord
from dpone.contracts.mssql_native_recovery_authority import (
    MssqlNativeRecoveryBindings,
    restore_mssql_native_recovery_admission,
)
from dpone.contracts.mssql_native_verification import NativeVerificationIdentityV2
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

RECOVERY_STATES = (
    "EMPTY_STAGING",
    "WRITING_UNKNOWN",
    "PARTIAL_PROVED",
    "VERIFIED_EOF",
    "REEXTRACT_REQUIRED",
    "PUBLICATION_UNKNOWN",
    "PUBLISHED",
    "SUCCEEDED",
    "RETIRED",
    "CUSTODY_RELEASED",
    "INCIDENT_RETAINED",
)
RECOVERY_ACTIONS = ("inspect", "reconcile", "resume", "retire")
RECOVERY_ATTEMPT_TERMINALS = (
    "INTENT",
    "STAGE_OWNED",
    "GRANTED",
    "WRITING",
    "WRITER_TERMINAL",
    "QUIESCENT",
    "VERIFIED",
    "PARTIAL_PROVED",
    "UNKNOWN",
    "FAILED_RETIRABLE",
    "INCIDENT_RETAINED",
    "RETIRED",
)


def _closed(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(properties),
        "properties": properties,
    }


def mssql_native_recovery_v2_schema() -> dict[str, Any]:
    """Return the deterministic closed Draft 7 recovery projection schema."""
    sha = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://dpone.dev/schemas/dpone.mssql-native-recovery.v2.schema.json",
        "title": "dpone MSSQL native recovery v2",
        **_closed(
            {
                "schema_version": {"const": 2},
                "kind": {"const": "dpone.mssql-native-recovery.v2"},
                "invocation_id": sha,
                "identity_sha256": sha,
                "state": {"enum": list(RECOVERY_STATES)},
                "diagnostic_code": {"type": "string", "pattern": "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*$"},
                "permitted_actions": {
                    "type": "array",
                    "uniqueItems": True,
                    "items": {"enum": list(RECOVERY_ACTIONS)},
                },
                "artifact_refs": {"type": "array", "uniqueItems": True, "items": sha},
                "attempts": {
                    "type": "array",
                    "items": _closed(
                        {
                            "ordinal": {"type": "integer", "minimum": 0},
                            "attempt_id_sha256": sha,
                            "stage_id_sha256": {"anyOf": [{"type": "null"}, sha]},
                            "terminal_event": {"enum": list(RECOVERY_ATTEMPT_TERMINALS)},
                        }
                    ),
                },
            }
        ),
    }


def classify_mssql_native_recovery(
    projection: dict[str, Any], *, custody_state: str | None = None
) -> tuple[str, str, list[str]]:
    """Map one already validated private journal to a coordinate-free operator state."""
    publication = projection["publication"]
    events = [chain[-1]["event"] for chain in projection["events"].values() if chain]
    if custody_state == "clear" and publication is not None and publication["phase"] == "succeeded":
        return "CUSTODY_RELEASED", "mssql_native.custody_released", ["inspect"]
    if events and all(event == "RETIRED" for event in events):
        actions = ["inspect", "retire"] if custody_state == "held" else ["inspect"]
        return "RETIRED", "mssql_native.stages_retired", actions
    if publication is not None:
        phase = publication["phase"]
        if phase == "succeeded":
            actions = ["inspect", "resume"] if custody_state == "held" else ["inspect"]
            return "SUCCEEDED", "mssql_native.recovery_succeeded", actions
        if phase in {"published", "evidence-complete"}:
            return "PUBLISHED", "mssql_native.published_recoverable", ["inspect", "resume"]
        if phase == "publishing":
            return "PUBLICATION_UNKNOWN", "mssql_native.publication_outcome_unknown", ["inspect", "reconcile"]
        return "VERIFIED_EOF", "mssql_native.verified_eof_recoverable", ["inspect", "resume"]
    if "INCIDENT_RETAINED" in events:
        return "INCIDENT_RETAINED", "mssql_native.incident_retained", ["inspect"]
    if "PARTIAL_PROVED" in events:
        return "PARTIAL_PROVED", "mssql_native.partial_stage_proved", ["inspect", "reconcile"]
    if "UNKNOWN" in events or "WRITING" in events or "GRANTED" in events:
        return "WRITING_UNKNOWN", "mssql_native.writer_ack_lost", ["inspect", "reconcile"]
    if projection["phase"] == "stage_complete":
        return "VERIFIED_EOF", "mssql_native.verified_eof_recoverable", ["inspect", "resume"]
    if projection["phase"] == "reextract_required" or events:
        return "REEXTRACT_REQUIRED", "mssql_native.pre_eof_reextract_required", ["inspect", "retire"]
    return "EMPTY_STAGING", "mssql_native.empty_staging", ["retire"]


__all__ = [
    "NativeTargetCustodyRecord",
    "MssqlNativeRecoveryBindings",
    "MssqlTransactionAdmission",
    "NativeVerificationIdentityV2",
    "RECOVERY_ACTIONS",
    "RECOVERY_ATTEMPT_TERMINALS",
    "RECOVERY_STATES",
    "classify_mssql_native_recovery",
    "canonical_json_bytes",
    "mssql_native_recovery_v2_schema",
    "restore_mssql_native_recovery_admission",
    "strict_json_object",
]
