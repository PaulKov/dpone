"""Closed public evidence contract for MSSQL native delivery v2."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\Z")
_TIMINGS = frozenset(
    {
        "source_read_seconds",
        "bulk_write_seconds",
        "target_digest_seconds",
        "preparation_seconds",
        "publication_seconds",
        "confirmed_visibility_seconds",
    }
)
_TOP = frozenset(
    {
        "schema_version",
        "kind",
        "subject",
        "backend",
        "timings",
        "resources",
        "correctness",
        "recovery",
        "privacy",
        "status",
    }
)


def _closed(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(properties),
        "properties": properties,
    }


def _nullable_nonnegative_number() -> dict[str, Any]:
    return {"anyOf": [{"type": "null"}, {"type": "number", "minimum": 0}]}


def mssql_native_delivery_evidence_v2_schema() -> dict[str, Any]:
    """Return the deterministic closed Draft 7 schema for public evidence."""
    sha256 = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
    nonnegative = {"type": "integer", "minimum": 0}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://dpone.dev/schemas/dpone.mssql-native-delivery-evidence.v2.schema.json",
        "title": "dpone MSSQL native delivery evidence v2",
        **_closed(
            {
                "schema_version": {"const": 2},
                "kind": {"const": "dpone.mssql-native-delivery-evidence.v2"},
                "subject": _closed(
                    {
                        "invocation_id": sha256,
                        "source_commit_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
                        "dirty": {"type": "boolean"},
                        "runner_image_sha256": sha256,
                        "environment_receipt_sha256": sha256,
                    }
                ),
                "backend": _closed(
                    {
                        "import_backend": {"enum": ["bcp", "mssql_sqlclient"]},
                        "backend_identity_sha256": sha256,
                        "wire_identity_sha256": sha256,
                        "package_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"},
                    }
                ),
                "timings": _closed({field: _nullable_nonnegative_number() for field in _TIMINGS}),
                "resources": _closed({"business_rows_read_back": nonnegative, "bcp_process_count": nonnegative}),
                "correctness": _closed(
                    {
                        "source_rows": nonnegative,
                        "published_rows": nonnegative,
                        "receipt_count": nonnegative,
                        "target_digest_verified": {"type": "boolean"},
                        "publication_verified": {"type": "boolean"},
                    }
                ),
                "recovery": _closed(
                    {
                        "retry_count": nonnegative,
                        "unknown_outcome_count": nonnegative,
                        "classification": {
                            "enum": ["not_required", "reconciled", "partial_retired", "incident_retained"]
                        },
                    }
                ),
                "privacy": _closed({"synthetic_only": {"type": "boolean"}, "scan_status": {"enum": ["PASS", "FAIL"]}}),
                "status": {"enum": ["PASS", "FAIL", "UNVERIFIED"]},
            }
        ),
    }


def _mapping(value: object, fields: frozenset[str] | set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("mssql_native.delivery_evidence.invalid_fields")
    return value


def _digest(value: object, pattern: re.Pattern[str]) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def validate_mssql_native_delivery_evidence(payload: object) -> None:
    """Validate evidence without accepting extension fields or false PASS claims."""
    value = _mapping(payload, _TOP)
    if value["schema_version"] != 2 or value["kind"] != "dpone.mssql-native-delivery-evidence.v2":
        raise ValueError("mssql_native.delivery_evidence.invalid_identity")
    subject = _mapping(
        value["subject"],
        {
            "invocation_id",
            "source_commit_sha",
            "dirty",
            "runner_image_sha256",
            "environment_receipt_sha256",
        },
    )
    if not (
        _digest(subject["invocation_id"], _SHA256)
        and _digest(subject["source_commit_sha"], _COMMIT)
        and type(subject["dirty"]) is bool
        and _digest(subject["runner_image_sha256"], _SHA256)
        and _digest(subject["environment_receipt_sha256"], _SHA256)
    ):
        raise ValueError("mssql_native.delivery_evidence.invalid_subject")
    backend = _mapping(
        value["backend"],
        {"import_backend", "backend_identity_sha256", "wire_identity_sha256", "package_version"},
    )
    if not (
        backend["import_backend"] in {"bcp", "mssql_sqlclient"}
        and _digest(backend["backend_identity_sha256"], _SHA256)
        and _digest(backend["wire_identity_sha256"], _SHA256)
        and _digest(backend["package_version"], _VERSION)
    ):
        raise ValueError("mssql_native.delivery_evidence.invalid_backend")
    timings = _mapping(value["timings"], _TIMINGS)
    if any(
        timing is not None and (type(timing) not in {int, float} or not math.isfinite(timing) or timing < 0)
        for timing in timings.values()
    ):
        raise ValueError("mssql_native.delivery_evidence.invalid_timing")
    resources = _mapping(value["resources"], {"business_rows_read_back", "bcp_process_count"})
    correctness = _mapping(
        value["correctness"],
        {"source_rows", "published_rows", "receipt_count", "target_digest_verified", "publication_verified"},
    )
    recovery = _mapping(value["recovery"], {"retry_count", "unknown_outcome_count", "classification"})
    privacy = _mapping(value["privacy"], {"synthetic_only", "scan_status"})
    if not all(_nonnegative_int(resources[field]) for field in resources):
        raise ValueError("mssql_native.delivery_evidence.invalid_resources")
    if not all(_nonnegative_int(correctness[field]) for field in ("source_rows", "published_rows", "receipt_count")):
        raise ValueError("mssql_native.delivery_evidence.invalid_correctness")
    if any(type(correctness[field]) is not bool for field in ("target_digest_verified", "publication_verified")):
        raise ValueError("mssql_native.delivery_evidence.invalid_correctness")
    if (
        not _nonnegative_int(recovery["retry_count"])
        or not _nonnegative_int(recovery["unknown_outcome_count"])
        or recovery["classification"]
        not in {
            "not_required",
            "reconciled",
            "partial_retired",
            "incident_retained",
        }
    ):
        raise ValueError("mssql_native.delivery_evidence.invalid_recovery")
    if type(privacy["synthetic_only"]) is not bool or privacy["scan_status"] not in {"PASS", "FAIL"}:
        raise ValueError("mssql_native.delivery_evidence.invalid_privacy")
    if value["status"] not in {"PASS", "FAIL", "UNVERIFIED"}:
        raise ValueError("mssql_native.delivery_evidence.invalid_status")
    if value["status"] == "PASS":
        if subject["dirty"]:
            raise ValueError("mssql_native.delivery_evidence.dirty_source")
        if any(timing is None for timing in timings.values()):
            raise ValueError("mssql_native.delivery_evidence.incomplete_timing")
        if backend["import_backend"] == "mssql_sqlclient":
            if resources["business_rows_read_back"] != 0:
                raise ValueError("mssql_native.delivery_evidence.business_rows_read_back")
            if resources["bcp_process_count"] != 0:
                raise ValueError("mssql_native.delivery_evidence.bcp_process_count")
        if correctness["source_rows"] != correctness["published_rows"]:
            raise ValueError("mssql_native.delivery_evidence.row_count_mismatch")
        expected_receipts = 0 if correctness["source_rows"] == 0 else 1
        if correctness["receipt_count"] < expected_receipts:
            raise ValueError("mssql_native.delivery_evidence.receipt_count")
        if not correctness["target_digest_verified"] or not correctness["publication_verified"]:
            raise ValueError("mssql_native.delivery_evidence.correctness_unproved")
        if privacy["scan_status"] != "PASS":
            raise ValueError("mssql_native.delivery_evidence.privacy_unproved")
        if recovery["classification"] == "not_required" and (
            recovery["retry_count"] != 0 or recovery["unknown_outcome_count"] != 0
        ):
            raise ValueError("mssql_native.delivery_evidence.recovery_inconsistent")
        if recovery["classification"] not in {"not_required", "reconciled"}:
            raise ValueError("mssql_native.delivery_evidence.recovery_unproved")


__all__ = ["mssql_native_delivery_evidence_v2_schema", "validate_mssql_native_delivery_evidence"]
