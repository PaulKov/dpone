"""Pure, versioned identity contract for dbt runtime payload trios."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_identifiers import dbt_workflow_id

DBT_RUNTIME_WIRE_V1 = "dpone.dbt-airflow-self-service.v1"
DBT_RUNTIME_WIRE_V2 = "dpone.dbt-airflow-self-service.v2"
MAX_DBT_RUNTIME_PAYLOAD_BYTES = 256 * 1024 * 1024
MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES = 512 * 1024 * 1024
MAX_DBT_RUNTIME_PAYLOADS = 64

_CONTENT_ID = re.compile(r"(dbt_project|dbt_manifest|dbt_selection)_sha256_([0-9a-f]{64})")

_PROJECT_MEDIA_TYPE = "application/vnd.dpone.dbt-project-bundle+gzip"
_MANIFEST_MEDIA_TYPE = "application/vnd.dbt.manifest+json"
_SELECTION_MEDIA_TYPE = "application/vnd.dpone.dbt-selection-lock+json"


def dbt_runtime_payload_read_limit(kind: str) -> int:
    """Retain tighter JSON reader bounds within the compact object size ceiling."""

    limits = {
        "dbt_project_bundle": MAX_DBT_RUNTIME_PAYLOAD_BYTES,
        "dbt_manifest": 16 * 1024 * 1024,
        "dbt_selection_lock": 1024 * 1024,
    }
    if kind not in limits:
        raise ValueError("unsupported dbt runtime payload kind")
    return limits[kind]


@dataclass(frozen=True, slots=True)
class DbtRuntimePayloadReference:
    """Canonical logical identity and location of one runtime payload."""

    id: str
    kind: str
    path: str
    media_type: str
    wire_contract: str = DBT_RUNTIME_WIRE_V1
    sha256: str | None = None

    def __post_init__(self) -> None:
        expected = _reference_fields(self.id, self.wire_contract)
        if (self.kind, self.path, self.media_type, self.sha256) != expected:
            raise ValueError("dbt runtime payload reference is not canonical")

    def descriptor(self, payload: bytes) -> dict[str, object]:
        """Return a descriptor only after validating exact content identity."""

        if not isinstance(payload, bytes) or not payload:
            raise ValueError("dbt runtime payload bytes must be non-empty")
        observed = sha256_bytes(payload)
        if self.sha256 is not None and observed != self.sha256:
            raise ValueError("dbt runtime payload differs from its content identity")
        return {
            "id": self.id,
            "kind": self.kind,
            "path": self.path,
            "sha256": observed,
            "bytes": len(payload),
            "media_type": self.media_type,
        }


def dbt_runtime_payload_reference(
    payload_id: str,
    *,
    wire_contract: str,
) -> DbtRuntimePayloadReference:
    """Resolve one ID under an explicit verified wire; never guess its version."""

    kind, path, media_type, digest = _reference_fields(payload_id, wire_contract)
    return DbtRuntimePayloadReference(payload_id, kind, path, media_type, wire_contract=wire_contract, sha256=digest)


def validate_dbt_runtime_payload_descriptor(
    value: object,
    *,
    wire_contract: str,
) -> DbtRuntimePayloadReference:
    """Validate received metadata without claiming its bytes were fetched.

    Consumers must separately compare the actual bytes to ``reference.descriptor``.
    Reject aliases/extra fields and bool sizes rather than canonicalizing signed input.
    """

    if not isinstance(value, Mapping) or set(value) != {"id", "kind", "path", "sha256", "bytes", "media_type"}:
        raise ValueError("dbt runtime payload descriptor fields are invalid")
    reference = dbt_runtime_payload_reference(value["id"], wire_contract=wire_contract)
    size = value["bytes"]
    if type(size) is not int or not 0 < size <= MAX_DBT_RUNTIME_PAYLOAD_BYTES:
        raise ValueError("dbt runtime payload size is invalid or exceeds its bound")
    _require_digest(value["sha256"])
    if (
        value["kind"] != reference.kind
        or value["path"] != reference.path
        or value["media_type"] != reference.media_type
        or (reference.sha256 is not None and value["sha256"] != reference.sha256)
    ):
        raise ValueError("dbt runtime payload descriptor is not canonical")
    return reference


def dbt_runtime_payload_trio(
    *,
    workflow_id: str,
    project_sha256: str,
    manifest_sha256: str,
    selection_lock_payload: bytes,
    wire_contract: str,
) -> tuple[str, str, str]:
    """Derive the exact ordered payload IDs owned by one workflow."""

    dbt_workflow_id(workflow_id)
    _require_digest(project_sha256)
    _require_digest(manifest_sha256)
    if not isinstance(selection_lock_payload, bytes) or not selection_lock_payload:
        raise ValueError("selection lock payload must be non-empty bytes")
    if wire_contract == DBT_RUNTIME_WIRE_V1:
        return "dbt_project", "dbt_manifest", f"dbt_selection_{workflow_id}"
    if wire_contract != DBT_RUNTIME_WIRE_V2:
        raise ValueError("unsupported dbt runtime wire contract")
    return (
        _content_id("dbt_project", project_sha256),
        _content_id("dbt_manifest", manifest_sha256),
        _content_id("dbt_selection", sha256_bytes(selection_lock_payload)),
    )


def validate_dbt_runtime_payload_trio(
    payload_ids: tuple[str, ...],
    *,
    wire_contract: str,
) -> None:
    """Require one ordered project/manifest/selection trio under one wire."""

    if len(payload_ids) != 3 or len(set(payload_ids)) != 3:
        raise ValueError("dbt runtime payload trio must contain three unique IDs")
    references = tuple(dbt_runtime_payload_reference(item, wire_contract=wire_contract) for item in payload_ids)
    if tuple(item.kind for item in references) != (
        "dbt_project_bundle",
        "dbt_manifest",
        "dbt_selection_lock",
    ):
        raise ValueError("dbt runtime payload trio has invalid kind ordering")


def _reference_fields(payload_id: str, wire_contract: str) -> tuple[str, str, str, str | None]:
    if not isinstance(payload_id, str):
        raise ValueError("dbt runtime payload ID must be text")
    if wire_contract == DBT_RUNTIME_WIRE_V1:
        return _legacy_fields(payload_id)
    if wire_contract == DBT_RUNTIME_WIRE_V2:
        return _content_fields(payload_id)
    raise ValueError("unsupported dbt runtime wire contract")


def _legacy_fields(payload_id: str) -> tuple[str, str, str, None]:
    if payload_id == "dbt_project":
        return "dbt_project_bundle", "runtime/dbt/project.tar.gz", _PROJECT_MEDIA_TYPE, None
    if payload_id == "dbt_manifest":
        return "dbt_manifest", "runtime/dbt/manifest.json", _MANIFEST_MEDIA_TYPE, None
    if not payload_id.startswith("dbt_selection_"):
        raise ValueError("unsupported legacy dbt runtime payload ID")
    workflow = dbt_workflow_id(payload_id.removeprefix("dbt_selection_"))
    return (
        "dbt_selection_lock",
        f"runtime/dbt/{workflow}.selection-lock.json",
        _SELECTION_MEDIA_TYPE,
        None,
    )


def _content_fields(payload_id: str) -> tuple[str, str, str, str]:
    match = _CONTENT_ID.fullmatch(payload_id)
    if match is None:
        raise ValueError("unsupported content-addressed dbt runtime payload ID")
    prefix, digest_hex = match.groups()
    kind, filename, media_type = {
        "dbt_project": ("dbt_project_bundle", "project.tar.gz", _PROJECT_MEDIA_TYPE),
        "dbt_manifest": ("dbt_manifest", "manifest.json", _MANIFEST_MEDIA_TYPE),
        "dbt_selection": ("dbt_selection_lock", "selection-lock.json", _SELECTION_MEDIA_TYPE),
    }[prefix]
    return (
        kind,
        f"runtime/dbt/objects/{digest_hex}/{filename}",
        media_type,
        f"sha256:{digest_hex}",
    )


def _content_id(prefix: str, digest: str) -> str:
    _require_digest(digest)
    return f"{prefix}_sha256_{digest.removeprefix('sha256:')}"


def _require_digest(value: str) -> None:
    if not is_canonical_sha256_digest(value):
        raise ValueError("dbt runtime payload digest is invalid")


__all__ = [
    "DBT_RUNTIME_WIRE_V1",
    "DBT_RUNTIME_WIRE_V2",
    "MAX_DBT_RUNTIME_PAYLOAD_BYTES",
    "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES",
    "MAX_DBT_RUNTIME_PAYLOADS",
    "DbtRuntimePayloadReference",
    "dbt_runtime_payload_reference",
    "dbt_runtime_payload_read_limit",
    "dbt_runtime_payload_trio",
    "validate_dbt_runtime_payload_descriptor",
    "validate_dbt_runtime_payload_trio",
]
