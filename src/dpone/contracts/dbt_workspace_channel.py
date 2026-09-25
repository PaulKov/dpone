"""Stable, credential-free workspace execution-channel identity.

Validation establishes shape and identity, not publication or SQL authority.
Callers must derive these coordinates from their trusted desired-state binding.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any
from urllib.parse import urlsplit

from dpone.contracts.airflow_desired_state_validation import digest, environment, project, text
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.strict_json import strict_json_object

CHANNEL_SCHEMA = "dpone.dbt-workspace-channel.v1"


class WorkspaceHandoverError(ValueError):
    """Content-free contract failure; never include raw authority or request data."""

    code = "DPONE_WORKSPACE_CHANNEL_AUTHORITY_MISMATCH"

    def __init__(self, reason: str, *, code: str = "DPONE_WORKSPACE_CHANNEL_AUTHORITY_MISMATCH") -> None:
        if code not in {
            "DPONE_WORKSPACE_CHANNEL_UNREGISTERED",
            "DPONE_WORKSPACE_CHANNEL_CAS_CONFLICT",
            "DPONE_WORKSPACE_HANDOVER_WAITING_ATTEMPTS",
            "DPONE_WORKSPACE_HANDOVER_COMMIT_UNKNOWN",
            "DPONE_WORKSPACE_HANDOVER_CONTINUATION_REQUIRED",
            "DPONE_WORKSPACE_CHANNEL_AUTHORITY_MISMATCH",
            "DPONE_WORKSPACE_REGISTRATION_PROOF_INVALID",
        }:
            raise ValueError("workspace handover diagnostic code is invalid")
        self.code = code
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


def require_workspace_document(
    value: object, *, schema: str, names: set[str], digest_field: str, maximum: int
) -> dict[str, Any]:
    """Read a closed bounded document and verify its canonical content digest."""
    if not isinstance(value, Mapping) or set(value) != names | {"schema", digest_field}:
        raise WorkspaceHandoverError("document_fields")
    result = dict(value)
    if result["schema"] != schema:
        raise WorkspaceHandoverError("document_schema")
    body = {key: item for key, item in result.items() if key != digest_field}
    require_workspace_size(result, maximum)
    if result[digest_field] != canonical_fingerprint(body):
        raise WorkspaceHandoverError("document_digest")
    return {key: result[key] for key in names}


def require_workspace_size(value: Mapping[str, object], maximum: int) -> None:
    """Bound the canonical document, including escaped embedded JSON payloads."""
    try:
        raw = json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise WorkspaceHandoverError("document_encoding") from None
    if len(raw) > maximum:
        raise WorkspaceHandoverError("document_size")


def decode_workspace_document(value: bytes | str, maximum: int) -> dict[str, Any]:
    """Reject oversized, duplicate-key or malformed JSON before typed parsing."""
    try:
        raw = value.encode("utf-8") if isinstance(value, str) else value
        if not isinstance(raw, bytes) or len(raw) > maximum:
            raise ValueError
        return strict_json_object(raw)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise WorkspaceHandoverError("document_json") from None


@dataclass(frozen=True, slots=True)
class WorkspaceChannel:
    """One desired object shared by replicas, never keyed by a watcher or cache."""

    desired_state_uri: str
    registry_scope_id: str
    environment: str
    source_project: str
    source_ref: str

    def __post_init__(self) -> None:
        try:
            text(self.desired_state_uri, field="desired_state_uri", maximum=32768)
            uri = urlsplit(self.desired_state_uri)
            if (
                uri.scheme != "s3"
                or not uri.netloc
                or not uri.path.startswith("/")
                or uri.username is not None
                or uri.password is not None
                or ":" in uri.netloc
                or uri.query
                or uri.fragment
                or "\\" in uri.path
                or any(part in {"", ".", ".."} for part in uri.path[1:].split("/"))
                or self.desired_state_uri != f"s3://{uri.netloc}{uri.path}"
            ):
                raise ValueError
            digest(self.registry_scope_id, field="registry_scope_id")
            environment(self.environment)
            project(self.source_project)
            text(self.source_ref, field="source_ref", maximum=256)
            require_workspace_size(self.to_dict(), 32 * 1024)
        except (TypeError, ValueError, AttributeError):
            raise WorkspaceHandoverError("channel_identity") from None

    @property
    def channel_sha256(self) -> str:
        """Hash only stable channel coordinates using repository canonical JSON."""
        return canonical_fingerprint({"schema": CHANNEL_SCHEMA, **asdict(self)})

    def to_dict(self) -> dict[str, object]:
        """Return a fresh closed mapping; mutating it cannot change this identity."""
        return {"schema": CHANNEL_SCHEMA, **asdict(self), "channel_sha256": self.channel_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceChannel:
        """Reject unknown fields and changed coordinates under a previous digest."""
        data = require_workspace_document(
            value,
            schema=CHANNEL_SCHEMA,
            names={item.name for item in fields(cls)},
            digest_field="channel_sha256",
            maximum=32 * 1024,
        )
        return cls(**data)

    @classmethod
    def from_json(cls, value: bytes | str) -> WorkspaceChannel:
        return cls.from_mapping(decode_workspace_document(value, 32 * 1024))
