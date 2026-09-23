"""Closed, dependency-free credential projection reader; never reads secrets.

The deployment authorizes exact source keys and workload membership. This module
validates that wire for both core and the thin provider, without importing core,
Airflow, Kubernetes or any credential backend.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

PROJECTION_SCHEMA = "dpone.runtime-credential-projection.v1"
PROJECTION_FILENAME = "credential-projection.json"
MAX_PROJECTION_BYTES = 1024 * 1024
MAX_PROJECTION_SOURCES = 1024
MAX_WORKLOAD_CONNECTIONS = 256
MOUNT_ROOT = "/run/secrets/dpone/airflow-connections"
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ROOT = frozenset(
    "schema environment release_id binding_set_sha256 source_registry_sha256 "
    "runtime_registry_sha256 publish_authority_sha256 workspace_authority_connection_ref sources workloads".split()
)


class CredentialProjectionError(ValueError):
    """Stable redacted failure; caller supplies no source or secret values."""

    def __init__(self, reason: str = "INVALID") -> None:
        self.code = f"DPONE_RUNTIME_CREDENTIAL_PROJECTION_{reason}"
        super().__init__(f"credential projection {reason.lower()}; rebuild with matching protected authority")


def canonical_projection_bytes(payload: Mapping[str, Any]) -> bytes:
    """One canonical identity encoding, shared by producer and readers."""
    return json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def projection_descriptor(payload: bytes) -> dict[str, Any]:
    """Address projection independently of deployment identity (no hash cycle)."""
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "artifact_ref": f"cache://runtime-credential-projections/sha256-{digest}/{PROJECTION_FILENAME}",
        "sha256": f"sha256:{digest}",
        "bytes": len(payload),
    }


def require_projection_descriptor(value: object) -> Mapping[str, Any]:
    """Reject unknown fields, unbounded bytes and mutable or mismatched paths."""
    raw = _exact(value, {"artifact_ref", "sha256", "bytes"})
    digest = _digest(raw["sha256"])
    size = raw["bytes"]
    if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= MAX_PROJECTION_BYTES:
        raise CredentialProjectionError("LIMIT_EXCEEDED")
    expected = f"cache://runtime-credential-projections/{digest.replace(':', '-')}/{PROJECTION_FILENAME}"
    if raw["artifact_ref"] != expected:
        raise CredentialProjectionError()
    return raw


@dataclass(frozen=True, slots=True)
class CredentialSource:
    registry_ref: str
    connection_id: str
    secret_name: str
    secret_key: str
    mount_path: str
    filename: str

    def to_dict(self) -> dict[str, str]:
        return {key: getattr(self, key) for key in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class CredentialMembership:
    connection_ref: str
    registry_ref: str
    role: str

    def to_dict(self) -> dict[str, str]:
        return {key: getattr(self, key) for key in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class WorkloadCredentials:
    workload_id: str
    connections: tuple[CredentialMembership, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"workload_id": self.workload_id, "connections": [item.to_dict() for item in self.connections]}


@dataclass(frozen=True, slots=True)
class CredentialProjection:
    environment: str
    release_id: str
    binding_set_sha256: str
    source_registry_sha256: str
    runtime_registry_sha256: str
    publish_authority_sha256: str
    workspace_authority_connection_ref: str
    sources: tuple[CredentialSource, ...]
    workloads: tuple[WorkloadCredentials, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": PROJECTION_SCHEMA,
            **{key: getattr(self, key) for key in _ROOT - {"schema", "sources", "workloads"}},
            "sources": [item.to_dict() for item in self.sources],
            "workloads": [item.to_dict() for item in self.workloads],
        }

    def membership(self, workload_id: str) -> tuple[CredentialMembership, ...]:
        for workload in self.workloads:
            if workload.workload_id == workload_id:
                return workload.connections
        raise CredentialProjectionError("MISMATCH")

    def selected_sources(self, workload_id: str) -> tuple[CredentialSource, ...]:
        selected = {item.registry_ref for item in self.membership(workload_id)}
        return tuple(item for item in self.sources if item.registry_ref in selected)

    def require_authority(self, *, control_ref: str, authority_sha256: str | None = None) -> None:
        """Compare pointer-derived ref, plus protected fingerprint when available."""
        if control_ref != self.workspace_authority_connection_ref or (
            authority_sha256 is not None and authority_sha256 != self.publish_authority_sha256
        ):
            raise CredentialProjectionError("MISMATCH")


def parse_credential_projection(
    payload: bytes,
    *,
    descriptor: Mapping[str, Any],
    environment: str,
    release_id: str,
    binding_set_sha256: str,
    runtime_registry_sha256: str,
) -> CredentialProjection:
    """Verify exact bytes and coordinate mirrors before returning immutable DTOs."""
    require_projection_descriptor(descriptor)
    if len(payload) > MAX_PROJECTION_BYTES or projection_descriptor(payload) != dict(descriptor):
        raise CredentialProjectionError("MISMATCH")
    try:
        raw = _exact(json.loads(payload, object_pairs_hook=_unique), _ROOT)
        if canonical_projection_bytes(raw) != payload or raw["schema"] != PROJECTION_SCHEMA:
            raise CredentialProjectionError()
        for key, expected in (
            ("environment", environment),
            ("release_id", release_id),
            ("binding_set_sha256", binding_set_sha256),
            ("runtime_registry_sha256", runtime_registry_sha256),
        ):
            if raw[key] != expected:
                raise CredentialProjectionError("MISMATCH")
        _token(raw["environment"])
        for key in (
            "release_id",
            "binding_set_sha256",
            "source_registry_sha256",
            "runtime_registry_sha256",
            "publish_authority_sha256",
        ):
            _digest(raw[key])
        control = _token(raw["workspace_authority_connection_ref"])
        sources = _sources(raw["sources"])
        workloads = _workloads(raw["workloads"], control=control)
        used = {item.registry_ref for workload in workloads for item in workload.connections}
        if any(source.registry_ref not in used for source in sources):
            raise CredentialProjectionError()
        return CredentialProjection(
            **{key: raw[key] for key in _ROOT - {"schema", "sources", "workloads"}},
            sources=sources,
            workloads=workloads,
        )
    except (TypeError, KeyError, UnicodeError, json.JSONDecodeError, RecursionError):
        raise CredentialProjectionError() from None


def require_registry_parity(
    projection: CredentialProjection,
    *,
    binding_set: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> None:
    """Prove every membership and file resolver matches the sealed snapshots."""
    bindings, connections = binding_set.get("bindings"), registry.get("connections")
    if not isinstance(bindings, Mapping) or not isinstance(connections, Mapping):
        raise CredentialProjectionError("MISMATCH")
    sources = {item.registry_ref: item for item in projection.sources}
    for workload in projection.workloads:
        for member in workload.connections:
            binding, entry = bindings.get(member.connection_ref), connections.get(member.registry_ref)
            if (
                not isinstance(binding, Mapping)
                or binding.get("connection_ref") != member.registry_ref
                or not isinstance(entry, Mapping)
            ):
                raise CredentialProjectionError("MISMATCH")
            credentials = entry.get("credentials")
            if not isinstance(credentials, Mapping):
                raise CredentialProjectionError("MISMATCH")
            source = sources.get(member.registry_ref)
            if source is None:
                if credentials.get("resolver") != "vault_kv":
                    raise CredentialProjectionError("UNSUPPORTED")
            elif any(
                credentials.get(key) != value
                for key, value in {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": source.secret_name,
                    "mount_path": source.mount_path,
                    "payload_format": "airflow_connection_uri",
                    "fields": {"uri": source.filename},
                }.items()
            ):
                raise CredentialProjectionError("MISMATCH")


def _sources(value: object) -> tuple[CredentialSource, ...]:
    rows = _list(value, MAX_PROJECTION_SOURCES)
    result = []
    keys: dict[tuple[str, str], str] = {}
    for row in rows:
        raw = _exact(row, set(CredentialSource.__dataclass_fields__))
        ref, connection_id = _token(raw["registry_ref"]), _token(raw["connection_id"])
        key = "AIRFLOW_CONN_" + re.sub(r"[^A-Za-z0-9_]", "_", connection_id).upper()
        if raw["secret_key"] != key or raw["mount_path"] != f"{MOUNT_ROOT}/{ref}" or raw["filename"] != "uri":
            raise CredentialProjectionError()
        secret = raw["secret_name"]
        if not isinstance(secret, str) or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,251}[a-z0-9])?", secret):
            raise CredentialProjectionError()
        source_key = (secret, key)
        if source_key in keys and keys[source_key] != connection_id:
            raise CredentialProjectionError()
        keys[source_key] = connection_id
        result.append(CredentialSource(**raw))
    _ordered_unique([item.registry_ref for item in result])
    return tuple(result)


def _workloads(value: object, *, control: str) -> tuple[WorkloadCredentials, ...]:
    result = []
    for row in _list(value, MAX_PROJECTION_SOURCES):
        raw = _exact(row, {"workload_id", "connections"})
        members = []
        for value in _list(raw["connections"], MAX_WORKLOAD_CONNECTIONS):
            member = _exact(value, set(CredentialMembership.__dataclass_fields__))
            _token(member["connection_ref"])
            _token(member["registry_ref"])
            if member["role"] not in {"workload", "workspace_control"} or (
                member["role"] == "workspace_control" and member["connection_ref"] != control
            ):
                raise CredentialProjectionError()
            members.append(CredentialMembership(**member))
        _ordered_unique([(item.connection_ref, item.role) for item in members])
        if not members or sum(item.role == "workspace_control" for item in members) != 1:
            raise CredentialProjectionError()
        result.append(WorkloadCredentials(_token(raw["workload_id"]), tuple(members)))
    _ordered_unique([item.workload_id for item in result])
    if not result:
        raise CredentialProjectionError()
    return tuple(result)


def _exact(value: object, fields: set[str] | frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise CredentialProjectionError()
    return value


def _list(value: object, maximum: int) -> list[Any]:
    if not isinstance(value, list):
        raise CredentialProjectionError()
    if len(value) > maximum:
        raise CredentialProjectionError("LIMIT_EXCEEDED")
    return value


def _token(value: object) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise CredentialProjectionError()
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise CredentialProjectionError()
    return value


def _ordered_unique(values: list[Any]) -> None:
    if values != sorted(set(values)):
        raise CredentialProjectionError()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(pairs)
    if len(result) != len(pairs):
        raise CredentialProjectionError()
    return result
