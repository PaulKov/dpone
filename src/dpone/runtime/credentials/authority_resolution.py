"""Pure selection and validation helpers for runtime connection authority."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from threading import Lock
from typing import Any

from dpone.contracts.api_sources import get_api_source_defaults
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    RuntimeConnectionAuthorityError,
    canonical_runtime_endpoint_type,
)
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext

_LEGACY_AUTHORITY_FIELDS = frozenset(
    {"connection_id", "connection_type", "credentials_source", "vault_mount_point", "vault_path"}
)
_LEGACY_WARNING_LOCK = Lock()
_legacy_warning_emitted = False
_LEGACY_POLICY_DISABLED = "disabled"
_LEGACY_POLICY_EXPLICIT_ONLY = "explicit_only"
_LEGACY_DEFAULTS_DISABLED_CODE = "DPONE_LEGACY_RUNTIME_DEFAULTS_DISABLED"
_LEGACY_DEFAULTS_DISABLED_MESSAGE = "Implicit runtime defaults are disabled; configure a logical connection_ref."


def warn_legacy_runtime_connections() -> None:
    """Emit the compatibility warning at most once in this Python process."""

    global _legacy_warning_emitted
    with _LEGACY_WARNING_LOCK:
        if _legacy_warning_emitted:
            return
        _legacy_warning_emitted = True
    warnings.warn(
        "Legacy runtime connection fields are deprecated; migrate to connection_ref.",
        DeprecationWarning,
        stacklevel=2,
    )


def source_ref(source: Mapping[str, Any]) -> str | None:
    if canonical_runtime_endpoint_type(source.get("type")) != "api":
        return canonical_ref(source, capability="source", required=True)
    defaults = get_api_source_defaults(source.get("api_type"))
    required = defaults.credentials_mode != "none"
    value = canonical_ref(source, capability="source", required=required)
    return value or None


def canonical_ref(endpoint: Mapping[str, Any], *, capability: str, required: bool) -> str:
    reject_mixed_authority(endpoint, capability=capability)
    raw = endpoint.get("connection_ref")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if required:
        raise error("DPONE_RUNTIME_CONNECTION_REF_REQUIRED", f"Strict runtime requires {capability}.connection_ref.")
    return ""


def optional_ref(endpoint: Mapping[str, Any], *, capability: str) -> str | None:
    if not endpoint:
        return None
    reject_mixed_authority(endpoint, capability=capability)
    raw = endpoint.get("connection_ref")
    if raw in (None, ""):
        if endpoint.get("proxy_enable") is True:
            raise error(
                "DPONE_RUNTIME_CONNECTION_REF_REQUIRED",
                f"Strict runtime requires {capability}.connection_ref.",
            )
        return None
    return canonical_ref(endpoint, capability=capability, required=True)


def nested_optional_ref(parent: Mapping[str, Any], field: str, *, capability: str) -> str | None:
    nested = mapping(parent.get(field))
    return optional_ref(nested, capability=capability) if nested else None


def state_ref(state: Mapping[str, Any], *, sink_ref: str, state_required: bool) -> str | None:
    if not state:
        if state_required:
            raise error(
                "DPONE_RUNTIME_CONNECTION_REF_REQUIRED",
                "Strict stateful runtime requires state.connection_ref or state.reuse: sink.",
            )
        return None
    if str(state.get("type") or "").lower() in {"disabled", "noop", "none", "off"}:
        return None
    reuse = str(state.get("reuse") or "")
    if reuse:
        reject_mixed_authority(state, capability="state")
        if reuse != "sink" or state.get("connection_ref") not in (None, ""):
            raise error(
                "DPONE_RUNTIME_CONNECTION_AUTHORITY_CONFLICT",
                "state.reuse must be exactly 'sink' and cannot be combined with connection_ref.",
            )
        return sink_ref
    return canonical_ref(state, capability="state", required=state_required)


def reject_canonical_without_context(*sections: Mapping[str, Any]) -> None:
    if any(_contains_connection_ref(section) for section in sections):
        raise error(
            "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED",
            "Canonical connection_ref execution requires a verified runtime connection context.",
        )


def require_explicit_legacy_authority(
    config: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    sink: Mapping[str, Any],
    state: Mapping[str, Any],
    proxy: Mapping[str, Any],
    object_storage: Mapping[str, Any],
) -> None:
    policy = _legacy_runtime_policy(config)
    sections = (source, sink, state, proxy, object_storage)
    if policy == _LEGACY_POLICY_EXPLICIT_ONLY and any(_contains_legacy_authority(item) for item in sections):
        return
    raise error(_LEGACY_DEFAULTS_DISABLED_CODE, _LEGACY_DEFAULTS_DISABLED_MESSAGE)


def _legacy_runtime_policy(config: Mapping[str, Any]) -> str:
    runtime = mapping(config.get("runtime"))
    compatibility = mapping(runtime.get("compatibility"))
    value = compatibility.get("legacy_runtime_connections")
    return _LEGACY_POLICY_EXPLICIT_ONLY if value == _LEGACY_POLICY_EXPLICIT_ONLY else _LEGACY_POLICY_DISABLED


def _contains_legacy_authority(value: Mapping[str, Any]) -> bool:
    if any(value.get(field) not in (None, "") for field in _LEGACY_AUTHORITY_FIELDS):
        return True
    return any(isinstance(item, Mapping) and _contains_legacy_authority(item) for item in value.values())


def _contains_connection_ref(value: Mapping[str, Any]) -> bool:
    if value.get("connection_ref") not in (None, ""):
        return True
    return any(isinstance(item, Mapping) and _contains_connection_ref(item) for item in value.values())


def reject_mixed_authority(endpoint: Mapping[str, Any], *, capability: str) -> None:
    conflicts = sorted(key for key in _LEGACY_AUTHORITY_FIELDS if endpoint.get(key) not in (None, ""))
    if conflicts:
        raise error(
            "DPONE_RUNTIME_CONNECTION_AUTHORITY_CONFLICT",
            f"{capability} contains legacy connection authority in strict runtime.",
        )


def reject_strict_legacy_authority(
    *,
    source: Mapping[str, Any],
    sink: Mapping[str, Any],
    state: Mapping[str, Any],
    proxy: Mapping[str, Any],
    object_storage: Mapping[str, Any],
) -> None:
    for capability, endpoint in (
        ("source", source),
        ("sink", sink),
        ("state", state),
        ("bigquery_proxy", proxy),
        ("object_storage", object_storage),
    ):
        reject_mixed_authority(endpoint, capability=capability)


def resolve(context: RuntimeConnectionContext, connection_ref: str) -> ResolvedBindingConnection:
    try:
        resolved = context.resolver.resolve(connection_ref)
    except RuntimeConnectionAuthorityError:
        raise
    except Exception as exc:
        raise error(
            "DPONE_RUNTIME_CONNECTION_RESOLUTION_FAILED",
            f"Runtime connection_ref {connection_ref!r} could not be resolved.",
        ) from exc
    if not isinstance(resolved, ResolvedBindingConnection):
        raise error(
            "DPONE_RUNTIME_CONNECTION_RESOLUTION_FAILED",
            "Runtime credential resolver returned an unsupported connection object.",
        )
    return resolved


def require_type(resolved: ResolvedBindingConnection, endpoint: Mapping[str, Any], *, capability: str) -> None:
    descriptor = resolved.descriptor
    if descriptor is None:
        raise error(
            "DPONE_RUNTIME_CONNECTION_TYPE_MISSING",
            f"Resolved {capability} connection has no declared type.",
        )
    endpoint_type = canonical_runtime_endpoint_type(endpoint.get("type"))
    expected = endpoint_type
    if endpoint_type == "api":
        expected = canonical_runtime_endpoint_type(endpoint.get("api_type") or "api")
    actual = canonical_runtime_endpoint_type(descriptor.connection_type)
    accepted = {expected, "api"} if endpoint_type == "api" else {expected}
    if expected and actual not in accepted:
        raise error(
            "DPONE_RUNTIME_CONNECTION_TYPE_MISMATCH",
            f"Resolved {capability} connection type does not match the manifest.",
        )


def mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def error(code: str, message: str) -> RuntimeConnectionAuthorityError:
    return RuntimeConnectionAuthorityError(code, message)


__all__ = [
    "canonical_runtime_endpoint_type",
    "canonical_ref",
    "mapping",
    "nested_optional_ref",
    "optional_ref",
    "reject_canonical_without_context",
    "reject_strict_legacy_authority",
    "require_explicit_legacy_authority",
    "require_type",
    "resolve",
    "source_ref",
    "state_ref",
    "warn_legacy_runtime_connections",
]
