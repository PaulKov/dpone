"""Resolve dpone binding-set connection refs inside the runtime plane."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.credential_resolution import (
    BACKEND_UNAVAILABLE,
    DAG_RUN_SCOPE_UNSUPPORTED,
    FIELD_MISSING,
    PINNED_VERSION_UNSUPPORTED,
    RESOLUTION_SCOPE_DAG_RUN_START,
    SUPPORTED_RESOLUTION_SCOPES,
    SUPPORTED_VERSION_POLICIES,
    VERSION_METADATA_MISSING,
    VERSION_POLICY_PINNED,
    CredentialResolutionError,
    is_valid_connection_ref,
    is_valid_env_var_name,
)
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.kubernetes_names import is_valid_kubernetes_dns_label
from dpone.runtime.credentials.airflow_uri_registry import credentials_from_airflow_connection_uri
from dpone.runtime.credentials.binding_evidence import (
    credential_policy_metadata,
    credential_reference_metadata,
    payload_format_metadata,
    resolved_at_metadata,
    safe_evidence_context,
)
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.vault_references import is_valid_vault_logical_path, is_valid_vault_mount

_PRODUCTION_ENV_NAMES = frozenset({"prod", "production"})
_UTC = timezone.utc  # noqa: UP017 - dpone's mypy baseline targets pre-3.11 datetime stubs.
_CREDENTIAL_CONFIG_FIELDS = frozenset(field.name for field in fields(CredentialsConfig)) - {"additional_params"}
# Registry ``connection.*`` owns endpoint topology; credential field maps must not
# supply or override these keys (no secret→registry precedence merge).
_REGISTRY_OWNED_ENDPOINT_FIELDS = frozenset(
    {
        "host",
        "endpoint",
        "port",
        "database",
        "schema",
        "secure",
        "project_id",
        "bootstrap_servers",
    }
)


class VaultKvClient(Protocol):
    def get_secret(self, *, mount_point: str, path: str) -> VaultSecretSnapshot | Mapping[str, Any]:
        """Return one Vault KV secret payload."""


class KubernetesSecretReader(Protocol):
    def read_secret(self, *, namespace: str, name: str) -> Mapping[str, Any]:
        """Return one Kubernetes Secret payload with already decoded field values."""


@dataclass(frozen=True)
class VaultSecretSnapshot:
    """One atomic Vault payload and its optional backend data version."""

    data: Mapping[str, Any]
    version: int | None = None


class BindingCredentialResolver:
    """Resolve backend-neutral binding-set references to runtime credentials."""

    def __init__(
        self,
        *,
        binding_set: Mapping[str, Any],
        connection_registry: Mapping[str, Any],
        vault_kv_reader: VaultKvClient | None = None,
        kubernetes_secret_reader: KubernetesSecretReader | None = None,
        evidence_context: Mapping[str, Any] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._binding_set = binding_set
        self._connection_registry = connection_registry
        self._vault_kv_reader = vault_kv_reader
        self._kubernetes_secret_reader = kubernetes_secret_reader
        self._evidence_context = safe_evidence_context(evidence_context)
        self._environment = str(binding_set.get("environment") or "")
        self._clock = clock or (lambda: datetime.now(_UTC))

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        if not is_valid_connection_ref(connection_ref):
            raise ValueError("connection_ref must be a logical connection_ref alias")
        ref = self._bound_ref(connection_ref)
        entry = self._registry_entry(ref)
        connection = _mapping(entry.get("connection"))
        credentials = _mapping(entry.get("credentials"))
        resolver = str(credentials.get("resolver") or "")
        _validate_credential_policy(credentials)
        if resolver == "env_var":
            resolved = self._resolve_env(connection=connection, credentials=credentials)
        elif resolver == "vault_kv":
            resolved = self._resolve_vault(connection=connection, credentials=credentials)
        elif resolver == "kubernetes_secret_volume":
            resolved = self._resolve_kubernetes_secret_volume(connection=connection, credentials=credentials)
        elif resolver == "kubernetes_secret_api":
            resolved = self._resolve_kubernetes_secret_api(connection=connection, credentials=credentials)
        elif resolver == "airflow_connection":
            raise ValueError(
                "airflow_connection resolver is an operator-side bridge; "
                "runtime pods must receive projected credentials instead of reading Airflow directly"
            )
        else:
            raise ValueError(f"Unsupported credential resolver: {resolver or '<missing>'}")
        return ResolvedBindingConnection(
            credentials=resolved[0],
            safe_metadata={
                "connection_ref": ref,
                "resolver": resolver,
                **payload_format_metadata(credentials),
                **credential_reference_metadata(resolver, credentials),
                **credential_policy_metadata(credentials),
                "resolved_version": resolved[1],
                **resolved_at_metadata(resolved[1], self._clock),
                **self._evidence_context,
            },
            descriptor=ResolvedConnectionDescriptor(
                connection_type=_str_value(entry.get("type")) or "",
                properties=connection,
            ),
        )

    def _bound_ref(self, connection_ref: str) -> str:
        bindings = _mapping(self._binding_set.get("bindings"))
        binding = _mapping(bindings.get(connection_ref))
        ref = binding.get("connection_ref")
        if not isinstance(ref, str) or not ref:
            raise KeyError(f"Connection ref is not bound: {connection_ref}")
        if not is_valid_connection_ref(ref):
            raise ValueError("binding-set connection_ref must be a logical connection_ref alias")
        return ref

    def _registry_entry(self, connection_ref: str) -> Mapping[str, Any]:
        if not is_valid_connection_ref(connection_ref):
            raise ValueError("registry connection_ref must be a logical connection_ref alias")
        connections = _mapping(self._connection_registry.get("connections"))
        entry = connections.get(connection_ref)
        if not isinstance(entry, Mapping):
            raise KeyError(f"Connection ref is missing from registry: {connection_ref}")
        return entry

    def _resolve_env(
        self,
        *,
        connection: Mapping[str, Any],
        credentials: Mapping[str, Any],
    ) -> tuple[CredentialsConfig, int | None]:
        if credentials.get("support") != "development_only":
            raise ValueError("env_var resolver requires support: development_only")
        if self._environment.lower() in _PRODUCTION_ENV_NAMES:
            raise ValueError("env_var resolver is development/legacy only")
        fields = _credential_fields(credentials, "env_var")
        values: dict[str, str] = {}
        for name, env_name in fields.items():
            if not is_valid_env_var_name(env_name):
                raise ValueError(f"env_var resolver field {name} must reference a valid environment variable name")
            value = os.getenv(env_name)
            if value is None:
                raise ValueError(f"Environment variable is not set: {env_name}")
            values[name] = value
        _require_mapped_values(values, resolver="env_var")
        return _credentials_config(connection, values), None

    def _resolve_vault(
        self,
        *,
        connection: Mapping[str, Any],
        credentials: Mapping[str, Any],
    ) -> tuple[CredentialsConfig, int | None]:
        _require_supported_vault_policy(credentials)
        mount = str(credentials.get("mount") or "secret")
        path = str(credentials.get("path") or "")
        if not is_valid_vault_mount(mount):
            raise ValueError("vault_kv resolver requires a logical Vault KV mount")
        if not is_valid_vault_logical_path(path):
            raise ValueError("vault_kv resolver requires a logical Vault KV path")
        if self._vault_kv_reader is None:
            raise CredentialResolutionError(
                BACKEND_UNAVAILABLE,
                "Credential backend is unavailable; restore it and retry the workload.",
                resolver="vault_kv",
            )
        try:
            snapshot = _vault_snapshot(self._vault_kv_reader.get_secret(mount_point=mount, path=path))
        except Exception:
            raise CredentialResolutionError(
                BACKEND_UNAVAILABLE,
                "Credential backend is unavailable; restore it and retry the workload.",
                resolver="vault_kv",
            ) from None
        resolved_version = snapshot.version
        if credentials.get("kv_version") == 2 and (resolved_version is None or resolved_version <= 0):
            raise CredentialResolutionError(
                VERSION_METADATA_MISSING,
                "Vault KV v2 response has no positive version metadata.",
                resolver="vault_kv",
            )
        fields = _credential_fields(credentials, "vault_kv")
        mapped = {name: snapshot.data.get(secret_key) for name, secret_key in fields.items()}
        _require_mapped_values(mapped, resolver="vault_kv")
        return _credentials_config(connection, mapped), resolved_version

    def _resolve_kubernetes_secret_volume(
        self,
        *,
        connection: Mapping[str, Any],
        credentials: Mapping[str, Any],
    ) -> tuple[CredentialsConfig, int | None]:
        if not is_valid_kubernetes_dns_label(credentials.get("secret_name")):
            raise ValueError("kubernetes_secret_volume resolver requires a safe Kubernetes Secret name")
        mount_path = str(credentials.get("mount_path") or "")
        if not mount_path:
            raise ValueError("kubernetes_secret_volume resolver requires mount_path")
        mount_root = Path(mount_path).resolve(strict=True)
        fields = _credential_fields(credentials, "kubernetes_secret_volume")
        values = {name: _read_secret_volume_file(mount_root, file_name) for name, file_name in fields.items()}
        _require_mapped_values(values, resolver="kubernetes_secret_volume")
        if credentials.get("payload_format") == "airflow_connection_uri":
            uri = _str_value(values.get("uri"))
            if not uri:
                raise ValueError("kubernetes_secret_volume airflow_connection_uri payload requires fields.uri")
            return credentials_from_airflow_connection_uri(uri, connection=connection), None
        return _credentials_config(connection, values), None

    def _resolve_kubernetes_secret_api(
        self,
        *,
        connection: Mapping[str, Any],
        credentials: Mapping[str, Any],
    ) -> tuple[CredentialsConfig, int | None]:
        if self._kubernetes_secret_reader is None:
            raise ValueError("kubernetes_secret_api resolver requires injected kubernetes_secret_reader")
        if not is_valid_kubernetes_dns_label(credentials.get("namespace")):
            raise ValueError("kubernetes_secret_api resolver requires a safe Kubernetes namespace")
        if not is_valid_kubernetes_dns_label(credentials.get("name")):
            raise ValueError("kubernetes_secret_api resolver requires a safe Kubernetes Secret name")
        namespace = str(credentials.get("namespace") or "")
        name = str(credentials.get("name") or "")
        secret = dict(self._kubernetes_secret_reader.read_secret(namespace=namespace, name=name))
        fields = _credential_fields(credentials, "kubernetes_secret_api")
        mapped = {field_name: secret.get(secret_key) for field_name, secret_key in fields.items()}
        _require_mapped_values(mapped, resolver="kubernetes_secret_api")
        return _credentials_config(connection, mapped), None


def _credentials_config(connection: Mapping[str, Any], secret_values: Mapping[str, Any]) -> CredentialsConfig:
    _reject_registry_owned_endpoint_fields(secret_values)
    values = {
        name: value
        for name, value in secret_values.items()
        if name in _CREDENTIAL_CONFIG_FIELDS and name not in _REGISTRY_OWNED_ENDPOINT_FIELDS
    }
    values.update(
        host=_str_value(connection.get("host")),
        port=_connection_port(connection),
        database=_str_value(connection.get("database")),
        username=_str_value(secret_values.get("username")),
        password=_str_value(secret_values.get("password")),
        schema=_str_value(connection.get("schema")),
        endpoint=_str_value(connection.get("endpoint")),
        token=_str_value(secret_values.get("token")),
        api_key=_str_value(secret_values.get("api_key")),
        secure=_connection_boolean(connection, "secure", default=False),
        project_id=_str_value(connection.get("project_id")),
        bootstrap_servers=_str_value(connection.get("bootstrap_servers")),
    )
    additional_params = deepcopy(dict(_mapping(connection.get("parameters"))))
    additional_params.update(
        {
            name: deepcopy(value)
            for name, value in secret_values.items()
            if name not in _CREDENTIAL_CONFIG_FIELDS and name not in _REGISTRY_OWNED_ENDPOINT_FIELDS
        }
    )
    values["additional_params"] = additional_params or None
    return CredentialsConfig(**values)


def _reject_registry_owned_endpoint_fields(secret_values: Mapping[str, Any]) -> None:
    """Fail closed when credential payloads claim registry-owned endpoint identity."""

    claimed = sorted(name for name in secret_values if name in _REGISTRY_OWNED_ENDPOINT_FIELDS)
    if claimed:
        raise ValueError(
            "credential fields must not supply registry-owned connection endpoint metadata "
            f"(DPONE_RUNTIME_CONNECTION_VALUE_MISMATCH): {', '.join(claimed)}"
        )


def _vault_snapshot(value: VaultSecretSnapshot | Mapping[str, Any]) -> VaultSecretSnapshot:
    if isinstance(value, VaultSecretSnapshot):
        return VaultSecretSnapshot(data=value.data, version=_int_value(value.version))
    data = dict(value)
    metadata = data.pop("_metadata", None)
    version = _int_value(metadata.get("version")) if isinstance(metadata, Mapping) else None
    return VaultSecretSnapshot(data=data, version=version)


def _read_secret_volume_file(mount_root: Path, file_name: str) -> str:
    relative_name = Path(file_name)
    if relative_name.is_absolute() or ".." in relative_name.parts:
        raise ValueError("secret volume file escapes mount_path")
    candidate = (mount_root / relative_name).resolve(strict=True)
    try:
        candidate.relative_to(mount_root)
    except ValueError as exc:
        raise ValueError("secret volume file escapes mount_path") from exc
    if not candidate.is_file():
        raise ValueError("secret volume field is not a file")
    return candidate.read_text(encoding="utf-8").rstrip("\n")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _credential_fields(credentials: Mapping[str, Any], resolver: str) -> dict[str, str]:
    fields = _mapping(credentials.get("fields"))
    if not fields:
        raise ValueError(f"{resolver} resolver requires non-empty fields")
    if any(not _non_empty_string(key) or not _non_empty_string(value) for key, value in fields.items()):
        raise ValueError(f"{resolver} fields must contain non-empty string mappings")
    return {str(key): str(value) for key, value in fields.items()}


def _require_mapped_values(values: Mapping[str, Any], *, resolver: str) -> None:
    if any(value is None or (isinstance(value, str) and not value.strip()) for value in values.values()):
        raise CredentialResolutionError(
            FIELD_MISSING,
            "A declared credential field is missing or empty.",
            resolver=resolver,
        )


def _validate_credential_policy(credentials: Mapping[str, Any]) -> None:
    _validate_optional_policy_value(credentials, key="version_policy", allowed=SUPPORTED_VERSION_POLICIES)
    _validate_optional_policy_value(credentials, key="resolution_scope", allowed=SUPPORTED_RESOLUTION_SCOPES)


def _require_supported_vault_policy(credentials: Mapping[str, Any]) -> None:
    if credentials.get("version_policy") == VERSION_POLICY_PINNED:
        raise CredentialResolutionError(
            PINNED_VERSION_UNSUPPORTED,
            "Pinned Vault data versions are not supported by the runtime client.",
            resolver="vault_kv",
        )
    if credentials.get("resolution_scope") == RESOLUTION_SCOPE_DAG_RUN_START:
        raise CredentialResolutionError(
            DAG_RUN_SCOPE_UNSUPPORTED,
            "DAG-run credential snapshots are not supported; use workload_start.",
            resolver="vault_kv",
        )


def _validate_optional_policy_value(credentials: Mapping[str, Any], *, key: str, allowed: frozenset[str]) -> None:
    if key not in credentials or credentials.get(key) in (None, ""):
        return
    value = credentials.get(key)
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"credentials {key} is invalid")


def _str_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _int_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _connection_port(connection: Mapping[str, Any]) -> int | None:
    if "port" not in connection or connection.get("port") is None:
        return None
    value = connection.get("port")
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError("connection port must be an integer between 1 and 65535")
    return value


def _connection_boolean(
    connection: Mapping[str, Any],
    field: str,
    *,
    default: bool,
) -> bool:
    if field not in connection or connection.get(field) is None:
        return default
    value = connection.get(field)
    if isinstance(value, bool):
        return value
    raise ValueError(f"connection {field} must be a JSON boolean")


__all__ = [
    "BindingCredentialResolver",
    "KubernetesSecretReader",
    "ResolvedBindingConnection",
    "ResolvedConnectionDescriptor",
    "VaultSecretSnapshot",
    "VaultKvClient",
]
