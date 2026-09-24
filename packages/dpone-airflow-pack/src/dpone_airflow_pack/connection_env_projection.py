"""Closed, reference-only metadata for execution-time Connection environments."""

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.connection_names import require_airflow_connection_id
from dpone_airflow_pack.connection_projection import airflow_conn_env_name
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError


def require_connection_env_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate exact transport fields, identities, aliases and URI overrides."""
    allowed = {
        "mode",
        "payload_format",
        "secret_values",
        "connections",
        "scheme_overrides",
        "database_overrides",
        "query_overrides",
    }
    if set(value) - allowed or value.get("mode") != "env":
        raise _invalid()
    if value.get("payload_format") != "airflow_connection_uri" or value.get("secret_values") is not False:
        raise _invalid()
    entries = value.get("connections")
    if not isinstance(entries, list) or not entries:
        raise _invalid()
    normalized: list[dict[str, str]] = []
    env_ids: dict[str, str] = {}
    aliases: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, Mapping) or set(entry) != {
            "connection_ref",
            "registry_connection_ref",
            "connection_id",
        }:
            raise _invalid()
        try:
            item = {
                key: require_airflow_connection_id(raw, context="environment projection identity")
                for key, raw in entry.items()
            }
        except ValueError:
            raise _invalid() from None
        identity = item["connection_id"]
        env_name = airflow_conn_env_name(identity)
        if env_name in env_ids and env_ids[env_name] != identity:
            raise _invalid()
        env_ids[env_name] = identity
        for key in ("connection_ref", "registry_connection_ref"):
            alias = item[key]
            if alias in aliases and aliases[alias] != identity:
                raise _invalid()
            aliases[alias] = identity
        normalized.append(item)
    closed: dict[str, Any] = {
        "mode": "env",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": normalized,
    }
    ids = set(env_ids.values())
    for name in ("scheme_overrides", "database_overrides", "query_overrides"):
        overrides = value.get(name, {})
        if not isinstance(overrides, Mapping) or set(overrides) - ids:
            raise _invalid()
        if name == "query_overrides":
            if any(
                not isinstance(raw, Mapping) or not _string_values(raw) or any(_credential_key(key) for key in raw)
                for raw in overrides.values()
            ):
                raise _invalid()
            closed[name] = {key: dict(raw) for key, raw in overrides.items()}
        else:
            if not _string_values(overrides):
                raise _invalid()
            closed[name] = dict(overrides)
    return closed


def _credential_key(key: str) -> bool:
    """Credential material belongs in Connections, never signed pack overrides."""
    normalized = "".join(character for character in key.casefold() if character.isalnum())
    return any(
        fragment in normalized
        for fragment in (
            "password",
            "passwd",
            "pwd",
            "secret",
            "token",
            "credential",
            "privatekey",
            "accesskey",
            "apikey",
            "authorization",
            "serviceaccount",
            "keyfile",
        )
    )


def _string_values(values: Mapping[Any, Any]) -> bool:
    return all(
        isinstance(key, str) and key.strip() and isinstance(raw, str) and raw.strip() for key, raw in values.items()
    )


def _invalid() -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID",
        "env projection requires closed connection identities and unambiguous canonical environment names",
    )
