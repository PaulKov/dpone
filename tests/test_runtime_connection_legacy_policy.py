from __future__ import annotations

import warnings
from typing import Any, NoReturn

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import RuntimeConnectionAuthorityError
from dpone.runtime.credentials import authority_resolution
from dpone.runtime.credentials.authority import resolve_runtime_connections
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext

_DISABLED_CODE = "DPONE_LEGACY_RUNTIME_DEFAULTS_DISABLED"
_DISABLED_MESSAGE = "Implicit runtime defaults are disabled; configure a logical connection_ref."


@pytest.fixture(autouse=True)
def _reset_legacy_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authority_resolution, "_legacy_warning_emitted", False)


@pytest.mark.parametrize("policy", [None, "disabled"])
def test_legacy_runtime_authority_is_disabled_by_default(policy: str | None) -> None:
    config = _legacy_config(policy=policy)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=None,
            )

    assert exc.value.code == _DISABLED_CODE
    assert str(exc.value) == _DISABLED_MESSAGE
    assert caught == []


def test_only_manifest_policy_can_enable_explicit_legacy_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_LEGACY_RUNTIME_CONNECTIONS", "explicit_only")
    config = _legacy_config()
    config["legacy_runtime_connections"] = "explicit_only"
    config["compatibility"] = {"legacy_runtime_connections": "explicit_only"}

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=None,
            )

    assert exc.value.code == _DISABLED_CODE
    assert str(exc.value) == _DISABLED_MESSAGE
    assert caught == []


def test_explicit_only_allows_authored_legacy_authority_and_warns_once() -> None:
    config = _legacy_config(policy="explicit_only")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        first = resolve_runtime_connections(
            config=config,
            load_config=_load_config(),
            context=None,
        )
        second = resolve_runtime_connections(
            config=config,
            load_config=_load_config(),
            context=None,
        )

    assert first.strict is False
    assert second.strict is False
    assert len(caught) == 1
    assert caught[0].category is DeprecationWarning
    assert str(caught[0].message) == ("Legacy runtime connection fields are deprecated; migrate to connection_ref.")


def test_explicit_only_does_not_restore_implicit_legacy_authority() -> None:
    config = {
        "runtime": {
            "compatibility": {
                "legacy_runtime_connections": "explicit_only",
            }
        },
        "source": {"type": "postgres"},
        "sink": {"type": "clickhouse"},
        "state": {"type": "disabled"},
    }

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=None,
            )

    assert exc.value.code == _DISABLED_CODE
    assert str(exc.value) == _DISABLED_MESSAGE
    assert caught == []


def test_unknown_legacy_policy_fails_with_a_bounded_error() -> None:
    unknown_value = "sensitive-unknown-policy-value"
    config = _legacy_config(policy=unknown_value)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=None,
            )

    assert exc.value.code == _DISABLED_CODE
    assert str(exc.value) == _DISABLED_MESSAGE
    assert unknown_value not in str(exc.value)
    assert caught == []


def test_canonical_authority_without_verified_context_remains_rejected() -> None:
    config = _canonical_config()
    config["runtime"] = {
        "compatibility": {
            "legacy_runtime_connections": "explicit_only",
        }
    }

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=None,
            )

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED"
    assert caught == []


@pytest.mark.parametrize(
    ("section", "nested_section", "field"),
    [
        ("source", None, "connection_id"),
        ("sink", None, "connection_type"),
        ("state", None, "vault_path"),
        ("bigquery_proxy", None, "vault_mount_point"),
        ("object_storage", None, "connection_id"),
        ("object_storage", "runtime_access", "credentials_source"),
        ("object_storage", "clickhouse_write_access", "connection_id"),
    ],
)
def test_explicit_only_never_weakens_strict_context(
    section: str,
    nested_section: str | None,
    field: str,
) -> None:
    config = _canonical_config()
    config["runtime"] = {
        "compatibility": {
            "legacy_runtime_connections": "explicit_only",
        }
    }
    target = config.setdefault(section, {})
    if nested_section is not None:
        target = target.setdefault(nested_section, {})
    target[field] = "legacy-value"

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeConnectionAuthorityError) as exc:
            resolve_runtime_connections(
                config=config,
                load_config=_load_config(),
                context=_strict_context(),
            )

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_AUTHORITY_CONFLICT"
    assert caught == []


def _legacy_config(*, policy: str | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {
        "source": {
            "type": "postgres",
            "connection_id": "source-main",
            "connection_type": "params",
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "sink-main",
            "connection_type": "params",
        },
        "state": {"type": "disabled"},
    }
    if policy is not None:
        config["runtime"] = {
            "compatibility": {
                "legacy_runtime_connections": policy,
            }
        }
    return config


def _canonical_config() -> dict[str, Any]:
    return {
        "source": {"type": "postgres", "connection_ref": "source-main"},
        "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
        "state": {"type": "disabled"},
    }


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source-main",
        target_conn_id="sink-main",
        source_schema="raw",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def _strict_context() -> RuntimeConnectionContext:
    return RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_ForbiddenResolver(),
    )


class _ForbiddenResolver:
    def resolve(self, connection_ref: str) -> NoReturn:
        raise AssertionError(f"strict rejection must precede resolution: {connection_ref}")
