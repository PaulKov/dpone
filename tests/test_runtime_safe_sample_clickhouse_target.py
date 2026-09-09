from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from dpone.readiness.safe_sample_clickhouse_target import (
    CredentialResolvingClickHouseTemporaryTargetAdapter,
)
from dpone.services.safe_sample_policy import TemporaryTargetPlan


@dataclass(frozen=True)
class _Resolved:
    credentials: object
    safe_metadata: dict[str, object]


class _Resolver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def resolve(self, connection_ref: str) -> _Resolved:
        self.calls.append(connection_ref)
        return _Resolved(
            credentials=object(),
            safe_metadata={
                "connection_ref": connection_ref,
                "resolver": "vault_kv",
                "resolved_version": 17,
            },
        )


class _ProviderFactory:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.credentials: list[object] = []

    def create(self, credentials: object):
        self.credentials.append(credentials)

        class Connection:
            def __init__(self, statements: list[str]) -> None:
                self._statements = statements

            def execute(self, statement: str) -> None:
                self._statements.append(statement)

        connection = Connection(self.statements)
        return lambda: connection


def _plan(connection_ref: str = "clickhouse_dev") -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref=connection_ref,
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_dev", "name": "orders_daily_sample"},
        ttl_seconds=3600,
        cleanup_required=True,
        pii_policy="masked",
    )


def test_runtime_clickhouse_target_resolves_once_and_executes_real_lifecycle() -> None:
    resolver = _Resolver()
    providers = _ProviderFactory()
    adapter = CredentialResolvingClickHouseTemporaryTargetAdapter(
        credential_resolver=resolver,
        connection_ref="clickhouse_dev",
        connection_provider_factory=providers,
    )

    created = adapter.create(_plan())
    dropped = adapter.drop(_plan())

    assert resolver.calls == ["clickhouse_dev"]
    assert len(providers.credentials) == 1
    assert created["applied"] is True
    assert created["server_side_expiry"] is True
    assert created["credential_resolution"] == {
        "connection_ref": "clickhouse_dev",
        "resolver": "vault_kv",
        "resolved_version": 17,
    }
    assert dropped["applied"] is True
    assert providers.statements[0] == "CREATE DATABASE IF NOT EXISTS `dpone_tmp_dev`"
    assert "CREATE TABLE `dpone_tmp_dev`.`orders_daily_sample` AS `analytics`.`orders`" in providers.statements
    assert providers.statements[-1] == "DROP TABLE IF EXISTS `dpone_tmp_dev`.`orders_daily_sample`"


def test_runtime_clickhouse_target_rejects_connection_ref_mismatch_before_resolution() -> None:
    resolver = _Resolver()
    adapter = CredentialResolvingClickHouseTemporaryTargetAdapter(
        credential_resolver=resolver,
        connection_ref="clickhouse_dev",
        connection_provider_factory=_ProviderFactory(),
    )

    with pytest.raises(ValueError, match="connection_ref"):
        adapter.create(_plan("other_clickhouse"))

    assert resolver.calls == []


def test_runtime_clickhouse_target_redacts_resolver_metadata() -> None:
    class UnsafeResolver:
        def resolve(self, connection_ref: str) -> Any:
            return _Resolved(
                credentials=object(),
                safe_metadata={
                    "connection_ref": connection_ref,
                    "resolver": "vault_kv",
                    "password": "must-not-leak",
                },
            )

    adapter = CredentialResolvingClickHouseTemporaryTargetAdapter(
        credential_resolver=UnsafeResolver(),
        connection_ref="clickhouse_dev",
        connection_provider_factory=_ProviderFactory(),
    )

    result = adapter.create(_plan())

    assert "password" not in result["credential_resolution"]
    assert "must-not-leak" not in repr(result)
