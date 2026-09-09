from __future__ import annotations

from typing import Any

import pytest

from dpone.runtime.connectors.proxy import GCPProxyManager
from dpone.runtime.reconciliation.soft_delete.clickhouse import soft_delete_clickhouse
from dpone.runtime.reconciliation.soft_delete.mssql import soft_delete_mssql
from dpone.runtime.reconciliation.soft_delete.postgres import soft_delete_postgres
from dpone.runtime.reconciliation.soft_delete.registry import SoftDeleteRegistry


class FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        self.events.append((event, payload))


def _load_bigquery_soft_delete_contracts() -> tuple[Any, Any]:
    pytest.importorskip("google.cloud.bigquery", reason="BigQuery is an optional runtime dependency")
    from dpone.runtime.reconciliation.soft_delete.bigquery import _extract_dml_stats, soft_delete_bigquery

    return _extract_dml_stats, soft_delete_bigquery


class FakeVaultManager:
    def __init__(self, secret: dict[str, Any]) -> None:
        self.secret = secret
        self.calls: list[tuple[str, str]] = []

    def get_secret(self, *, mount_point: str, path: str) -> dict[str, Any]:
        self.calls.append((mount_point, path))
        return self.secret


class FakeCredentials:
    requires_scopes = True

    def __init__(self) -> None:
        self.scopes: tuple[str, ...] | None = None

    def with_scopes(self, scopes: tuple[str, ...]) -> FakeCredentials:
        scoped = FakeCredentials()
        scoped.requires_scopes = False
        scoped.scopes = scopes
        return scoped


class FakeAuthorizedSession:
    def __init__(self, credentials: FakeCredentials) -> None:
        self.credentials = credentials
        self.trust_env = True
        self.proxies: dict[str, str] = {}


def test_gcp_proxy_manager_builds_encoded_proxy_urls() -> None:
    assert (
        GCPProxyManager._build_proxy_url(
            {
                "protocol": "https",
                "user": "paul@example.com",
                "password": "p@ ss?",
                "host": "proxy.example.com",
                "port": "8080",
            }
        )
        == "https://paul%40example.com:p%40%20ss%3F@proxy.example.com:8080"
    )


def test_gcp_proxy_manager_validates_and_caches_vault_config() -> None:
    secret = {
        "host": "proxy.example.com",
        "port": 8080,
        "protocol": "http",
        "user": "svc",
        "password": "secret",
        "proxy_name": "corp",
        "no_proxy": "169.254.169.254",
    }
    vault = FakeVaultManager(secret)
    manager = GCPProxyManager(
        credentials=FakeCredentials(),  # type: ignore[arg-type]
        proxy_mount_point="secret",
        proxy_path="/network/proxy/gcp/current/",
        vault_manager_loader=lambda: vault,
    )

    config = manager._load_proxy_config()
    cached_config = manager._load_proxy_config()

    assert config is cached_config
    assert config == {
        "host": "proxy.example.com",
        "port": "8080",
        "protocol": "http",
        "user": "svc",
        "password": "secret",
        "proxy_name": "corp",
        "no_proxy": "169.254.169.254",
        "vault_path": "secret//network/proxy/gcp/current/",
    }
    assert vault.calls == [("secret", "network/proxy/gcp/current")]


def test_gcp_proxy_manager_preserves_legacy_five_positional_arguments() -> None:
    resolved_config = {
        "host": "proxy.example.com",
        "port": "8080",
        "protocol": "http",
        "user": "svc",
        "password": "secret",
    }

    manager = GCPProxyManager(FakeCredentials(), "resolved", "resolved", None, resolved_config)  # type: ignore[arg-type]

    assert manager.resolved_proxy_config is resolved_config
    assert manager.authorized_session_factory is None
    assert manager._load_proxy_config() == resolved_config


def test_gcp_proxy_manager_reports_invalid_proxy_configuration() -> None:
    manager_without_path = GCPProxyManager(
        credentials=FakeCredentials(),  # type: ignore[arg-type]
        proxy_mount_point="",
        proxy_path="path",
    )
    with pytest.raises(ValueError, match="mount_point and path"):
        manager_without_path._load_proxy_config()

    manager_without_loader = GCPProxyManager(
        credentials=FakeCredentials(),  # type: ignore[arg-type]
        proxy_mount_point="secret",
        proxy_path="path",
    )
    with pytest.raises(RuntimeError, match="Vault manager loader"):
        manager_without_loader._load_proxy_config()

    missing_secret = FakeVaultManager({"host": "proxy.example.com"})
    manager_with_missing_fields = GCPProxyManager(
        credentials=FakeCredentials(),  # type: ignore[arg-type]
        proxy_mount_point="secret",
        proxy_path="path",
        vault_manager_loader=lambda: missing_secret,
    )
    with pytest.raises(ValueError, match="port, protocol, user, password"):
        manager_with_missing_fields._load_proxy_config()


def test_gcp_proxy_manager_creates_and_reuses_authorized_session() -> None:
    vault = FakeVaultManager(
        {
            "host": "proxy.example.com",
            "port": "8080",
            "protocol": "http",
            "user": "svc",
            "password": "p@ss",
            "no_proxy": "metadata.google.internal",
        }
    )
    manager = GCPProxyManager(
        credentials=FakeCredentials(),  # type: ignore[arg-type]
        proxy_mount_point="secret",
        proxy_path="proxy",
        vault_manager_loader=lambda: vault,
        authorized_session_factory=FakeAuthorizedSession,
    )

    session = manager.get_authorized_session()
    cached_session = manager.get_authorized_session()

    assert session is cached_session
    assert isinstance(session, FakeAuthorizedSession)
    assert session.trust_env is False
    assert session.credentials.scopes == GCPProxyManager.DEFAULT_SCOPES
    assert session.proxies == {
        "http": "http://svc:p%40ss@proxy.example.com:8080",
        "https": "http://svc:p%40ss@proxy.example.com:8080",
        "no_proxy": "metadata.google.internal",
    }


class FakeSqlConnector:
    def __init__(self, result: Any = 0) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def execute_query(self, query: Any, params: Any = None) -> Any:
        self.calls.append((query, params))
        return self.result


class FakeMSSQLSoftDeleteConnector(FakeSqlConnector):
    def quote_identifier(self, name: str) -> str:
        return "[" + str(name).replace("]", "]]") + "]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"


def test_soft_delete_postgres_uses_array_params_for_single_keys() -> None:
    logger = FakeLogger()
    connector = FakeSqlConnector(result=2)

    updated = soft_delete_postgres(
        connector,
        "public",
        "orders",
        ["id"],
        [{"id": "1"}, {"id": "2"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 2
    assert connector.calls[0][1] == (["1", "2"],)
    assert logger.events[0][1]["TargetTable"] == "public.orders (PostgreSQL)"


def test_soft_delete_postgres_uses_tuple_params_for_composite_keys() -> None:
    connector = FakeSqlConnector(result=3)

    updated = soft_delete_postgres(
        connector,
        "public",
        "orders",
        ["id", "tenant_id"],
        [{"id": "1", "tenant_id": "a"}, {"id": "2", "tenant_id": "b"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        FakeLogger(),  # type: ignore[arg-type]
    )

    assert updated == 3
    assert connector.calls[0][1] == ((("1", "a"), ("2", "b")),)


def test_soft_delete_clickhouse_uses_shadow_table_swap_for_single_and_composite_keys() -> None:
    logger = FakeLogger()
    single_connector = FakeClickHouseShadowSwapConnector()

    single_updated = soft_delete_clickhouse(
        single_connector,
        "analytics",
        "orders",
        ["id"],
        [{"id": "1"}, {"id": "2"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert single_updated == 2
    executed_sql = "\n".join(call[0] for call in single_connector.calls)
    assert "CREATE TEMPORARY TABLE `__dpone_deleted_keys_fixed`" in executed_sql
    assert "INSERT INTO `analytics`.`orders__dpone_reconcile_fixed`" in executed_sql
    assert "LEFT ANY JOIN `__dpone_deleted_keys_fixed` AS k" in executed_sql
    assert (
        "if(k.`__dpone__deleted_marker` = 1 AND t.`__dpone__deleted_at` IS NULL, now(), t.`__dpone__deleted_at`)"
        in executed_sql
    )
    assert "RENAME TABLE" in executed_sql
    assert "`analytics`.`orders` TO `analytics`.`orders__dpone_reconcile_backup_fixed`" in executed_sql
    assert "ALTER TABLE" not in executed_sql

    composite_connector = FakeClickHouseShadowSwapConnector()
    composite_updated = soft_delete_clickhouse(
        composite_connector,
        "analytics",
        "orders",
        ["id", "tenant_id"],
        [{"id": "1", "tenant_id": "a"}, {"id": "2", "tenant_id": "b"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert composite_updated == 2
    composite_sql = "\n".join(call[0] for call in composite_connector.calls)
    assert "toString(t.`id`) = k.`id` AND toString(t.`tenant_id`) = k.`tenant_id`" in composite_sql


class FakeClickHouseShadowSwapConnector(FakeSqlConnector):
    reconciliation_clickhouse_operation_id = "fixed"
    reconciliation_clickhouse_key_insert_chunk_size = 2

    def __init__(self, result: Any = 0, *, matched_rows: int = 2) -> None:
        super().__init__(result)
        self.matched_rows = matched_rows
        self.records_calls: list[tuple[Any, Any, bool]] = []

    def get_records(self, query: Any, params: Any = None, as_dict: bool = False) -> list[Any]:
        self.records_calls.append((query, params, as_dict))
        if "system.columns" in str(query):
            return [
                ("id", "UInt64"),
                ("tenant_id", "String"),
                ("amount", "Float64"),
                ("__dpone__loaded_at", "DateTime64(6)"),
                ("__dpone__deleted_at", "Nullable(DateTime64(6))"),
            ]
        return [(self.matched_rows,)]


def test_soft_delete_clickhouse_escapes_literals_and_chunks_key_inserts_without_mutations() -> None:
    logger = FakeLogger()
    connector = FakeClickHouseShadowSwapConnector(matched_rows=3)

    updated = soft_delete_clickhouse(
        connector,
        "analytics",
        "orders",
        ["id"],
        [{"id": "1"}, {"id": "o'reilly\\book"}, {"id": "3"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 3
    key_insert_queries = [call[0] for call in connector.calls if "INSERT INTO `__dpone_deleted_keys_fixed`" in call[0]]
    assert len(key_insert_queries) == 2
    assert "('1', 1), ('o\\'reilly\\\\book', 1)" in key_insert_queries[0]
    assert "('3', 1)" in key_insert_queries[1]
    executed_sql = "\n".join(call[0] for call in connector.calls)
    assert "ALTER TABLE" not in executed_sql
    assert "system.mutations" not in executed_sql


def test_soft_delete_clickhouse_skips_empty_deleted_key_batch() -> None:
    logger = FakeLogger()
    connector = FakeSqlConnector()

    updated = soft_delete_clickhouse(
        connector,
        "analytics",
        "orders",
        ["id"],
        [],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 0
    assert connector.calls == []
    assert logger.events[0][0] == "RECONCILIATION_SOFT_DELETE_SKIPPED"


def test_soft_delete_mssql_uses_parameterized_single_key_updates() -> None:
    logger = FakeLogger()
    connector = FakeMSSQLSoftDeleteConnector(result=2)

    updated = soft_delete_mssql(
        connector,
        "landing",
        "orders",
        ["id"],
        [{"id": "1"}, {"id": "2"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 2
    query, params = connector.calls[0]
    assert "UPDATE [landing].[orders]" in query
    assert "SET [__dpone__deleted_at] = SYSUTCDATETIME()," in query
    assert "[__dpone__loaded_at] = SYSUTCDATETIME()" in query
    assert "WHERE [__dpone__deleted_at] IS NULL" in query
    assert "AND [id] IN (?, ?)" in query
    assert params == ("1", "2")
    assert logger.events[0][1]["TargetTable"] == "landing.orders (MSSQL)"


def test_soft_delete_mssql_uses_parameterized_composite_key_updates() -> None:
    logger = FakeLogger()
    connector = FakeMSSQLSoftDeleteConnector(result=2)

    updated = soft_delete_mssql(
        connector,
        "landing",
        "orders",
        ["id", "tenant_id"],
        [{"id": "1", "tenant_id": "a"}, {"id": "2", "tenant_id": "b"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 2
    query, params = connector.calls[0]
    assert "([id] = ? AND [tenant_id] = ?)" in query
    assert " OR " in query
    assert params == ("1", "a", "2", "b")


def test_soft_delete_mssql_skips_empty_deleted_key_batch() -> None:
    logger = FakeLogger()
    connector = FakeMSSQLSoftDeleteConnector(result=2)

    updated = soft_delete_mssql(
        connector,
        "landing",
        "orders",
        ["id"],
        [],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
    )

    assert updated == 0
    assert connector.calls == []
    assert logger.events[0][0] == "RECONCILIATION_SOFT_DELETE_SKIPPED"


def test_soft_delete_registry_dispatches_to_mssql_handler() -> None:
    class MSSQLConnector(FakeMSSQLSoftDeleteConnector):
        pass

    logger = FakeLogger()
    connector = MSSQLConnector(result=1)
    registry = SoftDeleteRegistry(logger)  # type: ignore[arg-type]

    updated = registry.execute_soft_delete(
        connector,
        target_schema="landing",
        target_table="orders",
        unique_key_list=["id"],
        deleted_keys=[{"id": "1"}],
        tech_schema="tech",
    )

    assert updated == 1
    assert connector.calls[0][1] == ("1",)


class FakeBigQueryJob:
    def __init__(self, updated_rows: int) -> None:
        self._properties = {"statistics": {"query": {"dmlStats": {"updatedRowCount": str(updated_rows)}}}}
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeBigQueryConnection:
    def __init__(self, job: FakeBigQueryJob) -> None:
        self.job = job
        self.calls: list[tuple[str, Any]] = []

    def query(self, query: str, *, job_config: Any = None) -> FakeBigQueryJob:
        self.calls.append((query, job_config))
        return self.job


class FakeBigQuerySoftDeleteConnector:
    project_id = "demo-project"

    def __init__(self, job: FakeBigQueryJob) -> None:
        self.connection = FakeBigQueryConnection(job)

    def _build_query_config(self, *, params: Any = None) -> dict[str, Any]:
        return {"params": params}


def test_soft_delete_bigquery_executes_update_and_extracts_dml_stats() -> None:
    _, soft_delete_bigquery = _load_bigquery_soft_delete_contracts()
    logger = FakeLogger()
    job = FakeBigQueryJob(updated_rows=5)
    connector = FakeBigQuerySoftDeleteConnector(job)

    updated = soft_delete_bigquery(
        connector,
        "analytics",
        "orders",
        ["id"],
        [{"id": "1"}],
        "__dpone__loaded_at",
        "__dpone__deleted_at",
        logger,  # type: ignore[arg-type]
        tech_schema="tech",
    )

    assert updated == 5
    assert job.result_called is True
    query, job_config = connector.connection.calls[0]
    assert "`demo-project.analytics.orders`" in query
    assert job_config == {"params": None}
    assert logger.events[0][1]["UpdatedRows"] == 5


def test_extract_dml_stats_returns_default_for_missing_or_invalid_stats() -> None:
    _extract_dml_stats, _ = _load_bigquery_soft_delete_contracts()
    assert _extract_dml_stats(FakeBigQueryJob(updated_rows=9), "updatedRowCount") == 9  # type: ignore[arg-type]
    assert _extract_dml_stats(object(), "updatedRowCount", default=7) == 7  # type: ignore[arg-type]
