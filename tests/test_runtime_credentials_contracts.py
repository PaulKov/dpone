from __future__ import annotations

import logging
import sys
import types
from types import SimpleNamespace

import pytest

from dpone.runtime.api_registry import build_api_runtime_source
from dpone.runtime.credentials.airflow_env import airflow_conn_env_name, parse_airflow_connection_uri
from dpone.runtime.credentials.config import CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory
from dpone.runtime.credentials.manager import CredentialsManager
from dpone.runtime.credentials.providers import (
    AirflowCredentialsProvider,
    EnvironmentCredentialsProvider,
    VaultCredentialsProvider,
)


def test_environment_credentials_provider_reads_prefixed_connection_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_WAREHOUSE_HOST", "db.example.com")
    monkeypatch.setenv("DPONE_WAREHOUSE_PORT", "5433")
    monkeypatch.setenv("DPONE_WAREHOUSE_DATABASE", "analytics")
    monkeypatch.setenv("DPONE_WAREHOUSE_USERNAME", "reader")
    monkeypatch.setenv("DPONE_WAREHOUSE_PASSWORD", "secret")
    monkeypatch.setenv("DPONE_WAREHOUSE_SCHEMA", "public")
    monkeypatch.setenv("DPONE_WAREHOUSE_ADDITIONAL_SSLMODE", "require")

    creds = EnvironmentCredentialsProvider(prefix="dpone_").get_credentials("warehouse")

    assert creds == CredentialsConfig(
        host="db.example.com",
        port=5433,
        database="analytics",
        username="reader",
        password="secret",
        schema="public",
        additional_params={"sslmode": "require"},
    )


def test_environment_credentials_provider_defaults_to_canonical_dpone_conn_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_HOST", "db.example.com")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_PORT", "5433")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_DATABASE", "analytics")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_USERNAME", "reader")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_PASSWORD", "secret")

    creds = EnvironmentCredentialsProvider().get_credentials("warehouse")

    assert creds.host == "db.example.com"
    assert creds.port == 5433
    assert creds.database == "analytics"
    assert creds.username == "reader"
    assert creds.password == "secret"


def test_environment_credentials_provider_prefers_canonical_prefix_over_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAREHOUSE_HOST", "legacy.example.com")
    monkeypatch.setenv("DPONE_WAREHOUSE_HOST", "dpone-legacy.example.com")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_HOST", "canonical.example.com")
    monkeypatch.setenv("WAREHOUSE_ADDITIONAL_SSLMODE", "disable")
    monkeypatch.setenv("DPONE_CONN_WAREHOUSE_ADDITIONAL_SSLMODE", "require")

    creds = EnvironmentCredentialsProvider().get_credentials("warehouse")

    assert creds.host == "canonical.example.com"
    assert creds.additional_params == {"sslmode": "require"}


def test_environment_credentials_provider_reads_all_connector_family_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REST_ORDERS_ENDPOINT", "https://api.example.com")
    monkeypatch.setenv("REST_ORDERS_TOKEN", "token")
    monkeypatch.setenv("REST_ORDERS_API_KEY", "key")
    monkeypatch.setenv("REST_ORDERS_PROJECT_ID", "demo-project")
    monkeypatch.setenv("REST_ORDERS_SERVICE_ACCOUNT_INFO", '{"project_id":"demo-project"}')
    monkeypatch.setenv("REST_ORDERS_SECURE", "true")
    monkeypatch.setenv("REST_ORDERS_COMPRESSION", "false")
    monkeypatch.setenv("REST_ORDERS_CONNECT_TIMEOUT", "7")
    monkeypatch.setenv("REST_ORDERS_SEND_RECEIVE_TIMEOUT", "77")
    monkeypatch.setenv("REST_ORDERS_SETTINGS", '{"max_threads":4}')

    creds = EnvironmentCredentialsProvider().get_credentials("rest_orders")

    assert creds.endpoint == "https://api.example.com"
    assert creds.token == "token"
    assert creds.api_key == "key"
    assert creds.project_id == "demo-project"
    assert creds.service_account_info == {"project_id": "demo-project"}
    assert creds.secure is True
    assert creds.compression is False
    assert creds.connect_timeout == 7
    assert creds.send_receive_timeout == 77
    assert creds.settings == {"max_threads": 4}


def test_environment_credentials_provider_omits_empty_port_and_additional_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CACHE_HOST", "cache.example.com")
    monkeypatch.delenv("CACHE_PORT", raising=False)

    creds = EnvironmentCredentialsProvider().get_credentials("cache")

    assert creds.host == "cache.example.com"
    assert creds.port is None
    assert creds.additional_params is None


def test_airflow_credentials_provider_reports_missing_airflow() -> None:
    provider = AirflowCredentialsProvider()
    provider.BaseHook = None

    with pytest.raises(RuntimeError, match="AIRFLOW_CONN_WAREHOUSE"):
        provider.get_credentials("warehouse")


def test_airflow_credentials_provider_falls_back_to_airflow_conn_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "AIRFLOW_CONN_MSSQL_DWH",
        "mssql://etl:secret@sql.example.com:1433/analytics_staging"
        "?driver=ODBC%20Driver%2018%20for%20SQL%20Server&trust_server_certificate=yes&query_timeout=30",
    )
    provider = AirflowCredentialsProvider()
    provider.BaseHook = None

    creds = provider.get_credentials("mssql_dwh")

    assert airflow_conn_env_name("mssql-dwh") == "AIRFLOW_CONN_MSSQL_DWH"
    assert creds.host == "sql.example.com"
    assert creds.port == 1433
    assert creds.database == "analytics_staging"
    assert creds.username == "etl"
    assert creds.password == "secret"
    assert creds.driver == "ODBC Driver 18 for SQL Server"
    assert creds.trust_server_certificate == "yes"
    assert creds.query_timeout == 30


def test_airflow_env_uri_parser_maps_common_connector_families() -> None:
    postgres = parse_airflow_connection_uri(
        "postgres+psycopg2://reader:p%40ss@pg.example.com:5432/analytics?sslmode=require"
    )
    clickhouse = parse_airflow_connection_uri(
        'clickhouse://default:secret@ch.example.com:9440/analytics?secure=true&settings={"max_threads":2}'
    )
    kafka = parse_airflow_connection_uri(
        "kafka://user:secret@broker.example.com:9092?security_protocol=SASL_SSL&sasl_mechanism=PLAIN"
    )
    bigquery = parse_airflow_connection_uri(
        'google-cloud-platform:///?project_id=demo&keyfile_dict={"client_email":"bot@example.com"}'
    )
    rest = parse_airflow_connection_uri("https://api-user:token@api.example.com:443?api_key=key")
    aws = parse_airflow_connection_uri(
        "aws://access:secret@/?endpoint_url=https%3A%2F%2Fstorage.example.com&region_name=ru-central1"
    )
    generic = parse_airflow_connection_uri(
        'generic://default:secret@ch.example.com:9000/?secure=false&settings={"max_threads":4}'
    )

    assert postgres.password == "p@ss"
    assert postgres.additional_params == {"sslmode": "require"}
    assert clickhouse.secure is True
    assert clickhouse.settings == {"max_threads": 2}
    assert kafka.bootstrap_servers == "broker.example.com:9092"
    assert kafka.security_protocol == "SASL_SSL"
    assert bigquery.project_id == "demo"
    assert bigquery.service_account_info == {"client_email": "bot@example.com"}
    assert rest.endpoint == "https://api.example.com:443"
    assert rest.api_key == "key"
    assert aws.username == "access"
    assert aws.password == "secret"
    assert aws.endpoint == "https://storage.example.com"
    assert aws.additional_params == {"endpoint_url": "https://storage.example.com", "region_name": "ru-central1"}
    assert generic.host == "ch.example.com"
    assert generic.port == 9000
    assert generic.username == "default"
    assert generic.password == "secret"
    assert generic.secure is False
    assert generic.settings == {"max_threads": 4}


def test_airflow_env_uri_parser_expands_airflow_private_extra_payload() -> None:
    creds = parse_airflow_connection_uri(
        'clickhouse://default:secret@cloud.example.com:8443/marketing?__extra__={"driver":"http","secure":true,'
        '"ca_cert":"/etc/ssl/cloud.pem","settings":{"max_threads":2}}'
    )

    assert creds.host == "cloud.example.com"
    assert creds.port == 8443
    assert creds.database == "marketing"
    assert creds.driver == "http"
    assert creds.secure is True
    assert creds.settings == {"max_threads": 2}
    assert creds.additional_params == {
        "driver": "http",
        "secure": True,
        "ca_cert": "/etc/ssl/cloud.pem",
        "settings": {"max_threads": 2},
    }


def test_airflow_credentials_provider_parses_postgres_connections_and_ignores_invalid_extra() -> None:
    conn = SimpleNamespace(
        conn_type="postgres",
        host="db.example.com",
        port=5432,
        schema="analytics",
        login="reader",
        password="secret",
        extra='{"sslmode":"require"}',
    )
    invalid_extra_conn = SimpleNamespace(**{**conn.__dict__, "extra": "not-json"})

    creds = AirflowCredentialsProvider._from_postgres(conn)
    invalid_extra_creds = AirflowCredentialsProvider._from_postgres(invalid_extra_conn)

    assert creds.additional_params == {"sslmode": "require"}
    assert invalid_extra_creds.additional_params == {}


def test_airflow_credentials_provider_supports_clickhouse_bigquery_and_rest() -> None:
    clickhouse = AirflowCredentialsProvider._from_clickhouse(
        SimpleNamespace(
            conn_type="clickhouse",
            host="ch.example.com",
            port=9440,
            schema="analytics",
            login="reader",
            password="secret",
            extra='{"secure":true,"compression":false,"settings":{"max_threads":2}}',
        )
    )
    bigquery = AirflowCredentialsProvider._from_bigquery(
        SimpleNamespace(
            conn_type="google_cloud_platform",
            host=None,
            port=None,
            schema="demo-project",
            login=None,
            password=None,
            extra='{"project_id":"demo-project","keyfile_dict":{"client_email":"bot@example.com"}}',
        )
    )
    rest = AirflowCredentialsProvider._from_rest(
        SimpleNamespace(
            conn_type="https",
            host="api.example.com",
            port=443,
            schema=None,
            login="user",
            password="token",
            extra='{"endpoint":"https://api.example.com/v1","api_key":"key"}',
        )
    )
    aws = AirflowCredentialsProvider._from_aws(
        SimpleNamespace(
            conn_type="aws",
            host=None,
            port=None,
            schema=None,
            login="access-key",
            password="secret-key",
            extra='{"endpoint_url":"https://storage.example.com","region_name":"ru-central1"}',
        )
    )

    assert clickhouse.secure is True
    assert clickhouse.compression is False
    assert clickhouse.settings == {"max_threads": 2}
    assert bigquery.project_id == "demo-project"
    assert bigquery.service_account_info == {"client_email": "bot@example.com"}
    assert rest.endpoint == "https://api.example.com/v1"
    assert rest.token == "token"
    assert rest.api_key == "key"
    assert aws.username == "access-key"
    assert aws.password == "secret-key"
    assert aws.endpoint == "https://storage.example.com"
    assert aws.additional_params == {"endpoint_url": "https://storage.example.com", "region_name": "ru-central1"}


def test_airflow_credentials_provider_supports_generic_host_connection() -> None:
    creds = AirflowCredentialsProvider._from_generic(
        SimpleNamespace(
            conn_type="generic",
            host="ch.example.com",
            port=9000,
            schema=None,
            login="default",
            password="secret",
            extra='{"secure":false,"compression":true,"settings":{"max_threads":4}}',
        )
    )

    assert creds.host == "ch.example.com"
    assert creds.port == 9000
    assert creds.username == "default"
    assert creds.password == "secret"
    assert creds.secure is False
    assert creds.compression is True
    assert creds.settings == {"max_threads": 4}
    assert creds.additional_params == {
        "secure": False,
        "compression": True,
        "settings": {"max_threads": 4},
    }


def test_airflow_credentials_provider_maps_mysql_connection_types() -> None:
    provider = AirflowCredentialsProvider()
    provider.BaseHook = SimpleNamespace(
        get_connection=lambda connection_name: SimpleNamespace(
            conn_type="mysql",
            host="mysql.example.com",
            port=3306,
            schema="app",
            login="reader",
            password="secret",
            extra='{"connect_timeout": 15}',
        ),
    )

    creds = provider.get_credentials("mysql_oltp")
    assert creds.host == "mysql.example.com"
    assert creds.port == 3306
    assert creds.database == "app"
    assert creds.username == "reader"
    assert creds.password == "secret"
    assert creds.connect_timeout == 15


def test_airflow_credentials_provider_rejects_unsupported_connection_types() -> None:
    provider = AirflowCredentialsProvider()
    provider.BaseHook = SimpleNamespace(
        get_connection=lambda connection_name: SimpleNamespace(conn_type="oracle"),
    )

    with pytest.raises(NotImplementedError, match="Пока не поддерживаем oracle"):
        provider.get_credentials("warehouse")


def test_vault_credentials_provider_maps_common_secret_aliases_and_service_account_json() -> None:
    vault_manager = SimpleNamespace(
        get_secret=lambda *, mount_point, path: {
            "db_host": f"{path}.example.com",
            "db_port": "5432",
            "db_database": "analytics",
            "db_user": "reader",
            "db_password": "secret",
            "schema": "public",
            "credentials_json": '{"project_id":"demo-project","client_email":"bot@example.com"}',
            "custom": "value",
        }
    )

    creds = VaultCredentialsProvider(vault_manager=vault_manager).get_credentials(
        "warehouse",
        mount_point="kv",
        path="postgres/warehouse",
    )

    assert creds.host == "postgres/warehouse.example.com"
    assert creds.port == 5432
    assert creds.database == "analytics"
    assert creds.username == "reader"
    assert creds.password == "secret"
    assert creds.schema == "public"
    assert creds.project_id == "demo-project"
    assert creds.service_account_info == {"project_id": "demo-project", "client_email": "bot@example.com"}
    assert creds.additional_params == {
        "db_host": "postgres/warehouse.example.com",
        "db_port": "5432",
        "db_database": "analytics",
        "db_user": "reader",
        "db_password": "secret",
        "credentials_json": '{"project_id":"demo-project","client_email":"bot@example.com"}',
        "custom": "value",
    }


def test_vault_credentials_provider_wraps_invalid_service_account_json() -> None:
    vault_manager = SimpleNamespace(
        get_secret=lambda *, mount_point, path: {
            "credentials_json": "{",
        }
    )

    with pytest.raises(ValueError, match="Не удалось получить креденшиалы из Vault"):
        VaultCredentialsProvider(vault_manager=vault_manager).get_credentials("warehouse")


def test_vault_credentials_provider_never_logs_or_raises_connection_or_backend_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "sk-proj-aaaaaaaaaaaaaaaaaaaaaaaa"

    def fail_secret_read(*, mount_point: str, path: str):
        raise RuntimeError(f"backend rejected {secret}")

    vault_manager = SimpleNamespace(get_secret=fail_secret_read)

    with caplog.at_level(logging.ERROR), pytest.raises(ValueError) as exc:
        VaultCredentialsProvider(vault_manager=vault_manager).get_credentials(secret)

    assert secret not in caplog.text
    assert secret not in str(exc.value)
    assert "RuntimeError" in caplog.text


def test_credentials_manager_routes_to_selected_provider() -> None:
    env_provider = SimpleNamespace(
        get_credentials=lambda connection_name: CredentialsConfig(host=f"env:{connection_name}")
    )
    airflow_provider = SimpleNamespace(
        get_credentials=lambda connection_name: CredentialsConfig(host=f"airflow:{connection_name}")
    )
    vault_provider = SimpleNamespace(
        get_credentials=lambda connection_name, mount_point, path: CredentialsConfig(
            host=f"vault:{connection_name}:{mount_point}:{path}"
        )
    )
    manager = CredentialsManager(
        env_provider=env_provider,
        airflow_provider=airflow_provider,
        vault_provider=vault_provider,
    )

    assert manager.get_credentials("warehouse", CredentialsSource.ENVIRONMENT).host == "env:warehouse"
    assert manager.get_credentials("warehouse", CredentialsSource.AIRFLOW).host == "airflow:warehouse"
    assert (
        manager.get_credentials("warehouse", CredentialsSource.VAULT, mount_point="kv", path="db/warehouse").host
        == "vault:warehouse:kv:db/warehouse"
    )
    params_creds = manager.get_credentials(
        '{"bootstrap_servers":"localhost:9092","schema_registry_url":"http://localhost:8081"}',
        CredentialsSource.PARAMS,
    )
    assert params_creds.bootstrap_servers == "localhost:9092"
    assert params_creds.schema_registry_url == "http://localhost:8081"
    assert manager.get_credentials("localhost:9092", CredentialsSource.PARAMS).bootstrap_servers == "localhost:9092"
    all_fields = manager.get_credentials(
        (
            '{"host":"sql","port":1433,"database":"dwh","username":"sa","password":"secret",'
            '"driver":"ODBC Driver 18 for SQL Server","bcp_path":"bcp","project_id":"demo",'
            '"service_account_info":{"client_email":"bot@example.com"},'
            '"endpoint":"https://api.example.com","token":"token","api_key":"key",'
            '"secure":true,"compression":false,"settings":{"max_threads":2}}'
        ),
        CredentialsSource.PARAMS,
    )
    assert all_fields.driver == "ODBC Driver 18 for SQL Server"
    assert all_fields.bcp_path == "bcp"
    assert all_fields.project_id == "demo"
    assert all_fields.service_account_info == {"client_email": "bot@example.com"}
    assert all_fields.endpoint == "https://api.example.com"
    assert all_fields.token == "token"
    assert all_fields.api_key == "key"
    assert all_fields.secure is True
    assert all_fields.compression is False
    assert all_fields.settings == {"max_threads": 2}


def test_runtime_credentials_package_keeps_factory_exports_lazy() -> None:
    import dpone.runtime.credentials as credentials

    assert "SourceFactory" in dir(credentials)
    assert "SinkFactory" in dir(credentials)
    with pytest.raises(AttributeError, match="missing"):
        getattr(credentials, "missing")


def test_base_factory_creates_postgres_connector_from_complete_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakePostgresConnector:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    postgres_module = types.ModuleType("dpone.runtime.connectors.postgres")
    postgres_module.PostgresConnector = FakePostgresConnector
    monkeypatch.setitem(sys.modules, "dpone.runtime.connectors.postgres", postgres_module)
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="db.example.com",
                database="analytics",
                username="reader",
                password="secret",
            )
        ),
    )

    connector = BaseFactory._create_postgres_connector("warehouse", CredentialsSource.ENVIRONMENT)

    assert isinstance(connector, FakePostgresConnector)
    assert created == {
        "host": "db.example.com",
        "port": 5432,
        "database": "analytics",
        "user": "reader",
        "password": "secret",
        "application_name": "dpone",
        "autocommit": True,
    }


def test_base_factory_rejects_incomplete_postgres_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="db.example.com",
                database="analytics",
                username="reader",
            )
        ),
    )

    with pytest.raises(ValueError, match="Неполные креденшиалы для PostgreSQL"):
        BaseFactory._create_postgres_connector("warehouse", CredentialsSource.ENVIRONMENT)


def test_base_factory_creates_clickhouse_connector_with_secure_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeClickHouseConnector:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    clickhouse_module = types.ModuleType("dpone.runtime.connectors.clickhouse")
    clickhouse_module.ClickHouseConnector = FakeClickHouseConnector
    monkeypatch.setitem(sys.modules, "dpone.runtime.connectors.clickhouse", clickhouse_module)
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="ch.example.com",
                database="analytics",
                username="reader",
                password=None,
                secure=True,
                compression=False,
                connect_timeout=5,
                send_receive_timeout=30,
                settings={"max_threads": 2},
            )
        ),
    )

    connector = BaseFactory._create_clickhouse_connector("warehouse", CredentialsSource.ENVIRONMENT)

    assert isinstance(connector, FakeClickHouseConnector)
    assert created == {
        "host": "ch.example.com",
        "port": 9440,
        "database": "analytics",
        "user": "reader",
        "password": "",
        "application_name": "dpone-clickhouse",
        "secure": True,
        "compression": False,
        "connect_timeout": 5,
        "send_receive_timeout": 30,
        "settings": {"max_threads": 2},
        "driver": "native",
        "ca_cert": None,
    }


def test_base_factory_uses_default_clickhouse_database_when_connection_schema_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeClickHouseConnector:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    clickhouse_module = types.ModuleType("dpone.runtime.connectors.clickhouse")
    clickhouse_module.ClickHouseConnector = FakeClickHouseConnector
    monkeypatch.setitem(sys.modules, "dpone.runtime.connectors.clickhouse", clickhouse_module)
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="ch.example.com",
                database=None,
                username="default",
                password="secret",
            )
        ),
    )

    connector = BaseFactory._create_clickhouse_connector("warehouse", CredentialsSource.ENVIRONMENT)

    assert isinstance(connector, FakeClickHouseConnector)
    assert created["database"] == "default"
    assert created["host"] == "ch.example.com"
    assert created["user"] == "default"
    assert created["driver"] == "native"


def test_base_factory_creates_clickhouse_http_connector_from_generic_cloud_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeClickHouseConnector:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    clickhouse_module = types.ModuleType("dpone.runtime.connectors.clickhouse")
    clickhouse_module.ClickHouseConnector = FakeClickHouseConnector
    monkeypatch.setitem(sys.modules, "dpone.runtime.connectors.clickhouse", clickhouse_module)
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="cloud.example.com",
                port=None,
                database="marketing",
                username="reader",
                password="secret",
                secure=True,
                additional_params={
                    "interface": "http",
                    "ca_cert": "/etc/ssl/certs/cloud.pem",
                },
            )
        ),
    )

    connector = BaseFactory._create_clickhouse_connector("warehouse", CredentialsSource.AIRFLOW)

    assert isinstance(connector, FakeClickHouseConnector)
    assert created["host"] == "cloud.example.com"
    assert created["port"] == 8443
    assert created["database"] == "marketing"
    assert created["secure"] is True
    assert created["driver"] == "http"
    assert created["ca_cert"] == "/etc/ssl/certs/cloud.pem"


def test_base_factory_creates_clickhouse_http_connector_from_airflow_private_extra_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeClickHouseConnector:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    clickhouse_module = types.ModuleType("dpone.runtime.connectors.clickhouse")
    clickhouse_module.ClickHouseConnector = FakeClickHouseConnector
    monkeypatch.setitem(sys.modules, "dpone.runtime.connectors.clickhouse", clickhouse_module)

    creds = parse_airflow_connection_uri(
        'clickhouse://reader:secret@cloud.example.com:8443/marketing?__extra__={"driver":"http","secure":true,'
        '"ca_cert":"/etc/ssl/certs/cloud.pem"}'
    )
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(get_credentials=lambda connection_id, source, mount_point, path: creds),
    )

    connector = BaseFactory._create_clickhouse_connector("warehouse", CredentialsSource.AIRFLOW)

    assert isinstance(connector, FakeClickHouseConnector)
    assert created["host"] == "cloud.example.com"
    assert created["port"] == 8443
    assert created["database"] == "marketing"
    assert created["secure"] is True
    assert created["driver"] == "http"
    assert created["ca_cert"] == "/etc/ssl/certs/cloud.pem"


def test_base_factory_rejects_incomplete_clickhouse_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        BaseFactory,
        "manager",
        SimpleNamespace(
            get_credentials=lambda connection_id, source, mount_point, path: CredentialsConfig(
                host="ch.example.com",
                database="analytics",
            )
        ),
    )

    with pytest.raises(ValueError, match="Неполные креденшиалы для ClickHouse"):
        BaseFactory._create_clickhouse_connector("warehouse", CredentialsSource.ENVIRONMENT)


def test_generic_rest_api_runtime_source_uses_env_credentials_provider() -> None:
    manager = CredentialsManager(
        env_provider=SimpleNamespace(
            get_credentials=lambda connection_name: CredentialsConfig(
                endpoint=f"https://{connection_name}.example.com",
                token="token",
                api_key="key",
            )
        )
    )

    source = build_api_runtime_source(
        source_cfg={
            "type": "api",
            "api_type": "rest",
            "connection_id": "orders",
            "connection_type": "env",
            "options": {"records_path": "items"},
        },
        vault_path=None,
        credentials_manager=manager,
    )

    assert source.connector.credentials.endpoint == "https://orders.example.com"
    assert source.connector.credentials.token == "token"
    assert source.connector.credentials.api_key == "key"
