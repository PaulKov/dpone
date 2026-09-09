from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run_import_smoke(module_import: str, verification: str, blocked_prefixes: tuple[str, ...]) -> None:
    blocked = ", ".join(repr(prefix) for prefix in blocked_prefixes)
    script = textwrap.dedent(
        f"""
        import builtins
        import sys
        import types

        original_import = builtins.__import__
        blocked_prefixes = ({blocked},)

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if any(name == prefix or name.startswith(prefix + ".") for prefix in blocked_prefixes):
                raise ModuleNotFoundError(name)
            return original_import(name, globals, locals, fromlist, level)

        builtins.__import__ = guarded_import
        sys.path.insert(0, {str((ROOT / "src").resolve())!r})

        {module_import}
        {verification}
        """
    )

    subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        cwd=ROOT,
    )


def test_bigquery_connector_module_imports_without_optional_google_or_pandas() -> None:
    _run_import_smoke(
        "import dpone.runtime.connectors.bigquery as module",
        "assert module.postgres_headerless_csv_load_config().skip_leading_rows == 0",
        ("google", "pandas"),
    )


def test_gcp_proxy_manager_imports_without_optional_google() -> None:
    _run_import_smoke(
        "from dpone.runtime.connectors.proxy import GCPProxyManager",
        """
        assert GCPProxyManager.__name__ == "GCPProxyManager"
        manager = GCPProxyManager(
            credentials=object(),
            proxy_mount_point="resolved",
            resolved_proxy_config={
                "host": "proxy.example.com",
                "port": "8080",
                "protocol": "http",
                "user": "svc",
                "password": "secret",
            },
        )
        try:
            manager.get_authorized_session()
        except ModuleNotFoundError as exc:
            assert "Install dpone with the 'gcp' extra" in str(exc)
        else:
            raise AssertionError("The optional Google dependency boundary was not enforced")
        """,
        ("google",),
    )


def test_runtime_credentials_factory_imports_without_optional_google_or_postgres() -> None:
    _run_import_smoke(
        "import dpone.runtime.credentials.factory as module",
        "assert module.BaseFactory.__name__ == 'BaseFactory'",
        ("google", "psycopg"),
    )


def test_object_storage_access_models_import_without_cloud_sdks() -> None:
    _run_import_smoke(
        "import dpone.runtime.object_storage_access_models as module",
        "assert module.ObjectStorageAccessRequest.__name__ == 'ObjectStorageAccessRequest'",
        ("boto3", "google", "azure"),
    )


def test_columnar_fast_path_imports_without_runtime_heavy_deps() -> None:
    _run_import_smoke(
        """
        import dpone.runtime.columnar_fast_path as module
        import dpone.runtime.columnar_snapshot_provider as provider
        """,
        "assert module.ColumnarFastPathPlanner.__name__ == 'ColumnarFastPathPlanner'; "
        "assert provider.ColumnarSnapshotRequest.__name__ == 'ColumnarSnapshotRequest'",
        ("boto3", "google", "azure", "pandas", "pyarrow", "polars", "pyodbc", "clickhouse_driver"),
    )


def test_route_capability_modules_import_without_runtime_heavy_deps() -> None:
    _run_import_smoke(
        """
        import dpone.runtime.route_capabilities as taxonomy
        import dpone.runtime.columnar_route_capabilities as columnar_routes
        import dpone.runtime.route_runtime as route_runtime
        import dpone.runtime.columnar_route_runtime as columnar_runtime
        import dpone.runtime.route_runtime_factory as route_factory
        import dpone.runtime.columnar_runtime_assembly as columnar_assembly
        import dpone.runtime.sinks.clickhouse_capabilities as clickhouse
        """,
        "assert taxonomy.RouteCapabilityPlanner.__name__ == 'RouteCapabilityPlanner'; "
        "assert columnar_routes.ColumnarRouteCapabilityPlanner.__name__ == 'ColumnarRouteCapabilityPlanner'; "
        "assert route_runtime.RouteCapabilityOrchestrator.__name__ == 'RouteCapabilityOrchestrator'; "
        "assert columnar_runtime.ColumnarRouteCandidateProvider.__name__ == 'ColumnarRouteCandidateProvider'; "
        "assert route_factory.RouteCapabilityRuntimeFactory.__name__ == 'RouteCapabilityRuntimeFactory'; "
        "assert columnar_assembly.ColumnarRuntimeAssembly.__name__ == 'ColumnarRuntimeAssembly'; "
        "assert clickhouse.ClickHouseColumnarCapabilityProbe.__name__ == 'ClickHouseColumnarCapabilityProbe'",
        ("boto3", "google", "azure", "pandas", "pyarrow", "polars", "pyodbc", "clickhouse_driver"),
    )


def test_mssql_columnar_provider_imports_without_runtime_heavy_deps() -> None:
    _run_import_smoke(
        "import dpone.runtime.sources.strategies.mssql.mssql_columnar_provider as module",
        "assert module.MssqlColumnarSnapshotProvider.provider_id == 'mssql_odbc_arrow_parquet'",
        ("boto3", "google", "azure", "pandas", "pyarrow", "polars", "pyodbc", "clickhouse_driver"),
    )


def test_postgres_strategy_package_does_not_pull_xmin_google_deps_on_base_import() -> None:
    _run_import_smoke(
        """
        psycopg_module = types.ModuleType("psycopg")
        psycopg_module.sql = object()
        sys.modules["psycopg"] = psycopg_module
        from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy
        """,
        "assert PostgresBaseStrategy.__name__ == 'PostgresBaseStrategy'",
        ("google",),
    )


def test_api_to_bigquery_runtime_binding_does_not_require_psycopg() -> None:
    script = textwrap.dedent(
        f"""
        import builtins
        import sys
        import types

        original_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "psycopg" or name.startswith("psycopg."):
                raise ModuleNotFoundError(name)
            return original_import(name, globals, locals, fromlist, level)

        builtins.__import__ = guarded_import
        sys.path.insert(0, {str((ROOT / "src").resolve())!r})

        google = types.ModuleType("google")
        cloud = types.ModuleType("google.cloud")
        bigquery = types.ModuleType("google.cloud.bigquery")
        oauth2 = types.ModuleType("google.oauth2")
        service_account = types.ModuleType("google.oauth2.service_account")

        class FakeCredentials:
            requires_scopes = True

            def __init__(self, info):
                self.info = info
                self.scopes = ()

            @classmethod
            def from_service_account_info(cls, info):
                return cls(info)

            def with_scopes(self, scopes):
                self.scopes = tuple(scopes)
                return self

        cloud.bigquery = bigquery
        service_account.Credentials = FakeCredentials
        oauth2.service_account = service_account
        google.cloud = cloud
        google.oauth2 = oauth2

        sys.modules["google"] = google
        sys.modules["google.cloud"] = cloud
        sys.modules["google.cloud.bigquery"] = bigquery
        sys.modules["google.oauth2"] = oauth2
        sys.modules["google.oauth2.service_account"] = service_account

        from dpone.dag.config import ETLProcessConfig
        from dpone.runtime.credentials.config import CredentialsConfig
        from dpone.runtime.credentials.factory import BaseFactory

        class DummyManager:
            def get_credentials(self, connection_name, source, mount_point=None, path=None):
                del connection_name, source, mount_point, path
                return CredentialsConfig(
                    project_id="example-dp-prod",
                    service_account_info={{"project_id": "example-dp-prod", "client_email": "svc@example.com"}},
                )

        BaseFactory.manager = DummyManager()

        cfg = ETLProcessConfig.from_dict(
            {{
                "name": "cbr__smoke__to__landing",
                "runtime": {{
                    "compatibility": {{
                        "legacy_runtime_connections": "explicit_only",
                    }}
                }},
                "source": {{
                    "type": "api",
                    "api_type": "cbr",
                    "options": {{"resource": "xml_daily_asp"}},
                }},
                "sink": {{
                    "type": "bigquery",
                    "connection_id": "google_bigquery",
                    "connection_type": "vault",
                    "vault_path": "gcp/example-dp-prod/bq/example-etl-service",
                    "table": {{"schema": "landing__cbr__api", "name": "xml_daily_asp"}},
                    "strategy": {{"mode": "full_refresh"}},
                }},
            }}
        )

        assert cfg.source_obj is not None
        assert cfg.sink_obj is not None
        assert cfg.sink_obj.connector.project_id == "example-dp-prod"
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True, cwd=ROOT)


def test_mssql_runtime_modules_import_without_pyodbc() -> None:
    _run_import_smoke(
        "from dpone.runtime.connectors import MSSQLConnector; from dpone.runtime.sources import MSSQLSource; from dpone.runtime.sinks import MSSQLSink",
        "assert MSSQLConnector.__name__ == 'MSSQLConnector'; assert MSSQLSource.__name__ == 'MSSQLSource'; assert MSSQLSink.__name__ == 'MSSQLSink'",
        ("pyodbc",),
    )


def test_postgres_source_imports_without_psycopg() -> None:
    _run_import_smoke(
        "from dpone.runtime.sources.postgres import PostgresSource",
        "assert PostgresSource.__name__ == 'PostgresSource'",
        ("psycopg",),
    )
