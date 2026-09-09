from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.runtime.credentials.config import CredentialsConfig


def test_binding_resolver_env_var_builds_credentials_and_safe_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    monkeypatch.setenv("DPONE_CONN_MSSQL_DEV_USERNAME", "runtime_user")
    monkeypatch.setenv("DPONE_CONN_MSSQL_DEV_PASSWORD", "runtime_password")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "dev",
            "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "connection": {"host": "mssql.local", "port": 1433, "database": "dwh"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {
                            "username": "DPONE_CONN_MSSQL_DEV_USERNAME",
                            "password": "DPONE_CONN_MSSQL_DEV_PASSWORD",
                        },
                    },
                }
            },
        },
    )

    resolved = resolver.resolve("mssql_dev")

    assert isinstance(resolved.credentials, CredentialsConfig)
    assert resolved.credentials.host == "mssql.local"
    assert resolved.credentials.port == 1433
    assert resolved.credentials.database == "dwh"
    assert resolved.credentials.username == "runtime_user"
    assert resolved.credentials.password == "runtime_password"
    assert resolved.safe_metadata == {
        "connection_ref": "mssql_dev",
        "resolver": "env_var",
        "resolved_version": None,
    }
    assert "runtime_password" not in repr(resolved.safe_metadata)


def test_binding_resolver_reads_projected_airflow_connection_uri_volume(tmp_path: Path) -> None:
    from dpone.contracts.airflow_deployment import canonical_fingerprint
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    mount_path = tmp_path / "airflow-connections" / "mssql_dev"
    mount_path.mkdir(parents=True)
    (mount_path / "uri").write_text(
        "mssql://etl:secret@mssql.example.com:1433/analytics_staging"
        "?driver=ODBC%20Driver%2018%20for%20SQL%20Server&trust_server_certificate=yes",
        encoding="utf-8",
    )
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "mssql-dev",
                        "payload_format": "airflow_connection_uri",
                        "mount_path": mount_path.as_posix(),
                        "fields": {"uri": "uri"},
                    },
                }
            },
        },
        evidence_context={"deployment_id": "sha256:" + "d" * 64, "password": "must_not_leak"},
    )

    resolved = resolver.resolve("mssql_dev")

    assert resolved.credentials.host == "mssql.example.com"
    assert resolved.credentials.port == 1433
    assert resolved.credentials.database == "analytics_staging"
    assert resolved.credentials.username == "etl"
    assert resolved.credentials.password == "secret"
    assert resolved.credentials.driver == "ODBC Driver 18 for SQL Server"
    assert resolved.credentials.trust_server_certificate == "yes"
    assert resolved.safe_metadata == {
        "connection_ref": "mssql_dev",
        "resolver": "kubernetes_secret_volume",
        "payload_format": "airflow_connection_uri",
        "credential_ref_fingerprint": canonical_fingerprint(
            {
                "resolver": "kubernetes_secret_volume",
                "secret_name": "mssql-dev",
                "payload_format": "airflow_connection_uri",
                "mount_path": mount_path.as_posix(),
                "fields": {"uri": "uri"},
            }
        ),
        "resolved_version": None,
        "deployment_id": "sha256:" + "d" * 64,
    }
    metadata_repr = repr(resolved.safe_metadata)
    assert "mssql-dev" not in metadata_repr
    assert mount_path.as_posix() not in metadata_repr
    assert "mssql://" not in metadata_repr
    assert "etl:secret" not in metadata_repr
    assert "must_not_leak" not in metadata_repr


def test_binding_resolver_uri_payload_inherits_registry_database_and_schema(
    tmp_path: Path,
) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    mount_path = tmp_path / "airflow-connections" / "mssql_sample_metrics_state"
    mount_path.mkdir(parents=True)
    (mount_path / "uri").write_text(
        "mssql://etl:secret@192.0.2.211:1433/analytics_staging",
        encoding="utf-8",
    )
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"mssql_sample_metrics_state": {"connection_ref": "mssql_sample_metrics_state"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_sample_metrics_state": {
                    "type": "mssql",
                    "connection": {
                        "database": "Example_System",
                        "schema": "dbo",
                    },
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "dpone-airflow-connection-bridge",
                        "payload_format": "airflow_connection_uri",
                        "mount_path": mount_path.as_posix(),
                        "fields": {"uri": "uri"},
                    },
                }
            },
        },
    )

    resolved = resolver.resolve("mssql_sample_metrics_state")

    assert resolved.credentials.host == "192.0.2.211"
    assert resolved.credentials.port == 1433
    assert resolved.credentials.username == "etl"
    assert resolved.credentials.password == "secret"
    assert resolved.credentials.database == "Example_System"
    assert resolved.credentials.schema == "dbo"


def test_binding_resolver_env_var_requires_present_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    monkeypatch.delenv("DPONE_CONN_MSSQL_DEV_PASSWORD", raising=False)
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "dev",
            "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "connection": {"host": "mssql.local", "port": 1433, "database": "dwh"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"password": "DPONE_CONN_MSSQL_DEV_PASSWORD"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="Environment variable is not set: DPONE_CONN_MSSQL_DEV_PASSWORD"):
        resolver.resolve("mssql_dev")


def test_binding_resolver_env_var_rejects_inline_assignment_without_leaking_value() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "dev",
            "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"password": "PASSWORD=runtime_secret"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError) as exc:
        resolver.resolve("mssql_dev")

    message = str(exc.value)
    assert "env_var resolver field password must reference a valid environment variable name" in message
    assert "runtime_secret" not in message


def test_binding_resolver_env_var_rejects_production_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    monkeypatch.setenv("DPONE_MSSQL_PROD_PASSWORD", "runtime_password")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"mssql_prod": {"connection_ref": "mssql_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_prod": {
                    "type": "mssql",
                    "connection": {"host": "mssql.prod", "port": 1433, "database": "dwh"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"password": "DPONE_MSSQL_PROD_PASSWORD"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="env_var resolver is development/legacy only"):
        resolver.resolve("mssql_prod")


def test_binding_resolver_env_var_requires_development_support_at_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    monkeypatch.setenv("DPONE_CONN_MSSQL_DEV_PASSWORD", "runtime_password")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "dev",
            "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "connection": {"host": "mssql.local", "port": 1433, "database": "dwh"},
                    "credentials": {
                        "resolver": "env_var",
                        "fields": {"password": "DPONE_CONN_MSSQL_DEV_PASSWORD"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="support: development_only"):
        resolver.resolve("mssql_dev")


def test_connection_registry_schema_accepts_env_var_fields_and_requires_development_support() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "mssql_dev": {
                "type": "mssql",
                "credentials": {
                    "resolver": "env_var",
                    "support": "development_only",
                    "fields": {"username": "DPONE_CONN_MSSQL_DEV_USERNAME"},
                },
            }
        },
    }
    missing_support = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "mssql_dev": {
                "type": "mssql",
                "credentials": {"resolver": "env_var", "fields": {"username": "DPONE_CONN_MSSQL_DEV_USERNAME"}},
            }
        },
    }
    invalid_env_name = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "mssql_dev": {
                "type": "mssql",
                "credentials": {
                    "resolver": "env_var",
                    "support": "development_only",
                    "fields": {"password": "PASSWORD=secret"},
                },
            }
        },
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_support, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_env_name, schema)


@pytest.mark.parametrize(
    "connection",
    [
        {"host": "db.internal", "password": "inline-secret"},
        {"host": "db.internal", "port": "5432"},
        {"host": "db.internal", "port": 0},
        {"host": "db.internal", "port": 65536},
        {"host": "db.internal", "secure": "true"},
        {"host": "db.internal", "parameters": {"token": "inline-secret"}},
    ],
)
def test_connection_registry_schema_rejects_secret_or_mistyped_connection_metadata(
    connection: dict[str, object],
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "warehouse": {
                "type": "postgres",
                "connection": connection,
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "path": "prod/warehouse",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_connection_registry_schema_allows_secret_field_references() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "warehouse": {
                "type": "postgres",
                "connection": {
                    "host": "db.internal",
                    "port": 5432,
                    "secure": True,
                },
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "path": "prod/warehouse",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }

    jsonschema.validate(payload, schema)


def test_binding_set_schema_rejects_invalid_connection_ref_aliases() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/binding-set.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"sales.pg_prod-1": {"connection_ref": "platform.pg_prod-1"}},
        "runtime": {},
    }
    invalid_binding_key = {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"../pg_prod": {"connection_ref": "pg_prod"}},
        "runtime": {},
    }
    invalid_bound_ref = {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"pg_prod": {"connection_ref": "pg_prod\nPASSWORD=secret"}},
        "runtime": {},
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_binding_key, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_bound_ref, schema)


def test_connection_registry_schema_rejects_invalid_connection_ref_aliases() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    invalid_connection_key = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod/../../secret": {
                "type": "postgres",
                "credentials": {
                    "resolver": "env_var",
                    "support": "development_only",
                    "fields": {"username": "DPONE_CONN_PG_PROD_USERNAME"},
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_connection_key, schema)


def test_safe_sample_schemas_reject_invalid_connection_ref_aliases() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    unsafe_ref = "pg_prod\nPASSWORD=secret"
    invalid_payloads = [
        (
            "docs/schemas/gitops/safe-sample-source-request.schema.json",
            {
                "schema": "dpone.safe-sample-source-request.v1",
                "status": "planned",
                "mode": "pushdown",
                "sample_rows": 1000,
                "max_bytes": 1024,
                "timeout_seconds": 60,
                "source_read_only": True,
                "full_scan_allowed": False,
                "pii_policy": "masked",
                "target": {
                    "mode": "temporary",
                    "connection_ref": unsafe_ref,
                    "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
                    "ttl_seconds": 86400,
                },
                "errors": [],
            },
        ),
        (
            "docs/schemas/gitops/safe-sample-certified-copy-request.schema.json",
            {
                "schema": "dpone.safe-sample-certified-copy-request.v1",
                "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
                "source": {
                    "type": "mssql",
                    "connection_ref": unsafe_ref,
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_prod",
                    "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
                },
                "strategy": "incremental_merge",
                "sample_rows": 1000,
                "max_bytes": 1024,
                "timeout_seconds": 60,
                "source_read_only": True,
                "pii_policy": "masked",
                "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
            },
        ),
        (
            "docs/schemas/gitops/mssql-clickhouse-safe-sample-copy-config.schema.json",
            {
                "schema": "dpone.mssql-clickhouse-safe-sample-copy-config.v1",
                "source_connection_ref": unsafe_ref,
                "source_table": {"schema": "dbo", "name": "orders"},
            },
        ),
        (
            "docs/schemas/gitops/mssql-safe-sample-read-plan.schema.json",
            {
                "schema": "dpone.mssql-safe-sample-read-plan.v1",
                "sql": "SELECT TOP (@sample_rows) * FROM dbo.orders",
                "parameters": {"sample_rows": 1000},
                "sample_rows": 1000,
                "max_bytes": 1024,
                "timeout_seconds": 60,
                "source_read_only": True,
                "source": {
                    "type": "mssql",
                    "connection_ref": unsafe_ref,
                    "table": {"schema": "dbo", "name": "orders"},
                },
            },
        ),
        (
            "docs/schemas/gitops/clickhouse-safe-sample-insert-plan.schema.json",
            {
                "schema": "dpone.clickhouse-safe-sample-insert-plan.v1",
                "sql": "INSERT INTO dpone_tmp_prod.orders VALUES",
                "row_count": 0,
                "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": unsafe_ref,
                    "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
                },
            },
        ),
        (
            "docs/schemas/gitops/safe-sample-data-copy.schema.json",
            {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "blocked",
                "source_request": {
                    "schema": "dpone.safe-sample-source-request.v1",
                    "status": "planned",
                    "mode": "pushdown",
                    "sample_rows": 1000,
                    "max_bytes": 1024,
                    "timeout_seconds": 60,
                    "source_read_only": True,
                    "full_scan_allowed": False,
                    "pii_policy": "masked",
                    "target": {"connection_ref": unsafe_ref},
                    "errors": [],
                },
                "copy_request": {
                    "schema": "dpone.safe-sample-certified-copy-request.v1",
                    "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
                    "source": {
                        "type": "mssql",
                        "connection_ref": "mssql_prod",
                        "table": {"schema": "dbo", "name": "orders"},
                    },
                    "sink": {
                        "type": "clickhouse",
                        "connection_ref": "clickhouse_prod",
                        "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
                    },
                    "strategy": "incremental_merge",
                    "sample_rows": 1000,
                    "max_bytes": 1024,
                    "timeout_seconds": 60,
                    "source_read_only": True,
                    "pii_policy": "masked",
                    "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
                },
                "rows_read": 0,
                "rows_written": 0,
                "bytes_read": 0,
                "pii_policy": "masked",
                "diagnostics": {},
                "errors": [],
            },
        ),
    ]

    for schema_path, payload in invalid_payloads:
        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(payload, schema)


def _canonical_quarantine_data_copy() -> dict[str, object]:
    return {
        "schema": "dpone.safe-sample-data-copy.v1",
        "status": "copied_with_quarantine",
        "source_request": {
            "schema": "dpone.safe-sample-source-request.v1",
            "status": "planned",
            "mode": "pushdown",
            "sample_rows": 10,
            "max_bytes": 1024,
            "timeout_seconds": 60,
            "source_read_only": True,
            "full_scan_allowed": False,
            "estimated_read_bytes": None,
            "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
            "pii_policy": "masked",
            "target": {
                "connection_ref": "clickhouse_prod",
                "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
            },
            "errors": [],
        },
        "copy_request": {
            "schema": "dpone.safe-sample-certified-copy-request.v1",
            "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_prod",
                "table": {"schema": "dbo", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_ref": "clickhouse_prod",
                "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders"},
            },
            "strategy": "incremental_merge",
            "sample_rows": 10,
            "max_bytes": 1024,
            "timeout_seconds": 60,
            "source_read_only": True,
            "pii_policy": "masked",
            "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        },
        "rows_read": 10,
        "rows_written": 9,
        "quarantined_rows": 1,
        "bytes_read": 512,
        "pii_policy": "masked",
        "diagnostics": {
            "source": {"client": {"client": "dbapi_mssql", "rows_returned": 10}},
            "sink": {"client": {"client": "clickhouse_connection", "rows_sent": 9}},
        },
        "errors": [],
    }


def _runtime_execution_with_data_copy(data_copy: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "dpone.safe-sample-runtime-execution.v1",
        "execution_status": "succeeded",
        "data_outcome": "passed_with_quarantine",
        "release_id": None,
        "deployment_id": None,
        "data_copy": data_copy,
        "errors": [],
    }


def test_safe_sample_data_copy_schema_producers_match_generated_references() -> None:
    from dpone.gitops.schema_safe_sample_route_execution_contracts import safe_sample_data_copy_contract
    from dpone.gitops.schema_safe_sample_runtime_contracts import safe_sample_runtime_execution_contract

    standalone = json.loads(Path("docs/schemas/gitops/safe-sample-data-copy.schema.json").read_text(encoding="utf-8"))
    runtime = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )

    assert standalone == safe_sample_data_copy_contract().schema
    assert runtime == safe_sample_runtime_execution_contract().schema


def test_safe_sample_data_copy_schemas_accept_canonical_quarantine_result() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    standalone = json.loads(Path("docs/schemas/gitops/safe-sample-data-copy.schema.json").read_text(encoding="utf-8"))
    runtime = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    data_copy = _canonical_quarantine_data_copy()

    jsonschema.validate(data_copy, standalone)
    jsonschema.validate(_runtime_execution_with_data_copy(data_copy), runtime)


@pytest.mark.parametrize(
    "mutation",
    (
        pytest.param("copied_nonzero_quarantine", id="copied-nonzero-quarantine"),
        pytest.param("missing_quarantine", id="quarantine-count-missing"),
        pytest.param("zero_quarantine", id="quarantine-count-zero"),
        pytest.param("missing_diagnostics", id="success-diagnostics-missing"),
        pytest.param("wrong_schema", id="success-schema-mismatch"),
        pytest.param("unsupported_diagnostic", id="unsupported-diagnostic-object"),
    ),
)
def test_safe_sample_data_copy_schemas_reject_malformed_results(mutation: str) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    standalone = json.loads(Path("docs/schemas/gitops/safe-sample-data-copy.schema.json").read_text(encoding="utf-8"))
    runtime = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    data_copy = _canonical_quarantine_data_copy()
    if mutation == "copied_nonzero_quarantine":
        data_copy["status"] = "copied"
        data_copy["rows_written"] = 10
    elif mutation == "missing_quarantine":
        data_copy.pop("quarantined_rows")
    elif mutation == "zero_quarantine":
        data_copy["quarantined_rows"] = 0
        data_copy["rows_written"] = 10
    elif mutation == "missing_diagnostics":
        data_copy.pop("diagnostics")
    elif mutation == "wrong_schema":
        data_copy["schema"] = "dpone.safe-sample-data-copy.v2"
    else:
        data_copy["diagnostics"] = {"unsupported": object()}

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(data_copy, standalone)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(_runtime_execution_with_data_copy(data_copy), runtime)


def test_connection_check_rejects_invalid_env_var_name(tmp_path: Path) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "dev").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "dev" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "dev",
                "bindings": {"mssql_dev": {"connection_ref": "mssql_dev"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "dev" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "dev"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "dev.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "dev",
                "connections": {
                    "mssql_dev": {
                        "type": "mssql",
                        "credentials": {
                            "resolver": "env_var",
                            "support": "development_only",
                            "fields": {"password": "PASSWORD=secret"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={"processes": [{"source": {"connection_ref": "mssql_dev"}}]},
        environment="dev",
    )

    assert {error["code"] for error in result["errors"]} == {"DPONE_ENV_VAR_NAME_INVALID"}
    assert "secret" not in json.dumps(result["errors"])


def test_connection_check_rejects_invalid_connection_ref_alias_without_leaking_payload(tmp_path: Path) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    unsafe_ref = "pg_prod\nPASSWORD=secret"
    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "env_var",
                            "support": "development_only",
                            "fields": {"username": "DPONE_CONN_PG_PROD_USERNAME"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={"processes": [{"source": {"connection_ref": unsafe_ref}}]},
        environment="prod",
    )
    combined = json.dumps(result)

    assert {error["code"] for error in result["errors"]} == {"DPONE_CONNECTION_REF_INVALID"}
    assert result["connection_refs"] == []
    assert "PASSWORD=secret" not in combined


def test_connection_check_schema_rejects_invalid_report_connection_ref_aliases() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-check.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.connection-check.v1",
        "passed": True,
        "changes": [],
        "errors": [],
        "mode": "connections",
        "network": False,
        "secrets": False,
        "source_queries": False,
        "handshake": "configuration_only",
        "environment": "prod",
        "connection_refs": ["orders_source\nPASSWORD=secret"],
        "resolved_connection_refs": ["../pg_prod"],
        "airflow_connection_bridge": {
            "required": True,
            "execution_mode": "operator_bridge",
            "resolver_location": "operator_execution",
            "parse_safe": True,
            "secrets": False,
            "required_connection_ids": ["pg_prod"],
            "connections": [
                {
                    "connection_ref": "orders_source\nPASSWORD=secret",
                    "registry_connection_ref": "../pg_prod",
                    "connection_id": "pg_prod",
                    "env_name": "AIRFLOW_CONN_PG_PROD",
                }
            ],
            "projection": {
                "mode": "kubernetes_secret_volume",
                "secret_name": "dpone-airflow-connection-bridge",
                "mount_path": "/run/secrets/dpone/airflow-connections",
                "payload_format": "airflow_connection_uri",
                "secret_values": False,
                "connections": [
                    {
                        "connection_ref": "orders_source\nPASSWORD=secret",
                        "registry_connection_ref": "../pg_prod",
                        "connection_id": "pg_prod",
                        "secret_key": "AIRFLOW_CONN_PG_PROD",
                        "mount_path": "/run/secrets/dpone/airflow-connections/orders_source",
                        "fields": {"uri": "uri"},
                    }
                ],
            },
            "next_actions": [],
        },
        "binding_set_path": "environments/prod/binding-set.yaml",
        "connection_registry_path": "platform/connection-registries/prod.yaml",
        "credential_runtime_path": "environments/prod/credential-runtime.yaml",
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_binding_resolver_vault_kv_uses_injected_runtime_client() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            self.calls.append({"mount_point": mount_point, "path": path})
            return {
                "user": "dpone_runtime",
                "password": "secret",
                "_metadata": {"version": 17},
            }

    vault_kv_fake = VaultKvFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {
                        "host": "postgres.internal",
                        "port": 5432,
                        "database": "dwh",
                    },
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "kv_version": 2,
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {
                            "username": "user",
                            "password": "password",
                        },
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=vault_kv_fake,
        clock=lambda: datetime(2026, 7, 11, 12, 3, 11, tzinfo=UTC),
    )

    resolved = resolver.resolve("pg_prod")

    assert vault_kv_fake.calls == [{"mount_point": "kv", "path": "dpone/prod/credentials/pg_source"}]
    assert resolved.credentials.host == "postgres.internal"
    assert resolved.credentials.port == 5432
    assert resolved.credentials.database == "dwh"
    assert resolved.credentials.username == "dpone_runtime"
    assert resolved.credentials.password == "secret"
    assert resolved.safe_metadata == {
        "connection_ref": "pg_prod",
        "resolver": "vault_kv",
        "version_policy": "latest",
        "resolution_scope": "workload_start",
        "resolved_version": 17,
        "resolved_at": "2026-07-11T12:03:11Z",
    }


def test_binding_resolver_rejects_invalid_requested_connection_ref_without_leaking_payload() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    unsafe_ref = "pg_prod\nPASSWORD=secret"
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={"schema": "dpone.connection-registry.v1", "connections": {}},
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve(unsafe_ref)

    assert "logical connection_ref alias" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)


def test_binding_resolver_rejects_invalid_bound_connection_ref_without_leaking_payload() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    unsafe_ref = "pg_prod\nPASSWORD=secret"
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": unsafe_ref}},
        },
        connection_registry={"schema": "dpone.connection-registry.v1", "connections": {}},
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "logical connection_ref alias" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)


def test_credential_runtime_check_requires_environment(tmp_path: Path) -> None:
    from dpone.readiness.credential_runtime_checks import validate_credential_runtime

    runtime_path = tmp_path / "credential-runtime.yaml"
    payload = {
        "schema": "dpone.credential-runtime.v1",
        "vault": {
            "address": "https://vault.internal",
            "auth": {"method": "kubernetes", "role": "dpone-runtime-prod"},
        },
    }

    errors = validate_credential_runtime(payload, "prod", runtime_path)

    assert [error["code"] for error in errors] == ["DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_REQUIRED"]
    assert errors[0]["schema"] == "dpone.error.v1"
    assert errors[0]["expected_environment"] == "prod"


def test_credential_runtime_check_rejects_derived_secret_material(tmp_path: Path) -> None:
    from dpone.readiness.credential_runtime_checks import validate_credential_runtime

    runtime_path = tmp_path / "credential-runtime.yaml"
    payload = {
        "schema": "dpone.credential-runtime.v1",
        "environment": "prod",
        "vault": {
            "address": "https://vault.internal",
            "auth": {
                "method": "kubernetes",
                "role": "dpone-runtime-prod",
                "client_secret": "must-not-leak",
            },
        },
    }

    errors = validate_credential_runtime(payload, "prod", runtime_path)

    assert [error["code"] for error in errors] == ["DPONE_CREDENTIAL_RUNTIME_SECRET_MATERIAL_FORBIDDEN"]
    assert "client_secret" in errors[0]["message"]
    assert "must-not-leak" not in repr(errors)


def test_binding_resolver_vault_kv_requires_non_empty_field_mapping_values() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {"username": "dpone_runtime", "password": "runtime_password"}

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"username": "", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=VaultKvFake(),
    )

    with pytest.raises(ValueError, match="vault_kv fields must contain non-empty string mappings"):
        resolver.resolve("pg_prod")


def test_binding_resolver_vault_kv_rejects_invalid_mount_before_client_call() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            self.calls.append({"mount_point": mount_point, "path": path})
            return {"username": "dpone_runtime", "password": "runtime_password"}

    vault = VaultKvFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv\nPASSWORD=secret",
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"username": "username", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=vault,
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "Vault KV mount" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert vault.calls == []


def test_binding_resolver_vault_kv_rejects_invalid_path_before_client_call() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            self.calls.append({"mount_point": mount_point, "path": path})
            return {"username": "dpone_runtime", "password": "runtime_password"}

    vault = VaultKvFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "path": "dpone/prod/../credentials\nPASSWORD=secret",
                        "fields": {"username": "username", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=vault,
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "Vault KV path" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert vault.calls == []


@pytest.mark.parametrize(
    ("policy_key", "unsafe_value", "expected_message"),
    [
        ("version_policy", "latest\nPASSWORD=secret", "version_policy"),
        ("resolution_scope", "workload_start\nPASSWORD=secret", "resolution_scope"),
    ],
)
def test_binding_resolver_rejects_invalid_credential_policy_before_client_call(
    policy_key: str,
    unsafe_value: str,
    expected_message: str,
) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            self.calls.append({"mount_point": mount_point, "path": path})
            return {"username": "dpone_runtime", "password": "runtime_password"}

    credentials = {
        "resolver": "vault_kv",
        "mount": "kv",
        "path": "dpone/prod/credentials/pg_source",
        "fields": {"username": "username", "password": "password"},
        "version_policy": "latest",
        "resolution_scope": "workload_start",
        policy_key: unsafe_value,
    }
    vault = VaultKvFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": credentials,
                }
            },
        },
        vault_kv_reader=vault,
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert expected_message in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert vault.calls == []


def test_binding_resolver_adds_secret_free_deployment_evidence_context() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {"username": "dpone_runtime", "password": "runtime_password", "_metadata": {"version": 23}}

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"username": "username", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=VaultKvFake(),
        clock=lambda: datetime(2026, 7, 11, 12, 3, 11, tzinfo=UTC),
        evidence_context={
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "pack_fingerprint": "sha256:" + "c" * 64,
            "binding_set_fingerprint": "sha256:" + "d" * 64,
            "connection_registry_fingerprint": "sha256:" + "e" * 64,
            "credential_runtime_fingerprint": "sha256:" + "f" * 64,
            "runtime_image_digest": "sha256:" + "1" * 64,
            "airflow_bundle_ref": "git:7ac31f2",
            "password": "must-not-appear",
        },
    )

    resolved = resolver.resolve("pg_prod")

    assert resolved.safe_metadata == {
        "connection_ref": "pg_prod",
        "resolver": "vault_kv",
        "version_policy": "latest",
        "resolution_scope": "workload_start",
        "resolved_version": 23,
        "resolved_at": "2026-07-11T12:03:11Z",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "pack_fingerprint": "sha256:" + "c" * 64,
        "binding_set_fingerprint": "sha256:" + "d" * 64,
        "connection_registry_fingerprint": "sha256:" + "e" * 64,
        "credential_runtime_fingerprint": "sha256:" + "f" * 64,
        "runtime_image_digest": "sha256:" + "1" * 64,
        "airflow_bundle_ref": "git:7ac31f2",
    }
    assert "must-not-appear" not in repr(resolved.safe_metadata)


def test_binding_resolver_omits_empty_optional_evidence_context() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class VaultKvFake:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {"username": "dpone_runtime", "password": "runtime_password"}

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"username": "username", "password": "password"},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        vault_kv_reader=VaultKvFake(),
        evidence_context={
            "release_id": "sha256:" + "a" * 64,
            "airflow_bundle_ref": None,
        },
    )

    resolved = resolver.resolve("pg_prod")

    assert resolved.safe_metadata["release_id"] == "sha256:" + "a" * 64
    assert "airflow_bundle_ref" not in resolved.safe_metadata


def test_binding_resolver_rejects_malformed_evidence_context_without_leaking_value() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    with pytest.raises(ValueError) as exc_info:
        BindingCredentialResolver(
            binding_set={
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            },
            connection_registry={"schema": "dpone.connection-registry.v1", "connections": {}},
            evidence_context={"deployment_id": "password=must-not-leak"},
        )

    message = str(exc_info.value)
    assert "deployment_id" in message
    assert "sha256 digest" in message
    assert "must-not-leak" not in message


def test_connection_registry_schema_requires_vault_rotation_semantics() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "kv_version": 2,
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }
    missing_semantics = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_semantics, schema)


def test_connection_registry_schema_rejects_non_logical_vault_references() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    invalid_mount = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv/data",
                    "kv_version": 2,
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }
    invalid_path = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "kv_version": 2,
                    "path": "dpone/prod/../credentials",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_mount, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_path, schema)


def test_connection_check_rejects_non_logical_vault_references_without_leaking_payload(tmp_path: Path) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv/data\nPASSWORD=secret",
                            "kv_version": 2,
                            "path": "dpone/prod/../credentials\nPASSWORD=secret",
                            "fields": {"username": "username", "password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={
            "processes": [
                {
                    "source": {"connection_ref": "pg_prod"},
                    "sink": {"connection_ref": "pg_prod"},
                }
            ]
        },
        environment="prod",
    )
    combined = json.dumps(result)

    assert {error["code"] for error in result["errors"]} == {
        "DPONE_VAULT_MOUNT_NOT_LOGICAL",
        "DPONE_VAULT_PATH_NOT_LOGICAL",
    }
    assert "PASSWORD=secret" not in combined


def test_connection_registry_schema_rejects_empty_field_mapping_values() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    empty_value = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "kv_version": 2,
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"username": "", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }
    empty_key = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "kv",
                    "kv_version": 2,
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(empty_value, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(empty_key, schema)


def test_binding_resolver_kubernetes_secret_volume_reads_projected_files(tmp_path: Path) -> None:
    from dpone.contracts.airflow_deployment import canonical_fingerprint
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    secret_dir = tmp_path / "pg-source"
    secret_dir.mkdir()
    (secret_dir / "username").write_text("dpone_runtime\n", encoding="utf-8")
    (secret_dir / "password").write_text("runtime_password_value\n", encoding="utf-8")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "pg-prod",
                        "mount_path": secret_dir.as_posix(),
                        "fields": {
                            "username": "username",
                            "password": "password",
                        },
                    },
                }
            },
        },
    )

    resolved = resolver.resolve("pg_prod")

    assert resolved.credentials.host == "postgres.internal"
    assert resolved.credentials.port == 5432
    assert resolved.credentials.database == "dwh"
    assert resolved.credentials.username == "dpone_runtime"
    assert resolved.credentials.password == "runtime_password_value"
    assert resolved.safe_metadata == {
        "connection_ref": "pg_prod",
        "resolver": "kubernetes_secret_volume",
        "credential_ref_fingerprint": canonical_fingerprint(
            {
                "resolver": "kubernetes_secret_volume",
                "secret_name": "pg-prod",
                "mount_path": secret_dir.as_posix(),
                "fields": {"username": "username", "password": "password"},
            }
        ),
        "resolved_version": None,
    }
    assert "runtime_password_value" not in repr(resolved.safe_metadata)
    assert "pg-prod" not in repr(resolved.safe_metadata)
    assert secret_dir.as_posix() not in repr(resolved.safe_metadata)


def test_binding_resolver_kubernetes_secret_volume_requires_non_empty_fields(tmp_path: Path) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    secret_dir = tmp_path / "pg-source"
    secret_dir.mkdir()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "pg-prod",
                        "mount_path": secret_dir.as_posix(),
                        "fields": {},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="kubernetes_secret_volume resolver requires non-empty fields"):
        resolver.resolve("pg_prod")


def test_binding_resolver_kubernetes_secret_volume_blocks_symlink_escape(tmp_path: Path) -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    secret_dir = tmp_path / "pg-source"
    secret_dir.mkdir()
    outside = tmp_path / "outside-password"
    outside.write_text("secret\n", encoding="utf-8")
    (secret_dir / "username").write_text("dpone_runtime\n", encoding="utf-8")
    (secret_dir / "password").symlink_to(outside)
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "pg-prod",
                        "mount_path": secret_dir.as_posix(),
                        "fields": {
                            "username": "username",
                            "password": "password",
                        },
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="escapes mount_path"):
        resolver.resolve("pg_prod")


def test_connection_registry_schema_requires_kubernetes_secret_volume_fields() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": "pg-prod",
                    "mount_path": "/run/secrets/dpone/pg-prod",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }
    missing_fields = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": "pg-prod",
                    "mount_path": "/run/secrets/dpone/pg-prod",
                },
            }
        },
    }
    empty_fields = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": "pg-prod",
                    "mount_path": "/run/secrets/dpone/pg-prod",
                    "fields": {},
                },
            }
        },
    }
    unsafe_field_path = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": "pg-prod",
                    "mount_path": "/run/secrets/dpone/pg-prod",
                    "fields": {"username": "../username", "password": "nested/../password"},
                },
            }
        },
    }
    unsafe_mount_path = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": "pg-prod",
                    "mount_path": "/run/secrets/dpone/../outside",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_fields, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(empty_fields, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(unsafe_field_path, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(unsafe_mount_path, schema)


def test_connection_registry_schema_rejects_kubernetes_secret_volume_invalid_secret_name_without_leaking_payload() -> (
    None
):
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    invalid_secret_name = "pg-prod\nPASSWORD=secret"
    payload = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_volume",
                    "secret_name": invalid_secret_name,
                    "mount_path": "/run/secrets/dpone/pg-prod",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_connection_check_rejects_kubernetes_secret_volume_mount_path_escape(tmp_path: Path) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "kubernetes_secret_volume",
                            "secret_name": "pg-prod",
                            "mount_path": "/run/secrets/dpone/../outside",
                            "fields": {"username": "username", "password": "password"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={
            "processes": [
                {
                    "source": {"connection_ref": "pg_prod"},
                    "sink": {"connection_ref": "pg_prod"},
                }
            ]
        },
        environment="prod",
    )

    assert {error["code"] for error in result["errors"]} == {"DPONE_KUBERNETES_SECRET_VOLUME_MOUNT_PATH_INVALID"}


def test_connection_check_rejects_kubernetes_secret_volume_invalid_secret_name_without_leaking_payload(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "kubernetes_secret_volume",
                            "secret_name": "pg-prod\nPASSWORD=secret",
                            "mount_path": "/run/secrets/dpone/pg-prod",
                            "fields": {"username": "username", "password": "password"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={
            "processes": [
                {
                    "source": {"connection_ref": "pg_prod"},
                    "sink": {"connection_ref": "pg_prod"},
                }
            ]
        },
        environment="prod",
    )
    combined = json.dumps(result)

    assert {error["code"] for error in result["errors"]} == {"DPONE_KUBERNETES_SECRET_VOLUME_SECRET_NAME_INVALID"}
    assert "PASSWORD=secret" not in combined


def test_connection_check_rejects_kubernetes_secret_api_invalid_reference_without_leaking_payload(
    tmp_path: Path,
) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "kubernetes_secret_api",
                            "namespace": "airflow-example\nPASSWORD=secret",
                            "name": "pg-prod\nPASSWORD=secret",
                            "fields": {"username": "username", "password": "password"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={
            "processes": [
                {
                    "source": {"connection_ref": "pg_prod"},
                    "sink": {"connection_ref": "pg_prod"},
                }
            ]
        },
        environment="prod",
    )
    combined = json.dumps(result)

    assert {error["code"] for error in result["errors"]} == {
        "DPONE_KUBERNETES_SECRET_API_NAME_INVALID",
        "DPONE_KUBERNETES_SECRET_API_NAMESPACE_INVALID",
    }
    assert "PASSWORD=secret" not in combined


def test_binding_resolver_kubernetes_secret_volume_rejects_invalid_secret_name_before_filesystem() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "pg-prod\nPASSWORD=secret",
                        "mount_path": "/definitely/missing/secret-volume",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "Kubernetes Secret name" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)


def test_binding_resolver_kubernetes_secret_api_uses_injected_reader() -> None:
    from dpone.contracts.airflow_deployment import canonical_fingerprint
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class KubernetesSecretReaderFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def read_secret(self, *, namespace: str, name: str) -> dict[str, object]:
            self.calls.append({"namespace": namespace, "name": name})
            return {"username": "dpone_runtime", "password": "runtime_password_value"}

    secret_reader = KubernetesSecretReaderFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-prod",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
        kubernetes_secret_reader=secret_reader,
    )

    resolved = resolver.resolve("pg_prod")

    assert secret_reader.calls == [{"namespace": "airflow-example", "name": "pg-prod"}]
    assert resolved.credentials.host == "postgres.internal"
    assert resolved.credentials.port == 5432
    assert resolved.credentials.database == "dwh"
    assert resolved.credentials.username == "dpone_runtime"
    assert resolved.credentials.password == "runtime_password_value"
    assert resolved.safe_metadata == {
        "connection_ref": "pg_prod",
        "resolver": "kubernetes_secret_api",
        "credential_ref_fingerprint": canonical_fingerprint(
            {
                "resolver": "kubernetes_secret_api",
                "namespace": "airflow-example",
                "name": "pg-prod",
                "fields": {"username": "username", "password": "password"},
            }
        ),
        "resolved_version": None,
    }
    assert "runtime_password_value" not in repr(resolved.safe_metadata)
    assert "airflow-example" not in repr(resolved.safe_metadata)
    assert "pg-prod" not in repr(resolved.safe_metadata)


def test_binding_resolver_kubernetes_secret_api_requires_non_empty_fields() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class KubernetesSecretReaderFake:
        def read_secret(self, *, namespace: str, name: str) -> dict[str, object]:
            return {"username": "dpone_runtime", "password": "runtime_password_value"}

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-prod",
                        "fields": {},
                    },
                }
            },
        },
        kubernetes_secret_reader=KubernetesSecretReaderFake(),
    )

    with pytest.raises(ValueError, match="kubernetes_secret_api resolver requires non-empty fields"):
        resolver.resolve("pg_prod")


def test_binding_resolver_kubernetes_secret_api_requires_injected_reader() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-prod",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="requires injected kubernetes_secret_reader"):
        resolver.resolve("pg_prod")


def test_binding_resolver_kubernetes_secret_api_rejects_invalid_namespace_before_reader() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class KubernetesSecretReaderFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def read_secret(self, *, namespace: str, name: str) -> dict[str, object]:
            self.calls.append({"namespace": namespace, "name": name})
            return {"username": "dpone_runtime", "password": "runtime_password_value"}

    secret_reader = KubernetesSecretReaderFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example\nPASSWORD=secret",
                        "name": "pg-prod",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
        kubernetes_secret_reader=secret_reader,
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "Kubernetes namespace" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert secret_reader.calls == []


def test_binding_resolver_kubernetes_secret_api_rejects_invalid_name_before_reader() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    class KubernetesSecretReaderFake:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def read_secret(self, *, namespace: str, name: str) -> dict[str, object]:
            self.calls.append({"namespace": namespace, "name": name})
            return {"username": "dpone_runtime", "password": "runtime_password_value"}

    secret_reader = KubernetesSecretReaderFake()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432, "database": "dwh"},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-prod\nPASSWORD=secret",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
        kubernetes_secret_reader=secret_reader,
    )

    with pytest.raises(ValueError) as exc_info:
        resolver.resolve("pg_prod")

    assert "Kubernetes Secret name" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert secret_reader.calls == []


def test_connection_registry_schema_requires_kubernetes_secret_api_fields() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_api",
                    "namespace": "airflow-example",
                    "name": "pg-prod",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }
    missing_fields = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_api",
                    "namespace": "airflow-example",
                    "name": "pg-prod",
                },
            }
        },
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_fields, schema)


def test_connection_registry_schema_rejects_kubernetes_secret_api_invalid_namespace_and_name() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    invalid_namespace = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_api",
                    "namespace": "airflow-example\nPASSWORD=secret",
                    "name": "pg-prod",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }
    invalid_name = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "kubernetes_secret_api",
                    "namespace": "airflow-example",
                    "name": "pg-prod\nPASSWORD=secret",
                    "fields": {"username": "username", "password": "password"},
                },
            }
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_namespace, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_name, schema)


def test_binding_resolver_airflow_connection_is_operator_bridge_only() -> None:
    from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "credentials": {
                        "resolver": "airflow_connection",
                        "connection_id": "pg_prod",
                        "execution_mode": "operator_bridge",
                    },
                }
            },
        },
    )

    with pytest.raises(ValueError, match="operator-side bridge"):
        resolver.resolve("pg_prod")


def test_connection_registry_schema_requires_airflow_operator_bridge_mode() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    valid = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "pg_prod",
                    "execution_mode": "operator_bridge",
                },
            }
        },
    }
    missing_mode = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {"resolver": "airflow_connection", "connection_id": "pg_prod"},
            }
        },
    }
    invalid_connection_id = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "pg_prod": {
                "type": "postgres",
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "postgres://etl:secret@pg.internal:5432/dwh",
                    "execution_mode": "operator_bridge",
                },
            }
        },
    }

    jsonschema.validate(valid, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_mode, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_connection_id, schema)


def test_connection_check_rejects_airflow_connection_uri_as_connection_id(tmp_path: Path) -> None:
    from dpone.readiness.airflow_connection_checks import validate_connection_configuration

    (tmp_path / "environments" / "prod").mkdir(parents=True)
    (tmp_path / "platform" / "connection-registries").mkdir(parents=True)
    (tmp_path / "environments" / "prod" / "binding-set.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {"pg_prod": {"connection_ref": "pg_prod"}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "environments" / "prod" / "credential-runtime.yaml").write_text(
        json.dumps({"schema": "dpone.credential-runtime.v1", "environment": "prod"}),
        encoding="utf-8",
    )
    (tmp_path / "platform" / "connection-registries" / "prod.yaml").write_text(
        json.dumps(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "pg_prod": {
                        "type": "postgres",
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "postgres://etl:secret@pg.internal:5432/dwh",
                            "execution_mode": "operator_bridge",
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = validate_connection_configuration(
        root=tmp_path,
        pipeline_source={"processes": [{"source": {"connection_ref": "pg_prod"}}]},
        environment="prod",
    )

    assert {error["code"] for error in result["errors"]} == {"DPONE_AIRFLOW_CONNECTION_ID_INVALID"}
    assert "secret" not in json.dumps(result["errors"])
    assert result["airflow_connection_bridge"]["required"] is False


def test_binding_resolver_import_is_parse_safe() -> None:
    sys.modules.pop("vault_kv_client", None)

    import dpone.runtime.credentials.binding_resolver  # noqa: F401

    assert "vault_kv_client" not in sys.modules


_INIT_FETCH_RELEASE_ID = "sha256:" + "a" * 64
_INIT_FETCH_RELEASE_DIR = _INIT_FETCH_RELEASE_ID.replace(":", "-")
_INIT_FETCH_ARTIFACT_REF = f"cache://releases/{_INIT_FETCH_RELEASE_DIR}/packs/load_orders.airflow-pack.json"


def test_init_fetch_plan_requires_pinned_release_and_deployment() -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    with pytest.raises(ValueError, match="release_id"):
        build_init_fetch_plan(
            release_id="",
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            workload_packs=[],
        )

    with pytest.raises(ValueError, match="current"):
        build_init_fetch_plan(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref="current",
            workload_packs=[],
        )


@pytest.mark.parametrize(
    ("field", "invalid_digest"),
    (
        ("release_id", "sha256:release"),
        ("release_id", "sha256:" + "A" * 64),
        ("deployment_id", "sha256:" + "g" * 64),
        ("deployment_id", "sha256:" + "b" * 63),
    ),
)
def test_init_fetch_plan_requires_canonical_identity_digests(
    field: str,
    invalid_digest: str,
) -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    values = {
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
    }
    values[field] = invalid_digest

    with pytest.raises(ValueError, match=field):
        build_init_fetch_plan(
            release_id=values["release_id"],
            deployment_id=values["deployment_id"],
            artifact_registry_ref="dpone-prod-artifacts",
            workload_packs=[],
        )


@pytest.mark.parametrize(
    "invalid_digest",
    (
        "sha256:pack",
        "sha256:" + "C" * 64,
        "sha256:" + "z" * 64,
        "sha256:" + "c" * 65,
    ),
)
def test_init_fetch_plan_requires_canonical_artifact_digest(invalid_digest: str) -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    with pytest.raises(ValueError, match="sha256"):
        build_init_fetch_plan(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            workload_packs=[
                {
                    "id": "load_orders",
                    "artifact_ref": "cache://releases/sha256-aaa/packs/load_orders.airflow-pack.json",
                    "sha256": invalid_digest,
                    "bytes": 1,
                }
            ],
        )


@pytest.mark.parametrize(
    "artifact_registry_ref",
    (
        "CURRENT",
        "Latest",
        "registry/LaTeSt",
        "cache://registries/CuRrEnT",
    ),
)
def test_init_fetch_plan_rejects_mutable_registry_aliases(artifact_registry_ref: str) -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    with pytest.raises(ValueError, match="current/latest"):
        build_init_fetch_plan(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref=artifact_registry_ref,
            workload_packs=[],
        )


@pytest.mark.parametrize(
    "artifact_ref",
    (
        "cache://releases/current/load_orders.airflow-pack.json",
        "cache://releases/CURRENT/load_orders.airflow-pack.json",
        "cache://releases/latest/load_orders.airflow-pack.json",
        "cache://releases/LaTeSt/load_orders.airflow-pack.json",
    ),
)
def test_init_fetch_plan_rejects_mutable_artifact_aliases(artifact_ref: str) -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    with pytest.raises(ValueError, match="current/latest"):
        build_init_fetch_plan(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            workload_packs=[
                {
                    "id": "load_orders",
                    "artifact_ref": artifact_ref,
                    "sha256": "sha256:" + "c" * 64,
                    "bytes": 1,
                }
            ],
        )


@pytest.mark.parametrize("declared_bytes", (None, 0, -1, True))
def test_init_fetch_plan_requires_positive_declared_artifact_bytes(
    declared_bytes: int | None,
) -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    workload_pack: dict[str, object] = {
        "id": "load_orders",
        "artifact_ref": "cache://releases/sha256-aaa/packs/load_orders.airflow-pack.json",
        "sha256": "sha256:" + "c" * 64,
    }
    if declared_bytes is not None:
        workload_pack["bytes"] = declared_bytes

    with pytest.raises(ValueError, match="bytes"):
        build_init_fetch_plan(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            artifact_registry_ref="dpone-prod-artifacts",
            workload_packs=[workload_pack],
        )


def test_init_fetch_plan_is_digest_only_and_secret_free() -> None:
    from dpone.runtime.artifact_delivery import build_init_fetch_plan

    plan = build_init_fetch_plan(
        release_id=_INIT_FETCH_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        workload_packs=[
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": "sha256:" + "c" * 64,
                "bytes": 42,
            }
        ],
    )

    assert plan.to_dict() == {
        "mode": "init_fetch",
        "release_id": _INIT_FETCH_RELEASE_ID,
        "deployment_id": "sha256:" + "b" * 64,
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
        "artifacts": [
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": "sha256:" + "c" * 64,
                "bytes": 42,
            }
        ],
    }


def test_init_fetch_executor_downloads_pinned_artifact_and_writes_manifest(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import InitFetchExecutor, LocalArtifactRegistry, build_init_fetch_plan

    content = b'{"id":"load_orders"}\n'
    checksum = "sha256:" + hashlib.sha256(content).hexdigest()
    artifact_path = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "load_orders.airflow-pack.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(content)
    plan = build_init_fetch_plan(
        release_id=_INIT_FETCH_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        workload_packs=[
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": checksum,
                "bytes": len(content),
            }
        ],
        attestations="optional",
    )

    result = InitFetchExecutor(
        registry=LocalArtifactRegistry(tmp_path / "registry"),
        destination_root=tmp_path / "runtime-artifacts",
    ).execute(plan)

    assert result.passed is True
    assert result.release_id == plan.release_id
    assert result.deployment_id == plan.deployment_id
    assert result.artifacts[0].id == "load_orders"
    assert result.artifacts[0].path == (
        f"$RUNTIME_ARTIFACT_ROOT/releases/{_INIT_FETCH_RELEASE_DIR}/packs/load_orders.airflow-pack.json"
    )
    assert (
        tmp_path
        / "runtime-artifacts"
        / "releases"
        / _INIT_FETCH_RELEASE_DIR
        / "packs"
        / "load_orders.airflow-pack.json"
    ).read_bytes() == content
    assert result.manifest_path == "$RUNTIME_ARTIFACT_ROOT/init-fetch-manifest.json"
    manifest = (tmp_path / "runtime-artifacts" / "init-fetch-manifest.json").read_text(encoding="utf-8")
    assert "dpone.init-fetch-result.v1" in manifest
    assert "secret" not in manifest.lower()
    assert str(tmp_path) not in manifest
    assert str(Path.home()) not in manifest


def test_init_fetch_executor_rejects_checksum_mismatch(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import (
        InitFetchError,
        InitFetchExecutor,
        LocalArtifactRegistry,
        build_init_fetch_plan,
    )

    artifact_path = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "load_orders.airflow-pack.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"tampered\n")
    plan = build_init_fetch_plan(
        release_id=_INIT_FETCH_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        workload_packs=[
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": "sha256:" + "c" * 64,
                "bytes": len(b"tampered\n"),
            }
        ],
        attestations="optional",
    )

    with pytest.raises(InitFetchError) as exc:
        InitFetchExecutor(
            registry=LocalArtifactRegistry(tmp_path / "registry"),
            destination_root=tmp_path / "runtime-artifacts",
        ).execute(plan)

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert not (tmp_path / "runtime-artifacts").exists()
    assert not (tmp_path / "runtime-artifacts" / "init-fetch-manifest.json").exists()


def test_init_fetch_executor_rejects_declared_size_mismatch_without_partial_result(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import (
        InitFetchError,
        InitFetchExecutor,
        LocalArtifactRegistry,
        build_init_fetch_plan,
    )

    content = b'{"id":"load_orders"}\n'
    checksum = "sha256:" + hashlib.sha256(content).hexdigest()
    artifact_path = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "load_orders.airflow-pack.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(content)
    plan = build_init_fetch_plan(
        release_id=_INIT_FETCH_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        workload_packs=[
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": checksum,
                "bytes": len(content) + 1,
            }
        ],
        attestations="optional",
    )

    with pytest.raises(InitFetchError) as exc:
        InitFetchExecutor(
            registry=LocalArtifactRegistry(tmp_path / "registry"),
            destination_root=tmp_path / "runtime-artifacts",
        ).execute(plan)

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH"
    assert not (tmp_path / "runtime-artifacts").exists()


def test_init_fetch_executor_repeated_fetch_keeps_verified_tree_identity(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import (
        InitFetchError,
        InitFetchExecutor,
        LocalArtifactRegistry,
        build_init_fetch_plan,
    )

    content = b'{"id":"load_orders"}\n'
    checksum = "sha256:" + hashlib.sha256(content).hexdigest()
    artifact_path = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "load_orders.airflow-pack.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(content)
    plan = build_init_fetch_plan(
        release_id=_INIT_FETCH_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        artifact_registry_ref="dpone-prod-artifacts",
        workload_packs=[
            {
                "id": "load_orders",
                "artifact_ref": _INIT_FETCH_ARTIFACT_REF,
                "sha256": checksum,
                "bytes": len(content),
            }
        ],
        attestations="optional",
    )
    destination = tmp_path / "runtime-artifacts"
    executor = InitFetchExecutor(
        registry=LocalArtifactRegistry(tmp_path / "registry"),
        destination_root=destination,
    )

    executor.execute(plan)
    first_tree_identity = destination.stat().st_ino
    first_manifest = (destination / "init-fetch-manifest.json").read_bytes()
    executor.execute(plan)

    assert destination.stat().st_ino == first_tree_identity
    assert (destination / "init-fetch-manifest.json").read_bytes() == first_manifest
    downloaded_artifact = destination / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "load_orders.airflow-pack.json"
    assert downloaded_artifact.read_bytes() == content

    artifact_path.write_bytes(b"x" * len(content))
    with pytest.raises(InitFetchError) as exc:
        executor.execute(plan)

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert destination.stat().st_ino == first_tree_identity
    assert (destination / "init-fetch-manifest.json").read_bytes() == first_manifest
    assert downloaded_artifact.read_bytes() == content


def test_local_artifact_registry_rejects_path_escape(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import InitFetchError, LocalArtifactRegistry

    with pytest.raises(InitFetchError) as exc:
        LocalArtifactRegistry(tmp_path / "registry").read_bytes("cache://../outside.airflow-pack.json")

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"


def test_local_artifact_registry_reads_one_bounded_nofollow_descriptor_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.artifact_delivery as artifact_delivery

    content = b'{"id":"load_orders"}\n'
    artifact_path = tmp_path / "registry" / "releases" / "sha256-aaa" / "packs" / "load_orders.airflow-pack.json"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(content)
    real_open = artifact_delivery.os.open
    real_read = artifact_delivery.os.read
    artifact_descriptors: list[tuple[int, int]] = []
    requested_read_sizes: list[int] = []

    def tracked_open(path, flags, *args, **kwargs):
        descriptor = real_open(path, flags, *args, **kwargs)
        if path == artifact_path.name and kwargs.get("dir_fd") is not None:
            artifact_descriptors.append((descriptor, flags))
        return descriptor

    def tracked_read(descriptor: int, size: int) -> bytes:
        if artifact_descriptors and descriptor == artifact_descriptors[0][0]:
            requested_read_sizes.append(size)
        return real_read(descriptor, size)

    monkeypatch.setattr(artifact_delivery.os, "open", tracked_open)
    monkeypatch.setattr(artifact_delivery.os, "read", tracked_read)

    actual = artifact_delivery.LocalArtifactRegistry(tmp_path / "registry").read_bytes(
        "cache://releases/sha256-aaa/packs/load_orders.airflow-pack.json",
        declared_bytes=len(content),
    )

    assert actual == content
    assert len(artifact_descriptors) == 1
    assert artifact_descriptors[0][1] & os.O_NOFOLLOW
    assert requested_read_sizes
    assert max(requested_read_sizes) <= len(content) + 1


def test_local_artifact_registry_rejects_in_root_symlink(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import InitFetchError, LocalArtifactRegistry

    registry = tmp_path / "registry"
    target = registry / "releases" / "sha256-aaa" / "packs" / "target.airflow-pack.json"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"trusted\n")
    symlink = target.with_name("load_orders.airflow-pack.json")
    symlink.symlink_to(target.name)

    with pytest.raises(InitFetchError) as exc:
        LocalArtifactRegistry(registry).read_bytes(
            "cache://releases/sha256-aaa/packs/load_orders.airflow-pack.json",
            declared_bytes=len(b"trusted\n"),
        )

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"


def test_local_artifact_registry_rejects_oversized_artifact(tmp_path: Path) -> None:
    from dpone.runtime.artifact_delivery import InitFetchError, LocalArtifactRegistry

    artifact_path = tmp_path / "registry" / "releases" / "sha256-aaa" / "packs" / "load_orders.airflow-pack.json"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"x" * 11)

    with pytest.raises(InitFetchError) as exc:
        LocalArtifactRegistry(tmp_path / "registry", max_artifact_bytes=10).read_bytes(
            "cache://releases/sha256-aaa/packs/load_orders.airflow-pack.json"
        )

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_TOO_LARGE"
