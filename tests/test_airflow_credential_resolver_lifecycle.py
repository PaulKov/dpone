from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock

import pytest

from dpone.contracts.credential_resolution import CredentialResolutionError
from dpone.readiness.airflow_connection_credential_checks import validate_credentials
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver, VaultSecretSnapshot
from dpone.runtime.credentials.workload_scope import WorkloadScopedCredentialResolver
from dpone.services.mssql_clickhouse_safe_sample_execution import (
    CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor,
)
from dpone.services.safe_sample_policy import TemporaryTargetPlan
from dpone.services.safe_sample_target_lifecycle import (
    TemporaryTargetLifecycleError,
    TemporaryTargetLifecycleExecutor,
)


class _RotatingVault:
    def __init__(self, version: int = 17) -> None:
        self.version = version
        self.available = True
        self.calls = 0
        self._lock = Lock()

    def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
        with self._lock:
            self.calls += 1
            if not self.available:
                raise RuntimeError("vault path=restricted token=must-not-leak")
            version = self.version
        return {
            "username": f"runtime_v{version}",
            "password": f"password_v{version}",
            "_metadata": {"version": version},
        }


def _binding_set() -> dict[str, object]:
    return {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
    }


def _registry(
    *,
    version_policy: str = "latest",
    resolution_scope: str = "workload_start",
    kv_version: int = 2,
) -> dict[str, object]:
    return {
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
                    "kv_version": kv_version,
                    "path": "dpone/prod/credentials/pg_source",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": version_policy,
                    "resolution_scope": resolution_scope,
                },
            }
        },
    }


def _resolver(vault: object, **registry_options: object) -> BindingCredentialResolver:
    return BindingCredentialResolver(
        binding_set=_binding_set(),
        connection_registry=_registry(**registry_options),
        vault_kv_reader=vault,
        clock=lambda: datetime(2026, 7, 17, 12, 3, 11, tzinfo=UTC),
    )


def test_workload_scope_proves_rotation_outage_and_recovery() -> None:
    vault = _RotatingVault(version=17)

    workload_a = WorkloadScopedCredentialResolver(_resolver(vault))
    a_first = workload_a.resolve("pg_source")
    assert workload_a.resolve("pg_source") is a_first
    assert a_first.safe_metadata["resolved_version"] == 17
    assert vault.calls == 1

    vault.version = 18
    assert workload_a.resolve("pg_source") is a_first
    assert workload_a.resolve("pg_source").safe_metadata["resolved_version"] == 17

    workload_b = WorkloadScopedCredentialResolver(_resolver(vault))
    b_first = workload_b.resolve("pg_source")
    assert b_first.safe_metadata["resolved_version"] == 18
    assert vault.calls == 2

    vault.available = False
    workload_c = WorkloadScopedCredentialResolver(_resolver(vault))
    with pytest.raises(CredentialResolutionError) as exc_info:
        workload_c.resolve("pg_source")
    assert exc_info.value.code == "DPONE_CREDENTIAL_BACKEND_UNAVAILABLE"
    assert "restricted" not in str(exc_info.value)
    assert "must-not-leak" not in str(exc_info.value)
    assert vault.calls == 3

    assert workload_b.resolve("pg_source") is b_first
    assert vault.calls == 3

    vault.available = True
    vault.version = 19
    recovered = workload_c.resolve("pg_source")
    assert recovered.safe_metadata["resolved_version"] == 19
    assert vault.calls == 4


def test_workload_scope_serializes_concurrent_resolution() -> None:
    vault = _RotatingVault(version=23)
    resolver = WorkloadScopedCredentialResolver(_resolver(vault))

    with ThreadPoolExecutor(max_workers=16) as executor:
        resolved = list(executor.map(lambda _: resolver.resolve("pg_source"), range(32)))

    assert vault.calls == 1
    assert all(item is resolved[0] for item in resolved)
    assert resolved[0].safe_metadata["resolved_version"] == 23


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("port", "not-a-port", "connection port must be an integer"),
        ("port", True, "connection port must be an integer"),
        ("port", 0, "connection port must be an integer"),
        ("port", 65536, "connection port must be an integer"),
        ("secure", "tru", "connection secure must be a JSON boolean"),
        ("secure", 1, "connection secure must be a JSON boolean"),
    ],
)
def test_binding_resolver_rejects_mistyped_endpoint_metadata(
    field: str,
    value: object,
    message: str,
) -> None:
    vault = _RotatingVault()
    registry = _registry()
    connection = registry["connections"]["pg_prod"]["connection"]
    assert isinstance(connection, dict)
    connection[field] = value
    resolver = BindingCredentialResolver(
        binding_set=_binding_set(),
        connection_registry=registry,
        vault_kv_reader=vault,
    )

    with pytest.raises(ValueError, match=message):
        resolver.resolve("pg_source")


@pytest.mark.parametrize(
    ("registry_options", "expected_code"),
    [
        (
            {"version_policy": "pinned"},
            "DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED",
        ),
        (
            {"resolution_scope": "dag_run_start"},
            "DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED",
        ),
    ],
)
def test_unimplemented_vault_policy_fails_before_backend_io(
    registry_options: dict[str, object],
    expected_code: str,
) -> None:
    vault = _RotatingVault()
    resolver = _resolver(vault, **registry_options)

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolver.resolve("pg_source")

    assert exc_info.value.code == expected_code
    assert exc_info.value.resolver == "vault_kv"
    assert vault.calls == 0


def test_vault_kv_v2_requires_positive_version_metadata() -> None:
    class MetadataStrippingVault:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, str]:
            return {"username": "runtime", "password": "must-not-leak"}

    with pytest.raises(CredentialResolutionError) as exc_info:
        _resolver(MetadataStrippingVault()).resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_VERSION_METADATA_MISSING"
    assert "must-not-leak" not in str(exc_info.value)


def test_vault_kv_v2_never_treats_secret_version_field_as_metadata() -> None:
    class AmbiguousVault:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {
                "username": "runtime",
                "password": "must-not-leak",
                "version": 99,
                "metadata": {"version": 98},
            }

    with pytest.raises(CredentialResolutionError) as exc_info:
        _resolver(AmbiguousVault()).resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_VERSION_METADATA_MISSING"


def test_atomic_vault_snapshot_supplies_version_without_data_envelope() -> None:
    class SnapshotVault:
        def get_secret(self, *, mount_point: str, path: str) -> VaultSecretSnapshot:
            return VaultSecretSnapshot(
                data={"username": "runtime", "password": "must-not-leak"},
                version=31,
            )

    resolved = _resolver(SnapshotVault()).resolve("pg_source")

    assert resolved.safe_metadata["resolved_version"] == 31
    assert resolved.credentials.username == "runtime"
    assert "must-not-leak" not in repr(resolved.safe_metadata)


def test_vault_kv_v1_allows_unversioned_legacy_resolution() -> None:
    class VaultV1:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, str]:
            return {"username": "runtime", "password": "must-not-leak"}

    resolved = _resolver(VaultV1(), kv_version=1).resolve("pg_source")

    assert resolved.credentials.username == "runtime"
    assert resolved.safe_metadata["resolved_version"] is None
    assert "must-not-leak" not in repr(resolved.safe_metadata)


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_declared_vault_fields_are_required_and_secret_free(missing_value: object) -> None:
    class IncompleteVault:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {
                "username": "runtime",
                "password": missing_value,
                "other_secret": "must-not-leak",
                "_metadata": {"version": 7},
            }

    with pytest.raises(CredentialResolutionError) as exc_info:
        _resolver(IncompleteVault()).resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_FIELD_MISSING"
    assert "password" not in str(exc_info.value).lower()
    assert "must-not-leak" not in str(exc_info.value)


def test_vault_backend_exception_is_replaced_by_safe_error() -> None:
    class UnsafeVault:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            raise RuntimeError("path=dpone/prod/secret password=must-not-leak")

    with pytest.raises(CredentialResolutionError) as exc_info:
        _resolver(UnsafeVault()).resolve("pg_source")

    error = exc_info.value
    assert error.code == "DPONE_CREDENTIAL_BACKEND_UNAVAILABLE"
    assert error.resolver == "vault_kv"
    assert error.__cause__ is None
    assert "dpone/prod" not in str(error)
    assert "must-not-leak" not in str(error)


def test_empty_env_var_mapping_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_TEST_USERNAME", "   ")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "development",
            "bindings": {"pg_source": {"connection_ref": "pg_dev"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_dev": {
                    "type": "postgres",
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"username": "DPONE_TEST_USERNAME"},
                    },
                }
            },
        },
    )

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolver.resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_FIELD_MISSING"


def test_empty_projected_secret_field_fails_closed(tmp_path: Path) -> None:
    secret_dir = tmp_path / "pg-source"
    secret_dir.mkdir()
    (secret_dir / "username").write_text("\n", encoding="utf-8")
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "credentials": {
                        "resolver": "kubernetes_secret_volume",
                        "secret_name": "pg-source",
                        "mount_path": secret_dir.as_posix(),
                        "fields": {"username": "username"},
                    },
                }
            },
        },
    )

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolver.resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_FIELD_MISSING"


def test_missing_kubernetes_api_secret_field_fails_closed() -> None:
    class KubernetesReader:
        def read_secret(self, *, namespace: str, name: str) -> dict[str, str]:
            return {"other": "must-not-leak"}

    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-source",
                        "fields": {"username": "username"},
                    },
                }
            },
        },
        kubernetes_secret_reader=KubernetesReader(),
    )

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolver.resolve("pg_source")

    assert exc_info.value.code == "DPONE_CREDENTIAL_FIELD_MISSING"
    assert "must-not-leak" not in str(exc_info.value)


def test_connection_readiness_reports_unimplemented_policy_without_backend_io(tmp_path: Path) -> None:
    credentials = _registry(
        version_policy="pinned",
        resolution_scope="dag_run_start",
    )["connections"]["pg_prod"]["credentials"]

    errors = validate_credentials("pg_prod", credentials, "prod", tmp_path / "registry.yaml")

    assert {error["code"] for error in errors} == {
        "DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED",
        "DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED",
    }
    assert all(error["stage"] == "check_connections" for error in errors)
    assert {error["fixes"][0]["id"] for error in errors} == {
        "use_latest_version_policy",
        "use_workload_start_resolution_scope",
    }


def test_safe_sample_copy_preserves_typed_credential_error() -> None:
    class FailingResolver:
        def resolve(self, connection_ref: str) -> object:
            raise CredentialResolutionError(
                "DPONE_CREDENTIAL_BACKEND_UNAVAILABLE",
                "Credential backend is unavailable; restore it and retry the workload.",
                resolver="vault_kv",
            )

    class UnusedFactory:
        def create(self, credentials: object) -> object:
            raise AssertionError("factory must not run after credential failure")

    executor = CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor(
        credential_resolver=FailingResolver(),
        reader_factory=UnusedFactory(),
        writer_factory=UnusedFactory(),
    )

    result = executor.copy(
        {
            "source": {"connection_ref": "pg_source"},
            "sink": {"connection_ref": "clickhouse_sink"},
        }
    )

    assert result["errors"][0]["code"] == "DPONE_CREDENTIAL_BACKEND_UNAVAILABLE"


def test_temporary_target_lifecycle_preserves_typed_credential_error() -> None:
    class FailingAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            raise CredentialResolutionError(
                "DPONE_CREDENTIAL_VERSION_METADATA_MISSING",
                "Vault KV v2 response has no positive version metadata.",
                resolver="vault_kv",
            )

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {}

    plan = TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders",
        sink_type="clickhouse",
        connection_ref="clickhouse_sink",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp", "name": "orders_sample"},
        ttl_seconds=3600,
        cleanup_required=True,
        pii_policy="masked",
    )

    with pytest.raises(TemporaryTargetLifecycleError) as exc_info:
        TemporaryTargetLifecycleExecutor(adapter=FailingAdapter()).prepare(plan)

    assert exc_info.value.code == "DPONE_CREDENTIAL_VERSION_METADATA_MISSING"
