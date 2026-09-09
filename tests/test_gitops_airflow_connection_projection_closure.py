from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.gitops.airflow_compact_pack import (
    AirflowCompactPackBuilder,
    AirflowCompactPackBuildError,
    GitOpsAirflowCompactPackReport,
)
from dpone.gitops.airflow_compact_process_plans import CompactConnectionProjectionError
from dpone.gitops.airflow_connection_projection_closure import (
    AirflowConnectionProjectionClosureError,
    close_connection_projection,
    required_runtime_connection_refs,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogReport, GitOpsWorkloadDefinition
from dpone.services.gitops.airflow_compact_pack_reconcile_build import build_reconcile_artifacts


def _entry(logical_ref: str, physical_id: str) -> dict[str, object]:
    return {
        "connection_ref": logical_ref,
        "registry_connection_ref": logical_ref,
        "connection_id": physical_id,
        "secret_key": "AIRFLOW_CONN_" + physical_id.upper(),
        "mount_path": f"/run/secrets/dpone/airflow-connections/{logical_ref}",
        "fields": {"uri": "uri"},
    }


def _projection() -> dict[str, object]:
    return {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "connection_ids": ["pg_orders", "mssql_shared", "pg_customers"],
        "connections": [
            _entry("orders_source", "pg_orders"),
            _entry("orders_target", "mssql_shared"),
            _entry("orders_state", "mssql_shared"),
            _entry("customers_source", "pg_customers"),
            _entry("customers_target", "mssql_shared"),
            _entry("customers_state", "mssql_shared"),
        ],
        "scheme_overrides": {
            "orders_source": "postgresql",
            "orders_target": "mssql",
            "orders_state": "mssql",
            "customers_source": "postgresql",
            "customers_target": "mssql",
            "customers_state": "mssql",
            "unrelated": "clickhouse",
        },
        "database_overrides": {
            "mssql_shared": "DWH_USERS",
            "pg_customers": "customers",
            "unrelated": "raw",
        },
        "query_overrides": {
            "mssql_shared": {"multi_subnet_failover": "yes"},
            "orders_target": {"trust_server_certificate": "yes"},
            "orders_state": {"trust_server_certificate": "yes"},
            "customers_target": {"connect_timeout": "7"},
            "customers_state": {"connect_timeout": "7"},
            "unrelated": {"secure": "true"},
        },
    }


def test_required_runtime_connection_refs_follow_runtime_authority_paths() -> None:
    processes = (
        SimpleNamespace(
            raw_config={
                "source": {
                    "connection_ref": "source",
                    "options": {
                        "native_transfer": {
                            "snapshot": {
                                "materialization": {
                                    "work_connection_ref": "snapshot_work",
                                }
                            }
                        }
                    },
                },
                "sink": {"connection_ref": "sink"},
                "state": {"connection_ref": "state"},
                "bigquery_proxy": {"connection_ref": "proxy"},
                "object_storage": {
                    "runtime_access": {"connection_ref": "runtime_store"},
                    "clickhouse_write_access": {"connection_ref": "clickhouse_store"},
                    "unused": {"connection_ref": "not_a_runtime_authority"},
                },
            }
        ),
        SimpleNamespace(
            raw_config={
                "source": {"connection_ref": "source"},
                "sink": {"connection_ref": "sink"},
                "state": {"reuse": "sink"},
            }
        ),
        SimpleNamespace(raw_config={"state": {"type": "disabled", "connection_ref": "disabled_state"}}),
    )

    assert required_runtime_connection_refs(processes) == (
        "clickhouse_store",
        "proxy",
        "runtime_store",
        "sink",
        "snapshot_work",
        "source",
        "state",
    )


def test_closure_isolates_disjoint_workload_and_preserves_shared_physical_connection() -> None:
    closed = close_connection_projection(
        _projection(),
        required_refs=("orders_source", "orders_target", "orders_state"),
    )

    assert [entry["connection_ref"] for entry in closed["connections"]] == [
        "orders_source",
        "orders_target",
        "orders_state",
    ]
    assert [entry["connection_id"] for entry in closed["connections"]] == [
        "pg_orders",
        "mssql_shared",
        "mssql_shared",
    ]
    assert closed["connection_ids"] == ["pg_orders", "mssql_shared"]
    assert closed["scheme_overrides"] == {
        "pg_orders": "postgresql",
        "mssql_shared": "mssql",
    }
    assert closed["database_overrides"] == {"mssql_shared": "DWH_USERS"}
    assert closed["query_overrides"] == {
        "mssql_shared": {
            "multi_subnet_failover": "yes",
            "trust_server_certificate": "yes",
        }
    }
    assert "customers_source" not in repr(closed)
    assert "unrelated" not in repr(closed)


def test_closure_uses_the_other_disjoint_workload_aliases_only() -> None:
    closed = close_connection_projection(
        _projection(),
        required_refs=("customers_source", "customers_target", "customers_state"),
    )

    assert [entry["connection_ref"] for entry in closed["connections"]] == [
        "customers_source",
        "customers_target",
        "customers_state",
    ]
    assert closed["connection_ids"] == ["mssql_shared", "pg_customers"]
    assert closed["scheme_overrides"] == {
        "mssql_shared": "mssql",
        "pg_customers": "postgresql",
    }
    assert closed["database_overrides"] == {
        "mssql_shared": "DWH_USERS",
        "pg_customers": "customers",
    }
    assert closed["query_overrides"] == {
        "mssql_shared": {
            "connect_timeout": "7",
            "multi_subnet_failover": "yes",
        }
    }
    assert "orders_source" not in repr(closed)


def test_connection_free_workload_closes_projection_to_empty_mapping() -> None:
    assert close_connection_projection(_projection(), required_refs=()) == {}


def test_closure_fails_when_required_logical_ref_is_absent() -> None:
    with pytest.raises(AirflowConnectionProjectionClosureError) as exc_info:
        close_connection_projection(
            _projection(),
            required_refs=("orders_source", "missing_target"),
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING"
    assert "missing_target" in str(exc_info.value)


def test_closure_fails_on_conflicting_alias_overrides_for_shared_physical_connection() -> None:
    projection = _projection()
    projection["database_overrides"] = {
        "orders_target": "DWH_A",
        "orders_state": "DWH_B",
    }

    with pytest.raises(AirflowConnectionProjectionClosureError) as exc_info:
        close_connection_projection(
            projection,
            required_refs=("orders_target", "orders_state"),
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_OVERRIDE_CONFLICT"


def test_sample_metrics_pack_contains_exact_refs_and_excludes_crm_source(tmp_path: Path) -> None:
    manifest = tmp_path / "sample_metrics_metrics_value.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "name": "sample_metrics_metrics_value",
                "source": {
                    "type": "postgres",
                    "connection_ref": "postgres_sample_metrics_source",
                    "table": {"schema": "public", "name": "metrics_value"},
                    "options": {"incremental_strategy": "xmin"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_sample_metrics_target",
                    "table": {"schema": "sample_metrics", "name": "metrics_value"},
                    "strategy": {
                        "mode": "incremental_merge",
                        "unique_key": ["guid"],
                        "merge_policy": "update_insert",
                    },
                },
                "state": {
                    "type": "mssql",
                    "connection_ref": "mssql_sample_metrics_state",
                    "atomicity": "target_atomic",
                    "provisioning": "external",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": [
            _entry("postgres_sample_metrics_source", "postgres_sample_metrics_source"),
            _entry("postgres_crm_archive_source", "pg_crm_archive"),
            _entry("mssql_sample_metrics_target", "mssql_example"),
            _entry("mssql_sample_metrics_state", "mssql_example"),
        ],
        "scheme_overrides": {
            "postgres_sample_metrics_source": "postgresql",
            "postgres_crm_archive_source": "postgresql",
            "mssql_sample_metrics_target": "mssql",
            "mssql_sample_metrics_state": "mssql",
        },
        "query_overrides": {
            "mssql_example": {"trust_server_certificate": "yes"},
        },
    }
    workload = GitOpsWorkloadDefinition(
        workload_id="sample_metrics_metrics_value",
        manifest=manifest.name,
        domain="platform",
        catalog_path="domains/platform.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {"connection_projection": projection},
        },
        provenance={},
    )
    pack = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/sample_metrics_metrics_value/airflow-pack.json",
        repo_root=tmp_path,
    )

    assert [entry["connection_ref"] for entry in pack.connection_projection["connections"]] == [
        "postgres_sample_metrics_source",
        "mssql_sample_metrics_target",
        "mssql_sample_metrics_state",
    ]
    assert pack.connection_projection["connection_ids"] == [
        "postgres_sample_metrics_source",
        "mssql_example",
    ]
    assert "pg_crm_archive" not in repr(pack.connection_projection)


def _missing_ref_workload(tmp_path: Path) -> GitOpsWorkloadDefinition:
    manifest = tmp_path / "orders.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "name": "orders",
                "source": {
                    "type": "postgres",
                    "connection_ref": "orders_source",
                    "table": {"schema": "public", "name": "orders"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_ref": "orders_target",
                    "table": {"schema": "dwh", "name": "orders"},
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest.name,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {
                "connection_projection": {
                    **_projection(),
                    "connections": [_entry("orders_source", "pg_orders")],
                }
            },
        },
        provenance={},
    )


def test_compact_pack_fails_closed_when_required_logical_ref_is_missing(tmp_path: Path) -> None:
    workload = _missing_ref_workload(tmp_path)

    with pytest.raises(AirflowCompactPackBuildError) as exc_info:
        AirflowCompactPackBuilder().build(
            workload=workload,
            output_path="packs/orders/airflow-pack.json",
            repo_root=tmp_path,
        )

    build_cause = exc_info.value.__cause__
    assert isinstance(build_cause, CompactConnectionProjectionError)
    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING"
    assert build_cause.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING"
    closure_cause = build_cause.__cause__
    assert isinstance(closure_cause, AirflowConnectionProjectionClosureError)
    assert closure_cause.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING"


def test_reconcile_maps_missing_projection_ref_to_stable_build_blocker(tmp_path: Path) -> None:
    workload = _missing_ref_workload(tmp_path)
    catalog = GitOpsWorkloadCatalogReport(
        workload_set="workloads.yaml",
        env="prod",
        workloads=(workload,),
    )

    def build_pack(**_kwargs: object) -> GitOpsAirflowCompactPackReport:
        return AirflowCompactPackBuilder().build(
            workload=workload,
            output_path="packs/orders/airflow-pack.json",
            repo_root=tmp_path,
        )

    result = build_reconcile_artifacts(
        catalog=catalog,
        selected_ids=(workload.workload_id,),
        mssql_registry=SimpleNamespace(verify_unchanged=lambda _repo_root: None),
        dag_spec_builder=SimpleNamespace(build=lambda **_kwargs: SimpleNamespace(blockers=())),
        build_pack=build_pack,
        repo_root=tmp_path,
    )

    assert result.pack_reports == ()
    payload = result.blockers[0].to_jsonable()
    assert payload == {
        "code": "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING",
        "message": (
            "Airflow connection projection is missing required connection aliases: orders_target. "
            "Add a matching connection_projection.connections entry or correct the manifest authority alias."
        ),
        "path": workload.manifest,
        "source": "dpone gitops airflow reconcile",
    }
