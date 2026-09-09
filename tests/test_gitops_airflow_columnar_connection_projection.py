from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from dpone_airflow_pack.init_fetch_connection_bridge import require_closed_init_fetch_connection_bridge

from dpone.config import LoadConfig, LoadStrategy
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_connection_projection_closure import (
    AirflowConnectionProjectionClosureError,
    close_connection_projection,
    required_runtime_connection_refs,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.readiness.airflow_connection_runtime_registry import runtime_connection_registry_for_init_fetch
from dpone.runtime.columnar_runtime_assembly import ColumnarRuntimeAssembly


def _entry(logical_ref: str, registry_ref: str, physical_id: str) -> dict[str, object]:
    return {
        "connection_ref": logical_ref,
        "registry_connection_ref": registry_ref,
        "connection_id": physical_id,
        "secret_key": "AIRFLOW_CONN_" + physical_id.upper(),
        "mount_path": f"/run/secrets/dpone/airflow-connections/{logical_ref}",
        "fields": {"uri": "uri"},
    }


def _projection(*entries: dict[str, object]) -> dict[str, object]:
    return {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "connections": list(entries),
    }


@pytest.mark.parametrize("read_mode", ["connection", "presigned_url"])
def test_collector_follows_active_columnar_runtime_resolver_alias(read_mode: str) -> None:
    processes = (
        SimpleNamespace(
            raw_config={
                "source": {
                    "connection_ref": "source",
                    "options": {
                        "native_transfer": {
                            "snapshot": {
                                "columnar_fast_path": {
                                    "object_storage": {
                                        "runtime_access": {
                                            "connection_id": "s3_runtime_id",
                                            "connection_ref": "ignored_runtime_ref",
                                        },
                                        "clickhouse_read_access": {
                                            "mode": read_mode,
                                            "connection_id": "clickhouse_read_only",
                                        },
                                    }
                                }
                            }
                        },
                        "columnar_fast_path": {
                            "object_storage": {
                                "runtime_access": {"connection_ref": "ignored_fallback"},
                            }
                        },
                    },
                },
                "sink": {"connection_ref": "sink"},
            }
        ),
        SimpleNamespace(
            raw_config={
                "source": {
                    "options": {
                        "columnar_fast_path": {
                            "object_storage": {
                                "runtime_access": {"connection_ref": "s3_fallback_ref"},
                            }
                        }
                    }
                }
            }
        ),
    )

    assert required_runtime_connection_refs(processes) == (
        "s3_fallback_ref",
        "s3_runtime_id",
        "sink",
        "source",
    )


@pytest.mark.parametrize("mode", ["off", "benchmark_only"])
def test_collector_skips_inactive_columnar_runtime_access(mode: str) -> None:
    process = SimpleNamespace(
        raw_config={
            "source": {
                "options": {
                    "native_transfer": {
                        "snapshot": {
                            "columnar_fast_path": {
                                "mode": mode,
                                "object_storage": {
                                    "runtime_access": {"connection_id": "unused_runtime_alias"},
                                },
                            }
                        }
                    }
                }
            }
        }
    )

    assert required_runtime_connection_refs((process,)) == ()


def test_closure_resolves_registry_alias_without_shared_physical_leakage() -> None:
    projection = _projection(
        _entry("orders_runtime", "orders_registry", "aws_shared"),
        _entry("customers_runtime", "customers_registry", "aws_shared"),
    )

    closed = close_connection_projection(projection, required_refs=("orders_registry",))

    assert [entry["connection_ref"] for entry in closed["connections"]] == ["orders_runtime"]
    assert "customers_registry" not in repr(closed)


def test_closure_fails_when_only_shared_physical_alias_matches() -> None:
    projection = _projection(
        _entry("orders_runtime", "orders_registry", "aws_shared"),
        _entry("customers_runtime", "customers_registry", "aws_shared"),
    )

    with pytest.raises(AirflowConnectionProjectionClosureError) as exc_info:
        close_connection_projection(projection, required_refs=("aws_shared",))

    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_AMBIGUOUS"
    assert "aws_shared" in str(exc_info.value)


def test_closure_resolves_unique_physical_alias() -> None:
    projection = _projection(
        _entry("orders_runtime", "orders_registry", "orders_aws"),
        _entry("customers_runtime", "customers_registry", "customers_aws"),
    )

    closed = close_connection_projection(projection, required_refs=("orders_aws",))

    assert [entry["connection_ref"] for entry in closed["connections"]] == ["orders_runtime"]


def test_exact_logical_alias_wins_before_shared_physical_alias() -> None:
    projection = _projection(
        _entry("aws_shared", "orders_registry", "aws_shared"),
        _entry("customers_runtime", "customers_registry", "aws_shared"),
    )

    closed = close_connection_projection(projection, required_refs=("aws_shared",))

    assert [entry["connection_ref"] for entry in closed["connections"]] == ["aws_shared"]


def test_missing_alias_error_redacts_non_logical_secret_payload() -> None:
    with pytest.raises(AirflowConnectionProjectionClosureError) as exc_info:
        close_connection_projection(
            _projection(),
            required_refs=('{"password":"must-not-leak"}',),
        )

    assert "must-not-leak" not in str(exc_info.value)
    assert "<redacted-invalid-alias>" in str(exc_info.value)


def test_batch_defaults_pack_projects_nested_runtime_secret_and_registry_mount(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.batch.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.batch.v1",
                "naming": {"process_name": "{{ src_schema }}__{{ src_table }}"},
                "defaults": {
                    "source": {
                        "type": "mssql",
                        "connection_ref": "orders_source",
                        "options": {
                            "native_transfer": {
                                "snapshot": {
                                    "columnar_fast_path": {
                                        "mode": "required",
                                        "object_storage": {
                                            "uri_prefix": "s3://stage/orders/{run_id}/",
                                            "runtime_access": {
                                                "connection_type": "airflow",
                                                "connection_id": "orders_stage_registry",
                                            },
                                            "clickhouse_read_access": {
                                                "mode": "connection",
                                                "connection_id": "clickhouse_read_only",
                                            },
                                        },
                                    }
                                }
                            }
                        },
                    },
                    "sink": {
                        "type": "clickhouse",
                        "connection_ref": "orders_sink",
                        "table": {"schema": "raw", "name": "{{ src_table }}"},
                        "strategy": {"mode": "full_refresh"},
                    },
                },
                "schemas": {"dbo": {"tables": ["orders"]}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    projection = _projection(
        _entry("orders_source", "orders_source", "mssql_source"),
        _entry("orders_sink", "orders_sink", "clickhouse_sink"),
        _entry("orders_stage", "orders_stage_registry", "aws_shared"),
        _entry("customers_stage", "customers_stage_registry", "aws_shared"),
    )
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest.name,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {"connection_projection": projection},
        },
        provenance={},
    )

    pack = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/orders/airflow-pack.json",
        repo_root=tmp_path,
    )
    closed = require_closed_init_fetch_connection_bridge(pack.connection_projection)
    entries = {entry["registry_connection_ref"]: entry for entry in closed["connections"]}
    registry = runtime_connection_registry_for_init_fetch(
        {
            "schema": "dpone.connection-registry.v1",
            "connections": {
                ref: {
                    "type": "s3",
                    "connection": {"endpoint": "https://storage.example"},
                    "credentials": {
                        "resolver": "airflow_connection",
                        "connection_id": "aws_shared",
                        "execution_mode": "operator_bridge",
                    },
                }
                for ref in ("orders_stage_registry", "customers_stage_registry")
            },
        },
        projection=closed,
    )

    assert set(entries) == {"orders_source", "orders_sink", "orders_stage_registry"}
    assert entries["orders_stage_registry"]["secret_key"] == "AIRFLOW_CONN_AWS_SHARED"
    runtime_credentials = registry["connections"]["orders_stage_registry"]["credentials"]
    assert runtime_credentials["resolver"] == "kubernetes_secret_volume"
    assert runtime_credentials["mount_path"] == entries["orders_stage_registry"]["mount_path"]
    assert registry["connections"]["customers_stage_registry"]["credentials"]["resolver"] == "airflow_connection"


@pytest.mark.parametrize("read_mode", ["connection", "presigned_url"])
def test_columnar_runtime_resolves_only_runtime_access_alias(read_mode: str) -> None:
    resolver = _RecordingObjectStorageResolver()
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "source_options": {
                "native_transfer": {
                    "snapshot": {
                        "columnar_fast_path": {
                            "mode": "required",
                            "object_storage": {
                                "uri_prefix": "s3://stage/orders/{run_id}/",
                                "runtime_access": {
                                    "connection_id": "runtime_resolver_alias",
                                    "connection_ref": "ignored_runtime_alias",
                                },
                                "clickhouse_read_access": {
                                    "mode": read_mode,
                                    "connection_id": "clickhouse_render_only_alias",
                                },
                            },
                        }
                    }
                }
            }
        },
    )

    runtime = ColumnarRuntimeAssembly(object_storage_resolver=resolver).build(
        load_config=load_config,
        source=SimpleNamespace(connector=None),
        sink=SimpleNamespace(connector=None),
    )

    assert runtime is not None
    assert resolver.connection_ids == ["runtime_resolver_alias"]


class _RecordingObjectStorageResolver:
    def __init__(self) -> None:
        self.connection_ids: list[str] = []

    def build_client(self, *, ref: object, uri: object) -> object:
        del uri
        self.connection_ids.append(str(getattr(ref, "connection_id")))
        return object()
