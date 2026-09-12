"""MSSQL AIP-60 asset URI contract: authority, canonical form, fail-closed."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dpone.gitops.airflow_asset_uri import (
    MSSQL_ASSET_URI_INVALID,
    MssqlAssetAuthority,
    canonical_table_uri,
    canonicalize_declared_mssql_uri,
    canonicalize_table_uri,
    is_aip60_mssql_asset_uri,
    load_mssql_asset_authority_index,
    load_mssql_default_database_index,
)


def test_mssql_canonical_uri_uses_authority_port_and_encoding() -> None:
    uri = canonical_table_uri(
        "mssql",
        "assortment planning",
        "supply/forecast",
        database="dwh_example",
        authority=MssqlAssetAuthority(host="fixture01.invalid", port=1433),
    )
    assert uri == ("mssql://fixture01.invalid:1433/dwh_example/assortment%20planning/supply%2Fforecast")
    assert is_aip60_mssql_asset_uri(uri)


def test_mssql_named_instance_canonical_uri() -> None:
    uri = canonical_table_uri(
        "mssql",
        "dbo",
        "orders",
        database="DWH",
        authority=MssqlAssetAuthority(host="fixture01.invalid", port=1433, instance="MSSQL01"),
    )
    assert uri == "mssql://fixture01.invalid:1433/mssql01/DWH/dbo/orders"
    assert is_aip60_mssql_asset_uri(uri)


@pytest.mark.parametrize(
    "uri",
    [
        "mssql://assortment_planning/example_forecast",
        "mssql://dwh_example/assortment_planning/example_forecast",
        "mssql:///dwh_example/assortment_planning/example_forecast",
    ],
)
def test_compact_mssql_uris_are_not_aip60(uri: str) -> None:
    assert not is_aip60_mssql_asset_uri(uri)
    assert canonicalize_declared_mssql_uri(uri).uri is None


def test_compact_declared_uri_is_invalid() -> None:
    resolution = canonicalize_declared_mssql_uri("mssql://DWH/dbo/orders")
    assert resolution.uri is None
    assert resolution.issues
    assert resolution.issues[0].code == MSSQL_ASSET_URI_INVALID


def test_declared_uri_normalizes_default_port() -> None:
    # Host that looks like a server name is accepted; missing port is filled.
    # connection_ref-shaped hosts are discouraged by registry policy, not by URL syntax.
    resolution = canonicalize_declared_mssql_uri("mssql://fixture01.invalid/dwh_example/dbo/orders")
    assert resolution.ok
    assert resolution.uri == "mssql://fixture01.invalid:1433/dwh_example/dbo/orders"


def test_mssql_canonical_uri_requires_authority_and_database() -> None:
    missing_authority = canonicalize_table_uri(
        "mssql",
        "assortment_planning",
        "example_forecast",
        database="dwh_example",
    )
    assert missing_authority.uri is None
    assert missing_authority.issues[0].code == MSSQL_ASSET_URI_INVALID

    missing_database = canonicalize_table_uri(
        "mssql",
        "assortment_planning",
        "example_forecast",
        authority=MssqlAssetAuthority(host="fixture01.invalid"),
    )
    assert missing_database.uri is None
    assert missing_database.issues[0].code == MSSQL_ASSET_URI_INVALID


def test_non_mssql_engines_keep_historical_shape() -> None:
    assert (
        canonical_table_uri("clickhouse", "Example_Datamarts", "example_customer_mart")
        == "clickhouse://Example_Datamarts/example_customer_mart"
    )
    assert (
        canonical_table_uri(
            "postgres",
            "dst",
            "app",
            database="reporting",
        )
        == "postgres://reporting/dst/app"
    )


@pytest.mark.parametrize(
    "endpoint_type",
    ("mssql", "MSSQL", "microsoft mssql", "microsoft_mssql", "odbc", "sqlserver", "sql_server", "sql-server"),
)
def test_registry_asset_authority_and_database_indexes_accept_aliases(endpoint_type: str) -> None:
    registry = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "mssql_example": {
                "type": endpoint_type,
                "connection": {
                    "asset_authority": {
                        "host": "fixture01.invalid",
                        "port": 1433,
                    },
                    "database": "DWH",
                },
                "credentials": {"resolver": "airflow_connection", "connection_id": "x"},
            },
            "ClickHouse": {"type": "clickhouse", "credentials": {"resolver": "airflow_connection"}},
        },
    }

    assert load_mssql_asset_authority_index(registry) == {
        "mssql_example": MssqlAssetAuthority(host="fixture01.invalid", port=1433)
    }
    assert load_mssql_default_database_index(registry) == {"mssql_example": "DWH"}


def _write_registry(
    tmp_path: Path,
    *,
    env: str = "dev",
    host: str = "sql-prod.internal",
    port: int = 1433,
    instance: str | None = None,
    database: str | None = None,
) -> None:
    path = tmp_path / ".dpone" / "registry" / "connection-registries" / f"{env}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    authority: dict[str, object] = {"host": host, "port": port}
    if instance is not None:
        authority["instance"] = instance
    connection: dict[str, object] = {"asset_authority": authority}
    if database is not None:
        connection["database"] = database
    path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": env,
                "connections": {
                    "mssql_example": {
                        "type": "mssql",
                        "connection": connection,
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "mssql_example",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_incomplete_mssql_sink_emits_blocker(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path)
    path = tmp_path / MANIFEST_DIR / "incomplete_mssql.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "incomplete_mssql",
                "source": {
                    "type": "postgres",
                    "connection_id": "pg_src",
                    "table": {"schema": "public", "name": "orders"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_example",
                    "table": {
                        "schema": "assortment_planning",
                        "name": "example_forecast",
                    },
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"incomplete_mssql": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert len(report.nodes) == 1
    assert report.nodes[0].sink_uris == frozenset()
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)


def test_missing_asset_authority_emits_blocker(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    path = tmp_path / MANIFEST_DIR / "no_authority.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "no_authority",
                "source": {
                    "type": "clickhouse",
                    "connection_ref": "ClickHouse",
                    "table": {"schema": "src", "name": "t"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_example",
                    "table": {
                        "database": "dwh_example",
                        "schema": "assortment_planning",
                        "name": "example_forecast",
                    },
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"no_authority": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)
    assert report.nodes[0].sink_uris == frozenset()
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)
    assert any("asset_authority" in blocker.message for blocker in report.blockers)


def test_asset_graph_infers_aip60_mssql_sink_outlet(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path)
    path = tmp_path / MANIFEST_DIR / "supply_forecast.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "supply_forecast",
                "source": {
                    "type": "clickhouse",
                    "connection_ref": "ClickHouse",
                    "table": {
                        "schema": "assortment_planning_playground",
                        "name": "example_forecast",
                    },
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_example",
                    "table": {
                        "database": "dwh_example",
                        "schema": "assortment_planning",
                        "name": "example_forecast",
                    },
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"assortment_planning__example_forecast": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert len(report.nodes) == 1
    assert report.nodes[0].sink_uris == frozenset(
        {"mssql://sql-prod.internal:1433/dwh_example/assortment_planning/example_forecast"}
    )
    assert report.blockers == ()


@pytest.mark.parametrize(
    "sink_type",
    ("mssql", "MSSQL", "microsoft mssql", "microsoft_mssql", "odbc", "sqlserver", "sql_server", "sql-server"),
)
def test_load_config_database_fallback(tmp_path: Path, sink_type: str) -> None:
    from dpone.gitops.airflow_asset_graph_lineage import lineage_uri
    from dpone.gitops.airflow_asset_uri import MssqlAssetAuthority
    from dpone.manifest.models import ProcessSpec

    authority = {"mssql_a": MssqlAssetAuthority(host="sql.example", port=1433)}

    class _LoadConfig:
        source_schema = None
        source_table = None
        target_schema = "dbo"
        target_table = "orders"
        source_database = None
        target_database = "Sales"

    class _Config:
        load_config = _LoadConfig()

    process = ProcessSpec(
        name="p",
        config_path=tmp_path / "pipeline.yaml",
        config=_Config(),
        raw_config={
            "sink": {
                "type": sink_type,
                "connection_ref": "mssql_a",
                "table": {"schema": "dbo", "name": "orders"},
            }
        },
    )
    resolution = lineage_uri(
        process.raw_config["sink"],
        process,
        role="sink",
        authority_index=authority,
        path="w",
    )
    assert resolution is not None
    assert resolution.uri == "mssql://sql.example:1433/Sales/dbo/orders"


def test_explicit_compact_outlet_is_blocker(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path)
    path = tmp_path / MANIFEST_DIR / "compact_outlet.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "compact_outlet",
                "source": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t2"},
                    "mode": "append",
                },
                "gitops": {
                    "airflow": {
                        "execution": {
                            "outlets": ["mssql://DWH/dbo/orders"],
                        }
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"compact_outlet": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)


def test_stage_connection_registries_copies_env_bound_files(tmp_path: Path) -> None:
    from dpone.gitops.airflow_mssql_registry_staging import stage_connection_registries

    source = tmp_path / "project"
    build = tmp_path / "build"
    registry = source / "platform" / "connection-registries" / "dev.yaml"
    registry.parent.mkdir(parents=True)
    registry.write_text("schema: dpone.connection-registry.v1\nenvironment: dev\nconnections: {}\n", encoding="utf-8")
    (source / "platform" / "connection-registries" / "notes.txt").write_text("ignore\n", encoding="utf-8")

    staged = stage_connection_registries(source, build)

    assert staged == (build / "platform" / "connection-registries" / "dev.yaml",)
    assert staged[0].read_text(encoding="utf-8") == registry.read_text(encoding="utf-8")
    assert not (build / "platform" / "connection-registries" / "notes.txt").exists()


def test_env_bound_registry_uses_prod_not_dev(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path, env="dev", host="sql-dev.internal", port=1433)
    _write_registry(tmp_path, env="prod", host="sql-prod.internal", port=1433)
    path = tmp_path / MANIFEST_DIR / "env_bound.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "env_bound",
                "source": {
                    "type": "clickhouse",
                    "connection_ref": "ClickHouse",
                    "table": {"schema": "src", "name": "t"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_example",
                    "table": {
                        "database": "dwh_example",
                        "schema": "dbo",
                        "name": "orders",
                    },
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(tmp_path, workloads={"env_bound": manifest_ref(path.relative_to(tmp_path).as_posix())})
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="prod",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path, env="prod")

    assert report.nodes[0].sink_uris == frozenset({"mssql://sql-prod.internal:1433/dwh_example/dbo/orders"})
    assert report.blockers == ()


def test_lineage_asset_authority_must_match_registry(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path, host="sql-prod.internal")
    path = tmp_path / MANIFEST_DIR / "authority_mismatch.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "authority_mismatch",
                "source": {
                    "type": "clickhouse",
                    "connection_ref": "ClickHouse",
                    "table": {"schema": "src", "name": "t"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_example",
                    "lineage": {
                        "asset_authority": {"host": "sql-other.internal", "port": 1433},
                    },
                    "table": {
                        "database": "dwh_example",
                        "schema": "dbo",
                        "name": "orders",
                    },
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"authority_mismatch": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path, env="dev")
    assert report.nodes[0].sink_uris == frozenset()
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)
    assert any("must match registry" in blocker.message for blocker in report.blockers)


def test_explicit_uri_unknown_host_is_blocker(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path, host="sql-prod.internal")
    path = tmp_path / MANIFEST_DIR / "unknown_host.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "unknown_host",
                "source": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t2"},
                    "mode": "append",
                },
                "gitops": {
                    "airflow": {
                        "execution": {
                            "outlets": ["mssql://sql-typo.internal:1433/dwh_example/dbo/orders"],
                        }
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(tmp_path, workloads={"unknown_host": manifest_ref(path.relative_to(tmp_path).as_posix())})
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path, env="dev")
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)
    assert any("not an approved physical authority" in blocker.message for blocker in report.blockers)


def test_explicit_uri_registry_host_allowed(tmp_path: Path) -> None:
    from dpone.gitops.airflow_asset_graph import build_asset_graph
    from dpone.gitops.workload_catalog import WorkloadCatalogResolver
    from tests.airflow_dag_spec_repo import (
        MANIFEST_DIR,
        manifest_ref,
        write_domain,
        write_workload_set,
    )

    _write_registry(tmp_path, host="sql-prod.internal")
    path = tmp_path / MANIFEST_DIR / "approved_host.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "approved_host",
                "source": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "public", "name": "t2"},
                    "mode": "append",
                },
                "gitops": {
                    "airflow": {
                        "execution": {
                            "outlets": ["mssql://sql-prod.internal/dwh_example/dbo/orders"],
                        }
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(tmp_path, workloads={"approved_host": manifest_ref(path.relative_to(tmp_path).as_posix())})
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path, env="dev")
    assert "mssql://sql-prod.internal:1433/dwh_example/dbo/orders" in report.nodes[0].sink_uris
    assert "mssql://sql-prod.internal/dwh_example/dbo/orders" not in report.nodes[0].sink_uris
    assert report.blockers == ()


@pytest.mark.parametrize(
    "uri",
    [
        "mssql://fixture01.invalid:abc/dwh_example/dbo/orders",
        "mssql://fixture01.invalid:65536/dwh_example/dbo/orders",
        "mssql://fixture01.invalid:1433/dwh_example/dbo/orders?x=1",
        "mssql://fixture01.invalid:1433/dwh_example/dbo/orders#frag",
    ],
)
def test_malformed_declared_mssql_uri_is_structured_blocker(uri: str) -> None:
    resolution = canonicalize_declared_mssql_uri(uri)
    assert resolution.uri is None
    assert resolution.issues
    assert resolution.issues[0].code == MSSQL_ASSET_URI_INVALID


def test_hostname_normalizer_strips_and_lowercases() -> None:
    from dpone.gitops.airflow_asset_uri import normalize_hostname

    assert normalize_hostname(" FIXTure01.Invalid. ") == "fixture01.invalid"
