from __future__ import annotations

from pathlib import Path

import yaml
from dpone_airflow_pack.asset_outlets import outlet_uris_from_pack

from dpone.gitops.airflow_asset_uri import MSSQL_ASSET_URI_INVALID
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from tests.airflow_dag_spec_repo import write_manifest


def test_compact_pack_materializes_inferred_sink_outlet_with_provenance(tmp_path: Path) -> None:
    manifest = write_manifest(
        tmp_path,
        "producer",
        sink_schema="dst",
        sink_table="shared",
    )
    workload = GitOpsWorkloadDefinition(
        workload_id="producer",
        manifest=manifest,
        domain="marketing",
        catalog_path="domains/marketing.yaml",
        effective_config={},
        provenance={},
    )

    pack = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path="packs/producer.airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )

    assert pack["airflow"]["execution"]["outlets"] == [
        {
            "uri": "postgres://dst/shared",
            "provenance": "inferred:asset_graph.v1",
        }
    ]
    assert outlet_uris_from_pack(pack) == ("postgres://dst/shared",)


def _write_mssql_registry(tmp_path: Path, *, host: str = "sql-prod.internal") -> None:
    path = tmp_path / ".dpone" / "registry" / "connection-registries" / "dev.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "dev",
                "connections": {
                    "mssql_example": {
                        "type": "mssql",
                        "connection": {
                            "asset_authority": {"host": host, "port": 1433},
                        },
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


def _write_mssql_manifest(tmp_path: Path, name: str) -> str:
    from tests.airflow_dag_spec_repo import MANIFEST_DIR

    path = tmp_path / MANIFEST_DIR / f"{name}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": name,
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
    return path.relative_to(tmp_path).as_posix()


def test_compact_pack_propagates_mssql_blockers_and_skips_write(tmp_path: Path) -> None:
    """Standalone pack path must fail-closed on invalid MSSQL authority (no output file)."""

    manifest = _write_mssql_manifest(tmp_path, "missing_authority")
    # No registry → managed mssql sink cannot resolve authority.
    workload = GitOpsWorkloadDefinition(
        workload_id="missing_authority",
        manifest=manifest,
        domain="marketing",
        catalog_path="domains/marketing.yaml",
        effective_config={
            "airflow": {
                "execution": {
                    "outlets": ["mssql://sql-typo.internal:1433/dwh_example/dbo/orders"],
                }
            }
        },
        provenance={},
    )
    output_path = "packs/missing_authority.airflow-pack.json"
    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path=output_path,
        repo_root=tmp_path,
        env="dev",
    )

    assert report.passed is False
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)
    assert not (tmp_path / output_path).exists()


def test_compact_pack_emits_single_canonical_outlet_for_portless_declared(tmp_path: Path) -> None:
    _write_mssql_registry(tmp_path)
    manifest = _write_mssql_manifest(tmp_path, "portless_declared")
    workload = GitOpsWorkloadDefinition(
        workload_id="portless_declared",
        manifest=manifest,
        domain="marketing",
        catalog_path="domains/marketing.yaml",
        effective_config={
            "airflow": {
                "execution": {
                    "outlets": ["mssql://sql-prod.internal/dwh_example/dbo/orders"],
                }
            }
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/portless_declared.airflow-pack.json",
        repo_root=tmp_path,
        env="dev",
    )
    pack = report.to_jsonable()
    outlets = pack["airflow"]["execution"]["outlets"]
    uris = [item["uri"] if isinstance(item, dict) else item for item in outlets]

    assert report.passed
    assert uris == ["mssql://sql-prod.internal:1433/dwh_example/dbo/orders"]
    assert "mssql://sql-prod.internal/dwh_example/dbo/orders" not in uris
    assert outlet_uris_from_pack(pack) == ("mssql://sql-prod.internal:1433/dwh_example/dbo/orders",)
