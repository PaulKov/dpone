from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_compact_process_plans import resolve_compact_process_projection
from dpone.gitops.airflow_runner_contract import resolve_airflow_runner_contract
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.models import LoadedManifest


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
        "connections": [_entry("unrelated_source", "pg_unrelated")],
        "connection_ids": ["pg_unrelated"],
    }


def _workload(*, manifest: str, projection: dict[str, object]) -> GitOpsWorkloadDefinition:
    return GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {"connection_projection": projection},
        },
        provenance={},
    )


def _resolved_projection(projection: dict[str, object]) -> dict[str, object]:
    return resolve_airflow_runner_contract({"connection_projection": projection}).connection_projection


def test_exact_compiled_empty_refs_emit_no_operator_projection(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "name": "orders",
                "source": {
                    "type": "postgres",
                    "connection_id": "legacy_source",
                    "table": {"schema": "public", "name": "orders"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_id": "legacy_sink",
                    "table": {"schema": "dwh", "name": "orders"},
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    pack = AirflowCompactPackBuilder().build(
        workload=_workload(manifest=manifest.name, projection=_projection()),
        output_path="packs/orders/airflow-pack.json",
        repo_root=tmp_path,
    )

    assert pack.connection_projection == {}
    assert "DPONE_AIRFLOW_PROCESS_PLAN_COMPATIBILITY_FALLBACK" not in {warning.code for warning in pack.warnings}


def test_process_projection_derives_connection_refs_from_its_single_manifest_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
                    "connection_ref": "orders_sink",
                    "table": {"schema": "dwh", "name": "orders"},
                    "mode": "append",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    original_load = ManifestLoaderRouter.load
    load_count = 0

    def counted_load(
        loader: ManifestLoaderRouter,
        path: Path,
        *,
        metadata_only: bool = True,
    ) -> LoadedManifest:
        nonlocal load_count
        load_count += 1
        return original_load(loader, path, metadata_only=metadata_only)

    monkeypatch.setattr(ManifestLoaderRouter, "load", counted_load)
    projection = resolve_compact_process_projection(
        workload=_workload(manifest=manifest.name, projection=_projection()),
        runtime_manifest_path=manifest.name,
        runtime_manifest_kind="manifest",
        repo_root=tmp_path,
        output_path="packs/orders/airflow-pack.json",
    )

    assert load_count == 1
    assert projection.required_connection_refs == ("orders_sink", "orders_source")


def test_legacy_fallback_preserves_unresolved_connection_projection(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.yaml"
    manifest.write_text("source: {}\nsink: {}\n", encoding="utf-8")
    projection = _projection()

    pack = AirflowCompactPackBuilder().build(
        workload=_workload(manifest=manifest.name, projection=projection),
        output_path="packs/orders/airflow-pack.json",
        repo_root=tmp_path,
    )

    assert pack.connection_projection == _resolved_projection(projection)
    assert "DPONE_AIRFLOW_PROCESS_PLAN_COMPATIBILITY_FALLBACK" in {warning.code for warning in pack.warnings}


def test_unmaterialized_pack_preserves_unresolved_connection_projection() -> None:
    projection = _projection()

    pack = AirflowCompactPackBuilder().build(
        workload=_workload(manifest="pipelines/orders.yaml", projection=projection),
        output_path="packs/orders/airflow-pack.json",
        repo_root=None,
    )

    assert pack.connection_projection == _resolved_projection(projection)
    assert "runtime_manifest_repo_root_required" in {blocker.code for blocker in pack.blockers}
