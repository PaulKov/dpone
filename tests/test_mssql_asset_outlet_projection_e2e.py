"""MSSQL outlet projection E2E paths.

1. Projection-focused (inject): mutates a compiled ClickHouse-sink pack with one
   logical ``asset_ref`` to exercise deployment projection / provider wiring.
2. Natural compiler: MSSQL sink profile → compile → pack already carries
   ``asset_ref`` (no post-compile injection); same release id for dev/prod.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml
from dpone_airflow_pack.asset_outlets import build_asset_outlets, outlet_specs_from_pack, outlet_uris_from_pack
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError, init_fetch_context_from_payload
from dpone_airflow_pack.mssql_outlet_projection_contract import (
    PROJECTION_MISMATCH,
    PROJECTION_REQUIRED,
    uri_by_asset_ref_sha256,
)

from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.readiness.dbt_publish_release_materializer import DbtReleaseMaterializer
from tests.test_dbt_airflow_release_e2e import (
    DEMO,
    _artifact_writer,
    _authoritative_selection_resolver,
    _certified,
    _config_map_ref,
    _vault_connection,
)

ROOT = Path(__file__).parents[1]
_ASSET_REF = {
    "engine": "mssql",
    "connection_ref": "mssql_marts",
    "database": "DWH",
    "schema": "dbo",
    "table": "orders",
}


def test_projection_focused_inject_asset_ref_dev_prod_named_instance_e2e(tmp_path: Path) -> None:
    """Projection-focused E2E: inject asset_ref into a ClickHouse-sink pack."""

    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed

    # Intentionally mutate: this suite isolates projection/provider wiring when
    # the compiled DEMO profile still sinks to ClickHouse.
    _inject_mssql_asset_ref_outlet_for_projection(compiled_root)
    release_tree_before = _tree_sha(compiled_root)

    release = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )
    release_id = release.release_id
    release_bytes = (
        tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-") / "release-set.json"
    ).read_bytes()
    release_tree_after = _tree_sha(compiled_root)
    assert release_tree_after == release_tree_before

    _write_env(
        tmp_path,
        environment="dev",
        logical_ref="mssql_marts",
        bound_ref="mssql_dev_writer",
        host="sql-dev.internal",
        instance="DEV01",
    )
    _write_env(
        tmp_path,
        environment="prod",
        logical_ref="mssql_marts",
        bound_ref="mssql_prod_writer",
        host="sql-prod.internal",
        instance="PROD01",
    )

    image_digest = "sha256:" + "d" * 64
    service = AirflowDeploymentProjectionService(root=tmp_path)
    dev = service.materialize(
        release_id=release_id,
        environment="dev",
        trust_tier="non_production",
        runtime_image_ref=f"registry.example/dpone-runtime@{image_digest}",
        runtime_image_digest=image_digest,
        artifact_registry_ref="dpone-dev-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    prod = service.materialize(
        release_id=release_id,
        environment="prod",
        trust_tier="production",
        runtime_image_ref=f"registry.example/dpone-runtime@{image_digest}",
        runtime_image_digest=image_digest,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )

    # Same release bytes; distinct deployment identities.
    assert (
        tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-") / "release-set.json"
    ).read_bytes() == release_bytes
    assert dev.deployment["release_ref"] == prod.deployment["release_ref"] == release_id
    assert dev.deployment["deployment_id"] != prod.deployment["deployment_id"]
    assert dev.airflow_index["schema"] == "dpone.airflow-deployment-index.v3"
    assert prod.airflow_index["schema"] == "dpone.airflow-deployment-index.v3"
    assert dev.deployment["schema"] == "dpone.deployment-set.v3"

    dev_proj = dev.airflow_index["mssql_asset_outlet_projection"]
    prod_proj = prod.airflow_index["mssql_asset_outlet_projection"]
    assert dev.deployment["mssql_asset_outlet_projection"] == dev_proj
    assert prod.deployment["mssql_asset_outlet_projection"] == prod_proj
    assert "sql-dev.internal:1433/dev01/" in next(iter(uri_by_asset_ref_sha256(dev_proj).values()))
    assert "sql-prod.internal:1433/prod01/" in next(iter(uri_by_asset_ref_sha256(prod_proj).values()))

    index_schema = json.loads(
        (ROOT / "docs/schemas/gitops/airflow-deployment-index-v3.schema.json").read_text(encoding="utf-8")
    )
    projection_schema = json.loads(
        (ROOT / "docs/schemas/gitops/mssql-asset-outlet-projection.schema.json").read_text(encoding="utf-8")
    )
    for index in (dev.airflow_index, prod.airflow_index):
        jsonschema.Draft202012Validator(index_schema).validate(index)
        jsonschema.Draft202012Validator(projection_schema).validate(index["mssql_asset_outlet_projection"])

    prod_context = init_fetch_context_from_payload(prod.airflow_index)
    assert prod_context.mssql_asset_uri_by_ref is not None
    pack = _first_transfer_pack(tmp_path, prod.airflow_index)
    uris = outlet_uris_from_pack(pack, mssql_asset_uri_by_ref=prod_context.mssql_asset_uri_by_ref)
    assert "mssql://sql-prod.internal:1433/prod01/DWH/dbo/orders" in uris
    specs = outlet_specs_from_pack(pack, mssql_asset_uri_by_ref=prod_context.mssql_asset_uri_by_ref)
    assert any(spec.provenance == "deployment_projection" and "prod01" in spec.uri for spec in specs)
    mssql_specs = tuple(spec for spec in specs if spec.uri.startswith("mssql://"))
    assets = build_asset_outlets(mssql_specs)
    if assets:
        assert getattr(assets[0], "uri", None) == mssql_specs[0].uri or "mssql://" in str(assets[0])

    mutated = json.loads(json.dumps(prod.airflow_index))
    del mutated["mssql_asset_outlet_projection"]
    with pytest.raises(InitFetchProviderError) as missing_wire:
        init_fetch_context_from_payload(mutated)
    assert missing_wire.value.code in {PROJECTION_REQUIRED, "DPONE_AIRFLOW_INDEX_FIELD_INVALID"}

    mismatched = json.loads(json.dumps(prod.airflow_index))
    mismatched["mssql_asset_outlet_projection"]["projection_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(InitFetchProviderError) as mismatch:
        init_fetch_context_from_payload(mismatched)
    assert mismatch.value.code == PROJECTION_MISMATCH


def test_natural_mssql_sink_compile_asset_ref_same_release_dev_prod(tmp_path: Path) -> None:
    """Natural compiler E2E: MSSQL sink profile emits asset_ref without injection."""

    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(
        (DEMO / "dpone" / "dbt-publish-profiles.yml")
        .read_text(encoding="utf-8")
        .replace(
            "    sink:\n      type: clickhouse\n      connection_ref: clickhouse_dwh_dev\n      target_schema: DWH_Stage",
            "    sink:\n      type: mssql\n      connection_ref: mssql_marts\n      target_schema: dbo",
        ),
        encoding="utf-8",
    )
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=profiles_path,
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed
    pack = _first_transfer_pack_on_disk(compiled_root)
    outlets = ((pack.get("airflow") or {}).get("execution") or {}).get("outlets") or []
    assert any(isinstance(item, dict) and isinstance(item.get("asset_ref"), dict) for item in outlets)
    assert not any("mssql://" in json.dumps(item) for item in outlets)

    release = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )
    release_id = release.release_id
    release_bytes = (
        tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-") / "release-set.json"
    ).read_bytes()

    _write_env(
        tmp_path,
        environment="dev",
        logical_ref="mssql_marts",
        bound_ref="mssql_dev_writer",
        host="sql-dev.internal",
        instance="DEV01",
    )
    _write_env(
        tmp_path,
        environment="prod",
        logical_ref="mssql_marts",
        bound_ref="mssql_prod_writer",
        host="sql-prod.internal",
        instance="PROD01",
    )
    image_digest = "sha256:" + "d" * 64
    service = AirflowDeploymentProjectionService(root=tmp_path)
    common = {
        "release_id": release_id,
        "runtime_image_ref": f"registry.example/dpone-runtime@{image_digest}",
        "runtime_image_digest": image_digest,
        "registry_config_ref": _config_map_ref("registry", "1"),
        "trust_policy_ref": _config_map_ref("policy", "2"),
        "airflow_bundle_ref": "git:" + "d" * 40,
    }
    dev = service.materialize(
        **common,
        environment="dev",
        trust_tier="non_production",
        artifact_registry_ref="dpone-dev-artifacts",
    )
    prod = service.materialize(
        **common,
        environment="prod",
        trust_tier="production",
        artifact_registry_ref="dpone-prod-artifacts",
    )
    assert (
        tmp_path / ".dpone-cache" / "releases" / release_id.replace(":", "-") / "release-set.json"
    ).read_bytes() == release_bytes
    assert dev.deployment["release_ref"] == prod.deployment["release_ref"] == release_id
    assert "sql-dev.internal:1433/dev01/" in next(
        iter(uri_by_asset_ref_sha256(dev.airflow_index["mssql_asset_outlet_projection"]).values())
    )
    assert "sql-prod.internal:1433/prod01/" in next(
        iter(uri_by_asset_ref_sha256(prod.airflow_index["mssql_asset_outlet_projection"]).values())
    )


def _first_transfer_pack_on_disk(compiled_root: Path) -> dict[str, Any]:
    packs_dir = compiled_root / "packs"
    for path in sorted(packs_dir.glob("*.airflow-pack.json")):
        pack = json.loads(path.read_text(encoding="utf-8"))
        outlets = ((pack.get("airflow") or {}).get("execution") or {}).get("outlets") or []
        if any(isinstance(entry, dict) and isinstance(entry.get("asset_ref"), dict) for entry in outlets):
            return pack
    raise AssertionError("compiled packs must already contain mssql asset_ref outlets")


def _inject_mssql_asset_ref_outlet_for_projection(compiled_root: Path) -> None:
    from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

    from dpone.contracts.airflow_deployment import release_id as compute_release_id

    packs_dir = compiled_root / "packs"
    targets = sorted(packs_dir.glob("*.airflow-pack.json"))
    assert targets, "compiled release must contain airflow packs"
    pack_path = next(
        (path for path in targets if path.name.startswith("dbt_") and not path.name.startswith("dbt__")),
        targets[0],
    )
    pack = json.loads(pack_path.read_text(encoding="utf-8"))
    airflow = pack.setdefault("airflow", {})
    execution = airflow.setdefault("execution", {})
    outlets = list(execution.get("outlets") or [])
    outlets.append({"asset_ref": dict(_ASSET_REF)})
    execution["outlets"] = outlets
    fingerprint = compute_pack_fingerprint(pack)
    pack["pack_fingerprint"] = fingerprint
    pack_bytes = (json.dumps(pack, indent=2, sort_keys=True) + "\n").encode("utf-8")
    pack_path.write_bytes(pack_bytes)

    release_path = compiled_root / "release-set.json"
    release = json.loads(release_path.read_text(encoding="utf-8"))
    relative = pack_path.relative_to(compiled_root).as_posix()
    digest = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    for item in release.get("artifacts", {}).get("workload_packs", []):
        if item.get("path") == relative:
            item["sha256"] = digest
            item["bytes"] = len(pack_bytes)
            if "pack_fingerprint" in item:
                item["pack_fingerprint"] = fingerprint
    release["release_id"] = compute_release_id(release)
    release_path.write_text(json.dumps(release, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_env(
    root: Path,
    *,
    environment: str,
    logical_ref: str,
    bound_ref: str,
    host: str,
    instance: str,
) -> None:
    environment_dir = root / "environments" / environment
    environment_dir.mkdir(parents=True, exist_ok=True)
    (environment_dir / "binding-set.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": environment,
                "bindings": {
                    logical_ref: {"connection_ref": bound_ref},
                    "mssql_dwh_stage": {"connection_ref": "mssql_dwh_stage"},
                    "clickhouse_dwh_dev": {"connection_ref": "clickhouse_dwh_dev"},
                },
                "runtime": {
                    "kubernetes_namespace": "airflow-example",
                    "service_account": "dpone-runtime",
                    "pool": f"dpone-{environment}",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (environment_dir / "credential-runtime.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": environment,
                "vault": {
                    "address": "https://vault.internal",
                    "namespace": "data-platform",
                    "auth": {"method": "kubernetes", "role": f"dpone-runtime-{environment}"},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry = root / "platform" / "connection-registries"
    registry.mkdir(parents=True, exist_ok=True)
    connections: dict[str, Any] = {
        "mssql_dwh_stage": _vault_connection("mssql", 1433),
        "clickhouse_dwh_dev": _vault_connection("clickhouse", 9000),
        logical_ref: {
            "type": "mssql",
            "connection": {
                "host": "sql-logical-must-not-win.internal",
                "port": 1433,
                "database": "DWH",
                "asset_authority": {"host": "sql-logical-must-not-win.internal", "port": 1433},
            },
            "credentials": _vault_connection("mssql", 1433)["credentials"],
        },
        bound_ref: {
            "type": "mssql",
            "connection": {
                "host": host,
                "port": 1433,
                "database": "DWH",
                "asset_authority": {"host": host, "port": 1433, "instance": instance},
            },
            "credentials": _vault_connection("mssql", 1433)["credentials"],
        },
    }
    (registry / f"{environment}.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": environment,
                "connections": connections,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _first_transfer_pack(root: Path, airflow_index: dict[str, Any]) -> dict[str, Any]:
    for item in airflow_index["workload_packs"]:
        if not str(item["id"]).startswith("dbt_"):
            continue
        relative = str(item["artifact_ref"]).removeprefix("cache://")
        pack = json.loads((root / ".dpone-cache" / relative).read_text(encoding="utf-8"))
        outlets = ((pack.get("airflow") or {}).get("execution") or {}).get("outlets") or []
        if any(isinstance(entry, dict) and isinstance(entry.get("asset_ref"), dict) for entry in outlets):
            return pack
    raise AssertionError("no pack with mssql asset_ref outlet found")


def _tree_sha(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()
