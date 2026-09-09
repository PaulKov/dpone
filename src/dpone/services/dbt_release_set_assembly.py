"""Assemble immutable release metadata from already projected artifact bytes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_release import (
    DBT_RELEASE_WIRE_CONTRACT,
    build_dbt_release_from_files,
    dbt_release_dag_descriptors,
    require_dbt_release_wire_contract,
)


def build_release_set(
    *,
    dag_files: Mapping[str, bytes],
    pack_files: Mapping[str, bytes],
    pack_runtime_payload_ids: Mapping[str, tuple[str, ...]],
    runtime_files: Mapping[str, bytes],
    runtime_descriptors: list[dict[str, Any]],
    canonical_schema_files: Mapping[str, bytes],
    canonical_schema_descriptors: list[dict[str, Any]],
    source_snapshot_sha256: str,
    selection_authority: str,
    route_certifications: list[dict[str, Any]],
    selection_fingerprints: list[str],
    producer_version: str,
    wire_contract: str = DBT_RELEASE_WIRE_CONTRACT,
) -> dict[str, Any]:
    """Build a self-consistent release identity without volatile provenance."""

    from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

    require_dbt_release_wire_contract(wire_contract)
    dag_specs = dbt_release_dag_descriptors(dag_files)
    verified_packs = {}
    for item_id, payload in sorted(pack_files.items()):
        fingerprint = verify_pack_fingerprint(payload)
        selected = tuple(pack_runtime_payload_ids.get(item_id, ()))
        verified_packs[item_id] = (payload, fingerprint, selected)
    return build_dbt_release_from_files(
        dag_specs=dag_specs,
        dag_files=dag_files,
        pack_files=pack_files,
        verified_packs=verified_packs,
        runtime_files=runtime_files,
        runtime_descriptors=runtime_descriptors,
        canonical_schema_files=canonical_schema_files,
        canonical_schema_descriptors=canonical_schema_descriptors,
        source_snapshot_sha256=source_snapshot_sha256,
        selection_authority=selection_authority,
        route_certifications=route_certifications,
        selection_fingerprints=selection_fingerprints,
        producer_version=producer_version,
        wire_contract=wire_contract,
    )
