"""Canonical identity and trust checks for one dbt-backed release-set v2."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    is_canonical_sha256_digest,
)
from dpone.contracts.airflow_deployment import (
    release_id as compute_release_id,
)
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2

DBT_SELECTION_AUTHORITY = "dbt_cli"
DBT_RELEASE_WIRE_CONTRACT = DBT_RUNTIME_WIRE_V1
_PACKAGE_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}")
_PRODUCTION_CERTIFICATION_LEVELS = frozenset({"production-certified", "enterprise-certified"})
_CERTIFICATION_FIELDS = frozenset(
    {
        "variant_id",
        "route_id",
        "transport",
        "schema_evolution",
        "airflow_runtime_mode",
        "snapshot_id",
        "support",
        "certification_level",
        "evidence_status",
        "evidence_refs",
        "evidence_reason_codes",
    }
)


def require_dbt_release_wire_contract(wire_contract: str) -> None:
    """Reject an unsupported wire before acquiring or interpreting artifact data."""

    if wire_contract not in {DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2}:
        raise ValueError("unsupported dbt release wire contract")


def is_workspace_dbt_wire(wire_contract: str | None) -> bool:
    """Classify an observed producer without inferring support for future wires.

    None denotes a generic release with no dbt producer. Supported workspace
    readers and activation policy share this exact discriminator; unknown wires
    are errors, never an implicit singleton or workspace fallback.
    """

    if wire_contract is None:
        return False
    require_dbt_release_wire_contract(wire_contract)
    return wire_contract == DBT_RUNTIME_WIRE_V2


def build_dbt_release_from_files(
    *,
    dag_specs: list[dict[str, Any]],
    dag_files: Mapping[str, bytes],
    pack_files: Mapping[str, bytes],
    verified_packs: Mapping[str, tuple[bytes, str, tuple[str, ...]]],
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
    """Bind materialized bytes to release metadata using verified pack identities.

    Each observation binds the bytes observed by the framework verifier to its
    fingerprint and the runtime IDs captured immediately after that verification.
    Descriptors use those bytes, then check the materialized map
    independently so replacing an entry during verification cannot reuse trust.
    This pure function checks descriptors and identity, not pack authenticity.
    """

    require_dbt_release_wire_contract(wire_contract)
    if set(verified_packs) != set(pack_files):
        raise ValueError("verified pack membership differs from materialized files")
    dag_ids = [row.get("id") for row in dag_specs]
    if len(dag_ids) != len(set(dag_ids)) or set(dag_ids) != set(dag_files):
        raise ValueError("DAG descriptor membership differs from materialized files")
    workload_packs = []
    for item_id, (payload, fingerprint, selected) in sorted(verified_packs.items()):
        descriptor = dbt_release_artifact_descriptor(item_id, f"packs/{item_id}.airflow-pack.json", payload)
        descriptor["pack_fingerprint"] = fingerprint
        if selected:
            descriptor["runtime_payload_ids"] = list(selected)
        workload_packs.append(descriptor)
    release = build_dbt_release_metadata(
        dag_specs=dag_specs,
        workload_packs=workload_packs,
        runtime_descriptors=runtime_descriptors,
        canonical_schema_descriptors=canonical_schema_descriptors,
        source_snapshot_sha256=source_snapshot_sha256,
        selection_authority=selection_authority,
        route_certifications=route_certifications,
        selection_fingerprints=selection_fingerprints,
        producer_version=producer_version,
        wire_contract=wire_contract,
    )
    _verify_release_files(
        release,
        dag_files=dag_files,
        pack_files=pack_files,
        runtime_files=runtime_files,
        canonical_schema_files=canonical_schema_files,
    )
    return release


def dbt_release_dag_descriptors(dag_files: Mapping[str, bytes]) -> list[dict[str, Any]]:
    """Capture DAG identity before external verification can mutate caller maps."""

    return [
        dbt_release_artifact_descriptor(item_id, f"dags/{item_id}.dag-spec.json", payload)
        for item_id, payload in sorted(dag_files.items())
    ]


def _verify_release_files(
    release: Mapping[str, Any],
    *,
    dag_files: Mapping[str, bytes],
    pack_files: Mapping[str, bytes],
    runtime_files: Mapping[str, bytes],
    canonical_schema_files: Mapping[str, bytes],
) -> None:
    by_path = {
        **{f"dags/{key}.dag-spec.json": value for key, value in dag_files.items()},
        **{f"packs/{key}.airflow-pack.json": value for key, value in pack_files.items()},
        **dict(runtime_files),
        **dict(canonical_schema_files),
    }
    artifacts = release["artifacts"]
    for section in (
        "dag_specs",
        "workload_packs",
        "canonical_schemas",
        "runtime_payloads",
    ):
        for descriptor in artifacts[section]:
            payload = by_path.get(descriptor["path"])
            if payload is None or len(payload) != descriptor["bytes"] or sha256_bytes(payload) != descriptor["sha256"]:
                raise ValueError("release descriptor differs from materialized bytes")


def build_dbt_release_metadata(
    *,
    dag_specs: list[dict[str, Any]],
    workload_packs: list[dict[str, Any]],
    runtime_descriptors: list[dict[str, Any]],
    canonical_schema_descriptors: list[dict[str, Any]],
    source_snapshot_sha256: str,
    selection_authority: str,
    route_certifications: list[dict[str, Any]],
    selection_fingerprints: list[str],
    producer_version: str,
    wire_contract: str = DBT_RELEASE_WIRE_CONTRACT,
) -> dict[str, Any]:
    """Compute metadata identity; callers must verify actual artifact bytes.

    V1 retains selection multiplicity for its historical fingerprint. V2
    normalizes it before hashing. Other arrays keep their supplied order.
    This constructor is not an artifact/schema validator or signature verifier.
    """

    require_dbt_release_wire_contract(wire_contract)
    if wire_contract == DBT_RUNTIME_WIRE_V2:
        selection_fingerprints = sorted(set(selection_fingerprints))
    release: dict[str, Any] = {
        "schema": "dpone.release-set.v2",
        "release_id": "",
        "producer": {
            "dpone_version": producer_version,
            "wire_contract": wire_contract,
        },
        "artifacts": {
            "dag_specs": dag_specs,
            "workload_packs": workload_packs,
            "canonical_schemas": canonical_schema_descriptors,
            "runtime_payloads": runtime_descriptors,
        },
        "selection_authority": selection_authority,
        "selection_fingerprint": dbt_selection_fingerprint(
            source_snapshot_sha256=source_snapshot_sha256,
            selection_fingerprints=selection_fingerprints,
            route_certifications=route_certifications,
        ),
        "provenance": {
            "source": "dpone dbt compile",
            "source_snapshot_sha256": source_snapshot_sha256,
            "selection_fingerprints": sorted(set(selection_fingerprints)),
            "route_certifications": route_certifications,
        },
    }
    release["release_id"] = compute_release_id(release)
    return release


def dbt_release_artifact_descriptor(item_id: str, path: str, payload: bytes) -> dict[str, Any]:
    """Describe supplied bytes; no framework fingerprint verification is implied."""

    return {"id": item_id, "path": path, "sha256": sha256_bytes(payload), "bytes": len(payload)}


def dbt_selection_fingerprint(
    *,
    source_snapshot_sha256: str,
    selection_fingerprints: Sequence[str],
    route_certifications: Sequence[Mapping[str, object]],
) -> str:
    """Bind resolved dbt selection and production route proof into release identity."""

    return canonical_fingerprint(
        {
            "source_snapshot_sha256": source_snapshot_sha256,
            "selection_fingerprints": sorted(selection_fingerprints),
            "route_certifications": sorted(
                (dict(item) for item in route_certifications),
                key=lambda item: str(item.get("variant_id") or ""),
            ),
        }
    )


def dbt_release_authority_violation(
    release: Mapping[str, object],
    *,
    expected_wire_contract: str = DBT_RELEASE_WIRE_CONTRACT,
) -> str | None:
    """Return a safe reason when dbt release authority is not identity-bound."""

    if release.get("selection_authority") != DBT_SELECTION_AUTHORITY:
        return "publishable dbt releases require dbt-authoritative selection"
    producer_violation = dbt_release_producer_violation(release, expected_wire_contract=expected_wire_contract)
    if producer_violation is not None:
        return producer_violation
    provenance = release.get("provenance")
    if not isinstance(provenance, Mapping):
        return "publishable dbt releases require selection provenance"
    source_snapshot = provenance.get("source_snapshot_sha256")
    if not is_canonical_sha256_digest(source_snapshot):
        return "dbt release source snapshot identity is invalid"
    selections = provenance.get("selection_fingerprints")
    if (
        not isinstance(selections, list)
        or not selections
        or any(not is_canonical_sha256_digest(item) for item in selections)
        or selections != sorted(set(selections))
    ):
        return "dbt release selection fingerprints are invalid"
    certifications = provenance.get("route_certifications")
    if not isinstance(certifications, list) or not certifications:
        return "publishable dbt releases require production route certification"
    if any(_certification_violation(item) for item in certifications):
        return "publishable dbt releases require current production-certified route evidence"
    expected = dbt_selection_fingerprint(
        source_snapshot_sha256=str(source_snapshot),
        selection_fingerprints=tuple(str(item) for item in selections),
        route_certifications=tuple(item for item in certifications if isinstance(item, Mapping)),
    )
    if release.get("selection_fingerprint") != expected:
        return "dbt release selection fingerprint differs from authority evidence"
    return None


def dbt_release_producer_violation(
    release: Mapping[str, object],
    *,
    expected_dpone_version: str | None = None,
    expected_wire_contract: str = DBT_RELEASE_WIRE_CONTRACT,
) -> str | None:
    """Validate the compiler and wire contract bound into release identity."""

    producer = release.get("producer")
    if not isinstance(producer, Mapping) or set(producer) != {
        "dpone_version",
        "wire_contract",
    }:
        return "dbt release producer identity is invalid"
    version = producer.get("dpone_version")
    if not isinstance(version, str) or _PACKAGE_VERSION.fullmatch(version) is None:
        return "dbt release producer version is invalid"
    if (
        expected_wire_contract not in {DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2}
        or producer.get("wire_contract") != expected_wire_contract
    ):
        return "dbt release wire contract is unsupported"
    if expected_dpone_version is not None and version != expected_dpone_version:
        return "dbt release requires a different dpone version"
    return None


def dbt_release_runtime_wire_contract(release: Mapping[str, object]) -> str:
    """Read runtime wire authority, retaining only the historical unversioned v1 path.

    Legacy compact releases omitted producer metadata. That absence cannot opt
    into v2: the caller must still validate the selected trio as v1. A present
    malformed or unsupported producer always fails; it never falls back.
    """

    if "producer" not in release:
        return DBT_RUNTIME_WIRE_V1
    producer = release["producer"]
    wire = producer.get("wire_contract") if isinstance(producer, Mapping) else None
    if not isinstance(wire, str) or dbt_release_producer_violation(release, expected_wire_contract=wire) is not None:
        raise ValueError("dbt runtime release producer identity is invalid")
    if wire == DBT_RUNTIME_WIRE_V2 and release.get("schema") != "dpone.release-set.v2":
        raise ValueError("dbt runtime release schema is incompatible with its wire")
    return wire


def _certification_violation(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != _CERTIFICATION_FIELDS:
        return True
    route_id = value.get("route_id")
    variant_id = value.get("variant_id")
    snapshot_id = value.get("snapshot_id")
    evidence_refs = value.get("evidence_refs")
    reasons = value.get("evidence_reason_codes")
    return (
        not isinstance(route_id, str)
        or not route_id
        or not isinstance(variant_id, str)
        or not variant_id
        or any(
            not isinstance(value.get(field), str) or not value.get(field)
            for field in (
                "transport",
                "schema_evolution",
                "airflow_runtime_mode",
            )
        )
        or not is_canonical_sha256_digest(snapshot_id)
        or value.get("support") not in {"supported", "conditional"}
        or value.get("certification_level") not in _PRODUCTION_CERTIFICATION_LEVELS
        or value.get("evidence_status") != "PASS"
        or not isinstance(evidence_refs, list)
        or not evidence_refs
        or any(not is_canonical_sha256_digest(item) for item in evidence_refs)
        or evidence_refs != sorted(set(evidence_refs))
        or not isinstance(reasons, list)
        or any(not isinstance(item, str) for item in reasons)
    )


__all__ = [
    "DBT_SELECTION_AUTHORITY",
    "DBT_RELEASE_WIRE_CONTRACT",
    "dbt_release_authority_violation",
    "dbt_release_producer_violation",
    "dbt_release_runtime_wire_contract",
    "is_workspace_dbt_wire",
    "dbt_selection_fingerprint",
    "build_dbt_release_metadata",
    "build_dbt_release_from_files",
    "dbt_release_dag_descriptors",
    "require_dbt_release_wire_contract",
    "dbt_release_artifact_descriptor",
]
