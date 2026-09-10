"""Pure, closed metadata authority for the explicit release composition envelope.

Validation proves identities, descriptor ownership and bounded exact union only.
The service must separately verify registered schemas, captured artifact bytes,
DAG membership, ordinary transport transformations and physical write policy.
No reader may substitute this metadata check for complete source verification.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest, release_id
from dpone.contracts.airflow_release_artifacts import release_artifact_path
from dpone.contracts.dbt_contract_validation import artifact_json_bytes, sha256_bytes
from dpone.contracts.dbt_release import dbt_release_authority_violation, dbt_release_runtime_wire_contract
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.contracts.dbt_source_inventory import MAX_DBT_SOURCE_INVENTORY_BYTES
from dpone.contracts.release_composition import (
    COMPOSITION_PRODUCER,
    COMPOSITION_PROFILE,
    COMPOSITION_SCHEMA,
    MAX_COMPOSITION_FILE_BYTES,
    MAX_COMPOSITION_FILES,
    MAX_COMPOSITION_METADATA_BYTES,
    MAX_COMPOSITION_TOTAL_BYTES,
    NATIVE_SIDECARS,
)
from dpone.contracts.release_composition_ordinary import OrdinaryReleaseCapture
from dpone.contracts.strict_json import strict_json_object

_BASE_FIELDS = {"id", "path", "sha256", "bytes"}
_SECTIONS = {"dag_specs", "workload_packs", "canonical_schemas", "runtime_payloads", "composition_sources"}
_PROMOTION = {"schema": "dpone.compact-pack-release-promotion.v1", "profile": COMPOSITION_PROFILE}
_NATIVE_SOURCES = {
    "_composition/native/release-set.json",
    "_composition/native/dbt-source-snapshot.json",
    "_composition/native/release-subjects.sha256",
}


def validate_composition_metadata(release: Mapping[str, Any]) -> None:
    """Reject ambiguous ownership, partial native authority and orphan metadata.

    Unordered descriptor and constituent arrays may arrive in any order; their
    canonical fingerprint sorts by identity. Ordered runtime trios are preserved
    and checked with the native v2 policy. All failures raise ``ValueError``.
    """
    _exact(release, {"schema", "release_id", "producer", "promotion", "artifacts", "constituents"})
    producer = _exact(release["producer"], {"name", "version"})
    version = producer["version"]
    if (
        release["schema"] != COMPOSITION_SCHEMA
        or producer["name"] != COMPOSITION_PRODUCER
        or not isinstance(version, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}", version) is None
        or release["promotion"] != _PROMOTION
    ):
        raise ValueError("composition producer, schema or compact promotion is invalid")
    constituents = _indexed(release["constituents"], limit=2)
    if set(constituents) != {"native", "standalone"}:
        raise ValueError("composition requires exactly the two typed constituents")
    native = _native(constituents["native"])
    ordinary = _ordinary(constituents["standalone"])
    artifacts = _exact(release["artifacts"], _SECTIONS)
    _require_union(artifacts, native, ordinary)
    _require_sources(artifacts["composition_sources"], ordinary)
    _require_budget(artifacts)
    if not is_canonical_sha256_digest(release["release_id"]) or release["release_id"] != release_id(release):
        raise ValueError("composition release identity differs from its content")


def composition_native_release(release: Mapping[str, Any], workload_id: str | None = None) -> dict[str, Any]:
    """Return detached selected native authority after validating its parent.

    A standalone or unknown workload cannot inherit native dbt authority. Runtime
    still verifies the selected pack and ordered payload bytes using native v2.
    """
    validate_composition_metadata(release)
    native = next(row["release"] for row in release["constituents"] if row["id"] == "native")
    if workload_id is not None and workload_id not in {row["id"] for row in native["artifacts"]["workload_packs"]}:
        raise ValueError("selected workload is not owned by the native constituent")
    return deepcopy(dict(native))


def _native(constituent: Mapping[str, Any]) -> Mapping[str, Any]:
    _exact(constituent, {"id", "kind", "release"})
    native = constituent["release"]
    if constituent["kind"] != "dbt_workspace" or not isinstance(native, Mapping):
        raise ValueError("native composition constituent is invalid")
    if (
        native.get("schema") != "dpone.release-set.v2"
        or native.get("promotion") != _PROMOTION
        or dbt_release_runtime_wire_contract(native) != DBT_RUNTIME_WIRE_V2
        or dbt_release_authority_violation(native, expected_wire_contract=DBT_RUNTIME_WIRE_V2) is not None
        or native.get("release_id") != release_id(native)
    ):
        raise ValueError("native composition authority or identity is invalid")
    index = DbtReleaseArtifactIndex(native)
    selected: set[str] = set()
    for workload_id, descriptor in index.workloads.items():
        if "runtime_payload_ids" in descriptor:
            ids = descriptor["runtime_payload_ids"]
            assert isinstance(ids, tuple)  # The native index detaches lists as tuples.
            index.require_workload_trio(workload_id, ids)
            selected.update(ids)
    if selected != set(index.payloads):
        raise ValueError("native composition runtime inventory contains orphan payloads")
    return native


def _ordinary(constituent: Mapping[str, Any]) -> Mapping[str, Any]:
    _exact(constituent, {"id", "kind", "inventory", "inventory_sha256"})
    inventory = _exact(constituent["inventory"], {"schema", "dag_specs", "workload_packs"})
    if (
        constituent["kind"] != "workload_inventory"
        or inventory["schema"] != "dpone.workload-inventory.v1"
        or not is_canonical_sha256_digest(constituent["inventory_sha256"])
        or constituent["inventory_sha256"] != canonical_fingerprint(inventory)
    ):
        raise ValueError("standalone composition inventory identity is invalid")
    for section in ("dag_specs", "workload_packs"):
        for row in _indexed(inventory[section]).values():
            _ordinary_descriptor(row, section=section, source=True)
    return inventory


def _require_union(artifacts: Mapping[str, Any], native: Mapping[str, Any], ordinary: Mapping[str, Any]) -> None:
    for section in ("dag_specs", "workload_packs", "canonical_schemas", "runtime_payloads"):
        parent = _indexed(artifacts[section])
        child = _indexed(native["artifacts"][section])
        independent = _indexed(ordinary[section]) if section in ordinary else {}
        if child.keys() & independent.keys() or set(parent) != set(child) | set(independent):
            raise ValueError("composition artifact ownership is overlapping or incomplete")
        for item_id, descriptor in child.items():
            if parent[item_id] != descriptor:
                raise ValueError("composition native descriptor differs from its constituent")
        for item_id in independent:
            _ordinary_descriptor(parent[item_id], section=section, source=False)


def _ordinary_descriptor(row: Mapping[str, Any], *, section: str, source: bool) -> None:
    is_pack = section == "workload_packs"
    _exact(row, _BASE_FIELDS | ({"pack_fingerprint"} if is_pack else set()))
    item_id = row["id"]
    if not isinstance(item_id, str) or not item_id or any(char in item_id for char in "/\\"):
        raise ValueError("standalone artifact identity is invalid")
    if source:
        path = f"{item_id}/airflow-pack.json" if is_pack else f"_dags/{item_id}.dag-spec.json"
    else:
        path = f"packs/{item_id}.airflow-pack.json" if is_pack else f"dags/{item_id}.dag-spec.json"
    if row["path"] != path or (is_pack and not is_canonical_sha256_digest(row["pack_fingerprint"])):
        raise ValueError("standalone artifact path or fingerprint is invalid")
    _descriptor(row, limit=MAX_COMPOSITION_METADATA_BYTES)


def _require_sources(value: object, ordinary: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    sources = _indexed(value)
    ordinary_sources = {
        f"_composition/standalone/{row['path']}": row
        for section in ("dag_specs", "workload_packs")
        for row in ordinary[section]
    }
    if set(sources) != _NATIVE_SOURCES | set(ordinary_sources):
        raise ValueError("composition source inventory is incomplete or contains orphans")
    for item_id, row in sources.items():
        _exact(row, _BASE_FIELDS)
        if row["path"] != item_id:
            raise ValueError("composition source descriptor identity differs from its path")
        limit = MAX_COMPOSITION_METADATA_BYTES
        if item_id == "_composition/native/dbt-source-snapshot.json":
            limit = MAX_DBT_SOURCE_INVENTORY_BYTES
        _descriptor(row, limit=limit)
        original = ordinary_sources.get(item_id)
        if original is not None and (row["sha256"], row["bytes"]) != (original["sha256"], original["bytes"]):
            raise ValueError("composition standalone source differs from its inventory")
    return sources


def _require_budget(artifacts: Mapping[str, Any]) -> None:
    paths: set[str] = set()
    total = 0
    for section, rows in artifacts.items():
        for row in rows:
            path = str(release_artifact_path(row))
            if path in paths or (section != "composition_sources" and path.startswith("_composition/")):
                raise ValueError("composition artifact path has conflicting ownership")
            _descriptor(row, limit=MAX_COMPOSITION_FILE_BYTES)
            paths.add(path)
            total += row["bytes"]
    # The publication service additionally charges parent descriptor/subject bytes.
    if len(paths) + 2 > MAX_COMPOSITION_FILES or total > MAX_COMPOSITION_TOTAL_BYTES:
        raise ValueError("composition aggregate artifact bound exceeded")


def _descriptor(row: Mapping[str, Any], *, limit: int) -> None:
    release_artifact_path(row)
    if type(row["bytes"]) is not int or not 0 < row["bytes"] <= limit or not is_canonical_sha256_digest(row["sha256"]):
        raise ValueError("composition artifact digest or size is invalid")


def _indexed(value: object, *, limit: int = MAX_COMPOSITION_FILES) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list) or not 0 < len(value) <= limit:
        raise ValueError("composition inventory must be a nonempty bounded array")
    result: dict[str, Mapping[str, Any]] = {}
    for row in value:
        if not isinstance(row, Mapping):
            raise ValueError("composition inventory descriptor must be an object")
        item_id = row.get("id")
        if not isinstance(item_id, str) or not item_id or item_id in result:
            raise ValueError("composition inventory identity is invalid or duplicate")
        result[item_id] = row
    return result


def _exact(value: object, fields: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("composition object fields are invalid")
    return value


def assemble_composition_files(
    native: Mapping[str, Any], captured: Mapping[str, bytes], ordinary: OrdinaryReleaseCapture, *, producer_version: str
) -> dict[str, bytes]:
    """Bind the exact constituent union without rewriting native executable bytes."""
    files = {path: body for path, body in captured.items() if path not in NATIVE_SIDECARS}
    files.update({target: captured[source] for source, target in NATIVE_SIDECARS.items()})
    for path, body in {**ordinary.dag_files, **ordinary.pack_files}.items():
        if path in files:
            raise ValueError("composition artifact path collision")
        files[path] = body
    files.update({f"_composition/standalone/{path}": body for path, body in ordinary.files.items()})
    artifacts = {section: [dict(row) for row in rows] for section, rows in native["artifacts"].items()}
    for section, values in (("dag_specs", ordinary.dag_files), ("workload_packs", ordinary.pack_files)):
        for path, body in sorted(values.items()):
            parsed = strict_json_object(body)
            key = parsed["dag_id"] if section == "dag_specs" else parsed["workload"]["workload_id"]
            row = _artifact_descriptor(key, path, body)
            if section == "workload_packs":
                row["pack_fingerprint"] = parsed["pack_fingerprint"]
            artifacts[section].append(row)
    artifacts["composition_sources"] = [
        _artifact_descriptor(path, path, body)
        for path, body in sorted(files.items())
        if path.startswith("_composition/")
    ]
    release: dict[str, Any] = {
        "schema": COMPOSITION_SCHEMA,
        "release_id": "",
        "producer": {"name": COMPOSITION_PRODUCER, "version": producer_version},
        "promotion": dict(_PROMOTION),
        "artifacts": artifacts,
        "constituents": [
            {"id": "native", "kind": "dbt_workspace", "release": native},
            {
                "id": "standalone",
                "kind": "workload_inventory",
                "inventory": ordinary.inventory_dict(),
                "inventory_sha256": ordinary.inventory_sha256,
            },
        ],
    }
    for rows in artifacts.values():
        rows.sort(key=lambda row: row["id"])
    release["release_id"] = release_id(release)
    files["release-set.json"] = artifact_json_bytes(release)
    return files


def _artifact_descriptor(key: str, path: str, body: bytes) -> dict[str, Any]:
    return {"id": key, "path": path, "sha256": sha256_bytes(body), "bytes": len(body)}
