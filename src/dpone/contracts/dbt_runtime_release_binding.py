"""Unambiguous v2 runtime metadata binding, without fetching other projects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release_artifact_limits import MAX_DBT_RELEASE_DAG_BYTES, MAX_DBT_RELEASE_PACK_BYTES
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    MAX_DBT_RUNTIME_PAYLOAD_BYTES,
    MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES,
    MAX_DBT_RUNTIME_PAYLOADS,
    dbt_runtime_payload_read_limit,
    validate_dbt_runtime_payload_descriptor,
    validate_dbt_runtime_payload_trio,
)

_FORMATS = {
    "dag_specs": ("dags/", ".dag-spec.json"),
    "workload_packs": ("packs/", ".airflow-pack.json"),
    "canonical_schemas": ("schemas/dbt/", ".schema.json"),
}


@dataclass(frozen=True, slots=True, init=False)
class DbtReleaseArtifactIndex:
    """Detached v2 metadata for complete source verification, without file I/O.

    Construction validates the complete bounded canonical inventory once. It
    proves neither release authority nor actual bytes. Runtime-only binding
    remains separate and does not acquire whole-workspace sources or a sidecar.
    """

    workloads: Mapping[str, Mapping[str, object]]
    dags: Mapping[str, Mapping[str, object]]
    payloads: Mapping[str, Mapping[str, object]]
    schemas: Mapping[str, Mapping[str, object]]
    by_path: Mapping[str, Mapping[str, object]]

    def __init__(self, release: Mapping[str, object]) -> None:
        artifacts = release.get("artifacts")
        if not isinstance(artifacts, Mapping) or set(artifacts) != {*_FORMATS, "runtime_payloads"}:
            raise ValueError("workspace release artifact sections are invalid")
        sections = {section: dbt_release_artifact_inventory(artifacts[section]) for section in _FORMATS}
        sections["runtime_payloads"] = dbt_runtime_payload_inventory(artifacts["runtime_payloads"])
        paths: dict[str, Mapping[str, object]] = {}
        for section, rows in sections.items():
            for item_id, row in tuple(rows.items()):
                if any(char in item_id for char in "/\\"):
                    raise ValueError("workspace release descriptor identity is invalid")
                limit = {"dag_specs": MAX_DBT_RELEASE_DAG_BYTES, "workload_packs": MAX_DBT_RELEASE_PACK_BYTES}.get(
                    section, MAX_DBT_RUNTIME_PAYLOAD_BYTES
                )
                if section == "runtime_payloads":
                    # Already canonicalized and checked once above.
                    path = row["path"]
                    kind = row["kind"]
                    assert isinstance(path, str) and isinstance(kind, str)
                    limit = dbt_runtime_payload_read_limit(kind)
                else:
                    prefix, suffix = _FORMATS[section]
                    path = f"{prefix}{item_id}{suffix}"
                    fields = {"id", "path", "bytes", "sha256"}
                    if section == "workload_packs":
                        fields.add("pack_fingerprint")
                        if "runtime_payload_ids" in row:
                            fields.add("runtime_payload_ids")
                        if not is_canonical_sha256_digest(row.get("pack_fingerprint")):
                            raise ValueError("workspace workload fingerprint is invalid")
                    if set(row) != fields:
                        raise ValueError("workspace release descriptor fields are invalid")
                if row.get("path") != path or path in paths:
                    raise ValueError("workspace release descriptor path is noncanonical or duplicate")
                size = row.get("bytes")
                if type(size) is not int or not 0 < size <= limit:
                    raise ValueError("workspace release artifact size is invalid")
                if not is_canonical_sha256_digest(row.get("sha256")):
                    raise ValueError("workspace release artifact digest is invalid")
                detached = dict(row)
                if "runtime_payload_ids" in detached:
                    ids = detached["runtime_payload_ids"]
                    if not isinstance(ids, list) or any(not isinstance(value, str) for value in ids):
                        raise ValueError("workspace workload runtime membership is invalid")
                    detached["runtime_payload_ids"] = tuple(ids)
                rows[item_id] = paths[path] = MappingProxyType(detached)
                if len(paths) > 50_000:
                    raise ValueError("workspace release file-count bound exceeded")
        for field, section in (
            ("workloads", "workload_packs"),
            ("dags", "dag_specs"),
            ("payloads", "runtime_payloads"),
            ("schemas", "canonical_schemas"),
        ):
            object.__setattr__(self, field, MappingProxyType(sections[section]))
        object.__setattr__(self, "by_path", MappingProxyType(paths))

    @classmethod
    def from_release(cls, release: Mapping[str, object]) -> DbtReleaseArtifactIndex:
        """Use the same validating constructor; raw indexes are not accepted."""

        return cls(release)

    def require_workload_trio(self, workload_id: str, payload_ids: tuple[str, ...]) -> None:
        require_dbt_runtime_workload_trio(self.workloads, workload_id=workload_id, payload_ids=payload_ids)
        if any(item_id not in self.payloads for item_id in payload_ids):
            raise ValueError("dbt runtime trio is missing from its release")

    def require_bytes(self, path: str, body: bytes) -> None:
        """Validate every observation, including a reread after earlier verification."""

        descriptor = self.by_path.get(path)
        if descriptor is None or len(body) != descriptor["bytes"] or sha256_bytes(body) != descriptor["sha256"]:
            raise ValueError("canonical workspace artifact differs from its descriptor")


def dbt_runtime_payload_inventory(value: object) -> dict[str, Mapping[str, object]]:
    """Validate all v2 metadata once; exact duplicates are ambiguous too."""

    items = dbt_release_artifact_inventory(value, limit=MAX_DBT_RUNTIME_PAYLOADS)
    total = 0
    for item in items.values():
        validate_dbt_runtime_payload_descriptor(item, wire_contract=DBT_RUNTIME_WIRE_V2)
        size = item["bytes"]
        assert isinstance(size, int)  # Validated by the descriptor contract.
        total += size
    if total > MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES:
        raise ValueError("dbt runtime payload aggregate bound exceeded")
    return items


def bind_dbt_runtime_workload(
    release: Mapping[str, object],
    *,
    workload_id: str,
    payload_ids: tuple[str, ...],
) -> tuple[Mapping[str, object], ...]:
    """Bind the selected trio to one workload in an already verified v2 release.

    This verifies metadata only. The caller must verify release authority and
    selected artifact bytes, and must not treat this as whole-workspace proof.
    """

    validate_dbt_runtime_payload_trio(payload_ids, wire_contract=DBT_RUNTIME_WIRE_V2)
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("dbt runtime release artifacts are missing")
    workloads = dbt_release_artifact_inventory(artifacts.get("workload_packs"))
    require_dbt_runtime_workload_trio(workloads, workload_id=workload_id, payload_ids=payload_ids)
    payloads = dbt_runtime_payload_inventory(artifacts.get("runtime_payloads"))
    if any(item_id not in payloads for item_id in payload_ids):
        raise ValueError("dbt runtime trio is missing from its release")
    return tuple(payloads[item_id] for item_id in payload_ids)


def require_dbt_runtime_workload_trio(
    workloads: Mapping[str, Mapping[str, object]],
    *,
    workload_id: str,
    payload_ids: tuple[str, ...],
) -> None:
    """Bind one ordered trio using an already duplicate-checked workload index.

    Full-source callers validate the complete payload inventory once and then
    reuse this constant-size membership check for each workflow. This does not
    validate payload descriptors, their bytes or release authority itself.
    """

    validate_dbt_runtime_payload_trio(payload_ids, wire_contract=DBT_RUNTIME_WIRE_V2)
    workload = workloads.get(workload_id)
    declared = workload.get("runtime_payload_ids") if workload is not None else None
    if not isinstance(declared, (list, tuple)) or tuple(declared) != payload_ids:
        raise ValueError("dbt runtime trio differs from its release workload")


def dbt_release_artifact_inventory(value: object, *, limit: int = 50_000) -> dict[str, Mapping[str, object]]:
    """Index bounded release metadata without hiding duplicate identities."""

    if not isinstance(value, list) or not 0 < len(value) <= limit:
        raise ValueError("dbt release inventory must be a nonempty bounded array")
    result: dict[str, Mapping[str, object]] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError("dbt release descriptor is invalid")
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id or item_id in result:
            raise ValueError("dbt release descriptor identity is invalid or duplicate")
        result[item_id] = item
    return result


__all__ = [
    "DbtReleaseArtifactIndex",
    "bind_dbt_runtime_workload",
    "dbt_release_artifact_inventory",
    "dbt_runtime_payload_inventory",
    "require_dbt_runtime_workload_trio",
]
