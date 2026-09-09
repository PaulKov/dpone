"""Version dispatch and legacy identity for immutable dbt source snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_source_inventory import MAX_DBT_SOURCE_INVENTORY_BYTES, DbtSourceInventory
from dpone.contracts.strict_json import strict_json_object

DBT_LEGACY_SOURCE_SNAPSHOT_SCHEMA = "dpone.dbt-source-snapshot.v1"
_LEGACY_KEYS = frozenset({"schema", "project_bundle_sha256", "manifest_sha256", "snapshot_sha256"})


@dataclass(frozen=True, slots=True)
class LegacyDbtSourceSnapshot:
    """Published singleton identity, without inventing absent project metadata."""

    project_bundle_sha256: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not all(is_canonical_sha256_digest(item) for item in (self.project_bundle_sha256, self.manifest_sha256)):
            raise ValueError("legacy dbt source snapshot digest is invalid")

    @property
    def snapshot_sha256(self) -> str:
        return canonical_fingerprint(self._unsigned())

    def _unsigned(self) -> dict[str, str]:
        return {
            "schema": DBT_LEGACY_SOURCE_SNAPSHOT_SCHEMA,
            "project_bundle_sha256": self.project_bundle_sha256,
            "manifest_sha256": self.manifest_sha256,
        }

    def to_dict(self) -> dict[str, str]:
        unsigned = self._unsigned()
        return {**unsigned, "snapshot_sha256": canonical_fingerprint(unsigned)}

    @classmethod
    def from_mapping(cls, value: object) -> LegacyDbtSourceSnapshot:
        if (
            not isinstance(value, Mapping)
            or set(value) != _LEGACY_KEYS
            or value.get("schema") != DBT_LEGACY_SOURCE_SNAPSHOT_SCHEMA
        ):
            raise ValueError("legacy dbt source snapshot fields or version are invalid")
        project, manifest = value["project_bundle_sha256"], value["manifest_sha256"]
        if not isinstance(project, str) or not isinstance(manifest, str):
            raise ValueError("legacy dbt source snapshot digest is invalid")
        result = cls(project, manifest)
        if value["snapshot_sha256"] != result.snapshot_sha256:
            raise ValueError("legacy dbt source snapshot fingerprint is invalid")
        return result


def read_dbt_source_snapshot(
    payload: bytes,
    *,
    wire_contract: str,
) -> LegacyDbtSourceSnapshot | DbtSourceInventory:
    """Parse bounded bytes under a verified release wire, rejecting mixed versions."""

    if not isinstance(payload, bytes) or not payload or len(payload) > MAX_DBT_SOURCE_INVENTORY_BYTES:
        raise ValueError("dbt source snapshot bytes are missing or exceed the bound")
    if wire_contract == DBT_RUNTIME_WIRE_V2:
        return DbtSourceInventory.from_payload(payload)
    if wire_contract != DBT_RUNTIME_WIRE_V1:
        raise ValueError("unsupported dbt source snapshot wire")
    try:
        return LegacyDbtSourceSnapshot.from_mapping(strict_json_object(payload))
    except RecursionError as exc:
        raise ValueError("dbt source snapshot nesting exceeds its bound") from exc


__all__ = ["DBT_LEGACY_SOURCE_SNAPSHOT_SCHEMA", "LegacyDbtSourceSnapshot", "read_dbt_source_snapshot"]
