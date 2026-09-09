"""Pack / dag-spec storage mode policy (gitops | remote | hybrid)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

PACK_STORAGE_MODES = frozenset({"gitops", "remote", "hybrid"})
PACK_STORAGE_KINDS = frozenset({"s3", "gcs", "azure_blob", "filesystem"})
_MODE_ALIASES = {
    "object_storage": "remote",
    "object-storage": "remote",
    "s3": "remote",
}


@dataclass(frozen=True, slots=True)
class PackStoragePolicy:
    """Resolved storage policy for compiled Airflow artifacts."""

    mode: str
    kind: str | None = None
    uri_prefix: str | None = None
    latest_index_uri: str | None = None
    writer_connection_id: str | None = None
    reader_connection_id: str | None = None

    @property
    def uses_object_storage(self) -> bool:
        return self.mode in {"remote", "hybrid"}

    @property
    def commits_git_artifacts(self) -> bool:
        """Whether CI should treat git HEAD packs/dag-specs as the gate oracle."""
        return self.mode in {"gitops", "hybrid"}

    @property
    def allows_git_fallback(self) -> bool:
        """Whether scheduler may read ``.dpone/gitops`` on cache miss."""
        return self.mode == "hybrid"

    @property
    def requires_remote_publish(self) -> bool:
        return self.mode in {"remote", "hybrid"}


def normalize_pack_storage_mode(raw: object) -> str:
    """Normalize authored mode; default ``gitops``; alias ``object_storage``→``remote``."""
    if raw is None or raw == "":
        return "gitops"
    text = str(raw).strip().lower()
    text = _MODE_ALIASES.get(text, text)
    if text not in PACK_STORAGE_MODES:
        allowed = ", ".join(sorted(PACK_STORAGE_MODES))
        raise ValueError(f"artifacts.airflow_pack.storage.mode must be one of: {allowed}")
    return text


def pack_storage_policy_from_mapping(raw: object) -> PackStoragePolicy:
    """Build policy from ``gitops.artifacts.airflow_pack.storage`` mapping (or empty)."""
    values = raw if isinstance(raw, Mapping) else {}
    mode = normalize_pack_storage_mode(values.get("mode"))
    kind_raw = values.get("kind")
    kind = str(kind_raw).strip().lower() if kind_raw not in (None, "") else None
    if kind is not None and kind not in PACK_STORAGE_KINDS:
        allowed = ", ".join(sorted(PACK_STORAGE_KINDS))
        raise ValueError(f"artifacts.airflow_pack.storage.kind must be one of: {allowed}")
    policy = PackStoragePolicy(
        mode=mode,
        kind=kind,
        uri_prefix=_optional_str(values.get("uri_prefix")),
        latest_index_uri=_optional_str(values.get("latest_index_uri")),
        writer_connection_id=_optional_str(values.get("writer_connection_id")),
        reader_connection_id=_optional_str(values.get("reader_connection_id")),
    )
    validate_pack_storage_policy(policy)
    return policy


def pack_storage_policy_from_workload_set(raw: object) -> PackStoragePolicy:
    """Extract storage policy from a workload-set / gitops.yaml document."""
    root = raw if isinstance(raw, Mapping) else {}
    gitops = root.get("gitops") if isinstance(root.get("gitops"), Mapping) else root
    artifacts = gitops.get("artifacts") if isinstance(gitops, Mapping) else None
    airflow_pack = artifacts.get("airflow_pack") if isinstance(artifacts, Mapping) else None
    storage = airflow_pack.get("storage") if isinstance(airflow_pack, Mapping) else None
    return pack_storage_policy_from_mapping(storage)


def validate_pack_storage_policy(policy: PackStoragePolicy) -> None:
    if policy.mode == "gitops":
        return
    missing: list[str] = []
    if not policy.kind:
        missing.append("kind")
    if not policy.uri_prefix:
        missing.append("uri_prefix")
    if not policy.latest_index_uri:
        missing.append("latest_index_uri")
    if policy.mode == "remote" and not policy.reader_connection_id and policy.kind != "filesystem":
        missing.append("reader_connection_id")
    if missing:
        raise ValueError(
            "artifacts.airflow_pack.storage requires " + ", ".join(missing) + f" when mode={policy.mode!r}"
        )


def _optional_str(raw: object) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def storage_mapping_for_json(policy: PackStoragePolicy) -> dict[str, Any]:
    return {
        "mode": policy.mode,
        "kind": policy.kind,
        "uri_prefix": policy.uri_prefix,
        "latest_index_uri": policy.latest_index_uri,
        "writer_connection_id": policy.writer_connection_id,
        "reader_connection_id": policy.reader_connection_id,
        "commits_git_artifacts": policy.commits_git_artifacts,
        "allows_git_fallback": policy.allows_git_fallback,
        "requires_remote_publish": policy.requires_remote_publish,
    }


__all__ = [
    "PACK_STORAGE_KINDS",
    "PACK_STORAGE_MODES",
    "PackStoragePolicy",
    "normalize_pack_storage_mode",
    "pack_storage_policy_from_mapping",
    "pack_storage_policy_from_workload_set",
    "storage_mapping_for_json",
    "validate_pack_storage_policy",
]
