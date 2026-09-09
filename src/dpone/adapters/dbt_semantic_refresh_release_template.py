"""Read-only verifier for semantic templates in an immutable promoted release."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshReleaseTemplateSubject,
)

_MAX_RELEASE_BYTES = 4 * 1024 * 1024
_MAX_TEMPLATE_BYTES = 16 * 1024 * 1024
_RELEASE_FIELDS = {
    "artifacts",
    "producer",
    "provenance",
    "release_id",
    "schema",
    "selection_authority",
    "selection_fingerprint",
}
_ARTIFACT_FIELDS = {
    "canonical_schemas",
    "dag_specs",
    "runtime_payloads",
    "workload_packs",
}
_DESCRIPTOR_FIELDS = {
    "bytes",
    "id",
    "pack_fingerprint",
    "path",
    "sha256",
}
_TEMPLATE_SEMANTIC_FIELDS = {
    "mode",
    "package_artifacts_sha256",
    "pre_release_bundle_sha256",
    "topology",
}


class ImmutableSemanticRefreshReleaseTemplateVerifier:
    """Authenticate a template descriptor and bytes under one promoted root."""

    def __init__(self, release_root: Path) -> None:
        self._release_root = Path(release_root).absolute()

    def verify(self, subject: SemanticRefreshReleaseTemplateSubject) -> bool:
        """Return false unless the exact subject is reachable from the release set."""

        try:
            if not isinstance(subject, SemanticRefreshReleaseTemplateSubject):
                return False
            release = _json_object(
                _bounded_regular_file(
                    self._release_root,
                    PurePosixPath("release-set.json"),
                    maximum=_MAX_RELEASE_BYTES,
                )
            )
            if (
                set(release) != _RELEASE_FIELDS
                or release.get("schema") != "dpone.release-set.v2"
                or release.get("release_id") != subject.release_id
                or compute_release_id(release) != subject.release_id
            ):
                return False
            artifacts = _mapping(release.get("artifacts"))
            if set(artifacts) != _ARTIFACT_FIELDS:
                return False
            descriptors = artifacts.get("workload_packs")
            if not isinstance(descriptors, list):
                return False
            matches = [
                item
                for item in descriptors
                if isinstance(item, Mapping) and item.get("pack_fingerprint") == subject.template_pack_fingerprint
            ]
            return len(matches) == 1 and _verify_descriptor(
                self._release_root,
                matches[0],
                subject,
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError, RecursionError):
            return False


def _verify_descriptor(
    root: Path,
    descriptor: Mapping[str, object],
    subject: SemanticRefreshReleaseTemplateSubject,
) -> bool:
    fields = set(descriptor)
    if not (_DESCRIPTOR_FIELDS <= fields <= _DESCRIPTOR_FIELDS | {"runtime_payload_ids"}):
        return False
    item_id = descriptor.get("id")
    path = _release_path(descriptor.get("path"), item_id=item_id)
    content = _bounded_regular_file(root, path, maximum=_MAX_TEMPLATE_BYTES)
    if descriptor.get("bytes") != len(content) or descriptor.get("sha256") != _sha256(content):
        return False
    pack = _json_object(content)
    fingerprint = verify_pack_fingerprint(pack)
    semantic = _mapping(pack.get("semantic_refresh"))
    return (
        fingerprint == subject.template_pack_fingerprint
        and descriptor.get("pack_fingerprint") == fingerprint
        and pack.get("activation") == "POST_DEPLOYMENT_AUTHORITY_REQUIRED"
        and pack.get("executable") is False
        and set(semantic) == _TEMPLATE_SEMANTIC_FIELDS
        and semantic.get("mode") == "semantic_refresh_v2_template"
        and semantic.get("pre_release_bundle_sha256") == subject.pre_release_bundle_sha256
        and semantic.get("package_artifacts_sha256") == subject.package_artifacts_sha256
    )


def _release_path(value: object, *, item_id: object) -> PurePosixPath:
    if not isinstance(value, str) or not isinstance(item_id, str) or not item_id:
        raise ValueError("release descriptor locator is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.parts != ("packs", f"{item_id}.airflow-pack.json")
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("release template path is not confined")
    return path


def _bounded_regular_file(root: Path, relative: PurePosixPath, *, maximum: int) -> bytes:
    if not root.is_dir() or root.is_symlink():
        raise ValueError("promoted release root is unavailable")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("promoted release path contains a symbolic link")
    if not current.is_file() or current.stat().st_size > maximum:
        raise ValueError("promoted release file is unavailable or oversized")
    return current.read_bytes()


def _json_object(value: bytes) -> Mapping[str, object]:
    parsed = json.loads(
        value,
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )
    return _mapping(parsed)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("promoted release value must be an object")
    return value


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


__all__ = ["ImmutableSemanticRefreshReleaseTemplateVerifier"]
