"""Verify a live sample source against its immutable workload pack."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


class PinnedWorkloadSourceError(ValueError):
    """Reject a missing, corrupt, or mismatched primary-source pin."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class VerifiedPinnedWorkloadSource:
    workload_id: str
    manifest_path: str
    manifest_sha256: str
    pack_sha256: str
    authoring_dependencies: tuple[tuple[str, str], ...] = ()
    file_dependencies: tuple[tuple[str, str], ...] = ()


class PinnedWorkloadSourceVerifier:
    """Verify indexed pack bytes and their exact primary-manifest dependency."""

    def verify_snapshot(
        self,
        *,
        workload_id: str,
        release_id: str,
        indexed_packs: Iterable[Mapping[str, Any]],
        source_path: str,
        source_sha256: str,
        pack_reader: Callable[[str], bytes],
    ) -> VerifiedPinnedWorkloadSource:
        """Verify a serialized plan snapshot against its pinned workload pack."""

        pack, packed_workload_id, manifest_path, manifest_sha, actual_pack_sha = _verified_pack(
            workload_id=workload_id,
            release_id=release_id,
            indexed_packs=indexed_packs,
            pack_reader=pack_reader,
        )
        del pack
        if source_path != manifest_path or _normalized_digest(source_sha256) != manifest_sha:
            raise PinnedWorkloadSourceError(
                "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
                "Pipeline source snapshot does not match the pinned workload pack; rebuild the deployment.",
            )
        return VerifiedPinnedWorkloadSource(
            workload_id=packed_workload_id,
            manifest_path=manifest_path,
            manifest_sha256="sha256:" + manifest_sha,
            pack_sha256=actual_pack_sha,
        )

    def verify(
        self,
        *,
        workload_id: str,
        release_id: str,
        indexed_packs: Iterable[Mapping[str, Any]],
        source_path: Path,
        source_sha256: str,
        source_dependencies: Iterable[Mapping[str, Any]] = (),
        source_dependency_digester: Callable[[str], str] | None = None,
        source_root: str | Path,
        pack_reader: Callable[[str], bytes],
    ) -> VerifiedPinnedWorkloadSource:
        pack, packed_workload_id, manifest_path, manifest_sha, actual_pack_sha = _verified_pack(
            workload_id=workload_id,
            release_id=release_id,
            indexed_packs=indexed_packs,
            pack_reader=pack_reader,
        )
        _verify_source_path(
            source_path,
            manifest_path=manifest_path,
            source_root=Path(source_root),
        )
        if _normalized_digest(source_sha256) != manifest_sha:
            raise PinnedWorkloadSourceError(
                "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
                "Pipeline source content does not match the pinned workload pack; rebuild the deployment.",
            )
        authoring_dependencies = _verify_authoring_dependencies(pack, source_dependencies)
        file_dependencies = _verify_file_dependencies(pack, source_dependency_digester)
        return VerifiedPinnedWorkloadSource(
            workload_id=packed_workload_id,
            manifest_path=manifest_path,
            manifest_sha256="sha256:" + manifest_sha,
            pack_sha256=actual_pack_sha,
            authoring_dependencies=authoring_dependencies,
            file_dependencies=file_dependencies,
        )


def verify_execution_plan_source_pin(
    plan: Any,
    *,
    pack_reader: Callable[[str], bytes],
) -> VerifiedPinnedWorkloadSource:
    """Verify a runtime plan snapshot before init-fetch or evidence side effects."""

    context = getattr(plan, "deployment_context", None)
    target = getattr(plan, "temporary_target_plan", None)
    snapshot = getattr(plan, "source_snapshot", None)
    if context is None or target is None or snapshot is None:
        raise _pin_missing("Safe-sample runtime requires a canonical source snapshot and pinned workload pack.")
    if str(getattr(snapshot, "pipeline_id", "") or "") != str(getattr(target, "pipeline_id", "") or ""):
        raise PinnedWorkloadSourceError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source snapshot identity does not match the selected target plan.",
        )

    return PinnedWorkloadSourceVerifier().verify_snapshot(
        workload_id=str(getattr(target, "pipeline_id", "") or ""),
        release_id=str(getattr(context, "release_id", "") or ""),
        indexed_packs=tuple(getattr(context, "workload_packs", ()) or ()),
        source_path=str(getattr(snapshot, "path", "") or ""),
        source_sha256=str(getattr(snapshot, "sha256", "") or ""),
        pack_reader=pack_reader,
    )


def _verified_pack(
    *,
    workload_id: str,
    release_id: str,
    indexed_packs: Iterable[Mapping[str, Any]],
    pack_reader: Callable[[str], bytes],
) -> tuple[Mapping[str, Any], str, str, str, str]:
    if not workload_id or not release_id:
        raise _pin_missing("Safe-sample execution requires a pinned workload pack.")

    matching = [pack for pack in indexed_packs if str(pack.get("id") or "") == workload_id]
    if len(matching) != 1:
        raise _pin_missing("Pinned deployment must contain exactly one pack for the selected workload.")
    indexed_pack = matching[0]
    artifact_ref = str(indexed_pack.get("artifact_ref") or "")
    expected_pack_sha = str(indexed_pack.get("sha256") or "")
    _require_pinned_release_ref(artifact_ref, release_id=release_id)

    try:
        pack_bytes = pack_reader(artifact_ref)
    except Exception as exc:  # noqa: BLE001 - adapter errors become stable contract errors.
        code = str(getattr(exc, "code", "DPONE_CACHE_ARTIFACT_READ_FAILED"))
        raise PinnedWorkloadSourceError(code, "Pinned workload pack could not be read safely.") from exc
    actual_pack_sha = _sha256(pack_bytes)
    if actual_pack_sha != expected_pack_sha:
        raise PinnedWorkloadSourceError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "Pinned workload pack checksum does not match the deployment index.",
        )
    declared_bytes = indexed_pack.get("bytes")
    if isinstance(declared_bytes, int) and declared_bytes != len(pack_bytes):
        raise PinnedWorkloadSourceError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "Pinned workload pack size does not match the deployment index.",
        )

    pack = _pack_mapping(pack_bytes)
    workload = _mapping(pack.get("workload"))
    packed_workload_id = str(workload.get("workload_id") or "")
    manifest_path = str(workload.get("manifest") or "")
    if packed_workload_id != workload_id or not manifest_path:
        raise _pin_missing("Pinned workload pack does not identify the selected workload manifest.")
    manifest_sha = _manifest_dependency_sha(pack, manifest_path=manifest_path)
    return pack, packed_workload_id, manifest_path, manifest_sha, actual_pack_sha


def _pack_mapping(payload: bytes) -> Mapping[str, Any]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _pin_missing("Pinned workload pack is not valid JSON.") from exc
    if not isinstance(decoded, Mapping) or decoded.get("kind") != "gitops.airflow_pack":
        raise _pin_missing("Pinned workload pack has an unsupported contract.")
    return decoded


def _manifest_dependency_sha(pack: Mapping[str, Any], *, manifest_path: str) -> str:
    dependencies = pack.get("workload_dependencies")
    if not isinstance(dependencies, list):
        raise _pin_missing("Pinned workload pack does not contain a primary manifest dependency.")
    matches = [
        item
        for item in dependencies
        if isinstance(item, Mapping) and item.get("kind") == "manifest" and str(item.get("path") or "") == manifest_path
    ]
    if len(matches) != 1:
        raise _pin_missing("Pinned workload pack must contain exactly one matching manifest dependency.")
    digest = _normalized_digest(str(matches[0].get("sha256") or ""))
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise _pin_missing("Pinned workload manifest dependency has an invalid SHA-256 digest.")
    return digest


def _verify_authoring_dependencies(
    pack: Mapping[str, Any],
    source_dependencies: Iterable[Mapping[str, Any]],
) -> tuple[tuple[str, str], ...]:
    raw_pack_dependencies = pack.get("workload_dependencies")
    if not isinstance(raw_pack_dependencies, list):
        raise _pin_missing("Pinned workload pack does not contain workload dependencies.")
    kinds = ("authoring_fragment", "recipe", "profile", "component")
    packed = tuple(
        sorted(
            (kind, *dependency)
            for kind in kinds
            for dependency in _normalized_dependencies(raw_pack_dependencies, kind=kind, label="authoring source")
        )
    )
    compiled = tuple(
        sorted(
            (kind, *dependency)
            for kind in kinds
            for dependency in _normalized_dependencies(source_dependencies, kind=kind, label="authoring source")
        )
    )
    if packed != compiled:
        raise PinnedWorkloadSourceError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Compiled authoring sources do not match the pinned workload pack; rebuild the deployment.",
        )
    return tuple((path, digest) for _kind, path, digest in packed)


def _verify_file_dependencies(
    pack: Mapping[str, Any],
    digester: Callable[[str], str] | None,
) -> tuple[tuple[str, str], ...]:
    raw_dependencies = pack.get("workload_dependencies")
    if not isinstance(raw_dependencies, list):
        raise _pin_missing("Pinned workload pack does not contain workload dependencies.")
    packed = _normalized_dependencies(raw_dependencies, kind="sql_file", label="SQL file")
    if packed and digester is None:
        raise _pin_missing("Pinned SQL dependencies require a confined source digester.")
    for path, expected in packed:
        try:
            actual = _normalized_digest(digester(path) if digester is not None else "")
        except Exception as exc:  # noqa: BLE001 - filesystem adapters become stable pin failures.
            raise PinnedWorkloadSourceError(
                "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
                "A pinned SQL dependency could not be read safely; rebuild or restore the deployment source.",
            ) from exc
        if "sha256:" + actual != expected:
            raise PinnedWorkloadSourceError(
                "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
                "A SQL dependency does not match the pinned workload pack; rebuild the deployment.",
            )
    return packed


def _normalized_dependencies(
    dependencies: Iterable[Mapping[str, Any]],
    *,
    kind: str,
    label: str,
) -> tuple[tuple[str, str], ...]:
    normalized: list[tuple[str, str]] = []
    seen: set[str] = set()
    for dependency in dependencies:
        if dependency.get("kind") != kind:
            continue
        path = str(dependency.get("path") or "")
        digest = _normalized_digest(str(dependency.get("sha256") or ""))
        if (
            not _valid_dependency_path(path)
            or path in seen
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise _pin_missing(f"Pinned {label} dependency is invalid or duplicated.")
        seen.add(path)
        normalized.append((path, "sha256:" + digest))
    return tuple(sorted(normalized))


def _valid_dependency_path(value: str) -> bool:
    path = PurePosixPath(value)
    return bool(value) and "\\" not in value and not path.is_absolute() and ".." not in path.parts


def _verify_source_path(source_path: Path, *, manifest_path: str, source_root: Path) -> None:
    repo_root = source_root.resolve(strict=False)
    try:
        actual = source_path.resolve(strict=False).relative_to(repo_root).as_posix()
    except ValueError as exc:
        raise PinnedWorkloadSourceError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source is outside the repository that owns the pinned deployment.",
        ) from exc
    if actual != manifest_path:
        raise PinnedWorkloadSourceError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source path does not match the pinned workload manifest.",
        )


def _require_pinned_release_ref(artifact_ref: str, *, release_id: str) -> None:
    release_dir = release_id.replace(":", "-")
    if not artifact_ref.startswith(f"cache://releases/{release_dir}/"):
        raise PinnedWorkloadSourceError(
            "DPONE_CACHE_UNPINNED_REFERENCE",
            "Workload pack reference does not belong to the pinned release.",
        )


def _normalized_digest(value: str) -> str:
    return value.removeprefix("sha256:").strip().lower()


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _pin_missing(message: str) -> PinnedWorkloadSourceError:
    return PinnedWorkloadSourceError("DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING", message)


__all__ = [
    "PinnedWorkloadSourceError",
    "PinnedWorkloadSourceVerifier",
    "VerifiedPinnedWorkloadSource",
    "verify_execution_plan_source_pin",
]
