"""Deterministic, create-only catalog bundle publication."""

from __future__ import annotations

from pathlib import Path

from dpone.services.catalog_bundle_content import CatalogBundleContentCollector, CatalogPayloadFile
from dpone.services.catalog_bundle_io import (
    cleanup_directory,
    create_staging_directory,
    listed_regular_files,
    publish_directory,
    read_bundle_file,
    write_private_file,
)
from dpone.services.catalog_supply_chain_support import (
    BUNDLE_SCHEMA,
    MAX_FILE_BYTES,
    MAX_TOTAL_BYTES,
    CatalogBundleArtifact,
    CatalogBundleBuildRequest,
    CatalogBundleBuildResult,
    CatalogBundleError,
    bundle_id,
    canonical_json_bytes,
    sha256_bytes,
)


class CatalogBundleBuilder:
    """Build one environment-safe immutable directory from validated source."""

    def __init__(self, *, collector: CatalogBundleContentCollector | None = None) -> None:
        self._collector = collector or CatalogBundleContentCollector()

    def build(self, request: CatalogBundleBuildRequest) -> CatalogBundleBuildResult:
        _validate_request(request)
        project_root = Path(request.project_root).resolve(strict=True)
        bundle_root = _output_root(project_root, request.bundle_root)
        files = self._collector.collect(
            project_root=project_root,
            source=request.source,
            kind=request.kind,
            environment=request.environment,
        )
        total_bytes = sum(len(item.content) for item in files)
        if total_bytes > MAX_TOTAL_BYTES or any(len(item.content) > MAX_FILE_BYTES for item in files):
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_LIMIT_EXCEEDED", "Catalog payload exceeds safety limits.")
        artifacts = tuple(
            CatalogBundleArtifact(item.logical_id, item.payload_path, sha256_bytes(item.content), len(item.content))
            for item in files
        )
        payload = {
            "schema": BUNDLE_SCHEMA,
            "kind": request.kind,
            "publisher_id": request.publisher_id,
            "environment": request.environment,
            "entrypoint": files[0].payload_path,
            "artifacts": [item.to_dict() for item in artifacts],
        }
        identity = bundle_id(payload)
        manifest = {**payload, "bundle_id": identity}
        manifest_bytes = canonical_json_bytes(manifest)
        destination = bundle_root / identity.replace(":", "-")
        if destination.exists():
            _verify_existing(destination, manifest_bytes, files)
            return _result(identity, destination, len(files), "no_op")
        staging = create_staging_directory(bundle_root)
        try:
            for item in files:
                write_private_file(staging / item.payload_path, item.content)
            write_private_file(staging / "catalog-bundle.json", manifest_bytes)
            write_private_file(staging / "_SUCCESS", identity.encode("ascii") + b"\n")
            try:
                publish_directory(staging, destination)
            except FileExistsError:
                _verify_existing(destination, manifest_bytes, files)
                return _result(identity, destination, len(files), "no_op")
            return _result(identity, destination, len(files), "created")
        finally:
            cleanup_directory(staging)


def _verify_existing(destination: Path, manifest: bytes, files: tuple[CatalogPayloadFile, ...]) -> None:
    expected = {"_SUCCESS", "catalog-bundle.json"}
    expected.update(item.payload_path for item in files)
    if set(listed_regular_files(destination)) != expected:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_CONFLICT", "Existing bundle directory differs.")
    if read_bundle_file(destination, "catalog-bundle.json") != manifest:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_CONFLICT", "Existing bundle manifest differs.")
    identity = destination.name.replace("sha256-", "sha256:", 1)
    if read_bundle_file(destination, "_SUCCESS", max_bytes=256) != identity.encode() + b"\n":
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_CONFLICT", "Existing bundle is incomplete.")
    for item in files:
        if read_bundle_file(destination, item.payload_path) != item.content:
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_CONFLICT", "Existing bundle payload differs.")


def _validate_request(request: CatalogBundleBuildRequest) -> None:
    if request.kind not in {"recipe_catalog", "connection_registry"}:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_KIND_INVALID", "Catalog bundle kind is unsupported.")
    if not request.publisher_id.strip() or len(request.publisher_id) > 256:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INPUT_INVALID", "Publisher id is invalid.")
    if request.kind == "connection_registry" and not request.environment:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INPUT_INVALID", "Connection registry requires environment.")


def _output_root(project_root: Path, value: str) -> Path:
    raw = Path(value)
    resolved = raw.resolve(strict=False) if raw.is_absolute() else (project_root / raw).resolve(strict=False)
    try:
        resolved.relative_to(project_root)
    except ValueError as exc:
        raise CatalogBundleError("DPONE_CATALOG_PATH_UNSAFE", "Bundle root must stay inside project root.") from exc
    return resolved


def _result(identity: str, destination: Path, count: int, status: str) -> CatalogBundleBuildResult:
    return CatalogBundleBuildResult(
        bundle_id=identity,
        bundle_dir=destination.as_posix(),
        manifest_path=(destination / "catalog-bundle.json").as_posix(),
        artifacts=count,
        status=status,
    )


__all__ = ["CatalogBundleBuilder"]
