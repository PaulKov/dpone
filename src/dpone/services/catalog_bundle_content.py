"""Semantic validation and exact-byte collection for catalog bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.contracts.credential_security import forbidden_inline_secret_paths
from dpone.services.catalog_supply_chain_support import (
    MAX_CONTROL_NODES,
    MAX_CONTROL_TOKENS,
    MAX_FILE_BYTES,
    MAX_PAYLOAD_FILES,
    CatalogBundleError,
    RecipeBundleSupportError,
    collect_recipe_bundle_files,
    parse_bounded_mapping,
    read_project_file,
    safe_project_relative,
    sha256_bytes,
    validate_recipe_bundle,
    validate_registered_schema,
)


@dataclass(frozen=True, slots=True)
class CatalogPayloadFile:
    logical_id: str
    payload_path: str
    content: bytes


class CatalogBundleContentCollector:
    """Validate one closed bundle kind and return the exact bytes to publish."""

    def collect(
        self,
        *,
        project_root: Path,
        source: str,
        kind: str,
        environment: str | None,
    ) -> tuple[CatalogPayloadFile, ...]:
        root = project_root.resolve(strict=True)
        relative_source = _relative(root, source)
        if kind == "recipe_catalog":
            files = _recipe_catalog(root, relative_source)
        elif kind == "connection_registry":
            files = _connection_registry(root, relative_source, environment)
        else:
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_KIND_INVALID", "Catalog bundle kind is unsupported.")
        if len(files) > MAX_PAYLOAD_FILES:
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_LIMIT_EXCEEDED", "Catalog bundle has too many files.")
        return files


def validate_verified_bundle_content(
    *,
    kind: str,
    entrypoint: bytes,
    artifacts: Mapping[str, bytes],
    environment: str | None,
) -> None:
    """Re-run semantic validation without trusting the builder's decision."""

    try:
        if kind == "recipe_catalog":
            validate_recipe_bundle(
                entrypoint,
                artifacts,
                max_bytes=MAX_FILE_BYTES,
                max_tokens=MAX_CONTROL_TOKENS,
                max_nodes=MAX_CONTROL_NODES,
            )
            return
        if kind != "connection_registry":
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_KIND_INVALID", "Catalog bundle kind is unsupported.")
        _validate_registry(parse_bounded_mapping(entrypoint, max_bytes=MAX_FILE_BYTES), environment)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_CONTENT_INVALID", "Catalog semantic validation failed.") from exc


def _recipe_catalog(root: Path, source: str) -> tuple[CatalogPayloadFile, ...]:
    try:
        files = collect_recipe_bundle_files(root, source, max_bytes=MAX_FILE_BYTES)
    except RecipeBundleSupportError as exc:
        code = {
            "source_changed": "DPONE_CATALOG_SOURCE_CHANGED",
            "path_unsafe": "DPONE_CATALOG_PATH_UNSAFE",
        }.get(exc.code, "DPONE_CATALOG_CONTENT_INVALID")
        raise CatalogBundleError(code, "Recipe catalog validation failed.") from exc
    result = [CatalogPayloadFile("catalog", "payload/catalog.yaml", files[0].content)]
    payload_paths = {"payload/catalog.yaml"}
    for item in files[1:]:
        digest_hex = _sha256_hex(item.content)
        payload_path = f"payload/artifacts/{digest_hex}.yaml"
        if payload_path in payload_paths:
            raise CatalogBundleError(
                "DPONE_CATALOG_CONTENT_INVALID",
                "Recipe catalog contains duplicate artifact bytes under different logical identities.",
            )
        payload_paths.add(payload_path)
        result.append(CatalogPayloadFile(item.logical_id, payload_path, item.content))
    return tuple(result)


def _connection_registry(root: Path, source: str, environment: str | None) -> tuple[CatalogPayloadFile, ...]:
    try:
        source_bytes = read_project_file(root, source, max_bytes=MAX_FILE_BYTES)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_PATH_UNSAFE", "Catalog file could not be read safely.") from exc
    try:
        payload = parse_bounded_mapping(source_bytes, max_bytes=MAX_FILE_BYTES)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_CONTENT_INVALID", "Connection registry is invalid.") from exc
    _validate_registry(payload, environment)
    try:
        changed = source_bytes != read_project_file(root, source, max_bytes=MAX_FILE_BYTES)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_PATH_UNSAFE", "Catalog file could not be read safely.") from exc
    if changed:
        raise CatalogBundleError("DPONE_CATALOG_SOURCE_CHANGED", "Catalog source changed while it was built.")
    return (CatalogPayloadFile("connection_registry", "payload/connection-registry.yaml", source_bytes),)


def _validate_registry(payload: Mapping[str, object], environment: str | None) -> None:
    if not validate_registered_schema(
        payload,
        "dpone.connection-registry.v1",
    ) or forbidden_inline_secret_paths(payload):
        raise CatalogBundleError(
            "DPONE_CATALOG_CONTENT_INVALID",
            "Connection registry schema or secret-reference policy is invalid.",
        )
    actual_environment = payload.get("environment")
    if environment is not None and actual_environment not in {None, environment}:
        raise CatalogBundleError(
            "DPONE_CATALOG_CONTENT_INVALID",
            "Connection registry environment does not match the bundle environment.",
        )


def _relative(root: Path, value: str) -> str:
    try:
        return safe_project_relative(root, value)
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError(
            "DPONE_CATALOG_PATH_UNSAFE", "Catalog path must stay inside the project root."
        ) from exc


def _sha256_hex(content: bytes) -> str:
    return sha256_bytes(content).removeprefix("sha256:")


__all__ = ["CatalogBundleContentCollector", "CatalogPayloadFile", "validate_verified_bundle_content"]
