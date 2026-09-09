"""Descriptor-safe, bounded Studio planning query."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.managed import ExecutionPlanService
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_manifest_validation import (
    MAX_STUDIO_MANIFEST_BYTES,
    validate_manifest_size,
)
from dpone.readiness.studio_process_evidence import StudioProcessEvidencePolicy
from dpone.readiness.studio_project_paths import StudioProjectPathPolicy
from dpone.readiness.studio_route_validation import StudioRouteValidator
from dpone.services.manifest import resolve_single_process


class StudioManifestPlanningService:
    """Plan one explicit manifest source from a stable byte snapshot."""

    def __init__(
        self,
        *,
        root: Path,
        planner: ExecutionPlanService,
        paths: StudioProjectPathPolicy,
        routes: StudioRouteValidator,
        evidence: StudioProcessEvidencePolicy,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._planner = planner
        self._paths = paths
        self._routes = routes
        self._evidence = evidence

    def plan(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        unknown = sorted(set(payload) - {"manifest_path", "manifest_yaml", "selector"})
        if unknown:
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "Request fields do not match the Studio API schema.",
            )
        selector = payload.get("selector")
        manifest_path_present = "manifest_path" in payload
        manifest_yaml_present = "manifest_yaml" in payload
        if manifest_path_present == manifest_yaml_present:
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "Exactly one of manifest_path or manifest_yaml is required.",
            )
        if manifest_path_present:
            relative = self._paths.relative(_required_text(payload, "manifest_path"))
            manifest_yaml = self._read_manifest(relative)
            return self._plan_yaml(
                manifest_yaml,
                selector=selector,
                manifest_dir=(self._root / relative).parent,
            )
        return self._plan_yaml(
            _required_text(payload, "manifest_yaml"),
            selector=selector,
        )

    def _read_manifest(self, relative: str) -> str:
        try:
            return read_confined_file(
                self._root,
                relative,
                max_bytes=MAX_STUDIO_MANIFEST_BYTES,
            ).decode("utf-8")
        except ConfinedFileError as exc:
            code = (
                "DPONE_STUDIO_MANIFEST_TOO_LARGE"
                if exc.code == "file_too_large"
                else "DPONE_STUDIO_MANIFEST_UNAVAILABLE"
            )
            raise StudioError(
                code,
                "Manifest path could not be read as a bounded stable project file.",
            ) from exc
        except UnicodeDecodeError as exc:
            raise StudioError(
                "DPONE_STUDIO_MANIFEST_INVALID",
                "Manifest path must contain UTF-8 text.",
            ) from exc

    def _plan_yaml(
        self,
        manifest_yaml: str,
        *,
        selector: object,
        manifest_dir: Path | None = None,
    ) -> dict[str, Any]:
        validate_manifest_size(manifest_yaml)
        if selector is not None and not isinstance(selector, str):
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "selector must be a string when provided.",
            )
        with tempfile.TemporaryDirectory(prefix="dpone-studio-plan-") as raw_directory:
            temporary_root = Path(raw_directory)
            temporary_path = temporary_root / "manifest.batch.yaml"
            temporary_path.write_text(manifest_yaml, encoding="utf-8")
            try:
                manifest = ManifestLoaderRouter().load(
                    temporary_path,
                    metadata_only=True,
                )
                spec = resolve_single_process(manifest, selector=selector)
                spec = self._evidence.snapshot(
                    spec,
                    manifest_dir=manifest_dir or self._root,
                    snapshot_root=temporary_root,
                )
                self._routes.validate_processes((spec,))
                return self._planner.plan_process(
                    spec,
                    manifest_dir=manifest_dir or self._root,
                )
            except StudioError:
                raise
            except Exception as exc:
                raise StudioError(
                    "DPONE_STUDIO_PLAN_INVALID",
                    "The requested manifest could not produce a safe static plan.",
                ) from exc


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise StudioError(
            "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
            f"{key} must be a non-empty string.",
        )
    return value.strip()


__all__ = ["StudioManifestPlanningService"]
