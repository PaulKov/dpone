"""Truthful compatibility operations retained for deprecated Studio endpoints."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.capability_discovery_protocols import CapabilitySnapshotProvider
from dpone.readiness.capability_discovery_service import normalize_connector_ref
from dpone.readiness.capability_legacy_projection import (
    legacy_studio_certification_payload,
)
from dpone.readiness.managed import PerformanceAdvisor, StateInspectorService
from dpone.readiness.studio_api_catalog import (
    capabilities_payload,
    observability_slo_payload,
    schema_explorer_payload,
)
from dpone.readiness.studio_authoring import StudioAuthoringService, required_text
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_http_models import StudioAuditLog

_MAX_RUN_SCAN_ENTRIES = 5000
_MAX_RUN_SCAN_DIRECTORIES = 512
_MAX_RUN_DEPTH = 8
_MAX_RUN_ARTIFACTS = 1000


class StudioLegacyOperations:
    """Keep deprecated read-only endpoints safe during the migration window."""

    def __init__(
        self,
        *,
        root: Path,
        capabilities: CapabilitySnapshotProvider,
        authoring: StudioAuthoringService,
        audit: StudioAuditLog,
        doctor: Callable[[], dict[str, Any]],
        artifact_roots: tuple[str, ...] = (".dpone/runs", "test_artifacts"),
        state: StateInspectorService,
        perf: PerformanceAdvisor,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._capabilities = capabilities
        self._authoring = authoring
        self._audit = audit
        self._doctor = doctor
        self._artifact_roots = artifact_roots
        self._state = state
        self._perf = perf

    def state_inspect(self, query: Mapping[str, str]) -> dict[str, Any]:
        return self._state.inspect(
            query.get("backend", "postgres"),
            query.get("state_type", "xmin"),
            query.get("identity", "default"),
        )

    def runs(self, *, limit: int = 200, cursor: int = 0) -> dict[str, Any]:
        if not 1 <= limit <= 200 or cursor < 0:
            raise StudioError(
                "DPONE_STUDIO_PAGE_LIMIT_INVALID",
                "Run artifact pagination must use limit 1..200 and a non-negative cursor.",
            )
        artifacts: list[dict[str, Any]] = []
        roots = []
        scanned_entries = 0
        scanned_directories = 0
        truncated = False
        for configured in self._artifact_roots:
            relative = self._authoring.project_relative(configured)
            root = self._root / relative
            roots.append(relative)
            if not root.exists() or root.is_symlink() or not root.is_dir():
                continue
            stack = [(root, 0)]
            while stack:
                directory, depth = stack.pop()
                scanned_directories += 1
                if scanned_directories > _MAX_RUN_SCAN_DIRECTORIES:
                    truncated = True
                    break
                try:
                    with os.scandir(directory) as scanner:
                        entries = sorted(scanner, key=lambda entry: entry.name)
                except OSError:
                    continue
                for entry in entries:
                    scanned_entries += 1
                    if scanned_entries > _MAX_RUN_SCAN_ENTRIES:
                        truncated = True
                        break
                    try:
                        metadata = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    path = Path(entry.path)
                    if stat.S_ISDIR(metadata.st_mode) and depth < _MAX_RUN_DEPTH:
                        stack.append((path, depth + 1))
                    elif (
                        stat.S_ISREG(metadata.st_mode) and path.suffix == ".md" and len(artifacts) < _MAX_RUN_ARTIFACTS
                    ):
                        artifacts.append(
                            {
                                "path": path.relative_to(self._root).as_posix(),
                                "name": path.name,
                                "kind": "markdown",
                                "modified_at": metadata.st_mtime,
                            }
                        )
                if truncated:
                    break
            if truncated:
                break
        artifacts.sort(key=lambda item: item["path"])
        page = artifacts[cursor : cursor + limit]
        next_offset = cursor + len(page)
        return {
            "artifact_roots": roots,
            "artifacts": page,
            "cursor": cursor,
            "next_cursor": str(next_offset) if next_offset < len(artifacts) else None,
            "discovered": len(artifacts),
            "scanned_entries": scanned_entries,
            "truncated": truncated or len(artifacts) >= _MAX_RUN_ARTIFACTS,
        }

    def performance(self) -> dict[str, Any]:
        return {
            "recommendations": [
                item.to_dict()
                for item in self._perf.advise(
                    source_type="postgres",
                    sink_type="mssql",
                    strategy="incremental_merge",
                    options={"estimated_rows": 5_000_000},
                )
            ]
        }

    def doctor(self) -> dict[str, Any]:
        return self._doctor()

    def connection_capabilities(self) -> dict[str, Any]:
        capabilities = self._capabilities()
        payload = capabilities_payload()
        payload.update(
            {
                "sources": sorted(
                    _legacy_connector_id(item.id) for item in capabilities.connectors if "source" in item.roles
                ),
                "sinks": sorted(
                    _legacy_connector_id(item.id) for item in capabilities.connectors if "sink" in item.roles
                ),
                "strategies": sorted({item.strategy for item in capabilities.routes}),
                "snapshot_id": capabilities.snapshot_id,
            }
        )
        return payload

    def certification_matrix(self) -> dict[str, Any]:
        return legacy_studio_certification_payload(self._capabilities())

    def audit_events(self, *, limit: int) -> dict[str, Any]:
        return self._audit.snapshot(limit=limit)

    def record_audit(
        self,
        *,
        method: str,
        path: str,
        status: int,
        actor: str,
    ) -> None:
        self._audit.record(
            method=method,
            path=path,
            status=status,
            actor=actor,
        )

    @staticmethod
    def security_policy(
        *,
        token_required: bool,
        remote_enabled: bool,
    ) -> dict[str, Any]:
        return {
            "mode": "shared_token" if token_required else "local_only",
            "token_required": token_required,
            "remote_enabled": remote_enabled,
            "rbac": "not_implemented",
            "production_remote_deployment": "not_supported",
            "guardrails": [
                "loopback binding by default",
                "exact-origin CORS",
                "read-only v1 application API",
                "bounded request body, concurrency, pagination, and audit memory",
            ],
        }

    @staticmethod
    def observability_slo() -> dict[str, Any]:
        return observability_slo_payload()

    @staticmethod
    def deployment_guide() -> dict[str, Any]:
        return {
            "topology": "local_development_adapter",
            "command": {
                "argv": [
                    "dpone",
                    "studio",
                    "--serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8765",
                ]
            },
            "remote": {
                "status": "not_production_supported",
                "requirements": [
                    "--allow-remote",
                    "DPONE_STUDIO_TOKEN",
                    "exact --cors-origin values",
                ],
            },
            "rbac": "not_implemented",
        }

    def schema_explorer(self, query: dict[str, str]) -> dict[str, Any]:
        source = query.get("source", "postgres")
        sink = query.get("sink", "mssql")
        known = {item.id for item in self._capabilities().connectors}
        if _connector_id(source) not in known or _connector_id(sink) not in known:
            raise StudioError(
                "DPONE_ROUTE_NOT_SUPPORTED",
                "Schema explorer source or sink is not a declared connector.",
            )
        return schema_explorer_payload({"source": source, "sink": sink})

    def reconciliation_preview(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if set(payload) - {"source_type", "sink_type", "unique_key", "apply_deletes"}:
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "Reconciliation fields do not match the Studio API schema.",
            )
        source = required_text(payload, "source_type", default="postgres")
        sink = required_text(payload, "sink_type", default="mssql")
        self._authoring.require_supported_route(source, sink, "snapshot_diff")
        apply_deletes = payload.get("apply_deletes", False)
        if not isinstance(apply_deletes, bool):
            raise StudioError(
                "DPONE_STUDIO_BOOLEAN_INVALID",
                "apply_deletes must be a JSON boolean.",
            )
        return {
            "source_type": source,
            "sink_type": sink,
            "unique_key": required_text(payload, "unique_key", default="id"),
            "apply_deletes": apply_deletes,
            "strategy": "snapshot_reconciliation",
            "target_policy": _target_delete_policy(sink),
            "warnings": (
                [] if apply_deletes else ["physical deletes are previewed only; set apply_deletes=true to enable"]
            ),
        }


def _connector_id(value: str) -> str:
    return normalize_connector_ref(value)


def _legacy_connector_id(value: str) -> str:
    return "api" if value == "rest" else value


def _target_delete_policy(sink: str) -> str:
    policies = {
        "mssql": "staging table plus set-based delete/merge in transaction",
        "postgres": "temporary/staging table plus set-based delete/merge in transaction",
        "clickhouse": "shadow-table replacement or tombstone model",
        "bigquery": "staging table plus MERGE/DELETE under configured policy",
        "kafka": "keyed tombstones only when deletes are enabled and the key is non-null",
    }
    try:
        return policies[sink]
    except KeyError as exc:
        raise StudioError(
            "DPONE_ROUTE_NOT_SUPPORTED",
            "Delete reconciliation is not supported for the requested sink.",
        ) from exc


__all__ = ["StudioLegacyOperations"]
