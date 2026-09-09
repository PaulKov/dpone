"""Version-pinned artifact authority checks at the ClickHouse read boundary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_reader import (
        SemanticRefreshSealedArtifactReader,
        VersionPinnedSealedArtifact,
    )
    from dpone.ports.semantic_refresh_artifact_seal import SemanticRefreshSealAuthorizationPort
    from dpone.ports.semantic_refresh_clickhouse_authority import ClickHousePublicationAuthority


class ClickHouseArtifactAuthorityReader:
    """Resolve and authenticate the exact sealed artifact before ClickHouse I/O."""

    def __init__(
        self,
        *,
        artifact_reader: SemanticRefreshSealedArtifactReader,
        seal_authorization: SemanticRefreshSealAuthorizationPort,
        error_type: type[RuntimeError],
    ) -> None:
        self._artifact_reader = artifact_reader
        self._seal_authorization = seal_authorization
        self._error_type = error_type

    def read(
        self,
        plan: Mapping[str, object],
        authority: ClickHousePublicationAuthority,
    ) -> VersionPinnedSealedArtifact:
        """Require the durable seal and protected publication projection to agree."""

        try:
            seal_authorization = self._seal_authorization.load(
                workflow_execution_binding_sha256=str(plan["workflow_execution_binding_sha256"]),
                operation_id=str(plan["operation_id"]),
            )
            artifact = self._artifact_reader.read(
                manifest_key=str(plan["artifact_manifest_key"]),
                manifest_version=str(plan["artifact_manifest_version"]),
                artifact_manifest_sha256=str(plan["artifact_manifest_sha256"]),
                operation_id=str(plan["operation_id"]),
                operation_plan_sha256=str(plan["operation_plan_sha256"]),
                workflow_execution_binding_sha256=str(plan["workflow_execution_binding_sha256"]),
                attempt_binding_sha256=str(plan["attempt_binding_sha256"]),
                seal_authorization=seal_authorization,
            )
        except Exception as exc:
            raise self._error_type("version-pinned sealed artifact authority differs") from exc
        manifest = artifact.manifest
        expected = (
            authority.artifact_prefix,
            authority.artifact_provider,
            authority.effective_key_mapping_sha256,
            authority.route_certification_receipt_sha256,
            authority.encryption_policy_sha256,
            authority.retention_policy_sha256,
        )
        observed = (
            manifest.artifact_prefix,
            manifest.provider,
            manifest.effective_key_mapping_sha256,
            manifest.route_certification_receipt_sha256,
            manifest.encryption_policy_sha256,
            manifest.retention_policy_sha256,
        )
        if (
            observed != expected
            or manifest.total_bytes > authority.max_artifact_bytes
            or manifest.chunk_count != plan.get("artifact_chunk_count")
            or manifest.total_rows != plan.get("artifact_total_rows")
        ):
            raise self._error_type("sealed artifact differs from protected publication authority")
        return artifact


__all__ = [
    "ClickHouseArtifactAuthorityReader",
]
