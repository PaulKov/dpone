"""Deprecated positional adapters for canonical dbt publish contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_publish_models import (
    COMPILE_REPORT_SCHEMA,
    DbtColumnArtifact,
    DbtRouteCertificationProfile,
)
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel as _CanonicalCompiledDbtModel,
)
from dpone.contracts.dbt_publish_models import CompiledDbtWorkflow as CompiledDbtWorkflow
from dpone.contracts.dbt_publish_models import (
    DbtCompileReport as _CanonicalDbtCompileReport,
)
from dpone.contracts.dbt_publish_models import (
    DbtManifestArtifact as _CanonicalDbtManifestArtifact,
)
from dpone.contracts.dbt_publish_models import (
    DbtModelArtifact as _CanonicalDbtModelArtifact,
)
from dpone.contracts.dbt_publish_models import DbtPublishIntent as DbtPublishIntent
from dpone.contracts.dbt_publish_models import DbtPublishIssue as DbtPublishIssue
from dpone.contracts.dbt_publish_models import (
    DbtPublishProfile as _CanonicalDbtPublishProfile,
)
from dpone.contracts.dbt_publish_models import DbtWorkflowProfile as DbtWorkflowProfile
from dpone.contracts.dbt_publish_models import (
    __all__ as __all__,
)
from dpone.contracts.dbt_publish_models import fingerprint as fingerprint


class DbtModelArtifact(_CanonicalDbtModelArtifact):
    """Bind historical positional fields while retaining the canonical value type."""

    def __init__(
        self,
        unique_id: str,
        name: str,
        original_file_path: str,
        database: str | None,
        schema: str,
        alias: str,
        materialized: str,
        contract_enforced: bool,
        columns: tuple[str, ...],
        group: str | None,
        tags: tuple[str, ...],
        meta: Mapping[str, Any],
        unique_key: tuple[str, ...],
        depends_on: tuple[str, ...],
        test_ids: tuple[str, ...] = (),
        *,
        column_contracts: tuple[DbtColumnArtifact, ...] = (),
    ) -> None:
        super().__init__(
            unique_id=unique_id,
            name=name,
            original_file_path=original_file_path,
            database=database,
            schema=schema,
            alias=alias,
            materialized=materialized,
            contract_enforced=contract_enforced,
            columns=columns,
            column_contracts=column_contracts,
            group=group,
            tags=tags,
            meta=meta,
            unique_key=unique_key,
            depends_on=depends_on,
            test_ids=test_ids,
        )


class DbtManifestArtifact(_CanonicalDbtManifestArtifact):
    """Restore the pre-digest positional constructor for preview readers."""

    def __init__(
        self,
        path: str,
        schema_version: int,
        dbt_version: str | None,
        invocation_id: str | None,
        project_name: str | None,
        models: tuple[_CanonicalDbtModelArtifact, ...],
        *,
        sha256: str = "",
    ) -> None:
        super().__init__(
            path=path,
            sha256=sha256,
            schema_version=schema_version,
            dbt_version=dbt_version,
            invocation_id=invocation_id,
            project_name=project_name,
            models=models,
        )


class DbtPublishProfile(_CanonicalDbtPublishProfile):
    """Keep historical option mappings positional; new authority fields stay named."""

    def __init__(
        self,
        name: str,
        source_type: str,
        source_connection_ref: str,
        sink_type: str,
        sink_connection_ref: str,
        target_schema: str,
        staging_schema: str | None,
        runtime_image: str,
        source_options: Mapping[str, Any] | None = None,
        sink_options: Mapping[str, Any] | None = None,
        runtime: Mapping[str, Any] | None = None,
        physical_design: Mapping[str, Any] | None = None,
        execution: Mapping[str, Any] | None = None,
        quality: Mapping[str, Any] | None = None,
        lineage: Mapping[str, Any] | None = None,
        *,
        certification: DbtRouteCertificationProfile | None = None,
        state: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            source_type=source_type,
            source_connection_ref=source_connection_ref,
            sink_type=sink_type,
            sink_connection_ref=sink_connection_ref,
            target_schema=target_schema,
            staging_schema=staging_schema,
            runtime_image=runtime_image,
            certification=certification,
            state=dict(state or {}),
            source_options=dict(source_options or {}),
            sink_options=dict(sink_options or {}),
            runtime=dict(runtime or {}),
            physical_design=dict(physical_design or {}),
            execution=dict(execution or {}),
            quality=dict(quality or {}),
            lineage=dict(lineage or {}),
        )


class CompiledDbtModel(_CanonicalCompiledDbtModel):
    """Bind warnings to their original eighth positional field."""

    def __init__(
        self,
        model: _CanonicalDbtModelArtifact,
        intent: DbtPublishIntent,
        profile: _CanonicalDbtPublishProfile,
        strategy: Mapping[str, Any],
        physical_design: Mapping[str, Any],
        workload_id: str,
        manifest: Mapping[str, Any],
        warnings: tuple[DbtPublishIssue, ...] = (),
        *,
        route_capability: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            model=model,
            intent=intent,
            profile=profile,
            strategy=strategy,
            physical_design=physical_design,
            workload_id=workload_id,
            manifest=manifest,
            route_capability=dict(route_capability or {}),
            warnings=warnings,
        )


class DbtCompileReport(_CanonicalDbtCompileReport):
    """Restore the historical report payload order; new identity fields are named."""

    def __init__(
        self,
        manifest_path: str,
        manifest_schema_version: int | None,
        models: tuple[_CanonicalCompiledDbtModel, ...] = (),
        workflows: tuple[CompiledDbtWorkflow, ...] = (),
        warnings: tuple[DbtPublishIssue, ...] = (),
        blockers: tuple[DbtPublishIssue, ...] = (),
        artifacts: Mapping[str, str] | None = None,
        schema: str = COMPILE_REPORT_SCHEMA,
        *,
        manifest_sha256: str | None = None,
        dbt_version: str | None = None,
        dbt_adapter: str | None = None,
        dbt_adapter_version: str | None = None,
        release_id: str | None = None,
    ) -> None:
        super().__init__(
            manifest_path=manifest_path,
            manifest_schema_version=manifest_schema_version,
            manifest_sha256=manifest_sha256,
            dbt_version=dbt_version,
            dbt_adapter=dbt_adapter,
            dbt_adapter_version=dbt_adapter_version,
            release_id=release_id,
            models=models,
            workflows=workflows,
            warnings=warnings,
            blockers=blockers,
            artifacts=dict(artifacts or {}),
            schema=schema,
        )
