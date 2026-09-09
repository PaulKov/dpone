"""Spill-to-disk nested load orchestration."""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.runtime.etl.nested_load_config import table_load_config
from dpone.runtime.etl.nested_spill_rows import semantic_rows
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.normalization import (
    ChildQualityResult,
    ChildQualityService,
    NestedNormalizationOptions,
    NestedNormalizationService,
)
from dpone.runtime.normalization.fast_path import NestedSpillArtifactFactory, NestedSpillFastPathPlanner
from dpone.runtime.normalization.spill import SpilledNormalizationResult, SpillToDiskNormalizationService
from dpone.runtime.normalization.spill_child_quality import SpilledChildQualityService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact

SinglePayloadLoader = Callable[[LoadConfig, LoadPayload, Any, LoadAuditRecord], LoadResult]


class NestedSpillLoadService:
    """Load nested spill files through streaming or native file artifacts."""

    def __init__(
        self,
        *,
        normalization_service: NestedNormalizationService | None = None,
        spill_service: SpillToDiskNormalizationService | None = None,
        fast_path_planner: NestedSpillFastPathPlanner | None = None,
        fast_path_artifact_factory: NestedSpillArtifactFactory | None = None,
        child_quality_service: ChildQualityService | None = None,
        spilled_child_quality_service: SpilledChildQualityService | None = None,
    ) -> None:
        self.normalization_service = normalization_service or NestedNormalizationService()
        self.spill_service = spill_service or SpillToDiskNormalizationService(normalizer=self.normalization_service)
        self.fast_path_planner = fast_path_planner or NestedSpillFastPathPlanner()
        self.fast_path_artifact_factory = fast_path_artifact_factory or NestedSpillArtifactFactory()
        self.child_quality_service = child_quality_service or ChildQualityService()
        self.spilled_child_quality_service = spilled_child_quality_service or SpilledChildQualityService(
            policy_service=self.child_quality_service
        )

    def load(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        extract_result: Any,
        load_record: LoadAuditRecord,
        nested_options: NestedNormalizationOptions,
        load_one: SinglePayloadLoader,
        load_identity_service: Any,
        package_coordinator: Any,
        child_snapshot_store_factory: Any,
        child_snapshot_runtime: Any,
    ) -> LoadResult:
        snapshot_store = child_snapshot_store_factory.create(nested_options.child_snapshot_store)
        quality_results: tuple[ChildQualityResult, ...] = ()

        def validate_staged(candidate: SpilledNormalizationResult) -> None:
            nonlocal quality_results
            quality_results = self.spilled_child_quality_service.evaluate_package(
                root_table=load_config.target_table,
                table_rows={
                    table_name: semantic_rows(candidate, table_name, path)
                    for table_name, path in candidate.files.items()
                },
                table_keys=candidate.table_keys,
                options=nested_options.child_quality,
                table_parents=candidate.table_parents,
                work_dir=candidate.output_dir,
                ignored_hierarchy_tables=_ignored_hierarchy_tables(
                    load_config.target_table,
                    nested_options,
                ),
            )

        spill = self.spill_service.spill_rows(
            self.normalization_service.artifact_rows_reader.rows(payload.artifact),
            root_table=load_config.target_table,
            options=nested_options,
            output_dir=_spill_output_dir(nested_options, load_record.load_id),
            unique_key=load_config.unique_key,
            load_id=load_record.load_id,
            source_type=str((load_config.options or {}).get("source_type", "")),
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            output_format=nested_options.spill_output_format,
            staged_validator=validate_staged,
        )
        snapshot_stages: tuple[Any, ...] = ()
        package_staged = False
        results: list[LoadResult] = []
        native_fast_paths: dict[str, str] = {}
        try:
            snapshot_stages = self._stage_child_snapshots(
                load_config=load_config,
                nested_options=nested_options,
                load_record=load_record,
                spill=spill,
                snapshot_store=snapshot_store,
                child_snapshot_runtime=child_snapshot_runtime,
            )
            package_coordinator.mark_staged(load_identity_service, load_record, extracted_rows=spill.total_rows)
            package_staged = True
            for table_name, path in spill.files.items():
                table_config = table_load_config(
                    load_config,
                    table_name,
                    nested_options,
                    resolved_child_unique_key=spill.table_keys.get(table_name),
                )
                spill_format = spill.formats.get(table_name, "jsonl")
                decision = self.fast_path_planner.plan(
                    sink_type=_nested_sink_type(load_config),
                    spill_format=spill_format,
                    enabled=_native_fast_path_enabled(load_config),
                )
                artifact: Any
                if decision.uses_file_artifact:
                    artifact = self.fast_path_artifact_factory.file_artifact(
                        path=path,
                        schema=spill.schemas[table_name],
                        spill_format=spill_format,
                        estimated_rows=spill.row_counts.get(table_name),
                    )
                    if decision.native_route:
                        native_fast_paths[table_name] = decision.native_route
                else:
                    artifact = StreamingRowsArtifact(
                        semantic_rows(spill, table_name, path),
                        estimated_rows=spill.row_counts.get(table_name),
                    )
                table_payload = LoadPayload(artifact=artifact, schema=spill.schemas[table_name])
                results.append(load_one(table_config, table_payload, extract_result, load_record))
            aggregate = _aggregate_results(
                results,
                spill.row_counts,
                nested_options,
                spill=spill,
                native_fast_paths=native_fast_paths,
                quality_results=quality_results,
            )
            child_snapshot_runtime.commit(store=snapshot_store, stages=snapshot_stages)
            package_coordinator.mark_committed(load_identity_service, load_record, aggregate)
            return aggregate
        except BaseException as exc:
            _record_failed_attempt(
                exc,
                package_staged=package_staged,
                package_coordinator=package_coordinator,
                load_identity_service=load_identity_service,
                load_record=load_record,
                child_snapshot_runtime=child_snapshot_runtime,
                snapshot_store=snapshot_store,
                snapshot_stages=snapshot_stages,
            )
            _discard_failed_generation(spill)
            raise

    def _stage_child_snapshots(
        self,
        *,
        load_config: LoadConfig,
        nested_options: NestedNormalizationOptions,
        load_record: LoadAuditRecord,
        spill: SpilledNormalizationResult,
        snapshot_store: Any,
        child_snapshot_runtime: Any,
    ) -> tuple[Any, ...]:
        snapshot_stages: tuple[Any, ...] = ()
        for table_name, path in spill.files.items():
            if table_name == load_config.target_table:
                continue
            unique_key = spill.table_keys.get(table_name, ())
            if unique_key:
                snapshot_stages += child_snapshot_runtime.stage_rows(
                    store=snapshot_store,
                    root_table=load_config.target_table,
                    child_table=table_name,
                    rows=[dict(row) for row in semantic_rows(spill, table_name, path)],
                    unique_key=unique_key,
                    load_id=load_record.load_id,
                )
        return snapshot_stages


def _aggregate_results(
    results: list[LoadResult],
    table_counts: dict[str, int],
    nested_options: NestedNormalizationOptions,
    *,
    spill: SpilledNormalizationResult,
    native_fast_paths: dict[str, str],
    quality_results: tuple[ChildQualityResult, ...],
) -> LoadResult:
    metrics: dict[str, Any] = {
        "nested_normalization": {
            "tables": table_counts,
            "table_count": len(table_counts),
            "materialization": nested_options.materialization,
            "child_unique_keys": spill.table_keys,
            "child_quality": [result.to_dict() for result in quality_results],
            "spill_output_dir": str(spill.output_dir),
            "spill_files": {table: str(path) for table, path in spill.files.items()},
            "spill_formats": spill.formats,
            "native_fast_paths": native_fast_paths,
        }
    }
    return LoadResult(
        inserted_rows=sum(result.inserted_rows for result in results),
        updated_rows=sum(result.updated_rows for result in results),
        total_rows=sum(result.total_rows for result in results),
        staging_rows=sum(result.staging_rows or 0 for result in results),
        soft_deleted_rows=sum(result.soft_deleted_rows or 0 for result in results) or None,
        replaced_rows=sum(result.replaced_rows or 0 for result in results) or None,
        deleted_lookback_rows=sum(result.deleted_lookback_rows or 0 for result in results) or None,
        reconciliation_metrics=metrics,
    )


def _spill_output_dir(nested_options: NestedNormalizationOptions, load_id: str) -> Path:
    if nested_options.spill_output_dir:
        return Path(nested_options.spill_output_dir)
    return Path(".dpone") / "nested-spill" / load_id


def _nested_sink_type(load_config: LoadConfig) -> str | None:
    options = load_config.options or {}
    nested_fast_path = options.get("nested_fast_path")
    if isinstance(nested_fast_path, Mapping) and nested_fast_path.get("sink_type"):
        return str(nested_fast_path["sink_type"])
    for key in ("sink_type", "target_type"):
        if options.get(key):
            return str(options[key])
    return None


def _native_fast_path_enabled(load_config: LoadConfig) -> bool:
    nested_fast_path = (load_config.options or {}).get("nested_fast_path")
    if not isinstance(nested_fast_path, Mapping):
        return True
    return bool(nested_fast_path.get("enabled", True))


def _ignored_hierarchy_tables(
    root_table: str,
    nested_options: NestedNormalizationOptions,
) -> tuple[str, ...]:
    if not nested_options.raw_landing.enabled:
        return ()
    return (f"{root_table}{nested_options.raw_landing.table_suffix}",)


def _record_failed_attempt(
    primary: BaseException,
    *,
    package_staged: bool,
    package_coordinator: Any,
    load_identity_service: Any,
    load_record: LoadAuditRecord,
    child_snapshot_runtime: Any,
    snapshot_store: Any,
    snapshot_stages: Sequence[Any],
) -> None:
    operations = [
        lambda: child_snapshot_runtime.rollback(store=snapshot_store, stages=snapshot_stages),
    ]
    if package_staged:
        operations.append(lambda: package_coordinator.mark_failed(load_identity_service, load_record, primary))
    for operation in operations:
        try:
            operation()
        except Exception:  # noqa: BLE001 - preserve the primary package failure.
            pass


def _discard_failed_generation(spill: SpilledNormalizationResult) -> None:
    paths = (*spill.files.values(), *spill.semantic_files.values())
    generation_dirs = {path.parent for path in paths}
    if len(generation_dirs) != 1:
        return
    generation_dir = next(iter(generation_dirs))
    if generation_dir.parent == spill.output_dir and generation_dir.name.startswith(".dpone-spill-generation-"):
        shutil.rmtree(generation_dir, ignore_errors=True)
