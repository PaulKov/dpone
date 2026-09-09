"""Nested normalization load orchestration for ETLProcessor."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.lineage import LineageOptions
    from dpone.runtime.normalization import NestedNormalizationOptions
    from dpone.runtime.normalization.load_package import NestedPackageStagedMutation, PackageFinalizer


import hashlib
import re
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.runtime.etl.nested_load_config import table_load_config
from dpone.runtime.etl.nested_spill_load import NestedSpillLoadService
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.lineage import LoadIdentityService
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.normalization import (
    ChildQualityResult,
    ChildQualityService,
    NestedNormalizationService,
)
from dpone.runtime.normalization.load_package import (
    NestedLoadPackageCoordinator,
    NestedLoadPackageTransaction,
    require_nested_package_mutation,
)
from dpone.runtime.normalization.snapshot_factory import ChildSnapshotStoreFactory
from dpone.runtime.normalization.snapshot_runtime import ChildSnapshotRuntimeService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult

MemberPreparer = Callable[
    [LoadConfig, LoadPayload, Any, LoadAuditRecord],
    tuple[LoadConfig, LoadPayload],
]
_SAFE_SPILL_ATTEMPT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class NestedLoadService:
    """Loads normalized root/child tables through a staged package mutation port."""

    def __init__(
        self,
        *,
        normalization_service: NestedNormalizationService | None = None,
        load_identity_service: LoadIdentityService | None = None,
        spill_service: Any | None = None,
        fast_path_planner: Any | None = None,
        fast_path_artifact_factory: Any | None = None,
        package_coordinator: NestedLoadPackageCoordinator | None = None,
        child_snapshot_store_factory: ChildSnapshotStoreFactory | None = None,
        child_snapshot_runtime: ChildSnapshotRuntimeService | None = None,
        spill_load_service: NestedSpillLoadService | None = None,
        child_quality_service: ChildQualityService | None = None,
    ) -> None:
        self.normalization_service = normalization_service or NestedNormalizationService()
        self.load_identity_service = load_identity_service or LoadIdentityService()
        self.child_quality_service = child_quality_service or ChildQualityService()
        self.spill_load_service = spill_load_service or NestedSpillLoadService(
            normalization_service=self.normalization_service,
            spill_service=spill_service,
            fast_path_planner=fast_path_planner,
            fast_path_artifact_factory=fast_path_artifact_factory,
            child_quality_service=self.child_quality_service,
        )
        self.package_coordinator = package_coordinator or NestedLoadPackageCoordinator()
        self.child_snapshot_store_factory = child_snapshot_store_factory or ChildSnapshotStoreFactory()
        self.child_snapshot_runtime = child_snapshot_runtime or ChildSnapshotRuntimeService()

    def load(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        extract_result: Any,
        load_record: LoadAuditRecord,
        lineage_options: LineageOptions,
        nested_options: NestedNormalizationOptions,
        package_mutation: NestedPackageStagedMutation,
        prepare_member: MemberPreparer,
        evaluate_package: PackageFinalizer,
    ) -> LoadResult:
        """Load a nested package through a mandatory staged mutation port.

        Callers cannot inject a committing ``load_one``; members are staged only
        through ``package_mutation``, then evaluated, then finalized or aborted.
        """

        if not lineage_options.enabled:
            raise ValueError("nested normalization requires lineage; set sink.options.lineage.preset: hierarchical")
        mutation = require_nested_package_mutation(package_mutation)

        def stage_one(
            table_config: LoadConfig,
            table_payload: LoadPayload,
            table_extract_result: Any,
            table_load_record: LoadAuditRecord,
        ) -> LoadResult:
            prepared_config, prepared_payload = prepare_member(
                table_config,
                table_payload,
                table_extract_result,
                table_load_record,
            )
            return mutation.stage(prepared_config, prepared_payload)

        def finalize_package(package_result: LoadResult) -> LoadResult:
            evaluated = evaluate_package(package_result)
            finalized = mutation.finalize_all()
            return replace(
                evaluated,
                inserted_rows=sum(item.inserted_rows for item in finalized),
                updated_rows=sum(item.updated_rows for item in finalized),
                total_rows=sum(item.total_rows for item in finalized),
                staging_rows=sum(item.staging_rows or 0 for item in finalized),
                soft_deleted_rows=sum(item.soft_deleted_rows or 0 for item in finalized) or None,
                replaced_rows=sum(item.replaced_rows or 0 for item in finalized) or None,
                deleted_lookback_rows=sum(item.deleted_lookback_rows or 0 for item in finalized) or None,
            )

        transaction = NestedLoadPackageTransaction(
            root_table=load_config.target_table,
            load_one=stage_one,
            finalize_package=finalize_package,
            identity_service=self.load_identity_service,
            load_record=load_record,
            package_coordinator=self.package_coordinator,
            snapshot_runtime=self.child_snapshot_runtime,
            abort_package=mutation.abort_all,
        )
        if nested_options.materialization == "spill_to_disk":
            return self._load_spilled(
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
                lineage_options=lineage_options,
                nested_options=nested_options,
                transaction=transaction,
            )
        return self._load_in_memory(
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
            lineage_options=lineage_options,
            nested_options=nested_options,
            transaction=transaction,
        )

    def _load_in_memory(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        extract_result: Any,
        load_record: LoadAuditRecord,
        lineage_options: LineageOptions,
        nested_options: NestedNormalizationOptions,
        transaction: NestedLoadPackageTransaction,
    ) -> LoadResult:
        normalized = self.normalization_service.normalize_payload(
            payload,
            root_table=load_config.target_table,
            options=nested_options,
            run_id=load_record.run_id,
            load_id=load_record.load_id,
            source_type=str((load_config.options or {}).get("source_type", "")),
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            unique_key=load_config.unique_key,
            lineage_enabled=lineage_options.enabled,
        )
        table_keys = normalized.table_keys()
        quality_results = self.child_quality_service.evaluate_package(
            root_table=load_config.target_table,
            table_rows={table.name: table.rows for table in normalized.tables},
            table_keys=table_keys,
            options=nested_options.child_quality,
            table_parents=normalized.table_parents(),
            ignored_hierarchy_tables=_ignored_hierarchy_tables(
                load_config.target_table,
                nested_options,
            ),
        )
        snapshot_store = self.child_snapshot_store_factory.create(nested_options.child_snapshot_store)
        snapshot_stages = transaction.stage_result(
            store=snapshot_store,
            result=normalized,
            root_table=load_config.target_table,
            table_keys=table_keys,
            load_id=load_record.load_id,
        )
        transaction.mark_staged(
            self.load_identity_service, load_record, extracted_rows=sum(normalized.row_counts().values())
        )
        results: list[LoadResult] = []
        try:
            for table in normalized.tables:
                table_config = table_load_config(
                    load_config,
                    table.name,
                    nested_options,
                    resolved_child_unique_key=table_keys.get(table.name),
                )
                table_payload = LoadPayload(artifact=InMemoryRowsArtifact(table.rows), schema=table.schema)
                results.append(transaction.load_table(table_config, table_payload, extract_result, load_record))
            return self._commit(
                load_record,
                results,
                normalized.row_counts(),
                nested_options,
                table_keys=table_keys,
                quality_results=quality_results,
                transaction=transaction,
                snapshot_store=snapshot_store,
                snapshot_stages=snapshot_stages,
            )
        except Exception as exc:
            transaction.rollback(store=snapshot_store, stages=snapshot_stages)
            transaction.mark_failed(self.load_identity_service, load_record, exc)
            raise

    def _load_spilled(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        extract_result: Any,
        load_record: LoadAuditRecord,
        lineage_options: LineageOptions,
        nested_options: NestedNormalizationOptions,
        transaction: NestedLoadPackageTransaction,
    ) -> LoadResult:
        del lineage_options
        attempt_options = _attempt_spill_options(nested_options, load_record.load_id)
        aggregate = self.spill_load_service.load(
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
            nested_options=attempt_options,
            load_one=transaction.load_table,
            load_identity_service=self.load_identity_service,
            package_coordinator=transaction,
            child_snapshot_store_factory=self.child_snapshot_store_factory,
            child_snapshot_runtime=transaction,
        )
        return transaction.result_or(aggregate)

    def _commit(
        self,
        load_record: LoadAuditRecord,
        results: list[LoadResult],
        table_counts: dict[str, int],
        nested_options: NestedNormalizationOptions,
        *,
        table_keys: dict[str, tuple[str, ...]],
        quality_results: tuple[ChildQualityResult, ...],
        transaction: NestedLoadPackageTransaction,
        snapshot_store: Any | None = None,
        snapshot_stages: tuple[Any, ...] = (),
    ) -> LoadResult:
        aggregate = _aggregate_results(
            results,
            table_counts,
            nested_options,
            table_keys=table_keys,
            quality_results=quality_results,
        )
        transaction.commit(store=snapshot_store, stages=snapshot_stages)
        transaction.mark_committed(self.load_identity_service, load_record, aggregate)
        return transaction.result_or(aggregate)


def _aggregate_results(
    results: list[LoadResult],
    table_counts: dict[str, int],
    nested_options: NestedNormalizationOptions,
    *,
    table_keys: dict[str, tuple[str, ...]],
    quality_results: tuple[ChildQualityResult, ...],
) -> LoadResult:
    metrics: dict[str, Any] = {
        "nested_normalization": {
            "tables": table_counts,
            "table_count": len(table_counts),
            "materialization": nested_options.materialization,
            "child_unique_keys": table_keys,
            "child_quality": [result.to_dict() for result in quality_results],
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


def _ignored_hierarchy_tables(
    root_table: str,
    nested_options: NestedNormalizationOptions,
) -> tuple[str, ...]:
    if not nested_options.raw_landing.enabled:
        return ()
    return (f"{root_table}{nested_options.raw_landing.table_suffix}",)


def _attempt_spill_options(
    nested_options: NestedNormalizationOptions,
    load_id: str,
) -> NestedNormalizationOptions:
    root = Path(nested_options.spill_output_dir) if nested_options.spill_output_dir else Path(".dpone/nested-spill")
    return replace(nested_options, spill_output_dir=str(root / _spill_attempt_segment(load_id)))


def _spill_attempt_segment(load_id: str) -> str:
    if _SAFE_SPILL_ATTEMPT.fullmatch(load_id) and load_id not in {".", ".."}:
        return load_id
    return f"sha256-{hashlib.sha256(load_id.encode('utf-8')).hexdigest()}"
