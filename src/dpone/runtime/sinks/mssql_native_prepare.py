"""Prepare one duplicate-preserving native table from verified physical attempts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, replace
from typing import Any

from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, ConsumedPayloadPartEvidence
from dpone.runtime.sinks.mssql_native_prepare_models import (
    NativePreparedResources,
    NativeStageContext,
    require_prepared_resources,
    strategy_for_prepared,
)
from dpone.runtime.sinks.mssql_native_prepared_digests import digest_stage_projection, full_prepared_contract
from dpone.runtime.sinks.mssql_native_staged_load import NativePreparedStage
from dpone.runtime.sinks.staging_managers.mssql_staging_support import issue_direct_native_staging_authority
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import MssqlNativeLineageProjection
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer


class MssqlNativeStagePreparer:
    """Consume one stream, then apply existing native lineage before governance."""

    @staticmethod
    def _stage_digest(strategy: Any, stage: Any, context: NativeStageContext, *, all_columns: bool = False) -> str:
        return digest_stage_projection(strategy, stage, context, all_columns=all_columns)

    def __init__(self, sink: Any, context_factory: Callable[..., NativeStageContext]) -> None:
        self._sink = sink
        self._context_factory = context_factory

    def stage(self, config: Any, payload: Any, *, context: NativeStageContext | None = None) -> NativePreparedStage:
        admission = payload.mssql_transaction_admission
        if not isinstance(admission, MssqlTransactionAdmission) or admission.operation is None:
            raise ValueError("mssql_native.transaction_admission_required")
        strategy = self._sink._strategy_map[config.load_strategy]
        normalizer = MssqlNativeStagingNormalizer(strategy)
        schema = tuple(payload.schema)
        resolved = normalizer.resolve_schema(
            config,
            schema,
            relation_schema=payload.relation_schema,
            relation_metadata=payload.relation_metadata,
            relation_dialect=payload.relation_dialect,
            target_projection=payload.target_projection,
        )
        if any(wire != target for wire, target in resolved.wire_to_target.items()):
            raise ValueError("mssql_native.renamed_wire_columns_not_admitted")
        if payload.target_projection is not None:
            raise ValueError("mssql_native.foreign_projection_not_admitted")
        context = context or self._context_factory(config, payload, resolved)
        context.capacity_check(0)
        if context.recover:
            complete = context.executor.recover(context.plan, context.lease)
            lifecycle = context.completed_lifecycle
            if lifecycle is None:
                raise ValueError("mssql_native.recovery_source_lifecycle_required")
        else:
            from dpone.runtime.sinks.mssql_native_completed_payload import completion_metadata
            from dpone.runtime.sinks.mssql_native_source_values import _sized_native_source_rows

            adapted = context.observer.source_rows(
                iter(context.row_source()),
                lambda rows: _sized_native_source_rows(rows, context.wire_contract, context.max_row_bytes),
            )
            complete = context.executor.stage(
                context.plan,
                adapted,
                context.wire_contract,
                context.lease,
                completion_metadata=lambda: completion_metadata(
                    payload,
                    recovery_bindings=(
                        None if context.recovery_bindings is None else context.recovery_bindings(admission)
                    ),
                ),
                cancelled=context.cancelled,
            )
            lifecycle = payload.require_completed_extraction()
        receipts = tuple(complete.receipts)
        if tuple(receipt.ordinal for receipt in receipts) != tuple(range(len(receipts))):
            raise ValueError("mssql_native.receipt_coverage_invalid")
        if not receipts:
            durable = context.journal_factory().completed() if context.verification_identity is not None else None
            if durable is None or durable.rows != 0 or durable.receipts != () or complete.rows != 0:
                raise ValueError("mssql_native.receipt_coverage_invalid")
        with context.observer.recorder().phase("raw_verify", reason="preparation"):
            context.verify_receipts(receipts)
        context.capacity_check(0)
        lineage = MssqlNativeLineageProjection.resolve(config, lifecycle)
        resolved = resolved.with_lineage(lineage)
        options = dict(config.options or {})
        options.update(
            {
                "__dpone_mssql_native_staging": True,
                "__dpone_mssql_typed_file_staging_v1": True,
                "__dpone_mssql_native_column_types": dict(resolved.types),
                "__dpone_mssql_native_not_null_columns": [
                    name for name, nullable in resolved.nullability.items() if not nullable
                ],
                "__dpone_mssql_native_collations": dict(resolved.collations),
                "__dpone_mssql_native_omitted_columns": [column.name for column in resolved.generated_columns],
                "__dpone_mssql_wire_schema": [list(column) for column in schema],
            }
        )
        issue_direct_native_staging_authority(options)
        staging_config = replace(config, options=options, staging_table=None)
        from dpone.runtime.sinks.mssql_native_prepared_owner import create_prepared, planned_stage

        planned = planned_stage(context, staging_config)
        context.journal_factory().publication.preparation_started({"planned_stage": planned})
        with context.preparation_scope():
            stage = create_prepared(
                strategy,
                staging_config,
                resolved.target_schema,
                planned,
                before_create=lambda: context.capacity_check(1),
            )
            return self._populate(
                config, payload, context, complete, receipts, lifecycle, resolved, lineage, schema, stage, planned
            )

    def recover_stage(self, config: Any, context: NativeStageContext, admission: Any) -> NativePreparedStage:
        from dpone.runtime.sinks.mssql_native_completed_payload import restore_payload

        payload = restore_payload(context, admission)
        return self.stage(
            config, payload, context=replace(context, recover=True, completed_lifecycle=payload.lifecycle)
        )

    def _populate(
        self,
        config: Any,
        payload: Any,
        context: NativeStageContext,
        complete: Any,
        receipts: tuple[Any, ...],
        lifecycle: Any,
        resolved: Any,
        lineage: Any,
        schema: Any,
        stage: Any,
        planned: dict[str, str],
    ) -> NativePreparedStage:
        strategy = self._sink._strategy_map[config.load_strategy]
        normalizer = MssqlNativeStagingNormalizer(strategy)
        admission = payload.mssql_transaction_admission
        try:
            from dpone.runtime.sinks.mssql_native_prepared_insert import build_prepared_insert

            columns = ", ".join(strategy.connector.quote_identifier(name) for name, _dtype in schema)
            # Stage identifiers are verified by the invocation-owned importer
            # before they enter this SQL. UNION ALL retains business duplicates.
            recorder = context.observer.recorder()
            if receipts:
                queries = " UNION ALL ".join(f"SELECT {columns} FROM {receipt.stage_id}" for receipt in receipts)
                with recorder.phase("metadata_project", reason="insert_projection_build"):
                    insert_sql = build_prepared_insert(
                        target_sql=strategy._staging_name(stage),
                        source_sql=queries,
                        business_schema=tuple(
                            (name, resolved.types[name])
                            for name in resolved.ordered_target_names
                            if not name.casefold().startswith("__dpone__")
                        ),
                        resolved=resolved,
                        lineage=lineage,
                        quote_identifier=strategy.connector.quote_identifier,
                    )
                with recorder.phase("prepare_insert", rows=complete.rows):
                    strategy.connector.execute_query(insert_sql)
            stage.row_count = sum(receipt.rows for receipt in receipts)
            if stage.row_count != complete.rows:
                raise ValueError("mssql_native.complete_row_count_mismatch")
            evidence_fields = {field.name for field in fields(ConsumedPayloadPartEvidence)}
            parts = tuple(
                ConsumedPayloadPartEvidence(
                    **{key: value for key, value in receipt.consumed_part_evidence.items() if key in evidence_fields}
                )
                for receipt in receipts
            )
            stage.consumed_payload_evidence = (
                ConsumedPayloadEvidence(parts).require_complete(native=False)
                if receipts
                else ConsumedPayloadEvidence.empty()
            )
            normalizer._validate_direct_native(config, stage, schema, resolved)
            normalizer._complete_direct_native(config, stage, resolved, lineage)
            if context.persisted_hash_layout:
                strategy.connector.execute_query(
                    f"ALTER TABLE {strategy._staging_name(stage)} ADD [__dpone__mutation_version] rowversion NOT NULL"
                )
            from dpone.runtime.sinks.mssql_native_prepared_digests import digest_prepared_rows, digest_prepared_target

            full_contract = full_prepared_contract(stage)
            columns = ", ".join(strategy.connector.quote_identifier(column.name) for column in full_contract.columns)
            with recorder.phase("prepared_verify", reason="preparation", rows=stage.row_count):
                if context.verification_identity is not None:
                    digests = digest_prepared_target(
                        strategy.connector.get_records,
                        qualified_stage=strategy._staging_name(stage),
                        business_contract=context.wire_contract,
                        full_contract=full_contract,
                        expected_rows=stage.row_count,
                        include_mutation_watermark=context.persisted_hash_layout,
                    )
                else:
                    digests = digest_prepared_rows(
                        strategy.connector.get_records_iterator(
                            f"SELECT {columns} FROM {strategy._staging_name(stage)}"
                        ),
                        business_contract=context.wire_contract,
                        full_contract=full_contract,
                        max_row_bytes=context.max_row_bytes,
                        expected_rows=stage.row_count,
                    )
                from dpone.runtime.mssql_native_chunks_files import native_multiset_digest

                expected_sum = sum(int(receipt.consumed_part_evidence["native_typed_sum"]) for receipt in receipts) % (
                    1 << 256
                )
                if digests.business_digest != native_multiset_digest(stage.row_count, expected_sum):
                    raise ValueError("mssql_native.prepared_digest_mismatch")
            digest = digests.full_digest
            context.capacity_check(0)
            object_id = strategy.connector.get_records("SELECT OBJECT_ID(?)", (strategy._staging_name(stage),))[0][0]
            if type(object_id) is not int or object_id < 1:
                raise ValueError("mssql_native.prepared_object_identity_unavailable")
            prepared = NativePreparedStage(
                stage,
                admission,
                lifecycle,
                payload.mssql_target_mutation_plan,
                payload.target_projection,
                context.interval,
                (
                    NativePreparedResources(
                        context,
                        receipts,
                        digest,
                        schema,
                        object_id,
                        planned,
                        digests.mutation_watermark,
                    ),
                ),
            )
            from dpone.runtime.sinks.mssql_native_recovery import prepared_snapshot

            snapshot = prepared_snapshot(
                prepared,
                recovery={
                    "digest": digest,
                    "schema": [list(column) for column in schema],
                    "object_id": object_id,
                    "mutation_watermark": digests.mutation_watermark,
                },
            )
            snapshot["planned_stage"] = planned
            context.journal_factory().publication.prepared(snapshot)
            return prepared
        except BaseException as error:
            try:
                stage.cleanup()
            except Exception as cleanup_error:
                error.add_note(f"native prepare cleanup failed: {type(cleanup_error).__name__}")
            raise

    def reverify(self, prepared: NativePreparedStage) -> None:
        resources = require_prepared_resources(prepared)
        context = resources.context
        recorder = context.observer.recorder()
        with recorder.phase("raw_verify", reason="prepublication"):
            context.verify_receipts(resources.receipts)
        context.capacity_check(0)
        strategy = strategy_for_prepared(self._sink, prepared)
        from dpone.runtime.sinks.mssql_native_prepared_owner import require_prepared_owner

        require_prepared_owner(strategy.connector, resources.planned)
        object_id = strategy.connector.get_records("SELECT OBJECT_ID(?)", (strategy._staging_name(prepared.staging),))[
            0
        ][0]
        if object_id != resources.object_id:
            raise ValueError("mssql_native.prepared_object_identity_changed")
        with recorder.phase("prepared_verify", reason="prepublication", rows=prepared.staging.row_count):
            if resources.mutation_watermark is not None:
                from dpone.runtime.sinks.mssql_native_persisted_hash import (
                    build_repeat_watermark_sql,
                    decode_repeat_watermark,
                )

                rows = strategy.connector.get_records(
                    build_repeat_watermark_sql(strategy._staging_name(prepared.staging), prepared.staging.row_count)
                )
                if len(rows) != 1:
                    raise ValueError("mssql_native.persisted_hash_row_shape")
                observed = decode_repeat_watermark(tuple(rows[0]), expected_rows=prepared.staging.row_count)
                if observed.mutation_watermark != resources.mutation_watermark:
                    raise ValueError("mssql_native.prepared_content_changed")
            elif (
                digest_stage_projection(strategy, prepared.staging, context, all_columns=True)
                != resources.verify_digest
            ):
                raise ValueError("mssql_native.prepared_content_changed")

    def publication_started(self, prepared: NativePreparedStage) -> None:
        journal = require_prepared_resources(prepared).context.journal_factory()
        state = journal.publication.state()
        if state is None:
            raise ValueError("mssql_native.prepared_journal_required")
        journal.publication.publication_started(state["prepared"])

    def publication_confirmed(self, prepared: NativePreparedStage, result: Any) -> None:
        if not result.commit_receipt_id:
            raise ValueError("mssql_native.target_receipt_required")
        require_prepared_resources(prepared).context.journal_factory().publication.publication_confirmed(
            {
                "receipt_id": result.commit_receipt_id,
            }
        )

    def restore(self, config: Any, context: NativeStageContext, admission: Any) -> NativePreparedStage | None:
        from dpone.runtime.sinks.mssql_native_recovery import restore_prepared

        journal = context.journal_factory()
        state = journal.publication.state()
        if state is None or state["phase"] == "preparing":
            return None
        complete = journal.completed()
        if complete is None:
            raise ValueError("mssql_native.complete_stage_required")
        binding = state["prepared"]
        recovery = binding["recovery"]
        resources = NativePreparedResources(
            context,
            tuple(complete.receipts),
            recovery["digest"],
            tuple(tuple(column) for column in recovery["schema"]),
            recovery["object_id"],
            binding["planned_stage"],
            recovery.get("mutation_watermark"),
        )
        strategy = self._sink._strategy_map[config.load_strategy]
        return restore_prepared(
            binding,
            admission=admission,
            staging_manager=strategy.staging_manager,
            interval=context.interval,
            resources=(resources,),
        )

    def publication_scope(self, prepared: NativePreparedStage) -> Any:
        return require_prepared_resources(prepared).context.preparation_scope()

    def cleanup(self, prepared: NativePreparedStage) -> None:
        from dpone.runtime.sinks.mssql_native_prepared_owner import retire_exact_prepared

        resources = require_prepared_resources(prepared)
        if resources.context.verification_identity is not None:
            state = resources.context.journal_factory().publication.state()
            if state is None or state["phase"] != "succeeded":
                raise ValueError("mssql_native.published_cleanup_publication_required")
        strategy = strategy_for_prepared(self._sink, prepared)
        with resources.context.preparation_scope():
            retire_exact_prepared(
                strategy.connector,
                resources.planned,
                resources.object_id,
                prepared.staging.cleanup,
                timeout_seconds=resources.context.target_local_timeout_seconds,
            )
        resources.context.cleanup_receipts(resources.receipts)
