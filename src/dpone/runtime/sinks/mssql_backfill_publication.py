"""Crash-safe MSSQL shadow publication for chunked initial loads."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

from dpone.backfill.execution_policy import execution_policy_from_load_config
from dpone.backfill.shadow_append_authority import (
    SHADOW_APPEND_AUTHORITY_OPTION,
    issue_shadow_append_authority,
)
from dpone.backfill.state import BackfillLedger, BackfillPublicationRecord
from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.runtime.sinks.mssql_backfill_cutover import MssqlBackfillCutover
from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    SHADOW_OWNER_PROPERTY,
    MssqlPublicationTarget,
    publication_campaign_contract,
    publication_names,
    read_supported_indexes,
    require_matching_columns,
    require_matching_indexes,
    require_shadow_owner,
    require_unique_key_index,
)
from dpone.runtime.sinks.mssql_backfill_publication_evidence import (
    count_rows,
    has_duplicate_key,
    publication_evidence,
    publication_receipt_id,
    require_published_generation,
    utc_now,
)
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    acquire_publication_lock,
    bind_publication_generation_tokens,
    ensure_publication_generation_token,
    read_publication_generation,
)
from dpone.runtime.sinks.mssql_backfill_publication_migration import (
    MssqlBackfillPublicationMigration,
    predecessor_binding,
    publication_generation_complete,
)
from dpone.runtime.sinks.mssql_backfill_publication_objects import (
    MssqlBackfillPublicationObjects,
)
from dpone.runtime.sinks.mssql_shadow_swap_object_contract import (
    MssqlShadowSwapObjectContractGuard,
)


class MssqlBackfillShadowPublisher:
    """Prepare, validate and atomically publish one stable campaign shadow."""

    def __init__(
        self,
        connector: Any,
        *,
        object_contract_guard: Any | None = None,
        cutover_target_lock_acquirer: Any | None = None,
        cutover_target_identity_assertion: Any | None = None,
    ) -> None:
        self._connector = connector
        self._publication_objects = MssqlBackfillPublicationObjects(
            connector,
            shadow_owner_property=SHADOW_OWNER_PROPERTY,
            read_supported_indexes=read_supported_indexes,
            require_matching_columns=require_matching_columns,
            require_shadow_owner=require_shadow_owner,
            acquire_publication_lock=acquire_publication_lock,
            ensure_publication_generation_token=ensure_publication_generation_token,
        )
        self._object_contract_guard = object_contract_guard or MssqlShadowSwapObjectContractGuard(connector)
        self._cutover_target_lock_acquirer = cutover_target_lock_acquirer
        self._cutover_target_identity_assertion = cutover_target_identity_assertion

    def require_unmapped(self, *, selection: Any) -> None:
        if selection is not None:
            raise ValueError("shadow backfill publication requires one campaign-level unmapped orchestrator")

    def campaign_contract(self, load_config: Any) -> dict[str, Any]:
        policy = execution_policy_from_load_config(load_config)
        return publication_campaign_contract(load_config, policy=policy)

    def before_chunks(self, load_config: Any, ledger: BackfillLedger, store: Any) -> Any:
        """Prepare or re-adopt the campaign shadow before any source chunk I/O."""

        return self._before_chunks(load_config, ledger, store, campaign_owner=None)

    def before_chunks_fenced(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: Any,
        *,
        campaign_owner: str,
    ) -> Any:
        """Prepare while the parent MSSQL session owns the campaign fence."""

        capabilities = store.state_capabilities()
        if capabilities.get("continuous_campaign_session_fence") is not True:
            raise RuntimeError("mssql_backfill_publication.continuous_campaign_fence_required")
        return self._before_chunks(load_config, ledger, store, campaign_owner=campaign_owner)

    def _before_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: Any,
        *,
        campaign_owner: str | None,
    ) -> Any:
        """Implement direct compatibility and production-fenced preparation."""

        contract = normalize_mssql_load_strategy(load_config).backfill
        if contract is None or contract.publication_mode != "shadow_swap":
            raise ValueError("MSSQL shadow publisher requires publication.mode=shadow_swap")
        self._require_store(store)
        policy = execution_policy_from_load_config(load_config)
        live, shadow, backup = publication_names(
            load_config,
            run_key=ledger.run_key,
            artifact_scope=policy.publication.artifact_scope,
        )
        existing = ledger.publication
        self._require_record_coordinates(existing, live=live, shadow=shadow, backup=backup)
        require_unique_key_index(self._connector, live, load_config.unique_key)
        if existing is not None and existing.phase == "published":
            if len(ledger.committed_indexes()) != len(ledger.chunks):
                raise RuntimeError("mssql_backfill_publication.published_with_incomplete_chunks")
            self._publication_objects.require_table(live)
            self._publication_objects.require_table(backup)
            if self._publication_objects.table_exists(shadow):
                raise RuntimeError("mssql_backfill_publication.published_shadow_still_exists")
            if not publication_generation_complete(existing):
                MssqlBackfillPublicationMigration(self._connector).bind_published(
                    ledger,
                    store=store,
                    live=live,
                    backup=backup,
                    campaign_owner=campaign_owner,
                )
                existing = ledger.publication
                assert existing is not None
            live_generation = read_publication_generation(self._connector, live)
            require_published_generation(existing, live_generation)
            backup_generation = read_publication_generation(self._connector, backup)
            expected_backup = (
                existing.predecessor_object_id,
                existing.predecessor_create_token,
                existing.predecessor_receipt_id,
            )
            actual_backup = (
                backup_generation.object_id,
                backup_generation.create_token,
                backup_generation.publication_receipt_id,
            )
            if actual_backup != expected_backup:
                raise RuntimeError("mssql_backfill_publication.published_backup_generation_changed")
            return load_config

        object_contract = self._object_contract_guard.require_supported(live)
        if existing is not None and existing.object_contract_sha256 not in (None, object_contract.sha256):
            raise RuntimeError("mssql_backfill_publication.object_contract_changed")

        if self._publication_objects.table_exists(backup):
            raise RuntimeError("mssql_backfill_publication.backup_name_conflict")
        if self._publication_objects.table_exists(shadow):
            self._publication_objects.require_shadow_owner(shadow, ledger.run_key)
            require_matching_columns(self._connector, live, shadow)
            bind_publication_generation_tokens(self._connector, live, live, shadow)
        else:
            if existing is not None and ledger.committed_indexes():
                raise RuntimeError("mssql_backfill_publication.prepared_shadow_missing")
            self._publication_objects.create_shadow(live, shadow, run_key=ledger.run_key)
        self._object_contract_guard.require_loadable_shadow(object_contract, shadow)
        live_generation = read_publication_generation(self._connector, live)
        shadow_generation = read_publication_generation(self._connector, shadow)
        predecessor_object_id, predecessor_create_token, predecessor_receipt_id = predecessor_binding(
            existing,
            live_generation,
        )
        if live_generation.xmin_handoff_pending:
            raise RuntimeError("mssql_backfill_publication.predecessor_handoff_pending")
        if shadow_generation.publication_receipt_id is not None:
            raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")
        if existing is not None and existing.shadow_object_id not in (None, shadow_generation.object_id):
            raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")
        if existing is not None and existing.shadow_create_token not in (None, shadow_generation.create_token):
            raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")

        ledger.publication = BackfillPublicationRecord(
            mode="shadow_swap",
            target_table=live.dataset,
            shadow_table=shadow.dataset,
            backup_table=backup.dataset,
            phase="prepared",
            predecessor_object_id=predecessor_object_id,
            predecessor_create_token=predecessor_create_token,
            predecessor_receipt_id=predecessor_receipt_id,
            shadow_object_id=shadow_generation.object_id,
            shadow_create_token=shadow_generation.create_token,
            object_contract_sha256=object_contract.sha256,
            prepared_at=(existing.prepared_at if existing is not None else None) or utc_now(),
        )
        if existing is not None and not publication_generation_complete(existing):
            MssqlBackfillPublicationMigration(self._connector).bind_prepared(
                ledger,
                store=store,
                live=live,
                shadow=shadow,
                campaign_owner=campaign_owner,
            )
        else:
            store.save(ledger)
        options = deepcopy(load_config.options or {})
        options[SHADOW_APPEND_AUTHORITY_OPTION] = issue_shadow_append_authority(
            load_config,
            run_key=ledger.run_key,
            live_table=live.table,
            shadow_table=shadow.table,
        )
        return replace(load_config, target_table=shadow.table, options=options)

    def after_chunks(self, load_config: Any, ledger: BackfillLedger, store: Any) -> dict[str, Any]:
        """Validate exact aggregate evidence, build indexes once and publish."""

        return self._after_chunks(load_config, ledger, store, campaign_owner=None)

    def after_chunks_fenced(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: Any,
        *,
        campaign_owner: str,
    ) -> dict[str, Any]:
        """Publish only while the parent MSSQL session owns the campaign fence."""

        capabilities = store.state_capabilities()
        if capabilities.get("continuous_campaign_session_fence") is not True:
            raise RuntimeError("mssql_backfill_publication.continuous_campaign_fence_required")
        return self._after_chunks(load_config, ledger, store, campaign_owner=campaign_owner)

    def _after_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: Any,
        *,
        campaign_owner: str | None,
    ) -> dict[str, Any]:
        """Implement direct test compatibility and production-fenced publication."""

        self._require_store(store)
        policy = execution_policy_from_load_config(load_config)
        live, shadow, backup = publication_names(
            load_config,
            run_key=ledger.run_key,
            artifact_scope=policy.publication.artifact_scope,
        )
        publication = ledger.publication
        self._require_record_coordinates(publication, live=live, shadow=shadow, backup=backup)
        if publication is not None and publication.phase == "published":
            return publication_evidence(publication)
        if len(ledger.committed_indexes()) != len(ledger.chunks):
            raise RuntimeError("mssql_backfill_publication.incomplete_chunks")
        if any(record.rows_extracted != record.rows_loaded for record in ledger.chunks):
            raise RuntimeError("mssql_backfill_publication.chunk_row_count_mismatch")

        expected_rows = sum(record.rows_loaded for record in ledger.chunks)
        actual_rows = count_rows(self._connector, shadow)
        duplicate_keys = has_duplicate_key(self._connector, shadow, load_config.unique_key)
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"mssql_backfill_publication.row_count_mismatch:expected={expected_rows}:actual={actual_rows}"
            )
        if duplicate_keys:
            raise RuntimeError("mssql_backfill_publication.duplicate_unique_key")

        require_matching_columns(self._connector, live, shadow)
        self._publication_objects.build_deferred_indexes(live, shadow)
        require_matching_indexes(self._connector, live, shadow)

        receipt_id = publication_receipt_id(ledger, live, expected_rows)
        candidate = deepcopy(ledger)
        candidate.publication = BackfillPublicationRecord(
            mode="shadow_swap",
            target_table=live.dataset,
            shadow_table=shadow.dataset,
            backup_table=backup.dataset,
            phase="published",
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            duplicate_keys=0,
            receipt_id=receipt_id,
            predecessor_object_id=publication.predecessor_object_id if publication is not None else None,
            predecessor_create_token=(publication.predecessor_create_token if publication is not None else None),
            predecessor_receipt_id=publication.predecessor_receipt_id if publication is not None else None,
            shadow_object_id=publication.shadow_object_id if publication is not None else None,
            shadow_create_token=publication.shadow_create_token if publication is not None else None,
            object_contract_sha256=(publication.object_contract_sha256 if publication is not None else None),
            prepared_at=publication.prepared_at if publication is not None else None,
            published_at=utc_now(),
        )
        committed = MssqlBackfillCutover(
            self._connector,
            object_contract_guard=self._object_contract_guard,
            target_lock_acquirer=self._cutover_target_lock_acquirer,
            target_identity_assertion=self._cutover_target_identity_assertion,
        ).publish(
            candidate,
            store=store,
            live=live,
            shadow=shadow,
            backup=backup,
            unique_key=load_config.unique_key,
            campaign_owner=campaign_owner,
            load_config=load_config,
        )
        ledger.publication = committed.publication
        if committed.chunks:
            store.sync_local_cache(committed)
        assert committed.publication is not None
        return publication_evidence(committed.publication)

    @staticmethod
    def _require_store(store: Any) -> None:
        if (
            getattr(store, "dialect", None) != "mssql"
            or not callable(getattr(store, "persist_campaign_in_transaction", None))
            or not callable(getattr(store, "sync_local_cache", None))
        ):
            raise RuntimeError("mssql_backfill_publication.target_atomic_audit_state_required")

    @staticmethod
    def _require_record_coordinates(
        record: BackfillPublicationRecord | None,
        *,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
        backup: MssqlPublicationTarget,
    ) -> None:
        if record is None:
            return
        expected = ("shadow_swap", live.dataset, shadow.dataset, backup.dataset)
        actual = (record.mode, record.target_table, record.shadow_table, record.backup_table)
        if actual != expected:
            raise RuntimeError("mssql_backfill_publication.ledger_coordinates_mismatch")


__all__ = ["MssqlBackfillShadowPublisher"]
