"""Transactional generation-CAS cutover for an MSSQL backfill shadow."""

from __future__ import annotations

from typing import Any

from dpone.backfill.state import BackfillLedger
from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    MssqlPublicationTarget,
    publication_rename_statement,
    require_matching_columns,
    require_matching_indexes,
    require_shadow_owner,
    require_unique_key_index,
)
from dpone.runtime.sinks.mssql_backfill_publication_evidence import count_rows, has_duplicate_key
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    acquire_publication_lock,
    publish_generation_head,
    read_publication_generation,
)
from dpone.runtime.sinks.mssql_shadow_swap_object_contract import (
    MssqlShadowSwapObjectContractGuard,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_target_lock,
    target_lock_resource,
    transaction_lock_timeout_ms,
)
from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity
from dpone.runtime.state.mssql_target_identity import (
    assert_mssql_physical_target_identity,
    resolve_mssql_physical_target_identity,
)


def acquire_cutover_target_lock(
    connector: Any,
    live: MssqlPublicationTarget,
    load_config: Any,
) -> bytes:
    """Resolve and lock the immutable physical target shared by every writer."""

    database = _require_target_database(live)
    identity = resolve_mssql_physical_target_identity(
        connector,
        session=MssqlSessionIdentity.read(connector),
        database=database,
        schema=live.schema,
        table=live.table,
    )
    acquire_target_lock(
        connector,
        target_lock_resource(identity.digest),
        database=identity.database_name,
        timeout_ms=transaction_lock_timeout_ms(load_config),
    )
    return identity.digest


class MssqlBackfillCutover:
    """Re-prove and swap exactly one candidate under target-global authority."""

    def __init__(
        self,
        connector: Any,
        *,
        object_contract_guard: Any | None = None,
        target_lock_acquirer: Any | None = None,
        target_identity_assertion: Any | None = None,
    ) -> None:
        self._connector = connector
        self._object_contract_guard = object_contract_guard or MssqlShadowSwapObjectContractGuard(connector)
        self._target_lock_acquirer = (
            target_lock_acquirer if target_lock_acquirer is not None else acquire_cutover_target_lock
        )
        self._target_identity_assertion = (
            target_identity_assertion
            if target_identity_assertion is not None
            else assert_mssql_physical_target_identity
        )

    def publish(
        self,
        ledger: BackfillLedger,
        *,
        store: Any,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
        backup: MssqlPublicationTarget,
        unique_key: Any,
        campaign_owner: str | None,
        load_config: Any,
    ) -> BackfillLedger:
        publication = ledger.publication
        if publication is None or not publication.receipt_id:
            raise RuntimeError("mssql_backfill_publication.receipt_required")
        commit_attempted = False
        self._connector.begin()
        try:
            self._connector.execute_query("SET XACT_ABORT ON")
            self._connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            target_identity = self._target_lock_acquirer(self._connector, live, load_config)
            timeout_ms = transaction_lock_timeout_ms(load_config)
            acquire_publication_lock(
                self._connector,
                live,
                phase="publish",
                timeout_ms=timeout_ms,
            )
            self._target_identity_assertion(
                self._connector,
                database=_require_target_database(live),
                schema=live.schema,
                table=live.table,
                expected=target_identity,
            )
            self._fence_campaign(ledger, store=store, campaign_owner=campaign_owner)
            self._require_generation_cas(ledger, live=live, shadow=shadow)
            self._reprove_candidate(
                ledger,
                live=live,
                shadow=shadow,
                backup=backup,
                unique_key=unique_key,
            )
            self._connector.execute_query(publication_rename_statement(live, backup.table))
            self._connector.execute_query(publication_rename_statement(shadow, live.table))
            handoff = ledger.xmin_handoff
            publish_generation_head(
                self._connector,
                live,
                publication_receipt_id=publication.receipt_id,
                xmin_state_key_sha256=(handoff.state_key_sha256 if handoff is not None else None),
                xmin_seed_load_id=(handoff.seed_load_id if handoff is not None else None),
                xmin_receipt_id=(handoff.receipt_id if handoff is not None and handoff.status == "committed" else None),
            )
            ledger = self._persist(ledger, store=store, campaign_owner=campaign_owner)
            commit_attempted = True
            self._connector.commit_transaction()
            return ledger
        except BaseException as exc:
            if not commit_attempted:
                self._connector.rollback()
                raise
            self._connector.close()
            try:
                recovered = store.load(ledger.run_key)
            except BaseException:
                recovered = None
            if self._is_expected_receipt(recovered, ledger):
                return recovered
            raise RuntimeError("mssql_backfill_publication.commit_outcome_unknown") from exc

    def _fence_campaign(self, ledger: BackfillLedger, *, store: Any, campaign_owner: str | None) -> None:
        if campaign_owner is None:
            return
        fence = getattr(store, "fence_publication_in_transaction", None)
        if not callable(fence):
            raise RuntimeError("mssql_backfill_publication.transaction_fence_required")
        fence(ledger.run_key, owner=campaign_owner, connector=self._connector)

    def _persist(self, ledger: BackfillLedger, *, store: Any, campaign_owner: str | None) -> BackfillLedger:
        persisted = (
            store.persist_campaign_in_transaction(
                ledger,
                connector=self._connector,
                campaign_owner=campaign_owner,
            )
            if campaign_owner is not None
            else store.persist_campaign_in_transaction(ledger, connector=self._connector)
        )
        return persisted if isinstance(persisted, BackfillLedger) else ledger

    def _require_generation_cas(
        self,
        ledger: BackfillLedger,
        *,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
    ) -> None:
        publication = ledger.publication
        assert publication is not None
        live_generation = read_publication_generation(self._connector, live)
        shadow_generation = read_publication_generation(self._connector, shadow)
        expected_live = (
            publication.predecessor_object_id,
            publication.predecessor_create_token,
            publication.predecessor_receipt_id,
        )
        actual_live = (
            live_generation.object_id,
            live_generation.create_token,
            live_generation.publication_receipt_id,
        )
        if expected_live != actual_live:
            raise RuntimeError("mssql_backfill_publication.stale_live_generation")
        if (
            publication.shadow_object_id != shadow_generation.object_id
            or publication.shadow_create_token != shadow_generation.create_token
        ):
            raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")
        if shadow_generation.publication_receipt_id is not None:
            raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")

    def _reprove_candidate(
        self,
        ledger: BackfillLedger,
        *,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
        backup: MssqlPublicationTarget,
        unique_key: Any,
    ) -> None:
        publication = ledger.publication
        assert publication is not None
        if publication.object_contract_sha256 is None:
            raise RuntimeError("mssql_backfill_publication.object_contract_receipt_required")
        current_contract = self._object_contract_guard.require_supported(live)
        if current_contract.sha256 != publication.object_contract_sha256:
            raise RuntimeError("mssql_backfill_publication.object_contract_changed")
        self._object_contract_guard.require_matching_shadow(current_contract, shadow)
        self._require_table(live)
        self._require_table(shadow)
        if self._table_exists(backup):
            raise RuntimeError("mssql_backfill_publication.backup_name_conflict")
        require_shadow_owner(self._connector, shadow, run_key=ledger.run_key)
        if count_rows(self._connector, shadow) != publication.expected_rows:
            raise RuntimeError("mssql_backfill_publication.shadow_changed_after_validation")
        if has_duplicate_key(self._connector, shadow, unique_key):
            raise RuntimeError("mssql_backfill_publication.duplicate_unique_key")
        require_unique_key_index(self._connector, live, unique_key)
        require_matching_columns(self._connector, live, shadow)
        require_matching_indexes(self._connector, live, shadow)

    def _require_table(self, target: MssqlPublicationTarget) -> None:
        if not self._table_exists(target):
            raise RuntimeError(f"mssql_backfill_publication.table_missing:{target.dataset}")

    def _table_exists(self, target: MssqlPublicationTarget) -> bool:
        return bool(self._connector.table_exists(target.schema, target.table, database=target.database))

    @staticmethod
    def _is_expected_receipt(recovered: Any, expected: BackfillLedger) -> bool:
        expected_publication = expected.publication
        recovered_publication = getattr(recovered, "publication", None)
        return bool(
            recovered is not None
            and expected_publication is not None
            and recovered_publication is not None
            and recovered_publication.phase == "published"
            and recovered_publication.to_jsonable() == expected_publication.to_jsonable()
        )


def _require_target_database(target: MssqlPublicationTarget) -> str:
    database = str(target.database or "").strip()
    if not database:
        raise RuntimeError("mssql_backfill_publication.target_database_required")
    return database


__all__ = ["MssqlBackfillCutover", "acquire_cutover_target_lock"]
