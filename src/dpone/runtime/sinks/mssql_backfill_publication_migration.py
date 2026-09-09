"""One-time generation binding for legacy MSSQL publication records."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.backfill.state import BackfillLedger, BackfillPublicationRecord
from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    MssqlPublicationTarget,
    require_shadow_owner,
)
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    MssqlPublicationGeneration,
    acquire_publication_lock,
    ensure_publication_generation_token,
    publish_generation_head,
    read_publication_generation,
)


def predecessor_binding(
    existing: BackfillPublicationRecord | None,
    generation: MssqlPublicationGeneration,
) -> tuple[int, str, str | None]:
    """Return an exact predecessor or a provisional legacy binding."""

    if existing is None:
        return generation.object_id, generation.create_token, generation.publication_receipt_id
    if existing.predecessor_object_id is None:
        if generation.publication_receipt_id is not None:
            raise RuntimeError("mssql_backfill_publication.legacy_generation_unbound")
        return generation.object_id, generation.create_token, None
    actual = (
        generation.object_id,
        generation.create_token,
        generation.publication_receipt_id,
    )
    if existing.predecessor_create_token is None:
        if (
            existing.predecessor_object_id,
            existing.predecessor_receipt_id,
        ) != (
            generation.object_id,
            generation.publication_receipt_id,
        ):
            raise RuntimeError("mssql_backfill_publication.stale_live_generation")
        return actual
    expected = (
        existing.predecessor_object_id,
        existing.predecessor_create_token,
        existing.predecessor_receipt_id,
    )
    if expected != actual:
        raise RuntimeError("mssql_backfill_publication.stale_live_generation")
    return expected


def publication_generation_complete(publication: BackfillPublicationRecord) -> bool:
    """Return whether both SQL object generations are durably ABA-safe."""

    return all(
        value is not None
        for value in (
            publication.predecessor_object_id,
            publication.predecessor_create_token,
            publication.shadow_object_id,
            publication.shadow_create_token,
        )
    )


class MssqlBackfillPublicationMigration:
    """Bind legacy prepared/published rows under campaign and target fences."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def bind_prepared(
        self,
        ledger: BackfillLedger,
        *,
        store: Any,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
        campaign_owner: str | None,
    ) -> None:
        """Capture both object ids before resumed source dispatch."""

        current = self._begin_fenced(ledger, store=store, live=live, campaign_owner=campaign_owner)
        commit_attempted = False
        try:
            publication = self._publication(current, phase="prepared")
            _bind_object_contract(publication, ledger.publication)
            ensure_publication_generation_token(self._connector, live)
            ensure_publication_generation_token(self._connector, shadow)
            live_generation = read_publication_generation(self._connector, live)
            shadow_generation = read_publication_generation(self._connector, shadow)
            if live_generation.publication_receipt_id is not None or live_generation.xmin_handoff_pending:
                raise RuntimeError("mssql_backfill_publication.legacy_generation_unbound")
            require_shadow_owner(self._connector, shadow, run_key=ledger.run_key)
            predecessor_object_id, predecessor_create_token, predecessor_receipt_id = predecessor_binding(
                publication,
                live_generation,
            )
            _require_compatible_shadow(publication, shadow_generation)
            publication.predecessor_object_id = predecessor_object_id
            publication.predecessor_create_token = predecessor_create_token
            publication.predecessor_receipt_id = predecessor_receipt_id
            publication.shadow_object_id = shadow_generation.object_id
            publication.shadow_create_token = shadow_generation.create_token
            committed = self._persist(current, store=store, campaign_owner=campaign_owner)
            commit_attempted = True
            committed = self._commit_or_recover(committed, store=store, phase="prepared")
        except BaseException:
            if not commit_attempted:
                self._connector.rollback()
            raise
        ledger.publication = deepcopy(committed.publication)

    def bind_published(
        self,
        ledger: BackfillLedger,
        *,
        store: Any,
        live: MssqlPublicationTarget,
        backup: MssqlPublicationTarget,
        campaign_owner: str | None,
    ) -> None:
        """Migrate a pre-generation published receipt without reopening cutover."""

        current = self._begin_fenced(ledger, store=store, live=live, campaign_owner=campaign_owner)
        commit_attempted = False
        try:
            publication = self._publication(current, phase="published")
            if not publication.receipt_id:
                raise RuntimeError("mssql_backfill_publication.receipt_required")
            require_shadow_owner(self._connector, live, run_key=ledger.run_key)
            ensure_publication_generation_token(self._connector, live)
            ensure_publication_generation_token(self._connector, backup)
            live_generation = read_publication_generation(self._connector, live)
            backup_generation = read_publication_generation(self._connector, backup)
            expected_head = self._expected_head(current)
            self._bind_or_require_head(
                live,
                live_generation,
                publication_receipt_id=publication.receipt_id,
                expected_head=expected_head,
            )
            _require_compatible_published_generation(
                publication,
                live_generation=live_generation,
                backup_generation=backup_generation,
            )
            publication.predecessor_object_id = backup_generation.object_id
            publication.predecessor_create_token = backup_generation.create_token
            publication.predecessor_receipt_id = backup_generation.publication_receipt_id
            publication.shadow_object_id = live_generation.object_id
            publication.shadow_create_token = live_generation.create_token
            committed = self._persist(current, store=store, campaign_owner=campaign_owner)
            commit_attempted = True
            committed = self._commit_or_recover(committed, store=store, phase="published")
        except BaseException:
            if not commit_attempted:
                self._connector.rollback()
            raise
        ledger.publication = deepcopy(committed.publication)
        ledger.xmin_handoff = deepcopy(committed.xmin_handoff)

    def _begin_fenced(
        self,
        ledger: BackfillLedger,
        *,
        store: Any,
        live: MssqlPublicationTarget,
        campaign_owner: str | None,
    ) -> BackfillLedger:
        if not campaign_owner:
            raise RuntimeError("mssql_backfill_publication.legacy_rebind_campaign_fence_required")
        fence = getattr(store, "fence_publication_in_transaction", None)
        if not callable(fence):
            raise RuntimeError("mssql_backfill_publication.transaction_fence_required")
        self._connector.begin()
        try:
            self._connector.execute_query("SET XACT_ABORT ON")
            self._connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            acquire_publication_lock(self._connector, live, phase="legacy-rebind")
            current = fence(ledger.run_key, owner=campaign_owner, connector=self._connector)
            return current if isinstance(current, BackfillLedger) else deepcopy(ledger)
        except BaseException:
            self._connector.rollback()
            raise

    def _persist(self, ledger: BackfillLedger, *, store: Any, campaign_owner: str | None) -> BackfillLedger:
        persisted = store.persist_campaign_in_transaction(
            ledger,
            connector=self._connector,
            campaign_owner=campaign_owner,
        )
        return persisted if isinstance(persisted, BackfillLedger) else ledger

    def _commit_or_recover(self, expected: BackfillLedger, *, store: Any, phase: str) -> BackfillLedger:
        try:
            self._connector.commit_transaction()
            return expected
        except BaseException as exc:
            closer = getattr(self._connector, "close", None)
            if callable(closer):
                closer()
            recovered = store.load(expected.run_key)
            publication = recovered.publication if recovered is not None else None
            expected_publication = expected.publication
            if (
                publication is not None
                and expected_publication is not None
                and publication.phase == phase
                and publication.receipt_id == expected_publication.receipt_id
                and publication.predecessor_object_id == expected_publication.predecessor_object_id
                and publication.predecessor_create_token == expected_publication.predecessor_create_token
                and publication.predecessor_receipt_id == expected_publication.predecessor_receipt_id
                and publication.shadow_object_id == expected_publication.shadow_object_id
                and publication.shadow_create_token == expected_publication.shadow_create_token
                and publication.object_contract_sha256 == expected_publication.object_contract_sha256
            ):
                return recovered
            raise RuntimeError("mssql_backfill_publication.migration_commit_outcome_unknown") from exc

    @staticmethod
    def _publication(ledger: BackfillLedger, *, phase: str) -> BackfillPublicationRecord:
        publication = ledger.publication
        if publication is None or publication.phase != phase:
            raise RuntimeError("mssql_backfill_publication.legacy_phase_changed")
        return publication

    @staticmethod
    def _expected_head(ledger: BackfillLedger) -> tuple[str | None, str | None, str | None]:
        handoff = ledger.xmin_handoff
        if handoff is None:
            return None, None, None
        return (
            handoff.state_key_sha256,
            handoff.seed_load_id,
            handoff.receipt_id if handoff.status == "committed" else None,
        )

    def _bind_or_require_head(
        self,
        live: MssqlPublicationTarget,
        generation: MssqlPublicationGeneration,
        *,
        publication_receipt_id: str,
        expected_head: tuple[str | None, str | None, str | None],
    ) -> None:
        state_key, seed_load_id, xmin_receipt_id = expected_head
        if generation.publication_receipt_id is None:
            publish_generation_head(
                self._connector,
                live,
                publication_receipt_id=publication_receipt_id,
                xmin_state_key_sha256=state_key,
                xmin_seed_load_id=seed_load_id,
                xmin_receipt_id=xmin_receipt_id,
            )
            return
        actual = (
            generation.publication_receipt_id,
            generation.xmin_state_key_sha256,
            generation.xmin_seed_load_id,
            generation.xmin_receipt_id,
        )
        if actual != (publication_receipt_id, state_key, seed_load_id, xmin_receipt_id):
            raise RuntimeError("mssql_backfill_publication.published_generation_changed")


def _require_compatible_shadow(
    publication: BackfillPublicationRecord,
    generation: MssqlPublicationGeneration,
) -> None:
    if publication.shadow_object_id not in (None, generation.object_id):
        raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")
    if publication.shadow_create_token not in (None, generation.create_token):
        raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")
    if generation.publication_receipt_id is not None:
        raise RuntimeError("mssql_backfill_publication.shadow_generation_changed")


def _bind_object_contract(
    current: BackfillPublicationRecord,
    candidate: BackfillPublicationRecord | None,
) -> None:
    digest = candidate.object_contract_sha256 if candidate is not None else None
    if digest is None:
        raise RuntimeError("mssql_backfill_publication.object_contract_receipt_required")
    if current.object_contract_sha256 not in (None, digest):
        raise RuntimeError("mssql_backfill_publication.object_contract_changed")
    current.object_contract_sha256 = digest


def _require_compatible_published_generation(
    publication: BackfillPublicationRecord,
    *,
    live_generation: MssqlPublicationGeneration,
    backup_generation: MssqlPublicationGeneration,
) -> None:
    if publication.shadow_object_id not in (None, live_generation.object_id):
        raise RuntimeError("mssql_backfill_publication.published_generation_changed")
    if publication.shadow_create_token not in (None, live_generation.create_token):
        raise RuntimeError("mssql_backfill_publication.published_generation_changed")
    if publication.predecessor_object_id not in (None, backup_generation.object_id):
        raise RuntimeError("mssql_backfill_publication.published_backup_generation_changed")
    if publication.predecessor_create_token not in (None, backup_generation.create_token):
        raise RuntimeError("mssql_backfill_publication.published_backup_generation_changed")
    if (
        publication.predecessor_object_id is not None
        and publication.predecessor_receipt_id != backup_generation.publication_receipt_id
    ):
        raise RuntimeError("mssql_backfill_publication.published_backup_generation_changed")


__all__ = [
    "MssqlBackfillPublicationMigration",
    "predecessor_binding",
    "publication_generation_complete",
]
