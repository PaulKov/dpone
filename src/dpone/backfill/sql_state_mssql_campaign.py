"""Campaign-session lease and publication fencing for MSSQL backfills."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.backfill.campaign_lease import RECOVERABLE_CAMPAIGN_OWNER_PREFIX
from dpone.backfill.sql_locks import MSSQLApplicationCampaignLock, MSSQLPublicationTransactionGate
from dpone.backfill.sql_state_base import BackfillLedger
from dpone.backfill.sql_state_mssql_mutations import future_utc_iso


class MssqlBackfillCampaignFenceMixin:
    """Own the continuous MSSQL campaign session and publication handoff fence."""

    if TYPE_CHECKING:
        connector: Any
        _campaign_lock_connector: Any
        _independent_campaign_lock_session: bool
        _campaign_lock: MSSQLApplicationCampaignLock
        _publication_gate: MSSQLPublicationTransactionGate
        _held_legacy_campaigns: set[str]
        _held_recoverable_campaigns: dict[str, str]
        _mutations: Any
        _transitions: Any

        def _with_running_chunks(self, campaign: BackfillLedger | None) -> BackfillLedger | None: ...

        def _load_mssql_campaign_for_update(
            self,
            run_key: str,
            *,
            connector: Any,
        ) -> BackfillLedger | None: ...

    def _initialize_campaign_fence(self, campaign_lock_connector: Any | None) -> None:
        """Create the private session fence and its local ownership indexes."""

        self._campaign_lock_connector = campaign_lock_connector or self._open_campaign_lock_session()
        self._independent_campaign_lock_session = self._campaign_lock_connector is not self.connector
        self._campaign_lock = MSSQLApplicationCampaignLock(self._campaign_lock_connector)
        self._publication_gate = MSSQLPublicationTransactionGate()
        self._held_legacy_campaigns = set()
        self._held_recoverable_campaigns = {}

    def acquire_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool:
        expiry = future_utc_iso(lease_expires_at)
        held = run_key in self._held_legacy_campaigns or run_key in self._held_recoverable_campaigns
        if held or not self._campaign_lock.acquire(run_key):
            return False
        recoverable = owner.startswith(RECOVERABLE_CAMPAIGN_OWNER_PREFIX)
        acquired = False
        try:
            committed = self._mutations.run(
                run_key,
                lambda current: self._transitions.acquire_campaign(
                    self._with_running_chunks(current),
                    owner=owner,
                    expiry=expiry,
                ),
                before_load=lambda connector: self._publication_gate.acquire(connector, run_key),
            )
            acquired = bool(committed.result)
            if acquired:
                if recoverable:
                    self._held_recoverable_campaigns[run_key] = owner
                else:
                    self._held_legacy_campaigns.add(run_key)
            return acquired
        finally:
            if not acquired:
                self._campaign_lock.release(run_key)

    def release_campaign_lock(self, run_key: str, *, owner: str) -> None:
        locally_held = (
            self._held_recoverable_campaigns.get(run_key) == owner
            if owner.startswith(RECOVERABLE_CAMPAIGN_OWNER_PREFIX)
            else run_key in self._held_legacy_campaigns
        )
        if not locally_held:
            return
        if not self._campaign_lock.is_held(run_key):
            self._held_recoverable_campaigns.pop(run_key, None)
            self._held_legacy_campaigns.discard(run_key)
            return
        committed = self._mutations.run(
            run_key,
            lambda current: self._transitions.release_campaign(current, owner=owner),
        )
        if not committed.result:
            return
        self._held_recoverable_campaigns.pop(run_key, None)
        self._held_legacy_campaigns.discard(run_key)
        self._campaign_lock.release(run_key)

    def renew_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool:
        expiry = future_utc_iso(lease_expires_at)
        locally_held = (
            self._held_recoverable_campaigns.get(run_key) == owner
            if owner.startswith(RECOVERABLE_CAMPAIGN_OWNER_PREFIX)
            else run_key in self._held_legacy_campaigns
        )
        if not locally_held:
            return False
        if not self._campaign_lock.is_held(run_key):
            self._held_recoverable_campaigns.pop(run_key, None)
            self._held_legacy_campaigns.discard(run_key)
            return False
        return bool(
            self._mutations.run(
                run_key,
                lambda current: self._transitions.renew_campaign(
                    current,
                    owner=owner,
                    expiry=expiry,
                ),
            ).result
        )

    def state_capabilities(self) -> dict[str, Any]:
        capabilities = super().state_capabilities()  # type: ignore[misc]
        capabilities["continuous_campaign_session_fence"] = self._independent_campaign_lock_session
        return capabilities

    def persist_campaign_in_transaction(
        self,
        ledger: Any,
        *,
        connector: Any,
        campaign_owner: str | None = None,
    ) -> Any:
        if campaign_owner is not None:
            if not self._independent_campaign_lock_session or connector is self._campaign_lock_connector:
                raise RuntimeError("mssql_backfill_publication.campaign_session_mismatch")
            if self._held_recoverable_campaigns.get(
                ledger.run_key
            ) != campaign_owner or not self._campaign_lock.is_held(ledger.run_key):
                raise RuntimeError("mssql_backfill_publication.campaign_session_fence_lost")
        delegate = super().persist_campaign_in_transaction  # type: ignore[misc]
        return delegate(
            ledger,
            connector=connector,
            campaign_owner=campaign_owner,
        )

    def fence_publication_in_transaction(
        self,
        run_key: str,
        *,
        owner: str,
        connector: Any,
    ) -> BackfillLedger:
        """Capture live session authority under a transaction-owned handoff gate."""

        if not self._independent_campaign_lock_session or connector is self._campaign_lock_connector:
            raise RuntimeError("mssql_backfill_publication.campaign_session_mismatch")
        self._publication_gate.acquire(connector, run_key)
        held = self._held_recoverable_campaigns.get(run_key) == owner and self._campaign_lock.is_held(run_key)
        if not held:
            raise RuntimeError("mssql_backfill_publication.campaign_session_fence_lost")
        current = self._load_mssql_campaign_for_update(run_key, connector=connector)
        if current is None or current.lock_owner != owner or current.status == "cancel_requested":
            raise RuntimeError("mssql_backfill_publication.campaign_fence_lost")
        return current

    def _open_campaign_lock_session(self) -> Any:
        opener = getattr(self.connector, "open_session", None)
        if not callable(opener):
            return self.connector
        return opener(application_name="dpone-backfill-campaign-fence")


__all__ = ["MssqlBackfillCampaignFenceMixin"]
