"""Optional campaign-wide lifecycle port for backfill orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.backfill.state import BackfillLedger, BackfillStateStore


class BackfillCampaignLifecyclePort(Protocol):
    """Coordinate state that must surround all chunks in one campaign."""

    def require_unmapped(self, *, selection: Any) -> None: ...

    def campaign_contract(self, load_config: Any) -> Mapping[str, Any]: ...

    def before_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> BackfillLedger: ...

    def after_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> dict[str, Any]: ...


__all__ = ["BackfillCampaignLifecyclePort"]
