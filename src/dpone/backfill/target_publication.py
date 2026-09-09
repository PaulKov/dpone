"""Campaign-wide target publication port for governed backfills."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.backfill.state import BackfillLedger, BackfillStateStore
    from dpone.config.load_config import LoadConfig


class BackfillTargetPublicationPort(Protocol):
    """Prepare an isolated target and publish it after every chunk commits."""

    def require_unmapped(self, *, selection: Any) -> None: ...

    def campaign_contract(self, load_config: LoadConfig) -> Mapping[str, Any]: ...

    def before_chunks(
        self,
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> LoadConfig: ...

    def after_chunks(
        self,
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> dict[str, Any]: ...


__all__ = ["BackfillTargetPublicationPort"]
