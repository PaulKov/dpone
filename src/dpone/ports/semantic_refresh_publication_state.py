"""Durable publication-state capability for semantic refresh."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class SemanticRefreshPublicationState(Protocol):
    """Persist ordered journal transitions and terminal heads transactionally."""

    def persist_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist the exact PREPARED receipt before commit coordination."""

    def mark_committing(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Move PREPARED to COMMITTING before target exchange."""

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist exact proven exchange evidence before terminal head publication."""

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist that target commit is proven but terminal publication is incomplete."""

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist unresolved UUID reconciliation after an exchange attempt."""

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Reconcile PREPARED when UUID evidence proves exchange did not commit."""

    def publish_or_reconcile(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """CAS-publish or reconcile the exact idempotent durable transaction."""

    def publish_empty_scope(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Atomically advance scope/checkpoint/journal without target mutation."""
