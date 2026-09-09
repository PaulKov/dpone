"""Thin callback adapters for semantic-refresh ClickHouse/state composition."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class CallbackSemanticRefreshClickHouseGateway:
    """Delegate vendor I/O while keeping runtime plans as primitive mappings."""

    prepare_callback: Callable[[Mapping[str, object], Mapping[str, str]], Mapping[str, object]]
    revalidate_callback: Callable[[Mapping[str, object], Mapping[str, str]], Mapping[str, object]]
    inspect_uuid_callback: Callable[[Mapping[str, object]], Mapping[str, object]]
    exchange_callback: Callable[[Mapping[str, object]], None]
    inspect_target_authority_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    empty_scope_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    cleanup_callback: Callable[[Mapping[str, object]], None] | None = None

    def prepare(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        return self.prepare_callback(plan, sql)

    def inspect_uuid_map(self, request: Mapping[str, object]) -> Mapping[str, object]:
        return self.inspect_uuid_callback(request)

    def inspect_target_authority(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        if self.inspect_target_authority_callback is None:
            raise RuntimeError("target physical-authority callback is unavailable")
        return self.inspect_target_authority_callback(plan)

    def revalidate(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        return self.revalidate_callback(plan, sql)

    def exchange(self, request: Mapping[str, object]) -> None:
        self.exchange_callback(request)

    def cleanup_retained(self, request: Mapping[str, object]) -> None:
        if self.cleanup_callback is not None:
            self.cleanup_callback(request)

    def inspect_empty_scope(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        if self.empty_scope_callback is None:
            raise RuntimeError("empty-scope callback is unavailable")
        return self.empty_scope_callback(plan)


@dataclass(frozen=True)
class CallbackSemanticRefreshPublicationState:
    """Delegate one transactional multi-head publication call."""

    persist_prepared_callback: Callable[[Mapping[str, object]], Mapping[str, object]]
    mark_committing_callback: Callable[[Mapping[str, object]], Mapping[str, object]]
    publish_callback: Callable[[Mapping[str, object]], Mapping[str, object]]
    publish_empty_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    record_target_committed_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    record_committed_incomplete_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    record_commit_unknown_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None
    reconcile_prepared_callback: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None

    def persist_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        return self.persist_prepared_callback(request)

    def mark_committing(self, request: Mapping[str, object]) -> Mapping[str, object]:
        return self.mark_committing_callback(request)

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        callback = self.record_target_committed_callback or self.mark_committing_callback
        return callback(request)

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        callback = self.record_committed_incomplete_callback or self.mark_committing_callback
        return callback(request)

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]:
        callback = self.record_commit_unknown_callback or self.mark_committing_callback
        return callback(request)

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        callback = self.reconcile_prepared_callback or self.mark_committing_callback
        return callback(request)

    def publish_or_reconcile(self, request: Mapping[str, object]) -> Mapping[str, object]:
        return self.publish_callback(request)

    def publish_empty_scope(self, request: Mapping[str, object]) -> Mapping[str, object]:
        callback = self.publish_empty_callback or self.publish_callback
        return callback(request)


__all__ = [
    "CallbackSemanticRefreshClickHouseGateway",
    "CallbackSemanticRefreshPublicationState",
]
