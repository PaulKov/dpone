"""Session-bound authority for one PostgreSQL repeatable-read snapshot."""

from __future__ import annotations

from typing import Any

from dpone.runtime.extraction_lifecycle import (
    ExtractionLifecycleAuthority,
    ExtractionLifecycleReceipt,
    ExtractionLifecycleStateError,
)
from dpone.runtime.incremental_snapshot import snapshot_token_digest

_REPEATABLE_READ_AUTHORITY = "postgresql.repeatable_read"
_LEASE_BRAND = object()


class PostgresRepeatableReadSnapshotLease:
    """Prove that one connector session owns an active RR snapshot.

    Construction is restricted to :func:`issue_repeatable_read_snapshot_lease`
    after the vendor snapshot-token query succeeds.  Consumers must revalidate
    both connector and physical session identity immediately before reading.
    """

    __slots__ = (
        "_connector",
        "_lifecycle",
        "_raw_snapshot_token",
        "_session",
        "_snapshot_token_digest",
        "_visible_horizon",
    )

    def __init__(
        self,
        brand: object,
        *,
        connector: Any,
        session: object,
        lifecycle: ExtractionLifecycleAuthority,
        raw_snapshot_token: str,
        visible_horizon: int | None,
    ) -> None:
        if brand is not _LEASE_BRAND:
            raise TypeError("postgres_snapshot_lease.issuer_required")
        self._connector = connector
        self._session = session
        self._lifecycle = lifecycle
        self._raw_snapshot_token = raw_snapshot_token
        self._snapshot_token_digest = snapshot_token_digest(raw_snapshot_token)
        self._visible_horizon = visible_horizon

    @property
    def lifecycle(self) -> ExtractionLifecycleAuthority:
        """Return the single extraction authority owned by this lease."""

        return self._lifecycle

    @property
    def snapshot_token_digest(self) -> str:
        """Return the immutable digest used by downstream snapshot receipts."""

        return self._snapshot_token_digest

    @property
    def visible_horizon(self) -> int | None:
        """Return the snapshot xmax when the issuer queried it."""

        return self._visible_horizon

    def require_visible_horizon(self) -> int:
        """Return the XMin upper visibility boundary or fail closed."""

        if self._visible_horizon is None:
            raise ExtractionLifecycleStateError("postgres_snapshot_lease.visible_horizon_missing")
        return self._visible_horizon

    def require_for(self, connector: Any) -> ExtractionLifecycleReceipt:
        """Prove active RR evidence on the exact issuing connector/session."""

        if connector is not self._connector:
            raise ExtractionLifecycleStateError("postgres_snapshot_lease.connector_mismatch")
        if getattr(connector, "connection", None) is not self._session:
            raise ExtractionLifecycleStateError("postgres_snapshot_lease.session_mismatch")
        receipt = self._lifecycle.require_in_progress()
        if receipt.snapshot_authority != _REPEATABLE_READ_AUTHORITY:
            raise ExtractionLifecycleStateError("postgres_snapshot_lease.authority_mismatch")
        if receipt.source_token != self._snapshot_token_digest:
            raise ExtractionLifecycleStateError("postgres_snapshot_lease.token_mismatch")
        return receipt


def issue_repeatable_read_snapshot_lease(
    *,
    connector: Any,
    lifecycle: ExtractionLifecycleAuthority,
    raw_snapshot_token: str,
    visible_horizon: int | None = None,
) -> PostgresRepeatableReadSnapshotLease:
    """Issue a branded lease after exact vendor-token evidence is acquired."""

    token = str(raw_snapshot_token)
    if not token:
        raise ValueError("postgres_snapshot_lease.snapshot_token_missing")
    horizon = None if visible_horizon is None else int(visible_horizon)
    if horizon is not None and horizon < 0:
        raise ValueError("postgres_snapshot_lease.visible_horizon_invalid")
    digest = snapshot_token_digest(token)
    lifecycle.acquire_snapshot(
        snapshot_authority=_REPEATABLE_READ_AUTHORITY,
        source_token=digest,
    )
    return PostgresRepeatableReadSnapshotLease(
        _LEASE_BRAND,
        connector=connector,
        session=connector.connection,
        lifecycle=lifecycle,
        raw_snapshot_token=token,
        visible_horizon=horizon,
    )


__all__ = [
    "PostgresRepeatableReadSnapshotLease",
    "issue_repeatable_read_snapshot_lease",
]
