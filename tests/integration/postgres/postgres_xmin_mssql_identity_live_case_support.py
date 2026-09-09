"""Shared standard-route assertions for identity/authority live cases."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from tests.integration.postgres.postgres_xmin_mssql_identity_live_authority import (
    consumption_count,
    ownership_ledger,
    receipt_count,
    target_before_image,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import DAG_ID

_BASE = datetime(2026, 8, 15, 9, 0)


def run_route(route: Any, config: Any, minute: int) -> dict[str, Any]:
    """Run the public processor with a unique operational execution key."""

    return route.processor.run(
        config,
        dag_id=DAG_ID,
        execution_date=_BASE + timedelta(minutes=minute),
    )


def expect_error(operation: Any, token: str) -> str:
    """Execute one production failure path and assert its stable diagnostic."""

    try:
        operation()
    except Exception as exc:  # noqa: BLE001 - live matrix asserts exact production failure.
        message = str(exc)
        if token not in message:
            raise AssertionError(f"expected {token!r}, got {message!r}") from exc
        return token
    raise AssertionError(f"expected failure containing {token!r}")


@contextmanager
def count_exports(route: Any) -> Iterator[list[int]]:
    """Count real source payload exports without replacing their implementation."""

    strategy = route.processor.source._xmin_extract
    original = strategy._export_to_file_whole
    count = [0]

    def counted(*args: Any, **kwargs: Any) -> Any:
        count[0] += 1
        return original(*args, **kwargs)

    strategy._export_to_file_whole = counted
    try:
        yield count
    finally:
        strategy._export_to_file_whole = original


def atomic_before_image(route: Any) -> tuple[Any, ...]:
    """Return the exact business/checkpoint/receipt/consumption before-image."""

    checkpoint_rows = ownership_ledger(route)
    checkpoint_image = tuple(
        (
            bytes(row["state_key"]),
            int(row["xmin_value"]),
            int(row["state_revision"]),
            row["superseded_at_utc"],
            bytes(row["superseded_by_state_key"]) if row["superseded_by_state_key"] is not None else None,
        )
        for row in checkpoint_rows
    )
    return (
        target_before_image(route),
        checkpoint_image,
        receipt_count(route),
        consumption_count(route),
    )


def staging_count(route: Any) -> int:
    """Count disposable staging objects in the exact target database."""

    rows = route.target.get_records(
        f"SELECT COUNT_BIG(*) AS n FROM [{route.target_database}].sys.tables AS t "
        f"JOIN [{route.target_database}].sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'staging'",
        as_dict=True,
    )
    return int(rows[0]["n"])


def active_owner(route: Any, state_key: bytes) -> bool:
    """Return whether the exact binary state key is still active."""

    return ledger_row(ownership_ledger(route), state_key)["superseded_at_utc"] is None


def state_exists(route: Any, state_key: bytes) -> bool:
    """Check exact binary state presence without collation-sensitive predicates."""

    return any(bytes(row["state_key"]) == state_key for row in ownership_ledger(route))


def ledger_row(rows: list[dict[str, Any]], state_key: bytes) -> dict[str, Any]:
    """Select one exact binary owner from a complete ledger read."""

    matches = [row for row in rows if bytes(row["state_key"]) == state_key]
    if len(matches) != 1:
        raise AssertionError(f"expected one state row, found {len(matches)}")
    return matches[0]


def target_row(route: Any, key: str) -> dict[str, Any]:
    """Select one exact target key from the disposable target."""

    matches = [row for row in route.target_rows() if row["metric_code"] == key]
    if len(matches) != 1:
        raise AssertionError(f"expected one target row for {key!r}")
    return matches[0]


__all__ = [
    "active_owner",
    "atomic_before_image",
    "count_exports",
    "expect_error",
    "ledger_row",
    "run_route",
    "staging_count",
    "state_exists",
    "target_row",
]
