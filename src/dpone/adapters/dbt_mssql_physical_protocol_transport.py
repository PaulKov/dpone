"""Strict same-handle raw attach cursor lifecycle, with no connection ownership.

Composition must authenticate the packet before constructing this internal
helper. It is not a generic SQL API or an admission provider. Driver cancellation
and the owner's independent process deadline still require live qualification.
"""

from collections.abc import Callable, Mapping
from math import ceil
from threading import Event, Lock, get_ident
from time import monotonic_ns
from typing import Any

from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
from dpone.contracts.dbt_mssql_physical_protocol import (
    ATTACH_COLUMNS,
    ATTACH_SQL,
    require_attach_request,
    require_attach_row,
)
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery


class StrictPhysicalProtocolTransport:
    """One adapter-command budget, actual handle identity and terminal poison.

    Each thread may attach exactly once, including failed attempts. Replacing a
    handle never renews scope. No begin/commit/rollback, retries, reconnect or
    autocommit changes occur. Only already materialized handles may be supplied
    by the owning dbt manager, never through Jinja.
    """

    def __init__(
        self,
        *,
        delivery: PhysicalTransportDelivery,
        cancellation: Event,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        if type(delivery) is not PhysicalTransportDelivery:
            raise ValueError("strict transport requires exact delivered representation")
        delivery.__post_init__()
        self._delivery, self._cancel, self._clock = delivery, cancellation, clock
        self._remaining = delivery.limits.max_generation_bytes
        self._lock, self._poisoned = Lock(), False
        self._handles: dict[int, object] = {}

    def _check(self) -> int:
        remaining = self._delivery.deadline_monotonic_ns - self._clock()
        if self._poisoned or self._cancel.is_set() or remaining <= 0:
            raise ValueError("strict physical operation cancelled or expired")
        return max(1, ceil(remaining / 1_000_000_000))

    def execute(
        self,
        handle: Any,
        operation: object,
        parameters: Mapping[str, object],
    ) -> tuple[tuple[object, ...], ...]:
        """Accept one fully validated singleton only after terminal nextset/close.

        The integer driver timeout is rounded up, never reset to a fresh TTL.
        It is not a subsecond cancellation guarantee. Any uncertain response or
        close poisons the command; driver error messages are not exposed.
        """
        cursor = None
        failed = False
        try:
            with self._lock:
                if self._poisoned:
                    raise ValueError("strict physical command is poisoned")
                values = require_attach_request(operation, parameters, self._delivery)
                self._check()
                thread = get_ident()
                if thread in self._handles or any(item is handle for item in self._handles.values()):
                    raise ValueError("strict physical handle cannot be reattached or replaced")
                self._handles[thread] = handle
            handle.timeout = self._check()
            cursor = handle.cursor()
            self._check()
            cursor.execute(ATTACH_SQL, values)
            self._check()
            description = cursor.description
            if description is None or tuple(item[0] for item in description) != ATTACH_COLUMNS:
                raise ValueError("strict physical result description differs")
            limits = self._delivery.limits
            budget = CatalogReadBudget(
                self._delivery.deadline_monotonic_ns / 1_000_000_000,
                limits.max_metadata_bytes,
                lambda: self._clock() / 1_000_000_000,
            )
            rows: list[tuple[object, ...]] = []
            while True:
                self._check()
                observed = cursor.fetchone()
                self._check()
                if observed is None:
                    break
                if rows:
                    raise ValueError("strict physical singleton row bound exceeded")
                row = tuple(observed)
                if len(row) != len(ATTACH_COLUMNS):
                    raise ValueError("strict physical row width differs")
                before = budget.remaining_bytes
                budget.charge(row, limits.max_definition_utf16_bytes)
                with self._lock:
                    charged = before - budget.remaining_bytes
                    if self._poisoned or charged > self._remaining:
                        raise ValueError("strict physical command aggregate budget exceeded or poisoned")
                    self._remaining -= charged
                require_attach_row(row, delivery=self._delivery, parameters=values)
                rows.append(row)
            if not rows:
                raise ValueError("strict physical singleton is missing")
            self._check()
            if cursor.nextset() is not None:
                raise ValueError("strict physical response has extra or ambiguous sets")
            self._check()
            with self._lock:
                if self._poisoned:
                    raise ValueError("strict physical command was concurrently poisoned")
            return tuple(rows)
        except BaseException:
            failed = True
            with self._lock:
                self._poisoned = True
            if cursor is not None:
                try:
                    cursor.cancel()
                except BaseException:
                    pass
            raise ValueError("strict physical operation failed; no retry or settlement is implied") from None
        finally:
            try:
                if cursor is not None:
                    cursor.close()
                if not failed:
                    self._check()
            except BaseException:
                self._poisoned = True
                if not failed:
                    raise ValueError("strict physical cursor close/deadline failed") from None
