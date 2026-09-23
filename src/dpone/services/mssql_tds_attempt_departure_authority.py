"""Private, state-borrowing authority for a TDS departure interval."""

import math
from collections.abc import Callable, Iterator
from typing import Any, Protocol
from uuid import UUID


class _CreateDepartureHost(Protocol):
    _fresh_creation: bool
    _departure_helper_id: UUID | None
    _departure_baseline: tuple[Any, Any, Any, float] | None
    _busy: bool
    _poisoned: bool
    _lifecycle: Any
    _directory: Any

    def _owned(self) -> None: ...

    def _require_unpoisoned(self) -> None: ...

    def _assert_create_departure(self, helper_id: UUID, *, deadline: float) -> tuple[Any, Any]: ...


class _DepartureAssertionHost(Protocol):
    _pid: int
    _thread: Any
    _departure_asserting: bool
    _departure_baseline: tuple[Any, Any, Any, float] | None
    _departure_helper_id: UUID | None
    _observe_helper_id: UUID | None
    _busy: bool
    _poisoned: bool
    _lifecycle: Any
    _directory: Any

    def _require_unpoisoned(self) -> None: ...


def create_departure_sequence(
    host: _CreateDepartureHost,
    helper_id: UUID,
    create_identity: Any,
    *,
    deadline: float,
    contract_error: Callable[[str], BaseException],
    unknown_outcome: Callable[[tuple[Any, ...]], BaseException],
    departure_binding: Callable[..., None],
) -> Iterator[tuple[Any, Any, Any]]:
    host._owned()
    host._require_unpoisoned()
    if not host._fresh_creation or host._departure_helper_id is not None:
        raise contract_error("mssql_native.tds_attempt_departure_consumed")
    if type(helper_id) is not UUID or not helper_id.int:
        raise ValueError("mssql_native.tds_attempt_departure_arguments")
    if type(deadline) is not float or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError("mssql_native.tds_attempt_departure_arguments")
    host._busy = True
    try:
        parent = host._lifecycle.snapshot
        host._require_unpoisoned()
        directory = host._directory.observation.snapshot
        host._require_unpoisoned()
        if directory is None:
            raise unknown_outcome((host._lifecycle, host._directory))
        departure_binding(parent, directory, create_identity)
        host._departure_helper_id = helper_id
        host._departure_baseline = (parent, directory, create_identity, deadline)
        host._assert_create_departure(helper_id, deadline=deadline)
        yield parent, directory, unknown_outcome((host._lifecycle, host._directory))
        host._assert_create_departure(helper_id, deadline=deadline)
    except BaseException:
        host._poisoned = True
        raise
    finally:
        host._departure_baseline = None
        host._busy = False


def assert_create_departure(
    host: _DepartureAssertionHost,
    helper_id: UUID,
    *,
    deadline: float,
    contract_error: Callable[[str], BaseException],
    directory_authority: Callable[[], object],
    local: Callable[[int, object], None],
    departure_binding: Callable[..., None],
) -> tuple[Any, Any]:
    local(host._pid, host._thread)
    host._require_unpoisoned()
    if host._departure_asserting:
        host._poisoned = True
        raise contract_error("mssql_native.tds_attempt_reentrant")
    host._departure_asserting = True
    try:
        baseline = host._departure_baseline
        if (
            (not host._busy and host._observe_helper_id is None)
            or baseline is None
            or type(helper_id) is not UUID
            or helper_id != (host._observe_helper_id or host._departure_helper_id)
            or type(deadline) is not float
            or not math.isfinite(deadline)
            or not 0 < deadline <= baseline[3]
        ):
            raise contract_error("mssql_native.tds_attempt_departure_inactive")
        parent, directory, identity, _ = baseline
        host._lifecycle.assert_authority(deadline=deadline)
        host._require_unpoisoned()
        current_parent = host._lifecycle.snapshot
        host._require_unpoisoned()
        ack = host._directory.execute(directory_authority(), deadline=deadline)
        host._require_unpoisoned()
        current_directory = host._directory.observation.snapshot
        host._require_unpoisoned()
        final_parent = host._lifecycle.snapshot
        host._require_unpoisoned()
        if current_directory is None:
            raise ValueError("mssql_native.tds_attempt_departure_binding")
        departure_binding(current_parent, ack, identity, identity.command)
        departure_binding(current_parent, current_directory, identity, identity.command)
        departure_binding(final_parent, current_directory, identity, identity.command)
        if current_parent != parent or final_parent != parent or ack != directory or current_directory != ack:
            raise contract_error("mssql_native.tds_attempt_departure_changed")
        return current_parent, current_directory
    except BaseException:
        host._poisoned = True
        raise
    finally:
        host._departure_asserting = False
