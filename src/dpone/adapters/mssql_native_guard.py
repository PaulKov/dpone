"""Session-owned SQL Server application locks for native staging writers.

Every worker owns a separate connection. A replacement writer obtains the same
resource lock and checks its current lease before inspecting or deleting stages.
"""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dpone.contracts.bounded_window import WindowContractError, WindowLease
from dpone.ports.bounded_window import WindowStore


@contextmanager
def native_stage_writer_scope(connector: Any, store: WindowStore, lease: WindowLease, identity: str) -> Iterator[None]:
    """Wait for old session settlement, then revalidate ownership; never share sessions."""
    store.assert_lease(lease)
    resource = "dpone-native:" + hashlib.sha256((lease.target_id + "\0" + identity).encode()).hexdigest()
    result = connector.get_records(
        "DECLARE @code int; EXEC @code = sys.sp_getapplock @Resource=?, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=30000; SELECT @code;",
        (resource,),
    )
    if not result or type(result[0][0]) is not int or result[0][0] < 0:
        raise WindowContractError("mssql_native.writer_not_settled")
    try:
        store.assert_lease(lease)
        yield
    finally:
        primary = sys.exc_info()[1]
        try:
            released = connector.get_records(
                "DECLARE @code int; EXEC @code = sys.sp_releaseapplock @Resource=?, @LockOwner='Session'; SELECT @code;",
                (resource,),
            )
            if not released or released[0][0] < 0:
                raise WindowContractError("mssql_native.writer_release_failed")
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note(f"native stage lock cleanup failed: {type(error).__name__}")
