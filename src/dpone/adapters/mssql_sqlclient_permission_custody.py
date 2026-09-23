"""Bounded verification of the permission child's original pidfd custody."""

from __future__ import annotations

import time


def await_permission_custody(custody, executor, identity, deadline: float) -> None:
    """Require the executor's one exact retained pidfd before protocol I/O."""
    if time.monotonic() >= deadline:
        raise TimeoutError("mssql_native.sqlclient_permission_process_unknown")
    if executor is None or not executor.ready.wait(max(0.0, deadline - time.monotonic())):
        raise TimeoutError("mssql_native.sqlclient_permission_process_unknown")
    if time.monotonic() >= deadline:
        raise TimeoutError("mssql_native.sqlclient_permission_process_unknown")
    handle = custody.handle
    retained = [resource for resource in custody._resources if resource.kind == "pidfd"]
    if (
        executor.failed
        or handle is None
        or len(retained) != 1
        or retained[0].state != "OWNED"
        or retained[0].value is not handle
        or handle.identity != identity
    ):
        raise ValueError("mssql_native.sqlclient_permission_process_unknown")
    custody.check_owner()
