"""Structural runtime contract for MSSQL operation-lease controllers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.etl.mssql_operation_lease import (
    LEASE_OPTION,
    MssqlOperationLeaseControllerContractError,
    MssqlOperationLeaseHeartbeat,
    lease_from_config,
)
from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices


class _IpcLeaseController:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def start(self) -> None:
        self.calls.append("start")

    def assert_healthy(self) -> None:
        self.calls.append("assert_healthy")

    def stop(self) -> None:
        self.calls.append("stop")


def _config(controller: object | None = None) -> SimpleNamespace:
    options = {} if controller is None else {LEASE_OPTION: controller}
    return SimpleNamespace(options=options)


def test_lease_from_config_preserves_default_heartbeat_controller() -> None:
    heartbeat = object.__new__(MssqlOperationLeaseHeartbeat)

    assert lease_from_config(_config(heartbeat)) is heartbeat


def test_processor_runtime_accepts_structural_ipc_controller() -> None:
    controller = _IpcLeaseController()

    started = ProcessorRuntimeServices.start_transaction_lease(_config(controller))
    assert started is controller

    started.assert_healthy()
    started.stop()
    assert controller.calls == ["start", "assert_healthy", "stop"]


def test_lease_from_config_returns_none_only_when_controller_is_absent() -> None:
    assert lease_from_config(_config()) is None


@pytest.mark.parametrize(
    "invalid",
    [object(), SimpleNamespace(start=lambda: None, stop=lambda: None)],
)
def test_lease_from_config_rejects_incomplete_runtime_controller(invalid: object) -> None:
    with pytest.raises(
        MssqlOperationLeaseControllerContractError,
        match="mssql_transaction.operation_lease_controller_invalid",
    ):
        lease_from_config(_config(invalid))
