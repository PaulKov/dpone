from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.etl.mssql_operation_lease import LEASE_OPTION, lease_from_config
from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices


class _StructuralLeaseController:
    def __init__(self) -> None:
        self.events: list[str] = []

    def start(self) -> None:
        self.events.append("start")

    def assert_healthy(self) -> None:
        self.events.append("assert_healthy")

    def stop(self) -> None:
        self.events.append("stop")


def test_lease_from_config_accepts_structural_process_controller() -> None:
    controller = _StructuralLeaseController()
    config = SimpleNamespace(options={LEASE_OPTION: controller})

    assert lease_from_config(config) is controller


def test_processor_runtime_drives_the_structural_controller_lifecycle() -> None:
    controller = _StructuralLeaseController()
    config = SimpleNamespace(options={LEASE_OPTION: controller})

    lease = ProcessorRuntimeServices.start_transaction_lease(config)
    assert lease is controller
    lease.assert_healthy()
    lease.stop()

    assert controller.events == ["start", "assert_healthy", "stop"]


def test_lease_from_config_rejects_invalid_injected_controller() -> None:
    config = SimpleNamespace(options={LEASE_OPTION: object()})

    with pytest.raises(RuntimeError, match="mssql_transaction.operation_lease_controller_invalid"):
        ProcessorRuntimeServices.start_transaction_lease(config)


def test_lease_from_config_rejects_non_callable_structural_attributes() -> None:
    invalid = SimpleNamespace(start=None, assert_healthy=None, stop=None)
    config = SimpleNamespace(options={LEASE_OPTION: invalid})

    with pytest.raises(RuntimeError, match="mssql_transaction.operation_lease_controller_invalid"):
        ProcessorRuntimeServices.start_transaction_lease(config)
