"""Retained CREATE outcomes must not normalize equal-valued scalar aliases."""

from dataclasses import replace

import pytest

from dpone.app.mssql_tds_coordinator_supervisor import TdsCoordinatorCreateOutcome
from tests.test_mssql_tds_coordinator_supervisor import Harness


@pytest.mark.parametrize("field,value", [("exit_code", False), ("exit_code", 0.0), ("reaped", 1)])
def test_outcome_rejects_mutated_exit_scalars(field, value):
    original = Harness().run()
    forged = replace(original.local_exit)
    object.__setattr__(forged, field, value)
    with pytest.raises(ValueError):
        TdsCoordinatorCreateOutcome(original.response, forged, original.receipts)


def test_outcome_rejects_mutated_process_pid():
    original = Harness().run()
    process = replace(original.local_exit.identity)
    object.__setattr__(process, "pid", float(process.pid))
    local = replace(original.local_exit, identity=process)
    with pytest.raises(ValueError):
        TdsCoordinatorCreateOutcome(original.response, local, original.receipts)


@pytest.mark.parametrize("field", ["byte_count", "kind"])
def test_outcome_rejects_mutated_receipt_scalars(field):
    original = Harness().run()
    receipt = replace(original.receipts[0])
    value = float(receipt.byte_count) if field == "byte_count" else receipt.kind.value
    object.__setattr__(receipt, field, value)
    with pytest.raises(ValueError):
        TdsCoordinatorCreateOutcome(original.response, original.local_exit, (receipt, *original.receipts[1:]))
