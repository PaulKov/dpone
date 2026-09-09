"""Only explicit supported producers opt runtime into the new dbt wire."""

from __future__ import annotations

import pytest

from dpone.contracts.dbt_release import (
    dbt_release_producer_violation,
    dbt_release_runtime_wire_contract,
    is_workspace_dbt_wire,
)
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_runtime_wire_comes_from_validated_producer(wire: str) -> None:
    release = {"producer": {"dpone_version": "0.74.28", "wire_contract": wire}}
    assert dbt_release_runtime_wire_contract(release) == wire
    assert dbt_release_producer_violation(release, expected_wire_contract=wire) is None


def test_default_producer_gate_does_not_enable_v2_implicitly() -> None:
    release = {"producer": {"dpone_version": "0.74.28", "wire_contract": DBT_RUNTIME_WIRE_V2}}
    assert dbt_release_producer_violation(release) == "dbt release wire contract is unsupported"
    assert dbt_release_runtime_wire_contract({}) == DBT_RUNTIME_WIRE_V1


@pytest.mark.parametrize(
    "producer",
    [
        None,
        {},
        {"wire_contract": DBT_RUNTIME_WIRE_V2},
        {"wire_contract": "unsupported", "dpone_version": "0.74.28"},
        {"wire_contract": DBT_RUNTIME_WIRE_V2, "dpone_version": "bad version"},
        {"wire_contract": DBT_RUNTIME_WIRE_V2, "dpone_version": "0.74.28", "extra": True},
    ],
)
def test_present_but_invalid_producer_never_falls_back_to_legacy(producer: object) -> None:
    with pytest.raises(ValueError):
        dbt_release_runtime_wire_contract({"producer": producer})


@pytest.mark.parametrize("wire, workspace", [(None, False), (DBT_RUNTIME_WIRE_V1, False), (DBT_RUNTIME_WIRE_V2, True)])
def test_reader_and_activation_share_explicit_workspace_classification(wire, workspace):
    assert is_workspace_dbt_wire(wire) is workspace


@pytest.mark.parametrize("wire", ["", "dpone.dbt-airflow-self-service.v3", "unsupported"])
def test_unknown_wire_is_not_an_implicit_legacy_or_workspace(wire):
    with pytest.raises(ValueError, match="unsupported"):
        is_workspace_dbt_wire(wire)
