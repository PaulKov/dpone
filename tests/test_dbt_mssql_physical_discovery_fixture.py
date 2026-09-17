"""Offline fixture composition checks; no live SQL authority claim."""

from types import SimpleNamespace

import pytest

from dpone.contracts.dbt_publish_schema_contract_v4 import validate_native_policy_v4
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from tests.support import dbt_mssql_physical_source_authority as source_module
from tests.support.dbt_mssql_physical_catalog_v2_live import ArchiveAuthority
from tests.support.dbt_mssql_physical_discovery_live import DiscoveryArchiveAuthority, DiscoverySourceFixture


def test_filegroup_is_authored_before_base_archive_production(tmp_path, monkeypatch):
    original = source_module.native_policy
    captured = []

    def registration(self, *args):
        policy = source_module.native_policy()
        validate_native_policy_v4(encode_native_delivery_json(policy), max_bytes=1048576)
        captured.append(policy)
        return "claim"

    monkeypatch.setattr(ArchiveAuthority, "registration", registration)
    authority = DiscoveryArchiveAuthority(tmp_path, monkeypatch, 1)
    assert authority.registration(None, None, None, None) == "claim"
    assert captured[0]["profiles"]["local"]["native_execution"]["physical_filegroup"] == {"name": "PRIMARY"}
    assert source_module.native_policy is original
    assert "physical_filegroup" not in original()["profiles"]["local"]["native_execution"]


def test_owner_override_stops_before_generation_and_keeps_actual_output(monkeypatch):
    fixture = DiscoverySourceFixture(object(), "legacy-correctness-v1")
    result = object()
    calls = []

    def owner(registration, admin, name):
        calls.append((registration, admin, name))
        return result

    fixture.registration = object()
    monkeypatch.setattr(fixture, "authority", SimpleNamespace(admit_physical_owner=owner))
    fixture._admit()
    assert fixture.owner is result
    assert len(calls) == 1 and calls[0][0] is fixture.registration
    assert calls[0][2] == fixture.credentials["metadata"][0]
    assert not hasattr(fixture, "executor") and not hasattr(fixture, "reserved")


def test_policy_producer_is_restored_after_failure(tmp_path, monkeypatch):
    original = source_module.native_policy

    def fail(*args):
        assert source_module.native_policy is not original
        raise RuntimeError("archive failure")

    monkeypatch.setattr(ArchiveAuthority, "registration", fail)
    with pytest.raises(RuntimeError):
        DiscoveryArchiveAuthority(tmp_path, monkeypatch, 1).registration(None, None, None, None)
    assert source_module.native_policy is original


def epochs():
    from datetime import datetime, timedelta

    before: dict[str, dict[str, tuple]] = {
        "control": {
            "semantic_refresh_ddl_epoch": ((1, 7, "ALTER_PROCEDURE", datetime(2026, 9, 16)),),
            "semantic_refresh_guards": (("guard", 1, "HELD"),),
        },
        "model": {"registration": ("exact",)},
    }
    after = {"control": dict(before["control"]), "model": dict(before["model"])}
    after["control"]["semantic_refresh_ddl_epoch"] = (
        (1, 8, "CREATE_VIEW", datetime(2026, 9, 16) + timedelta(seconds=1)),
    )
    return before, after


def test_expected_ddl_transition_is_exact_and_does_not_mutate_snapshots():
    from copy import deepcopy
    from datetime import timedelta

    from tests.support.dbt_mssql_physical_discovery_live import require_ddl_transition

    before, created = epochs()
    saved = deepcopy((before, created))
    require_ddl_transition(before, created, layout="same_database", event="CREATE_VIEW")
    dropped = deepcopy(created)
    row = created["control"]["semantic_refresh_ddl_epoch"][0]
    dropped["control"]["semantic_refresh_ddl_epoch"] = ((1, 9, "DROP_VIEW", row[3] + timedelta(seconds=1)),)
    require_ddl_transition(created, dropped, layout="same_database", event="DROP_VIEW")
    assert (before, created) == saved
    require_ddl_transition(before, deepcopy(before), layout="two_database", event="CREATE_VIEW")


@pytest.mark.parametrize(
    "damage", ["epoch", "event", "timestamp", "singleton", "extra_row", "guard", "model", "two_database"]
)
def test_ddl_transition_rejects_any_unexpected_change(damage):
    from tests.support.dbt_mssql_physical_discovery_live import require_ddl_transition

    before, after = epochs()
    row = list(after["control"]["semantic_refresh_ddl_epoch"][0])
    if damage == "epoch":
        row[1] = 9
    if damage == "event":
        row[2] = "ALTER_VIEW"
    if damage == "timestamp":
        row[3] = before["control"]["semantic_refresh_ddl_epoch"][0][3]
    if damage == "singleton":
        row[0] = 2
    after["control"]["semantic_refresh_ddl_epoch"] = (tuple(row),) * (2 if damage == "extra_row" else 1)
    if damage == "guard":
        after["control"]["semantic_refresh_guards"] = (("guard", 2, "HELD"),)
    if damage == "model":
        after["model"]["registration"] = ("changed",)
    with pytest.raises(AssertionError):
        require_ddl_transition(
            before, after, layout="two_database" if damage == "two_database" else "same_database", event="CREATE_VIEW"
        )


@pytest.mark.parametrize(
    "name,number",
    [("DPONE_DISCOVERY_NAMESPACE_COLLISION", 51480), ("DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED", 51301)],
)
def test_original_sql_diagnostic_requires_exact_name_and_number(name, number):
    from dpone.adapters.dbt_mssql_physical_discovery import PhysicalDiscoveryReadError
    from tests.support.dbt_mssql_physical_discovery_live import require_sql_rejection

    error = PhysicalDiscoveryReadError("P-only discovery could not be observed")
    error.__cause__ = RuntimeError("42000", f"[SQL Server]{name} ({number}) (SQLExecDirectW)")
    require_sql_rejection(error, name=name, number=number)
    for cause in (
        None,
        RuntimeError(name),
        RuntimeError(f"{name} (1{number})"),
        RuntimeError(f"{name}_OTHER ({number})"),
        RuntimeError(f"OTHER ({number})"),
    ):
        error.__cause__ = cause
        with pytest.raises(AssertionError):
            require_sql_rejection(error, name=name, number=number)
