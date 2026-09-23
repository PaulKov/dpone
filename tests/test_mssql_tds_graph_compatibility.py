"""Compatibility contracts for modules consolidated by the TDS graph repair."""

import importlib


def test_server_capability_module_reexports_canonical_implementations() -> None:
    old = importlib.import_module("dpone.adapters.composition_mssql_server_capabilities")
    owner = importlib.import_module("dpone.adapters.composition_mssql_catalog_query")

    expected = ("ledger_catalog_projections", "require_database_collation", "supports_ledger_catalog")
    assert old.__all__ == expected
    for name in expected:
        assert getattr(old, name) is getattr(owner, name)


def test_attempt_ownership_module_reexports_canonical_implementations() -> None:
    old = importlib.import_module("dpone.services.mssql_tds_attempt_ownership")
    owner = importlib.import_module("dpone.services.mssql_tds_attempt_retirement")

    expected = (
        "AttemptDepartureViewMixin",
        "ShutdownCapability",
        "TdsAttemptUnknown",
        "assert_local_owner",
        "assert_retirement_descendant",
        "same_state_tree",
        "validate_deadline",
    )
    assert old.__all__ == expected
    for name in expected:
        assert getattr(old, name) is getattr(owner, name)
