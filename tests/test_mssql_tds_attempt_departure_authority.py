"""Compatibility checks for the private departure-authority extraction."""

import ast
import inspect
from time import monotonic
from uuid import UUID

import pytest

from dpone.services import mssql_tds_attempt
from tests.test_mssql_tds_attempt import setup as setup
from tests.test_mssql_tds_attempt_departure import reserved


def test_departure_methods_keep_original_defining_module_and_signatures():
    sequence = mssql_tds_attempt.TdsAttempt._create_departure_sequence
    assertion = mssql_tds_attempt.TdsAttempt._assert_create_departure

    assert sequence.__module__ == "dpone.services.mssql_tds_attempt"
    assert assertion.__module__ == "dpone.services.mssql_tds_attempt"
    assert str(inspect.signature(sequence)) == (
        "(self, helper_id: 'UUID', create_identity: 'TdsCoordinatorIdentity', *, "
        "deadline: 'float') -> 'Iterator[tuple[TdsAttemptSnapshot, "
        "TdsDirectorySnapshot, TdsAttemptUnknown]]'"
    )
    assert str(inspect.signature(assertion)) == (
        "(self, helper_id: 'UUID', *, deadline: 'float') -> 'tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]'"
    )


def test_extracted_leaf_has_no_runtime_dpone_imports():
    from dpone.services import mssql_tds_attempt_departure_authority as authority

    tree = ast.parse(inspect.getsource(authority))
    runtime_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and (
            any(alias.name == "dpone" or alias.name.startswith("dpone.") for alias in node.names)
            if isinstance(node, ast.Import)
            else node.module == "dpone" or (node.module or "").startswith("dpone.")
        )
    ]

    assert runtime_imports == []


def test_active_sequence_resolves_module_binding_at_each_assertion(setup, monkeypatch):
    attempt, identity = reserved(setup)
    helper_id = UUID(int=99)
    deadline = monotonic() + 2
    marker = RuntimeError("dynamic departure binding")

    def changed_binding(*args):
        raise marker

    with pytest.raises(RuntimeError, match="dynamic departure binding"):
        with attempt._create_departure_sequence(helper_id, identity, deadline=deadline):
            monkeypatch.setattr(mssql_tds_attempt, "_departure_binding", changed_binding)

    assert attempt._departure_baseline is None
    assert attempt._busy is False
    assert attempt._poisoned is True
    attempt.close(deadline=deadline)


def test_active_sequence_resolves_module_locality_at_each_assertion(setup, monkeypatch):
    attempt, identity = reserved(setup)
    helper_id = UUID(int=99)
    deadline = monotonic() + 2
    marker = RuntimeError("dynamic locality")

    def changed_locality(*args):
        raise marker

    with pytest.raises(RuntimeError, match="dynamic locality"):
        with attempt._create_departure_sequence(helper_id, identity, deadline=deadline):
            monkeypatch.setattr(mssql_tds_attempt, "_local", changed_locality)

    assert attempt._departure_baseline is None
    assert attempt._busy is False
    assert attempt._poisoned is True
    monkeypatch.undo()
    attempt.close(deadline=deadline)
