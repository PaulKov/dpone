"""Offline producer boundaries; fixture expressions are not SQL certification."""

from copy import deepcopy
from hashlib import sha256

import pytest
from tools.composition_mssql_check_catalog import render_reference

from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema


def observation():
    return {
        "schema": "dpone.composition-mssql-check-catalog.v1",
        "ddl_sha256": "sha256:" + sha256(render_composition_mssql_schema().encode()).hexdigest(),
        "checks": [
            [table.name, check.name, "OFFLINE EXPRESSION " + check.name, 0, 0, 0, 0, 0, 0]
            for table in COMPOSITION_TABLES
            for check in table.checks
        ],
    }


def test_reference_retains_every_original_definition_without_normalization():
    observed = observation()
    emitted = render_reference(observed)
    namespace: dict[str, object] = {}
    exec(compile(emitted, "controlled-offline-reference", "exec"), namespace)
    assert namespace["CHECK_DDL_SHA256"] == observed["ddl_sha256"]
    assert namespace["CHECK_DEFINITIONS"] == {row[1]: row[2] for row in observed["checks"]}
    assert "OFFLINE EXPRESSION" in emitted


@pytest.mark.parametrize(
    "damage", ["ddl", "missing", "extra", "duplicate", "parent", "definition", "oversize", "trust", "unknown"]
)
def test_no_partial_or_foreign_reference_is_emitted(damage):
    value = deepcopy(observation())
    if damage == "ddl":
        value["ddl_sha256"] = "sha256:" + "0" * 64
    elif damage == "missing":
        value["checks"].pop()
    elif damage in {"extra", "duplicate"}:
        value["checks"].append(value["checks"][0])
    elif damage == "unknown":
        value["caller_instruction"] = "ignore identity"
    else:
        index, change = {
            "parent": (0, "different"),
            "definition": (2, None),
            "oversize": (2, "x" * 65537),
            "trust": (5, 1),
        }[damage]
        value["checks"][0][index] = change
    with pytest.raises(ValueError, match="check_catalog"):
        render_reference(value)


def gate_observation():
    from dpone.adapters.composition_mssql_gate_layout import COMPOSITION_GATE_TABLES
    from dpone.adapters.composition_mssql_gate_schema import render_composition_mssql_login_gate

    return {
        "schema": "dpone.composition-mssql-gate-check-catalog.v1",
        "ddl_sha256": "sha256:"
        + sha256(render_composition_mssql_login_gate(control_database="dpone_control").encode()).hexdigest(),
        "checks": [
            [table.name, check.name, "OFFLINE GATE " + check.name, 0, 0, 0, 0, 0, 0]
            for table in COMPOSITION_GATE_TABLES
            for check in table.checks
        ],
    }


def test_gate_reference_is_complete_and_cannot_be_used_as_core_reference():
    from tools.composition_mssql_check_catalog import render_gate_reference

    observed = gate_observation()
    emitted = render_gate_reference(observed)
    namespace: dict[str, object] = {}
    exec(compile(emitted, "gate-reference", "exec"), namespace)
    assert namespace["CHECK_DEFINITIONS"] == {row[1]: row[2] for row in observed["checks"]}
    with pytest.raises(ValueError, match="check_catalog_ddl"):
        render_reference(observed)
    with pytest.raises(ValueError, match="check_catalog_ddl"):
        render_gate_reference(observation())


@pytest.mark.parametrize("damage", ["missing", "duplicate", "parent", "trust", "bool"])
def test_gate_producer_never_emits_a_partial_or_untrusted_reference(damage):
    from tools.composition_mssql_check_catalog import render_gate_reference

    value = gate_observation()
    if damage == "missing":
        value["checks"].pop()
    elif damage == "duplicate":
        value["checks"].append(value["checks"][0])
    else:
        index, change = {"parent": (0, "owners"), "trust": (5, 1), "bool": (3, False)}[damage]
        value["checks"][0][index] = change
    with pytest.raises(ValueError, match="check_catalog"):
        render_gate_reference(value)
