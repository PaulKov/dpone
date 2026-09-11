"""Offline producer boundaries; fixture expressions are not SQL certification."""

import subprocess
import sys
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest
from tools import composition_mssql_check_catalog as producer
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
    emitted = render_reference(observed, observed_context())
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
        render_reference(value, observed_context())


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
    emitted = render_gate_reference(observed, observed_context())
    namespace: dict[str, object] = {}
    exec(compile(emitted, "gate-reference", "exec"), namespace)
    assert namespace["CHECK_DEFINITIONS"] == {row[1]: row[2] for row in observed["checks"]}
    with pytest.raises(ValueError, match="check_catalog_ddl"):
        render_reference(observed, observed_context())
    with pytest.raises(ValueError, match="check_catalog_ddl"):
        render_gate_reference(observation(), observed_context())


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
        render_gate_reference(value, observed_context())


def observed_context():
    """Synthetic documentary context; no original run authentication."""
    return {
        "database": "owned",
        "compatibility_level": 160,
        "options_mask": 21816,
        "product_version": "16.0.4265.3",
        "product_version_status": "OBSERVED",
        "session_id": 54,
        "database_collation": "Latin1_General_100_BIN2",
    }


def test_controlled_metadata_and_context_are_emitted_without_normalization():
    value = observation()
    value["checks"][0][3] = 1
    for row in value["checks"]:
        row[7] = 1
    emitted = render_reference(value, observed_context())
    namespace: dict[str, object] = {}
    exec(compile(emitted, "offline-metadata-reference", "exec"), namespace)
    assert namespace["CHECK_DEFINITIONS"] == {row[1]: row[2] for row in value["checks"]}
    assert namespace["CHECK_METADATA"] == {row[1]: (row[3], row[7]) for row in value["checks"]}
    assert namespace["CHECK_DATABASE_COLLATION"] == observed_context()["database_collation"]


@pytest.mark.parametrize("gate", [False, True])
@pytest.mark.parametrize(
    "index,value",
    [(3, -1), (3, 1000), (3, True), (3, 1.0), (7, -1), (7, 2), (7, True), (7, None), (4, 1), (5, 1), (6, 1), (8, 1)],
)
def test_only_bounded_binding_and_exact_safe_flags_can_be_emitted(gate, index, value):
    observed = gate_observation() if gate else observation()
    observed["checks"][0][index] = value
    render = producer.render_gate_reference if gate else render_reference
    with pytest.raises(ValueError, match="^check_catalog_original$"):
        render(observed, observed_context())


@pytest.mark.parametrize("key", list(observed_context()))
def test_every_original_context_field_is_mandatory(key):
    context = observed_context()
    del context[key]
    with pytest.raises(ValueError, match="^check_catalog_context$"):
        render_reference(observation(), context)


@pytest.mark.parametrize(
    "key,value",
    [
        ("unknown", 1),
        ("database", ""),
        ("database", "a.b"),
        ("database", None),
        ("compatibility_level", True),
        ("compatibility_level", 0),
        ("compatibility_level", 256),
        ("options_mask", 1.0),
        ("options_mask", -1),
        ("options_mask", 2147483648),
        ("session_id", True),
        ("session_id", 0),
        ("session_id", 32768),
        ("product_version", None),
        ("product_version", ""),
        ("product_version", "16"),
        ("product_version", "16.private"),
        ("product_version", "1" * 129),
        ("product_version_status", "UNVERIFIED"),
        ("product_version_status", None),
        ("database_collation", None),
        ("database_collation", ""),
        ("database_collation", "Latin1_General_100_BIN2 "),
        ("database_collation", "x" * 129),
    ],
)
def test_unknown_malformed_or_unobserved_context_rejects(key, value):
    context = observed_context()
    context[key] = value
    with pytest.raises(ValueError, match="^check_catalog_context$"):
        producer.render_gate_reference(gate_observation(), context)


@pytest.mark.parametrize("gate", [False, True])
@pytest.mark.parametrize(
    "expression",
    ['"double quoted"', "'single' and \"double\"", "slash\\back\r\nline\n\t", "Пример 漢字 😀", "x" * 65536],
    ids=["double_quotes", "mixed_quotes", "backslash_crlf", "unicode", "maximum"],
)
def test_real_locked_formatter_preserves_original_values_and_accepts_output(gate, expression):
    observed = gate_observation() if gate else observation()
    observed["checks"][0][2] = expression
    observed["checks"][0][3] = 1
    for row in observed["checks"]:
        row[7] = 1
    render = producer.render_gate_reference if gate else render_reference
    source = render(observed, observed_context())
    namespace: dict[str, object] = {}
    exec(compile(source, "offline-reference", "exec"), namespace)
    assert namespace["CHECK_DEFINITIONS"] == {row[1]: row[2] for row in observed["checks"]}
    assert namespace["CHECK_METADATA"] == {row[1]: (row[3], row[7]) for row in observed["checks"]}
    assert namespace["CHECK_DDL_SHA256"] == observed["ddl_sha256"]
    assert namespace["CHECK_DATABASE_COLLATION"] == observed_context()["database_collation"]
    config = Path(producer.__file__).resolve().parents[1] / "pyproject.toml"
    checked = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            "--no-cache",
            "--config",
            str(config),
            "--stdin-filename",
            "composition_check_reference.py",
            "-",
        ],
        input=source,
        encoding="utf-8",
        capture_output=True,
        timeout=10,
    )
    assert checked.returncode == 0, checked.stderr


@pytest.mark.parametrize(
    "error",
    [
        OSError("private-canary"),
        subprocess.TimeoutExpired("private-canary", 10),
        subprocess.CalledProcessError(1, "private-canary", stderr="private-canary"),
    ],
)
def test_formatter_errors_are_fixed_and_cannot_return_unformatted_source(monkeypatch, error):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(producer.subprocess, "run", fail)
    with pytest.raises(ValueError, match="^check_catalog_formatter$") as caught:
        render_reference(observation(), observed_context())
    assert "private" not in str(caught.value) and caught.value.__suppress_context__


def test_capture_does_not_invoke_formatter_or_accept_reference_authority(monkeypatch):
    observed = observation()

    class Cursor:
        def execute(self, sql, name):
            self.name = name
            assert "sys.check_constraints" in sql

        def fetchall(self):
            return [row[1:] for row in observed["checks"] if self.name == f"[control].[composition_{row[0]}]"]

    def forbidden(*args):
        raise AssertionError("capture cannot format or emit a reference")

    monkeypatch.setattr(producer, "_format_reference", forbidden)
    assert producer.capture_check_catalog(Cursor(), "control") == observed


@pytest.mark.parametrize("expression", ["😀" * 32769, "\ud800"], ids=["utf16_overflow", "invalid_utf16"])
def test_impossible_sql_original_expression_is_rejected(expression):
    observed = observation()
    observed["checks"][0][2] = expression
    with pytest.raises(ValueError, match="^check_catalog_original$"):
        render_reference(observed, observed_context())
