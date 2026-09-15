"""Strict native control names retain their legacy behavior and import identity."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from dpone.adapters.native_originals_mssql import native_control_schema as legacy_validator
from dpone.contracts import mssql_object_name
from dpone.contracts.mssql_object_name import native_control_schema

_ERROR = "control_schema must be a simple SQL identifier of at most 128 characters"


class StringSubclass(str):
    """Coercible string subclasses are outside the existing exact-str contract."""


@pytest.mark.parametrize("value", ["a", "_", "control_1", "A" * 128, "_" + "9" * 127])
def test_valid_control_name_is_returned_without_normalization(value: str) -> None:
    assert native_control_schema(value) is value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "a" * 129,
        "1control",
        " control",
        "control ",
        "control\n",
        "control\x00",
        "schema.table",
        "[control]",
        "control-name",
        "control;SELECT 1",
        "caf\u00e9",
        "\uff43ontrol",
        None,
        True,
        1,
        b"control",
        StringSubclass("control"),
    ],
)
def test_invalid_control_name_preserves_exact_exception(value: object) -> None:
    with pytest.raises(ValueError) as caught:
        native_control_schema(value)  # type: ignore[arg-type]
    assert type(caught.value) is ValueError
    assert str(caught.value) == _ERROR


def test_legacy_import_is_the_same_function() -> None:
    assert legacy_validator is native_control_schema
    assert "native_control_schema" in mssql_object_name.__all__


@pytest.mark.parametrize(
    "module",
    ["native_originals_mssql_schema", "native_generation_mssql_schema", "native_generation_mssql"],
)
def test_native_sql_consumers_do_not_depend_on_binding_adapter_for_validation(module: str) -> None:
    source = Path(__file__).resolve().parents[1] / "src" / "dpone" / "adapters" / f"{module}.py"
    imports = [node for node in ast.walk(ast.parse(source.read_text())) if isinstance(node, ast.ImportFrom)]
    assert not any(node.module == "dpone.adapters.native_originals_mssql" for node in imports)
    assert any(
        node.module == "dpone.contracts.mssql_object_name"
        and any(alias.name == "native_control_schema" for alias in node.names)
        for node in imports
    )
