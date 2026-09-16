"""Lexical policy tests only; no SQL availability assertion."""

import pytest

from dpone.contracts.dbt_mssql_physical_collation import require_physical_collation_token
from dpone.contracts.dbt_native_execution_policy import require_physical_collation_name


@pytest.mark.parametrize("name", ["A", "A" * 128, "SQL_Latin1_General_CP1_CI_AS"])
def test_exact_name_preserved(name):
    assert require_physical_collation_token(name) == name
    assert require_physical_collation_name({"physical_collation": {"name": name}}) == name


@pytest.mark.parametrize(
    "name",
    [
        None,
        True,
        1,
        "",
        "A" * 129,
        "database_default",
        "DATABASE_DEFAULT",
        " database_default ",
        " A",
        "A ",
        "A\n",
        "A\t",
        "A\x00",
        "é",
        "Ａ",
        "1A",
        "_A",
        "[A]",
        "'A'",
        '"A"',
        "A.B",
        "A;--",
        "A/*x*/",
    ],
)
def test_invalid_names_are_not_normalized(name):
    with pytest.raises(ValueError):
        require_physical_collation_token(name)
    with pytest.raises(ValueError):
        require_physical_collation_name({"physical_collation": {"name": name}})


@pytest.mark.parametrize(
    "native",
    [{}, {"physical_collation": None}, {"physical_collation": {}}, {"physical_collation": {"name": "A", "extra": 1}}],
)
def test_accessor_requires_closed_selection(native):
    with pytest.raises(ValueError, match="physical_collation"):
        require_physical_collation_name(native)
