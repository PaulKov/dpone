"""Lossless permission observations are bounded metadata, never launch authority."""

import pytest

from dpone.contracts.mssql_sqlclient_grant_inventory import (
    SqlClientGrantInventoryLimits,
    SqlClientPermissionRow,
)


@pytest.mark.parametrize("state", ["D", "R", "W", "G"])
def test_permission_states_and_negative_objects_remain_visible(state):
    row = SqlClientPermissionRow(1, -12, 3, 0, 1, "SL", "SELECT", state)
    assert row.major_id == -12 and row.minor_id == 3 and row.state == state


@pytest.mark.parametrize(
    "field,bad", [("permission_rows", 4097), ("members", 1025), ("observation_bytes", 8388609), ("members", True)]
)
def test_component_ceiling_is_not_expandable(field, bad):
    with pytest.raises(ValueError):
        SqlClientGrantInventoryLimits(**{field: bad})


@pytest.mark.parametrize(
    "column,value",
    [
        ("class_id", True),
        ("major_id", 1.0),
        ("minor_id", -1),
        ("grantee_principal_id", False),
        ("grantor_principal_id", -1),
        ("type", "ABCDE"),
        ("permission_name", None),
        ("state", "X"),
    ],
)
def test_raw_permission_scalars_reject_without_coercion(column, value):
    from dataclasses import replace

    row = SqlClientPermissionRow(1, -1, 0, 5, 1, "SL  ", "SELECT", "D")
    with pytest.raises(ValueError):
        replace(row, **{column: value})


def test_unknown_class_and_raw_padding_are_not_acceptance_policy():
    row = SqlClientPermissionRow(255, -2147483648, 2147483647, 0, 1, "SL  ", "UNCLASSIFIED", "R")
    assert row.type == "SL  " and row.class_id == 255
