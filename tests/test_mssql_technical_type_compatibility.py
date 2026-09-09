"""Exact compatibility boundary for SQL Server framework columns."""

from __future__ import annotations

import pytest

from dpone.readiness.mssql_technical_type_compatibility import (
    mssql_fixed_technical_types_compatible,
)


@pytest.mark.parametrize(
    ("column", "width"),
    (
        ("__dpone__run_id", 26),
        ("__dpone__load_id", 26),
        ("__dpone__row_id", 64),
        ("__dpone__parent_row_id", 64),
        ("__dpone__root_row_id", 64),
        ("__dpone__row_hash", 64),
    ),
)
def test_fixed_framework_ascii_storage_accepts_exact_char_varchar_pair(
    column: str,
    width: int,
) -> None:
    assert mssql_fixed_technical_types_compatible(
        column,
        f"varchar({width})",
        f"char({width})",
    )
    assert mssql_fixed_technical_types_compatible(
        column,
        f"char({width})",
        f"varchar({width})",
    )


@pytest.mark.parametrize(
    ("column", "desired", "existing"),
    (
        ("business_id", "varchar(26)", "char(26)"),
        ("__dpone__run_id", "varchar(25)", "char(25)"),
        ("__dpone__run_id", "varchar(26)", "char(25)"),
        ("__dpone__run_id", "nvarchar(26)", "char(26)"),
        ("__dpone__op", "varchar(32)", "char(32)"),
        ("__dpone__run_id", "varchar(26)", "varchar(26)"),
    ),
)
def test_fixed_framework_ascii_storage_rejects_broader_equivalence(
    column: str,
    desired: str,
    existing: str,
) -> None:
    assert not mssql_fixed_technical_types_compatible(column, desired, existing)
