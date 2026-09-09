from __future__ import annotations

import pytest

from dpone.runtime.support.mssql_types import MSSQLTypeMapper, MSSQLTypeMappingError


def test_mssql_type_mapper_renders_clickhouse_types_for_safe_staging_ddl() -> None:
    assert MSSQLTypeMapper.to_mssql("Nullable(UInt64)") == "decimal(20,0)"
    assert MSSQLTypeMapper.to_mssql("LowCardinality(Nullable(String))") == "nvarchar(max)"
    assert MSSQLTypeMapper.to_mssql("DateTime64(3, 'Europe/Moscow')") == "datetime2(3)"
    assert MSSQLTypeMapper.to_mssql("DateTime64(9)") == "datetime2(7)"
    assert MSSQLTypeMapper.to_mssql("Decimal(18,4)") == "decimal(18,4)"
    # Unsupported ClickHouse types fail fast with a workaround instead of the
    # former silent nvarchar(max) fallback (strict allowlist policy; see
    # docs/type-mapping-matrix.md and tests/test_mssql_type_mapper_contracts.py).
    with pytest.raises(MSSQLTypeMappingError, match="Workaround"):
        MSSQLTypeMapper.to_mssql("Decimal(76,4)")
    with pytest.raises(MSSQLTypeMappingError, match="toJSONString"):
        MSSQLTypeMapper.to_mssql("Array(String)")


def test_mssql_type_mapper_keeps_existing_postgres_and_mssql_contracts() -> None:
    assert MSSQLTypeMapper.to_mssql("integer") == "int"
    assert MSSQLTypeMapper.to_mssql("timestamp with time zone") == "datetimeoffset"
    assert MSSQLTypeMapper.to_mssql("varchar(128)") == "varchar(128)"
    assert MSSQLTypeMapper.to_mssql("decimal(18,2)") == "decimal(18,2)"
