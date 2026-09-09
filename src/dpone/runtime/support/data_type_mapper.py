"""Compatibility facade for runtime data type mapping.

New code should import focused parser/dialect services from
``dpone.runtime.support.type_mapping``. This module keeps the historical
``dpone.runtime.support.data_type_mapper`` import path stable.
"""

from __future__ import annotations

from dpone.runtime.support.type_mapping import CanonicalType, DataTypeMapper, ParsedType, TimestampConfig
from dpone.runtime.support.type_mapping.dialects import _BigQueryDialect, _PostgresDialect
from dpone.runtime.support.type_mapping.parsers import _ClickHouseTypeParser, _PgTypeParser, _PythonTypeParser

__all__ = [
    "CanonicalType",
    "DataTypeMapper",
    "ParsedType",
    "TimestampConfig",
    "_BigQueryDialect",
    "_ClickHouseTypeParser",
    "_PgTypeParser",
    "_PostgresDialect",
    "_PythonTypeParser",
]
