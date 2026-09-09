"""Lazy public facade for source -> sink type-matrix certification."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "DecisionCategory": "dpone.type_system.source_sink.certification_models",
    "TypeCertificationCase": "dpone.type_system.source_sink.certification_models",
    "TypeCertificationSuite": "dpone.type_system.source_sink.certification_models",
    "TypeCertificationSuiteRegistry": "dpone.type_system.source_sink.certification_suites",
    "assert_certification_suite_matches_runtime_decisions": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_clickhouse_mssql": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mssql_bigquery": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mssql_clickhouse": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mysql_bigquery": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mysql_clickhouse": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mysql_mssql": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_mysql_postgres": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_postgres_bigquery": "dpone.type_system.source_sink.certification_payloads",
    "matrix_entry_for_postgres_mssql": "dpone.type_system.source_sink.certification_payloads",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
