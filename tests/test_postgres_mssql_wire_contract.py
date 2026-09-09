"""Typed PostgreSQL→MSSQL public/runtime wire parity."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config.postgres_mssql_wire_contract import (
    PostgresMssqlWireContractError,
    normalize_postgres_mssql_wire,
)


def _config(**options):
    return SimpleNamespace(
        export_format=options.pop("export_format", "csv"),
        compress_export=options.pop("compress_export", False),
        options=options,
    )


def test_omitted_batch_mode_projects_to_one_safe_copy() -> None:
    policy = normalize_postgres_mssql_wire(_config())

    assert policy.public_export_format == "csv"
    assert policy.runtime_export_format == "mssql-delimited"
    assert policy.batch_commit_mode == "whole"
    assert policy.snapshot_scope == "single_copy_statement"


@pytest.mark.parametrize(
    ("config", "blocker"),
    (
        (_config(batch_commit_mode="separate"), "postgres_mssql.batch_commit_mode"),
        (_config(partitioning={"column": "id", "enabled": True}), "postgres_mssql.partitioning"),
        (_config(export_format="binary"), "postgres_mssql.export_format"),
        (_config(compress_export=True), "postgres_mssql.compress_export"),
        (
            _config(schema_contract={"enforcement": "warn", "columns": {}}),
            "postgres_mssql.type_contract.file_enforcement_unsupported:warn",
        ),
        (
            _config(schema_contract={"enforcement": "coerce", "columns": {}}),
            "postgres_mssql.type_contract.file_enforcement_unsupported:coerce",
        ),
        (
            _config(schema_contract={"enforcement": "quarantine", "columns": {}}),
            "postgres_mssql.type_contract.file_enforcement_unsupported:quarantine",
        ),
    ),
)
def test_unsupported_wire_combinations_fail_with_typed_blocker(config, blocker: str) -> None:
    with pytest.raises(PostgresMssqlWireContractError) as raised:
        normalize_postgres_mssql_wire(config)

    assert raised.value.code == "DPONE_POSTGRES_MSSQL_WIRE_CONTRACT_BLOCKED"
    assert raised.value.blocker == blocker


def test_disabled_partition_section_is_equivalent_to_no_partitioning() -> None:
    policy = normalize_postgres_mssql_wire(_config(partitioning={"enabled": False, "column": "id"}))

    assert policy.batch_commit_mode == "whole"
