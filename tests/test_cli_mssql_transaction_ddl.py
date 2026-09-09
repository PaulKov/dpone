from __future__ import annotations

import pytest

from dpone.cli.parser import build_parser
from dpone.runtime.state.mssql_generic_transaction_names import GENERIC_TRANSACTION_TABLES


def test_state_cli_renders_exact_generic_mssql_catalog_without_app_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = build_parser().parse_args(
        [
            "state",
            "render-mssql-transaction-ddl",
            "--database",
            "Example_System",
            "--schema",
            "governance",
        ]
    )

    assert args._command.requires_app_context is False
    assert args._command.run(args, None) == 0
    ddl = capsys.readouterr().out
    assert ddl.startswith("-- dpone generic MSSQL transaction catalog v2\n")
    assert "UNIQUE NONCLUSTERED (invocation_digest)" in ddl
    assert {
        table for table in GENERIC_TRANSACTION_TABLES if f"CREATE TABLE [Example_System].[governance].[{table}]" in ddl
    } == set(GENERIC_TRANSACTION_TABLES)
    assert ddl.count("CREATE TABLE ") == 4


def test_state_cli_rejects_unsafe_mssql_catalog_identifier(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(
            [
                "state",
                "render-mssql-transaction-ddl",
                "--database",
                "DWH;DROP_DATABASE",
                "--schema",
                "governance",
            ]
        )

    assert exc_info.value.code == 2
    assert "safe unquoted SQL Server identifier" in capsys.readouterr().err


def test_state_cli_renders_locked_idempotent_v1_to_v2_migration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = build_parser().parse_args(
        [
            "state",
            "render-mssql-transaction-ddl",
            "--database",
            "Example_System",
            "--schema",
            "governance",
            "--upgrade-from",
            "1",
        ]
    )

    assert args._command.run(args, None) == 0
    ddl = capsys.readouterr().out
    assert ddl.startswith("-- dpone generic MSSQL transaction catalog v1 -> v2\n")
    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in ddl
    assert "sys.sp_getapplock" in ddl
    assert "DPONE_GENERIC_TRANSACTION_V2_DUPLICATE_INVOCATION" in ddl
    assert "IF NOT EXISTS (" in ddl
    assert "ADD CONSTRAINT [uq_dpone_load_attempt_invocation]" in ddl
    assert "UNIQUE NONCLUSTERED (invocation_digest)" in ddl
    assert "DPONE_GENERIC_TRANSACTION_V2_INDEX_CONTRACT_MISMATCH" in ddl
    assert "IF XACT_STATE() <> 0 ROLLBACK TRANSACTION" in ddl
