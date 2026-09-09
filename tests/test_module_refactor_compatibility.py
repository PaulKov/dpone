"""Compatibility guards for cohesive module-size refactors."""

from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLFullRefreshStrategy as ConcreteFullRefreshStrategy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_load_strategies import MSSQLFullRefreshStrategy
from dpone.runtime.state.mssql_contract import load_audit_additive_ddl
from dpone.runtime.state.mssql_load_audit_contract import load_audit_additive_ddl as render_load_audit_additive_ddl


def test_mssql_load_strategy_facade_preserves_public_class_identity() -> None:
    """Existing consumers keep receiving the same concrete strategy class."""

    assert MSSQLFullRefreshStrategy is ConcreteFullRefreshStrategy


def test_mssql_contract_reexports_load_audit_migration_renderer() -> None:
    """The established import path delegates to the extracted contract."""

    ddl = load_audit_additive_ddl(fq_table="[state].[loads]", object_id="state.loads")
    assert ddl == render_load_audit_additive_ddl(fq_table="[state].[loads]", object_id="state.loads")
    assert "ALTER TABLE [state].[loads] ADD [deleted_rows] bigint NULL;" in ddl
    assert "ALTER TABLE [state].[loads] ADD [commit_outcome] nvarchar(64) NULL;" in ddl
