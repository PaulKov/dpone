from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.state import mssql_generic_transaction_contract as subject
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    ATTEMPT_TRIGGER,
    FENCE_TABLE,
    FENCE_TRIGGER,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
    RECEIPT_TRIGGER,
)
from dpone.runtime.support.mssql_trigger_integrity import require_exact_table_trigger_set


def test_generic_catalog_requires_non_identity_columns_and_exact_object_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shapes: dict[str, object] = {}
    integrity: dict[str, object] = {}
    trigger_sets: list[tuple[str, frozenset[str]]] = []

    def capture_shape(_connector, *, database, schema, table, contract) -> None:
        del database, schema
        shapes[table] = contract

    def capture_integrity(_connector, *, database, schema, table, contract) -> None:
        del database, schema
        integrity[table] = contract

    def capture_trigger_set(_connector, *, database, schema, table, triggers, error_code) -> None:
        del database, schema, error_code
        trigger_sets.append((table, triggers))

    monkeypatch.setattr(subject, "require_external_table_shape", capture_shape)
    monkeypatch.setattr(subject, "require_table_catalog_integrity", capture_integrity)
    monkeypatch.setattr(subject, "require_exact_table_trigger_set", capture_trigger_set)
    monkeypatch.setattr(subject, "require_exact_immutable_trigger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subject, "require_governance_trigger", lambda *_args, **_kwargs: None)

    subject.require_generic_transaction_catalog(object(), database="Example_System", schema="dbo")

    expected_tables = {FENCE_TABLE, ATTEMPT_TABLE, OPERATION_TABLE, RECEIPT_TABLE}
    assert set(shapes) == set(integrity) == expected_tables
    assert all(column.identity is False for contract in shapes.values() for column in contract.shapes)
    assert all(
        contract.exact_indexes and contract.exact_foreign_keys and contract.exact_checks and contract.exact_defaults
        for contract in integrity.values()
    )
    assert set(trigger_sets) == {
        (FENCE_TABLE, frozenset({FENCE_TRIGGER})),
        (ATTEMPT_TABLE, frozenset({ATTEMPT_TRIGGER})),
        (OPERATION_TABLE, frozenset({OPERATION_TRIGGER})),
        (RECEIPT_TABLE, frozenset({RECEIPT_TRIGGER})),
    }


@pytest.mark.parametrize(
    "rows",
    [
        [{"trigger_name": "expected"}, {"trigger_name": "unexpected"}],
        [{"trigger_name": "Expected"}],
        [],
    ],
)
def test_exact_table_trigger_set_rejects_extra_case_variant_or_missing_trigger(
    rows: list[dict[str, str]],
) -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda *_args, **_kwargs: rows,
    )

    with pytest.raises(RuntimeError, match="mssql_generic_transaction_trigger_set"):
        require_exact_table_trigger_set(
            connector,
            database="Example_System",
            schema="dbo",
            table="state_table",
            triggers=frozenset({"expected"}),
            error_code="mssql_generic_transaction_trigger_set",
        )


def test_exact_table_trigger_set_accepts_only_the_declared_trigger() -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda *_args, **_kwargs: [{"trigger_name": "expected"}],
    )

    require_exact_table_trigger_set(
        connector,
        database="Example_System",
        schema="dbo",
        table="state_table",
        triggers=frozenset({"expected"}),
        error_code="mssql_generic_transaction_trigger_set",
    )
