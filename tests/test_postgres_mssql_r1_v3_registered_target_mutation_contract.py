from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_registered_target_catalog import MssqlR1RegisteredTargetColumnRefV1
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import MssqlR1RegisteredTargetIndexV1
from tests.test_postgres_mssql_r1_v3_registered_target_catalog_contract import registered_target_catalog


@pytest.mark.parametrize(
    ("case_id", "mutation"),
    (
        ("must_reject__default", lambda column: replace(column, default_definition_digest=b"d" * 32)),
        (
            "must_reject__computed_definition",
            lambda column: replace(column, computed_definition_digest=b"c" * 32),
        ),
        ("must_reject__identity", lambda column: replace(column, identity=True)),
        ("must_reject__computed", lambda column: replace(column, computed=True)),
        ("must_reject__sparse", lambda column: replace(column, sparse=True)),
        ("must_reject__rowguidcol", lambda column: replace(column, rowguidcol=True)),
        ("must_reject__generated_always", lambda column: replace(column, generated_always_type=1)),
    ),
)
def test_every_closed_column_feature_is_rejected(case_id, mutation) -> None:
    column = registered_target_catalog().ordered_columns[0]

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        mutation(column)


@pytest.mark.parametrize(
    ("case_id", "field_name", "value"),
    (
        ("must_reject__temporal", "temporal_type", 1),
        ("must_reject__ledger", "ledger_type", 1),
        ("must_reject__memory_optimized", "memory_optimized", True),
        ("must_reject__durability", "durability_desc", "SCHEMA_ONLY"),
        ("must_reject__filetable", "filetable", True),
        ("must_reject__graph_node", "graph_node", True),
        ("must_reject__graph_edge", "graph_edge", True),
        ("must_reject__trigger", "ordered_trigger_digests", (b"t" * 32,)),
        ("must_reject__inbound_fk", "ordered_inbound_foreign_key_digests", (b"i" * 32,)),
        ("must_reject__outbound_fk", "ordered_outbound_foreign_key_digests", (b"o" * 32,)),
        ("must_reject__check", "ordered_check_constraint_digests", (b"c" * 32,)),
        ("must_reject__indexed_view", "ordered_indexed_view_dependency_digests", (b"v" * 32,)),
        ("must_reject__encryption", "ordered_encrypted_column_ordinals", (1,)),
    ),
)
def test_every_closed_target_object_feature_is_rejected(case_id, field_name, value) -> None:
    features = registered_target_catalog().feature_observation

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        replace(features, **{field_name: value})


def test_must_reject__filtered_index() -> None:
    primary = registered_target_catalog().primary_key

    with pytest.raises(MssqlR1V3ContractError):
        replace(primary, filter_definition_digest=b"f" * 32)


def test_column_and_index_ordinals_close_shared_cardinality_bounds() -> None:
    catalog = registered_target_catalog()
    column = catalog.ordered_columns[0]
    index = catalog.primary_key

    assert replace(column, ordinal=1024).ordinal == 1024
    assert replace(index, ordinal=1000).ordinal == 1000
    integer_subclass = type("IntSubclass", (int,), {})(1)
    for value, ordinal in (
        (column, 1025),
        (index, 1001),
        (column, True),
        (index, True),
        (column, integer_subclass),
        (index, integer_subclass),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(value, ordinal=ordinal)


def test_valid_distinct__secondary_index_and_non_key_reference_change_catalog_authority() -> None:
    before = registered_target_catalog()
    payload_column = replace(before.ordered_columns[0], ordinal=2, name="payload", nullable=True)
    secondary = MssqlR1RegisteredTargetIndexV1(
        2,
        "IX_orders_payload",
        "nonclustered",
        False,
        False,
        False,
        False,
        False,
        False,
        None,
        (2,),
        (False,),
        (),
    )
    after = replace(
        before,
        ordered_columns=(*before.ordered_columns, payload_column),
        ordered_secondary_indexes=(secondary,),
    )
    reference = MssqlR1RegisteredTargetColumnRefV1.from_catalog(after, 2)

    assert after.digest != before.digest
    assert reference.primary_key_ordinal is None
    reference.validate_against_catalog(after)
