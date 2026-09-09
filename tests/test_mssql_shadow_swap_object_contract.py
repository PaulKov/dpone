"""Fail-closed MSSQL shadow-swap object contract tests."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.backfill.sql_state_mssql_invariants import merge_publication
from dpone.backfill.state import BackfillPublicationRecord
from dpone.runtime.sinks.mssql_backfill_publication_catalog import publication_target
from dpone.runtime.sinks.mssql_shadow_swap_object_contract import (
    MssqlShadowSwapIndexOptions,
    MssqlShadowSwapObjectContractGuard,
    MssqlShadowSwapResidualCatalog,
    read_shadow_swap_residual_catalog,
)
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlCatalogColumnState,
    MssqlCheckConstraintState,
    MssqlForeignKeyState,
    MssqlIndexState,
    MssqlPermissionState,
    MssqlSchemaCatalogSnapshot,
    MssqlTableBehaviorState,
    MssqlTriggerState,
)

_TARGET = publication_target(database="DWH_Dev", schema="dbo", table="orders")


def test_publication_contract_digest_allows_one_legacy_bind_then_is_immutable() -> None:
    legacy = BackfillPublicationRecord("shadow_swap", "dbo.orders", "dbo.shadow", "dbo.backup", "prepared")
    bound = replace(legacy, object_contract_sha256="sha256:" + "a" * 64)

    assert merge_publication(legacy, bound) == bound
    assert BackfillPublicationRecord.from_dict(bound.to_jsonable()) == bound
    with pytest.raises(ValueError, match="object contract changed"):
        merge_publication(bound, replace(bound, object_contract_sha256="sha256:" + "b" * 64))
    with pytest.raises(ValueError, match="digest is invalid"):
        replace(bound, object_contract_sha256="bad")


def test_exact_supported_contract_is_stable_and_target_bound() -> None:
    seen: list[tuple[str, str, str]] = []

    def read_catalog(_owner, config):
        seen.append((config.target_database, config.target_schema, config.target_table))
        return _snapshot()

    guard = MssqlShadowSwapObjectContractGuard(
        object(),
        catalog_reader=read_catalog,
        residual_reader=lambda _connector, _target: _residual(),
    )

    first = guard.require_supported(_TARGET)
    second = guard.require_supported(_TARGET)

    assert first.sha256 == second.sha256
    assert first.sha256.startswith("sha256:") and len(first.sha256) == 71
    assert seen == [("DWH_Dev", "dbo", "orders"), ("DWH_Dev", "dbo", "orders")]


def test_supported_index_lock_options_match_native_sql_server_index_kinds() -> None:
    """Columnstore reports both lock flags off; rowstore reports both on."""

    guard = MssqlShadowSwapObjectContractGuard(
        object(),
        catalog_reader=lambda _owner, _config: _snapshot(),
        residual_reader=lambda _connector, _target: _residual(),
    )

    guard.require_supported(_TARGET)

    residual = _residual()
    for index_position in (0, 1):
        options = list(residual.index_options)
        current = options[index_position]
        options[index_position] = replace(
            current,
            allow_row_locks=not current.allow_row_locks,
            allow_page_locks=not current.allow_page_locks,
        )
        changed = replace(residual, index_options=tuple(options))
        rejected = MssqlShadowSwapObjectContractGuard(
            object(),
            catalog_reader=lambda _owner, _config: _snapshot(),
            residual_reader=lambda _connector, _target, value=changed: value,
        )
        with pytest.raises(RuntimeError, match="index_options_unsupported"):
            rejected.require_supported(_TARGET)


@pytest.mark.parametrize(
    ("case", "error"),
    (
        ("check", "check_constraint_unsupported"),
        ("foreign_key", "foreign_key_unsupported"),
        ("trigger", "trigger_unsupported"),
        ("permission", "explicit_permission_unsupported"),
        ("primary_key", "key_constraint_unsupported"),
        ("unique_constraint", "key_constraint_unsupported"),
        ("identity", "column_behavior_unsupported"),
        ("default", "column_behavior_unsupported"),
        ("alias_type", "user_type_unsupported"),
        ("temporal", "table_behavior_unsupported"),
        ("residual", "residual_surface_unsupported"),
        ("included", "index_shape_unsupported"),
        ("fill_factor", "index_physical_unsupported"),
        ("partitioned", "index_physical_unsupported"),
        ("index_option", "index_options_unsupported"),
    ),
)
def test_unsupported_object_surface_fails_closed(case: str, error: str) -> None:
    snapshot, residual = _blocked(case)
    guard = MssqlShadowSwapObjectContractGuard(
        object(),
        catalog_reader=lambda _owner, _config: snapshot,
        residual_reader=lambda _connector, _target: residual,
    )

    with pytest.raises(RuntimeError, match=error):
        guard.require_supported(_TARGET)


def test_supported_but_changed_live_contract_changes_durable_digest() -> None:
    baseline = _snapshot()
    changed = replace(
        baseline,
        columns=(replace(baseline.columns[0], nullable=True),),
    )

    first = _contract(baseline, _residual())
    second = _contract(changed, _residual())

    assert first != second


def test_shadow_allows_exact_columnstore_subset_but_cutover_requires_full_contract() -> None:
    live_snapshot = _snapshot()
    shadow_snapshot = replace(live_snapshot, indexes=(live_snapshot.indexes[0],))
    live_residual = _residual()
    shadow_residual = replace(live_residual, index_options=(live_residual.index_options[0],))
    snapshots = {"orders": live_snapshot, "orders__shadow": shadow_snapshot}
    residuals = {"orders": live_residual, "orders__shadow": shadow_residual}
    guard = MssqlShadowSwapObjectContractGuard(
        object(),
        catalog_reader=lambda _owner, config: snapshots[config.target_table],
        residual_reader=lambda _connector, target: residuals[target.table],
    )
    shadow = _TARGET.with_table("orders__shadow")
    live = guard.require_supported(_TARGET)

    guard.require_loadable_shadow(live, shadow)
    with pytest.raises(RuntimeError, match="shadow_object_contract_mismatch"):
        guard.require_matching_shadow(live, shadow)

    snapshots[shadow.table] = live_snapshot
    residuals[shadow.table] = live_residual
    guard.require_matching_shadow(live, shadow)


def test_residual_catalog_queries_are_database_qualified_and_exact() -> None:
    connector = _ResidualConnector()

    residual = read_shadow_swap_residual_catalog(connector, _TARGET)

    assert residual == _residual()
    catalog_queries = [query for query in connector.queries if "SERVERPROPERTY" not in query]
    assert len(catalog_queries) == 2
    assert all("[DWH_Dev].sys." in query for query in catalog_queries)
    assert "column_store_order_ordinal" in catalog_queries[-1]

    connector.view_definition = 0
    with pytest.raises(RuntimeError, match="metadata_visibility_required"):
        read_shadow_swap_residual_catalog(connector, _TARGET)


def test_generation_property_is_reserved_but_foreign_property_fails_closed() -> None:
    connector = _ResidualConnector()
    connector.extended_properties.add("dpone_backfill_generation_id")
    guard = MssqlShadowSwapObjectContractGuard(
        connector,
        catalog_reader=lambda _owner, _config: _snapshot(),
    )

    guard.require_supported(_TARGET)

    connector.extended_properties.add("application_owned_property")
    with pytest.raises(RuntimeError, match="residual_surface_unsupported"):
        guard.require_supported(_TARGET)


def _contract(snapshot: MssqlSchemaCatalogSnapshot, residual: MssqlShadowSwapResidualCatalog) -> str:
    return (
        MssqlShadowSwapObjectContractGuard(
            object(),
            catalog_reader=lambda _owner, _config: snapshot,
            residual_reader=lambda _connector, _target: residual,
        )
        .require_supported(_TARGET)
        .sha256
    )


def _blocked(case: str) -> tuple[MssqlSchemaCatalogSnapshot, MssqlShadowSwapResidualCatalog]:
    snapshot = _snapshot()
    residual = _residual()
    unique = snapshot.indexes[1]
    if case == "check":
        snapshot = replace(snapshot, checks=(MssqlCheckConstraintState("ck", 1, "[id] IS NOT NULL", False, False),))
    elif case == "foreign_key":
        snapshot = replace(snapshot, foreign_keys=(_foreign_key(),))
    elif case == "trigger":
        snapshot = replace(snapshot, triggers=(MssqlTriggerState("tr", True, False, False, False, (), None, None),))
    elif case == "permission":
        snapshot = replace(snapshot, permissions=(MssqlPermissionState("reader", "SQL_USER", "SELECT", "GRANT", None),))
    elif case == "primary_key":
        snapshot = replace(snapshot, indexes=(snapshot.indexes[0], replace(unique, primary_key=True)))
    elif case == "unique_constraint":
        snapshot = replace(snapshot, indexes=(snapshot.indexes[0], replace(unique, unique_constraint=True)))
    elif case == "identity":
        snapshot = replace(snapshot, columns=(replace(snapshot.columns[0], identity=True),))
    elif case == "default":
        snapshot = replace(snapshot, columns=(replace(snapshot.columns[0], default_definition="((0))"),))
    elif case == "alias_type":
        snapshot = replace(
            snapshot,
            columns=(replace(snapshot.columns[0], user_type_schema="dbo", user_type_name="order_id"),),
        )
    elif case == "temporal":
        snapshot = replace(snapshot, behavior=replace(snapshot.behavior, temporal_type=2))
    elif case == "residual":
        residual = replace(residual, unsupported_feature_counts=(("security_predicates", 1),))
    elif case == "included":
        snapshot = replace(snapshot, indexes=(snapshot.indexes[0], replace(unique, included_columns=("payload",))))
    elif case == "fill_factor":
        snapshot = replace(snapshot, indexes=(snapshot.indexes[0], replace(unique, fill_factor=90)))
    elif case == "partitioned":
        snapshot = replace(
            snapshot,
            indexes=(snapshot.indexes[0], replace(unique, partition_compression=("NONE", "NONE"))),
        )
    elif case == "index_option":
        residual = replace(
            residual,
            index_options=(residual.index_options[0], replace(residual.index_options[1], padded=True)),
        )
    else:  # pragma: no cover - parametrization owns the finite cases.
        raise AssertionError(case)
    return snapshot, residual


def _snapshot() -> MssqlSchemaCatalogSnapshot:
    return MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=(_column(),),
        indexes=(
            MssqlIndexState(
                "cci_orders",
                "CLUSTERED COLUMNSTORE",
                False,
                False,
                False,
                False,
                False,
                False,
                None,
                (),
                ("id",),
                (),
                0,
                "PRIMARY",
                "ROWS_FILEGROUP",
                ("COLUMNSTORE",),
            ),
            MssqlIndexState(
                "ux_orders_id",
                "NONCLUSTERED",
                True,
                False,
                False,
                False,
                False,
                False,
                None,
                ("id",),
                (),
                (False,),
                0,
                "PRIMARY",
                "ROWS_FILEGROUP",
                ("NONE",),
            ),
        ),
        behavior=MssqlTableBehaviorState.ordinary_disk_table(),
        default_filegroup="PRIMARY",
        available_row_filegroups=("PRIMARY",),
    )


def _column() -> MssqlCatalogColumnState:
    return MssqlCatalogColumnState(
        1,
        "id",
        "sys",
        "uniqueidentifier",
        "sys",
        "uniqueidentifier",
        16,
        0,
        0,
        False,
        None,
        False,
        False,
        False,
        False,
        0,
        None,
        None,
        None,
        None,
    )


def _foreign_key() -> MssqlForeignKeyState:
    return MssqlForeignKeyState(
        "fk_orders_customer",
        "outbound",
        "dbo",
        "orders",
        ("id",),
        "dbo",
        "customer",
        ("id",),
        "NO_ACTION",
        "NO_ACTION",
        False,
        False,
        False,
    )


def _residual() -> MssqlShadowSwapResidualCatalog:
    return MssqlShadowSwapResidualCatalog(
        "PRIMARY",
        "ROWS_FILEGROUP",
        None,
        None,
        (),
        tuple(
            (name, 0)
            for name in (
                "unsupported_extended_properties",
                "security_predicates",
                "fulltext_indexes",
                "change_tracking_tables",
                "non_simple_columns",
                "schema_bound_references",
            )
        ),
        (
            MssqlShadowSwapIndexOptions("cci_orders", False, False, False, 0, False, 0),
            MssqlShadowSwapIndexOptions("ux_orders_id", False, True, True, None, False, 0),
        ),
    )


class _ResidualConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.view_definition = 1
        self.extended_properties: set[str] = set()

    def get_records(self, sql, params=(), *, as_dict=True):
        assert as_dict
        self.queries.append(sql)
        if "dpone_shadow_swap_residual_catalog_v1" in sql:
            assert len(params) == 8
            reserved = set(params[:-2])
            return [
                {
                    "base_data_space_name": "PRIMARY",
                    "base_data_space_type": "ROWS_FILEGROUP",
                    **{name: 0 for name in _TABLE_FLAG_NAMES},
                    **{name: 0 for name, _count in _residual().unsupported_feature_counts},
                    "unsupported_extended_properties": len(self.extended_properties - reserved),
                }
            ]
        if "dpone_shadow_swap_index_capabilities_v1" in sql:
            assert params == ()
            return [{"major": 16, "edition": 3, "view_definition": self.view_definition}]
        if "dpone_shadow_swap_index_options_v1" in sql:
            assert params == ("dbo", "orders")
            return [
                {
                    "name": "cci_orders",
                    "allow_row_locks": False,
                    "allow_page_locks": False,
                    "compression_delay": 0,
                },
                {
                    "name": "ux_orders_id",
                    "allow_row_locks": True,
                    "allow_page_locks": True,
                },
            ]
        raise AssertionError(sql)


_TABLE_FLAG_NAMES = (
    "is_replicated",
    "has_replication_filter",
    "is_merge_published",
    "is_sync_tran_subscribed",
    "is_tracked_by_cdc",
    "is_remote_data_archive_enabled",
    "ansi_nulls_disabled",
    "filestream_enabled",
    "large_values_out_of_row",
    "text_in_row_enabled",
)
