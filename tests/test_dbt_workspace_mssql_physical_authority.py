"""SQL-observed equivalence classes become platform-global workspace guards."""

from __future__ import annotations

from uuid import UUID

from dpone.adapters.dbt_workspace_mssql_physical_authority import MssqlDbtWorkspacePhysicalAuthority
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
from dpone.contracts.dbt_workspace_observation import (
    MssqlWorkspaceHeader,
    MssqlWorkspaceObservation,
    MssqlWorkspaceObservationRequest,
    MssqlWorkspaceSlot,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.services.dbt_workspace_activation_preparation import _merge_physical_resources


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _observation(*, equivalence: tuple[int, int] = (11, 11)) -> MssqlWorkspaceObservation:
    writes = tuple(
        DbtRelationWrite(
            "dbt/demo",
            "orders",
            f"model.demo.orders_{index}",
            "model",
            "mssql",
            "warehouse",
            "DWH",
            "mart",
            relation,
            role="target" if index == 0 else "intermediate",
        )
        for index, relation in enumerate(("Orders", "orders"))
    )
    request = MssqlWorkspaceObservationRequest(
        release_id=_digest("a"),
        runtime_context_sha256=_digest("b"),
        macro_authority_sha256=DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
        pin=MssqlDatabaseAuthorityPin(
            database_name="DWH",
            database_id=7,
            database_guid=UUID("00000000-0000-0000-0000-000000000007"),
            create_token="2026-01-01T00:00:00.000",
        ),
        default_database="DWH",
        invocation_databases=("DWH",),
        writes=writes,
    )
    header = MssqlWorkspaceHeader(
        database_id=7,
        database_name="DWH",
        server_facts_sha256=_digest("1"),
        original_login_sid_sha256=_digest("2"),
        effective_login_sid_sha256=_digest("2"),
        database_principal_sid_sha256=_digest("3"),
        engine_version="16.0.1000",
        server_collation="Latin1_General_100_CI_AS",
        database_collation="Latin1_General_100_CI_AS",
        catalog_collation="Latin1_General_100_CI_AS",
    )
    slots = tuple(
        MssqlWorkspaceSlot(index, equivalence[index], 5, "mart", None, None, None, None, None) for index in range(2)
    )
    return MssqlWorkspaceObservation(request, header, slots, ())


def test_collation_equivalent_declared_relations_share_one_guard() -> None:
    observation = _observation()

    resources = MssqlDbtWorkspacePhysicalAuthority().authorize_mssql(
        connection_ref="warehouse",
        observation=observation,
        write_subjects=(_digest("4"), _digest("5")),
    )

    assert len(resources) == 1
    assert resources[0].write_subjects == (_digest("4"), _digest("5"))
    assert resources[0].observation_sha256 == observation.observation_sha256


def test_distinct_sql_equivalence_classes_share_conservative_database_guard() -> None:
    resources = MssqlDbtWorkspacePhysicalAuthority().authorize_mssql(
        connection_ref="warehouse",
        observation=_observation(equivalence=(11, 12)),
        write_subjects=(_digest("4"), _digest("5")),
    )

    assert len(resources) == 1


def test_connection_alias_does_not_change_physical_database_guard() -> None:
    authority = MssqlDbtWorkspacePhysicalAuthority()
    observation = _observation()

    warehouse = authority.authorize_mssql(
        connection_ref="warehouse",
        observation=observation,
        write_subjects=(_digest("4"), _digest("5")),
    )
    rotated_alias = authority.authorize_mssql(
        connection_ref="warehouse_rotated_credentials",
        observation=observation,
        write_subjects=(_digest("4"), _digest("5")),
    )

    assert warehouse[0].guard_id == rotated_alias[0].guard_id


def test_connection_alias_resources_coalesce_into_one_request_partition() -> None:
    authority = MssqlDbtWorkspacePhysicalAuthority()
    observation = _observation()
    first = authority.authorize_mssql(
        connection_ref="warehouse",
        observation=observation,
        write_subjects=(_digest("4"), _digest("5")),
    )[0]
    second = authority.authorize_mssql(
        connection_ref="warehouse_alias",
        observation=observation,
        write_subjects=(_digest("6"), _digest("7")),
    )[0]

    merged = _merge_physical_resources((first, second))

    assert len(merged) == 1
    assert merged[0].write_subjects == tuple(_digest(character) for character in "4567")
