"""Classification of permission-bearing SQL objects as owned dpone stages."""

from dataclasses import replace

import pytest

from dpone.app.mssql_sqlclient_grant_inventory_snapshot import project_stage
from tests.test_mssql_sqlclient_stage_identity import stage
from tests.test_mssql_sqlclient_stage_locator import DATABASE


class Catalog:
    def __init__(self, *, kind: str = "USER_TABLE", owner=..., incarnation=...) -> None:
        self.stage = stage()
        self.kind = kind
        self.owner = self.stage.owner_binding if owner is ... else owner
        self.incarnation = str(self.stage.object_nonce) if incarnation is ... else incarnation

    def member(self, object_id: int):
        value = self.stage
        return [(object_id, value.schema_id, value.schema_name, value.table_name, self.kind, value.create_date)]

    def object_properties(self, schema_id: int, table: str):
        value = self.stage
        return [
            (
                value.object_id,
                table,
                value.create_date,
                self.owner,
                self.incarnation,
                0,
                0,
                0,
                0,
            )
        ]

    def features(self, object_id: int):
        return [(0,) * 12]

    def columns(self, object_id: int):
        return [
            (
                column.ordinal,
                column.name,
                column.type.value,
                column.nullable,
                column.max_length,
                column.precision,
                column.scale,
                column.collation,
                0,
                False,
                False,
            )
            for column in self.stage.columns
        ]


@pytest.mark.parametrize("kind", ["VIEW", "SYSTEM_TABLE", "SQL_STORED_PROCEDURE"])
def test_non_user_table_permission_is_not_a_stage(kind):
    catalog = Catalog(kind=kind)
    assert project_stage(catalog, DATABASE, catalog.stage.object_id) is None


@pytest.mark.parametrize("absent", [None, ""])
def test_unmarked_user_table_permission_is_not_a_stage(absent):
    catalog = Catalog(owner=absent, incarnation=absent)
    assert project_stage(catalog, DATABASE, catalog.stage.object_id) is None


@pytest.mark.parametrize("owner,incarnation", [(None, ...), (..., None), ("", ...), (..., "")])
def test_partial_stage_marker_fails_closed(owner, incarnation):
    catalog = Catalog(owner=owner, incarnation=incarnation)
    with pytest.raises(ValueError, match="sqlclient_grant_inventory_unknown"):
        project_stage(catalog, DATABASE, catalog.stage.object_id)


def test_both_valid_markers_project_exact_stage():
    catalog = Catalog()
    expected = replace(
        catalog.stage,
        database_guid=DATABASE.database_guid,
        database_id=DATABASE.database_id,
        database_name=DATABASE.name,
    )
    assert project_stage(catalog, DATABASE, catalog.stage.object_id) == expected
