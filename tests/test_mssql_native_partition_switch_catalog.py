"""Strict catalog projection and SQL-boundary tests; no live SQL authority."""

import copy
import json
from dataclasses import replace

import pytest

from dpone.contracts.native_mssql_switch import NativeSwitchRejected
from dpone.runtime.sinks.mssql_native_switch import NativeSwitchCatalog, execute_native_switch, plan_native_switch
from dpone.runtime.sinks.mssql_native_switch.catalog_parse import parse_table
from dpone.runtime.sinks.mssql_native_switch.catalog_sql import DATABASE_SQL, TABLE_SQL, TRANSACTION_SQL
from tests.test_mssql_native_partition_switch import switch_case


def catalog_table(obj, owner_tag):
    """Literal SQL projection fixture: changes to required metadata break tests."""
    return {
        "object_id": obj.object_id,
        "schema_id": 1,
        "schema_name": obj.schema,
        "name": obj.name,
        "created_at": obj.created_at,
        "modified_at": "unchanged",
        "principal_id": 1,
        "owner_tag": owner_tag,
        "temporal_type": 0,
        "is_memory_optimized": False,
        "is_filetable": False,
        "is_replicated": False,
        "has_replication_filter": False,
        "is_merge_published": False,
        "is_sync_tran_subscribed": False,
        "is_tracked_by_cdc": False,
        "is_remote_data_archive_enabled": False,
        "is_external": False,
        "ledger_type": 0,
        "is_node": False,
        "is_edge": False,
        "filestream_data_space_id": 0,
        "lob_data_space_id": 0,
        "columns": [
            {
                "column_id": 1,
                "name": "day",
                "type_name": "date",
                "system_type_id": 40,
                "user_type_id": 40,
                "max_length": 3,
                "precision": 10,
                "scale": 0,
                "is_nullable": False,
                "collation_name": None,
                "is_identity": False,
                "is_computed": False,
                "is_sparse": False,
                "is_column_set": False,
                "is_filestream": False,
                "generated_always_type": 0,
                "is_hidden": False,
                "is_masked": False,
                "encryption_type": None,
                "is_user_defined": False,
                "is_assembly_type": False,
                "xml_collection_id": 0,
                "is_rowguidcol": False,
                "is_ansi_padded": True,
                "default_object_id": 0,
                "rule_object_id": 0,
            }
        ],
        "indexes": [
            {
                "index_id": 0,
                "name": None,
                "type": 0,
                "is_unique": False,
                "data_space_id": 5,
                "is_disabled": False,
                "is_hypothetical": False,
                "has_filter": False,
                "filter_definition": None,
                "is_primary_key": False,
                "is_unique_constraint": False,
                "ignore_dup_key": False,
                "fill_factor": 0,
                "is_padded": False,
                "allow_row_locks": True,
                "allow_page_locks": True,
                "columns": [
                    {
                        "column_id": 1,
                        "index_column_id": 1,
                        "key_ordinal": 0,
                        "partition_ordinal": 1,
                        "is_descending_key": False,
                        "is_included_column": False,
                    }
                ],
            }
        ],
        "functions": [
            {
                "scheme_id": 5,
                "function_id": 11,
                "name": "pf",
                "range_right": True,
                "system_type_id": 40,
                "user_type_id": 40,
                "max_length": 3,
                "precision": 10,
                "scale": 0,
                "boundaries": [{"boundary_id": 1, "value": "2026-01-01"}, {"boundary_id": 2, "value": "2026-02-01"}],
            }
        ],
        "partitions": [
            {
                "index_id": 0,
                "partition_number": n,
                "partition_id": obj.object_id * 10 + n,
                "hobt_id": obj.object_id * 100 + n,
                "data_compression": 0,
                "xml_compression": 0,
                "filegroup_id": 1,
            }
            for n in (1, 2, 3)
        ],
        "children": [],
        "incoming_fks": 0,
        "bound_dependencies": 0,
        "security_predicates": 0,
        "change_tracking": 0,
        "fulltext_indexes": 0,
    }


class CatalogSql:
    """Named-result SQL double; unexpected SQL fails instead of returning success."""

    def __init__(self, *, target_rows=7, prepared_rows=3):
        _, self.interval, self.binding = switch_case(prepared_rows=prepared_rows)
        b = self.binding
        self.metadata = {
            obj.name: catalog_table(obj, b.tag(role) if role != "target" else None)
            for obj, role in zip(
                (b.target, b.prepared, b.switch_out), ("target", "prepared", "switch_out"), strict=True
            )
        }
        self.rows = {"target": target_rows, "prepared": prepared_rows, "old": 0}
        self.outside = 0
        self.events = []
        self.database = dict(
            major=16,
            edition=3,
            visible=1,
            server_visible=1,
            database_triggers=0,
            server_triggers=0,
            database_id=7,
            name="fixture",
            generation="database-generation",
        )
        self.state = dict(state=1, depth=1, session_id=51, database_id=7, xact_abort=16384, isolation=4)
        self.fail_switch = None
        self.switches = 0

    def query(self, sql, parameters=()):
        self.events.append((sql, parameters))
        if sql == DATABASE_SQL:
            return [self.database]
        if sql == TRANSACTION_SQL:
            return [self.state.copy()]
        if sql == TABLE_SQL:
            assert parameters[0] == "dbo"
            return [{"metadata": json.dumps([self.metadata[parameters[1]]])}]
        if sql.startswith("SELECT COUNT_BIG(*) AS row_count FROM [fixture].[dbo]."):
            name = next(name for name in self.rows if f"[{name}]" in sql)
            if " IS NULL OR " in sql:
                assert parameters == (self.interval.start, self.interval.end)
                return [{"row_count": self.outside}]
            if " WHERE " in sql:
                assert parameters == (self.interval.start, self.interval.end)
            return [{"row_count": self.rows[name]}]
        raise AssertionError(sql)

    def assert_authority(self, binding):
        assert binding == self.binding
        self.events.append(("authority", ()))

    def verify_prepared(self, plan):
        assert plan.owner_binding == self.binding
        assert len([query for query, _ in self.events if "TABLOCKX" in query]) == 3
        self.events.append(("verify_prepared", ()))

    def execute(self, sql):
        self.events.append((sql, ()))
        self.switches += 1
        if self.switches == self.fail_switch:
            raise OSError("switch failed")
        if self.switches == 1:
            assert sql == "ALTER TABLE [fixture].[dbo].[target] SWITCH PARTITION 2 TO [fixture].[dbo].[old] PARTITION 2"
            self.rows["old"], self.rows["target"] = self.rows["target"], 0
        elif self.switches == 2:
            assert (
                sql
                == "ALTER TABLE [fixture].[dbo].[prepared] SWITCH PARTITION 2 TO [fixture].[dbo].[target] PARTITION 2"
            )
            self.rows["target"], self.rows["prepared"] = self.rows["prepared"], 0
        else:
            raise AssertionError("unexpected replay")

    def plan(self):
        snapshot = NativeSwitchCatalog(self).snapshot(self.binding, interval=self.interval)
        result = plan_native_switch(snapshot, interval=self.interval, owner_binding=self.binding)
        assert result.reasons == ()
        return result.plan


def test_adapter_fingerprints_distinguish_layout_from_object_identity():
    sql = CatalogSql()
    snapshot = NativeSwitchCatalog(sql).snapshot(sql.binding, interval=sql.interval)
    assert snapshot.target.layout_digest == snapshot.prepared.layout_digest == snapshot.switch_out.layout_digest
    assert snapshot.target.catalog_digest != snapshot.prepared.catalog_digest
    assert snapshot.prepared_rows == 3
    assert all("SELECT DISTINCT" not in query for query, _ in sql.events)


@pytest.mark.parametrize(
    "missing", ["columns", "indexes", "functions", "partitions", "owner_tag", "is_tracked_by_cdc", "incoming_fks"]
)
def test_missing_metadata_never_defaults_to_compatible(missing):
    sql = CatalogSql()
    del sql.metadata["prepared"][missing]
    with pytest.raises(NativeSwitchRejected, match="metadata_unknown"):
        sql.plan()


@pytest.mark.parametrize(
    "field",
    ["temporal_type", "is_tracked_by_cdc", "is_replicated", "ledger_type", "is_external", "is_memory_optimized"],
)
def test_unsupported_table_dependencies(field):
    sql = CatalogSql()
    sql.metadata["target"][field] = 1
    with pytest.raises(NativeSwitchRejected, match="unsupported_table_feature"):
        sql.plan()


@pytest.mark.parametrize("field", ["incoming_fks", "bound_dependencies", "security_predicates", "change_tracking"])
def test_unsupported_external_dependencies(field):
    sql = CatalogSql()
    sql.metadata["target"][field] = 1
    with pytest.raises(NativeSwitchRejected, match="unsupported_dependency"):
        sql.plan()


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("major", 17, "unsupported_server"),
        ("visible", None, "metadata_visibility_required"),
        ("name", "other", "database_binding_mismatch"),
    ],
)
def test_server_visibility_and_database_are_required_before_table_reads(field, value, reason):
    sql = CatalogSql()
    sql.database[field] = value
    with pytest.raises(NativeSwitchRejected, match=reason):
        sql.plan()
    assert len(sql.events) == 1


@pytest.mark.parametrize("payload", [None, "{}", "[]", "[{},{}]", "null", "broken"])
def test_malformed_catalog(payload):
    with pytest.raises(NativeSwitchRejected):
        parse_table(payload)


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("columns", "is_nullable", True),
        ("columns", "scale", 7),
        ("columns", "collation_name", "changed"),
        ("columns", "max_length", 4),
        ("columns", "type_name", "xml"),
        ("indexes", "is_disabled", True),
        ("indexes", "data_space_id", 6),
        ("indexes", "is_unique", True),
        ("indexes", "type", 5),
        ("partitions", "filegroup_id", 8),
        ("partitions", "data_compression", 2),
        ("partitions", "xml_compression", 1),
        ("functions", "range_right", False),
    ],
)
def test_every_physical_shape_difference_fails_closed(section, field, value):
    sql = CatalogSql()
    sql.metadata["prepared"][section][0][field] = value
    with pytest.raises(NativeSwitchRejected):
        sql.plan()


@pytest.mark.parametrize("field,value", [("object_id", 999), ("created_at", "recreated"), ("owner_tag", "foreign")])
def test_object_and_invocation_binding_precede_data_reads(field, value):
    sql = CatalogSql()
    sql.metadata["prepared"][field] = value
    with pytest.raises(NativeSwitchRejected):
        sql.plan()
    assert not any("COUNT_BIG(*) AS row_count" in q for q, _ in sql.events)


@pytest.mark.parametrize("mutation", ["catalog", "owner", "boundary", "nonempty", "outside", "prepared_count"])
def test_drift_under_transaction_rejects_before_switch(mutation):
    sql = CatalogSql()
    plan = sql.plan()
    if mutation == "catalog":
        sql.metadata["target"]["modified_at"] = "later"
    elif mutation == "owner":
        sql.metadata["old"]["owner_tag"] = "foreign"
    elif mutation == "boundary":
        sql.metadata["target"]["functions"][0]["boundaries"][0]["value"] = "2026-01-02"
    elif mutation == "nonempty":
        sql.rows["old"] = 1
    elif mutation == "outside":
        sql.outside = 1
    else:
        sql.rows["prepared"] += 1
    with pytest.raises(NativeSwitchRejected):
        execute_native_switch(plan, transaction=sql)
    assert sql.switches == 0


def test_forged_partition_number_cannot_change_mutation_scope():
    sql = CatalogSql()
    with pytest.raises(NativeSwitchRejected, match="catalog_drift"):
        execute_native_switch(replace(sql.plan(), partition_number=1), transaction=sql)
    assert sql.switches == 0


def test_snapshot_is_detached_from_mutable_driver_records():
    sql = CatalogSql()
    plan = sql.plan()
    original = copy.deepcopy(plan)
    sql.metadata["target"]["columns"][0]["name"] = "changed"
    assert plan == original


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("server_visible", None, "metadata_visibility_required"),
        ("database_triggers", 1, "unsupported_dependency"),
        ("server_triggers", 1, "unsupported_dependency"),
        ("server_triggers", None, "metadata_unknown"),
    ],
)
def test_ddl_trigger_visibility_and_exclusion_before_any_table_read(field, value, reason):
    sql = CatalogSql()
    sql.database[field] = value
    with pytest.raises(NativeSwitchRejected, match=reason):
        sql.plan()
    assert len(sql.events) == 1


@pytest.mark.parametrize("field", ["default_object_id", "rule_object_id"])
def test_standalone_bound_rules_and_defaults_are_not_hidden_by_empty_children(field):
    sql = CatalogSql()
    sql.metadata["prepared"]["columns"][0][field] = 71
    assert sql.metadata["prepared"]["children"] == []
    with pytest.raises(NativeSwitchRejected, match="unsupported_column"):
        sql.plan()


def test_fulltext_index_is_explicitly_excluded():
    sql = CatalogSql()
    sql.metadata["target"]["fulltext_indexes"] = 1
    with pytest.raises(NativeSwitchRejected, match="unsupported_dependency"):
        sql.plan()


def test_aligned_clustered_rowstore_index_names_may_differ():
    sql = CatalogSql()
    for name, table in sql.metadata.items():
        index = table["indexes"][0]
        index.update(index_id=1, type=1, name="ix_" + name, is_unique=True)
        index["columns"][0]["key_ordinal"] = 1
        for partition in table["partitions"]:
            partition["index_id"] = 1
    assert sql.plan().partition_number == 2


def test_datetime2_utc_precision_preserves_exact_boundary():
    from datetime import UTC, datetime

    sql = CatalogSql()
    sql.interval = replace(
        sql.interval, start=datetime(2026, 1, 1, 0, 0, 0, 123456, UTC), end=datetime(2026, 2, 1, 0, 0, 0, 123456, UTC)
    )
    for table in sql.metadata.values():
        column = table["columns"][0]
        column.update(type_name="datetime2", system_type_id=42, user_type_id=42, max_length=8, precision=26, scale=6)
        function = table["functions"][0]
        function.update(system_type_id=42, user_type_id=42, max_length=8, precision=26, scale=6)
        function["boundaries"][0]["value"] = "2026-01-01T00:00:00.1234560"
        function["boundaries"][1]["value"] = "2026-02-01T00:00:00.1234560"
    snapshots = [parse_table(json.dumps([table])) for table in sql.metadata.values()]
    assert snapshots[0].boundaries == (sql.interval.start, sql.interval.end)
    from dpone.runtime.sinks.mssql_native_switch.catalog import interval_parameters

    assert interval_parameters(sql.interval) == tuple(value.replace(tzinfo=None) for value in snapshots[0].boundaries)
    snapshot, _, binding = switch_case()
    snapshot = replace(snapshot, target=snapshots[0], prepared=snapshots[1], switch_out=snapshots[2])
    assert plan_native_switch(snapshot, interval=sql.interval, owner_binding=binding).plan.partition_number == 2


@pytest.mark.parametrize("malformed", [None, "not-a-date", "2026-01-01T00:00:00Z"])
def test_boundary_unknown_never_converts_to_valid_partition(malformed):
    sql = CatalogSql()
    sql.metadata["target"]["functions"][0]["boundaries"][0]["value"] = malformed
    with pytest.raises(NativeSwitchRejected):
        sql.plan()
