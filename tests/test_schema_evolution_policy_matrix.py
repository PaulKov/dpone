from __future__ import annotations

from itertools import product
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.schema_evolution import SchemaEvolutionError
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sinks.mssql_target_metrics import MssqlTargetMetrics

TABLE_MODES = ("evolve", "freeze", "ignore")
COLUMN_MODES = ("evolve", "freeze", "ignore", "quarantine")
DATA_TYPE_MODES = ("widen", "variant_column", "freeze", "quarantine")
DDL_MODES = ("online", "safe_window", "plan_only", "manual_approval")
CHANGE_BEHAVIORS = ("apply", "notify", "fail", "disable_pipeline")
POLICY_MATRIX = tuple(product(TABLE_MODES, COLUMN_MODES, DATA_TYPE_MODES, DDL_MODES, CHANGE_BEHAVIORS))


def _expected_decision(
    *,
    change_family: str,
    columns: str,
    data_type: str,
    ddl_mode: str,
    on_schema_change: str,
) -> str:
    if on_schema_change in {"fail", "disable_pipeline"}:
        return "fail"
    if change_family in {"add_column", "add_generated_column"}:
        if columns == "quarantine":
            return "quarantine"
        if columns in {"freeze", "ignore"}:
            return "defer"
    if change_family in {"add_generated_column", "type_widen"}:
        if data_type == "quarantine":
            return "quarantine"
        if data_type == "freeze":
            return "defer"
    if ddl_mode == "manual_approval":
        return "manual_approval"
    if ddl_mode == "plan_only":
        return "defer"
    if change_family == "type_widen" and ddl_mode == "online":
        return "defer"
    if on_schema_change == "notify":
        return "notify"
    return "apply"


def _schema_plan(change_family: str):
    if change_family == "add_column":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False), ColumnDef("new_value", "text")],
            target=[ColumnDef("id", "bigint", False)],
        )
    if change_family == "add_generated_column":
        return SchemaComparator(SchemaEvolutionPolicy(on_type_change="new_column")).compare(
            source=[ColumnDef("amount", "text")],
            target=[ColumnDef("amount", "int")],
        )
    if change_family == "type_widen":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False)],
            target=[ColumnDef("id", "int", False)],
        )
    raise AssertionError(f"unsupported test change family: {change_family}")


@pytest.mark.parametrize("change_family", ("add_column", "add_generated_column", "type_widen"))
@pytest.mark.parametrize(
    ("tables", "columns", "data_type", "ddl_mode", "on_schema_change"),
    POLICY_MATRIX,
)
def test_governance_policy_matrix_never_marks_non_apply_action_as_passed(
    change_family: str,
    tables: str,
    columns: str,
    data_type: str,
    ddl_mode: str,
    on_schema_change: str,
) -> None:
    governed = OnlineSchemaPlanner().plan(
        schema_plan=_schema_plan(change_family),
        dialect="postgres",
        table="landing.orders",
        policy=DdlGovernancePolicy(
            tables=tables,
            columns=columns,
            data_type=data_type,
            ddl_mode=ddl_mode,
            on_schema_change=on_schema_change,
        ),
    )

    expected = _expected_decision(
        change_family=change_family,
        columns=columns,
        data_type=data_type,
        ddl_mode=ddl_mode,
        on_schema_change=on_schema_change,
    )
    assert governed.actions[0].decision == expected
    assert bool(governed.blockers) is (expected != "apply")
    assert governed.passed is (expected == "apply")


class _Source:
    connector = object()

    def __init__(self, *, include_new_column: bool = True) -> None:
        row: dict[str, object] = {"id": 1}
        schema = [("id", "bigint")]
        if include_new_column:
            row["new_value"] = "value"
            schema.append(("new_value", "text"))
        self._result = SimpleNamespace(
            artifact=InMemoryRowsArtifact([row]),
            schema=schema,
            state=None,
            force_full_refresh=False,
        )

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, state):
        del load_config, state
        return self._result


class _PolicySink:
    connector = SimpleNamespace()

    def __init__(self, *, exists: bool, target_schema: list[tuple[str, str]]) -> None:
        self.exists = exists
        self.target_schema = target_schema
        self.applied: list[str] = []
        self.received_payload = None

    def target_table_exists(self, load_config) -> bool:
        del load_config
        return self.exists

    def get_target_schema(self, load_config):
        del load_config
        return self.target_schema

    def apply_schema_plan(self, load_config, plan) -> None:
        self.applied.extend(plan.ddl_sql("postgres", f"{load_config.target_schema}.{load_config.target_table}"))

    def load(self, load_config, payload):
        del load_config
        self.received_payload = payload
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


class _MeasuredPolicySink(_PolicySink):
    def __init__(self, *, row_count: int) -> None:
        super().__init__(exists=True, target_schema=[("id", "bigint")])
        self.row_count = row_count

    def get_target_row_count(self, load_config) -> int:
        del load_config
        return self.row_count


class _MssqlPolicySink(_PolicySink):
    pass


def _load_config(schema_evolution: dict[str, object]) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False, "schema_evolution": schema_evolution},
    )


@pytest.mark.parametrize("columns", ("freeze", "ignore", "quarantine"))
@pytest.mark.parametrize("on_schema_change", CHANGE_BEHAVIORS)
def test_metadata_only_non_apply_policy_fails_before_ddl_or_load(columns: str, on_schema_change: str) -> None:
    sink = _PolicySink(exists=True, target_schema=[("id", "bigint")])

    with pytest.raises(SchemaEvolutionError) as exc_info:
        ETLProcessor(_Source(), sink).run(_load_config({"columns": columns, "on_schema_change": on_schema_change}))

    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
    assert "schema_evolution." in str(exc_info.value)
    assert sink.applied == []
    assert sink.received_payload is None


@pytest.mark.parametrize(
    ("tables", "columns", "on_schema_change", "apply_safe"),
    product(TABLE_MODES, COLUMN_MODES, CHANGE_BEHAVIORS, (False, True)),
)
def test_existing_table_apply_authority_cross_product_is_atomic(
    tables: str,
    columns: str,
    on_schema_change: str,
    apply_safe: bool,
) -> None:
    sink = _PolicySink(exists=True, target_schema=[("id", "bigint")])
    may_apply = columns == "evolve" and on_schema_change == "apply" and apply_safe
    options = {
        "tables": tables,
        "columns": columns,
        "on_schema_change": on_schema_change,
        "apply_safe": apply_safe,
    }

    if may_apply:
        ETLProcessor(_Source(), sink).run(_load_config(options))
        assert any("new_value" in statement for statement in sink.applied)
        assert sink.received_payload is not None
    else:
        with pytest.raises(SchemaEvolutionError) as exc_info:
            ETLProcessor(_Source(), sink).run(_load_config(options))
        assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
        assert "schema_evolution." in str(exc_info.value)
        assert sink.applied == []
        assert sink.received_payload is None


@pytest.mark.parametrize(("tables", "ddl_mode", "on_schema_change"), product(TABLE_MODES, DDL_MODES, CHANGE_BEHAVIORS))
def test_missing_target_table_obeys_table_and_global_change_policy(
    tables: str,
    ddl_mode: str,
    on_schema_change: str,
) -> None:
    sink = _PolicySink(exists=False, target_schema=[])
    may_create = tables == "evolve" and ddl_mode in {"online", "safe_window"} and on_schema_change == "apply"

    if may_create:
        ETLProcessor(_Source(include_new_column=False), sink).run(
            _load_config(
                {
                    "tables": tables,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                }
            )
        )
        assert sink.received_payload is not None
    else:
        with pytest.raises(SchemaEvolutionError) as exc_info:
            ETLProcessor(_Source(include_new_column=False), sink).run(
                _load_config(
                    {
                        "tables": tables,
                        "ddl_mode": ddl_mode,
                        "on_schema_change": on_schema_change,
                    }
                )
            )
        assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
        assert "create_table" in str(exc_info.value)
        assert sink.received_payload is None


def test_disabled_schema_evolution_remains_an_explicit_policy_bypass() -> None:
    sink = _PolicySink(exists=False, target_schema=[])

    ETLProcessor(_Source(include_new_column=False), sink).run(
        _load_config(
            {
                "enabled": False,
                "tables": "freeze",
                "ddl_mode": "manual_approval",
                "on_schema_change": "disable_pipeline",
            }
        )
    )

    assert sink.applied == []
    assert sink.received_payload is not None


def test_missing_target_respects_apply_safe_false() -> None:
    sink = _PolicySink(exists=False, target_schema=[])

    with pytest.raises(SchemaEvolutionError) as exc_info:
        ETLProcessor(_Source(include_new_column=False), sink).run(
            _load_config({"tables": "evolve", "apply_safe": False})
        )

    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
    assert "schema_evolution.apply_safe:create_table" in str(exc_info.value)
    assert sink.received_payload is None


def test_blocked_mixed_plan_applies_no_partial_safe_ddl() -> None:
    sink = _PolicySink(exists=True, target_schema=[("id", "int")])

    with pytest.raises(SchemaEvolutionError) as exc_info:
        ETLProcessor(_Source(), sink).run(_load_config({}))

    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
    assert "type_widen:id" in str(exc_info.value)
    assert sink.applied == []
    assert sink.received_payload is None


def test_inline_ddl_budget_fails_closed_when_target_count_is_unavailable() -> None:
    sink = _PolicySink(exists=True, target_schema=[("id", "bigint")])

    with pytest.raises(SchemaEvolutionError) as exc_info:
        ETLProcessor(_Source(), sink).run(_load_config({"max_table_size_for_inline_ddl": 1}))

    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
    assert "schema_evolution.table_size_unknown:add_column:new_value" in str(exc_info.value)
    assert sink.applied == []
    assert sink.received_payload is None


@pytest.mark.parametrize(("row_count", "budget", "blocked"), ((1, 0, True), (1, 1, False)))
def test_inline_ddl_budget_uses_concrete_target_count(row_count: int, budget: int, blocked: bool) -> None:
    sink = _MeasuredPolicySink(row_count=row_count)

    if blocked:
        with pytest.raises(SchemaEvolutionError) as exc_info:
            ETLProcessor(_Source(), sink).run(_load_config({"max_table_size_for_inline_ddl": budget}))
        assert "schema_evolution.table_size_budget:add_column:new_value" in str(exc_info.value)
        assert sink.applied == []
        assert sink.received_payload is None
    else:
        ETLProcessor(_Source(), sink).run(_load_config({"max_table_size_for_inline_ddl": budget}))
        assert any("new_value" in statement for statement in sink.applied)
        assert sink.received_payload is not None


def test_mssql_statement_timeout_is_rejected_before_target_mutation() -> None:
    sink = _MssqlPolicySink(exists=True, target_schema=[("id", "bigint")])

    with pytest.raises(SchemaEvolutionError) as exc_info:
        ETLProcessor(_Source(), sink).run(_load_config({"statement_timeout_seconds": 1}))

    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
    assert "schema_evolution.unsupported_option:statement_timeout_seconds:mssql" in str(exc_info.value)
    assert sink.applied == []
    assert sink.received_payload is None


@pytest.mark.parametrize(
    ("dialect", "option", "blocked"),
    (
        ("postgres", {"lock_timeout_seconds": 1}, False),
        ("postgres", {"statement_timeout_seconds": 1}, False),
        ("mssql", {"lock_timeout_seconds": 1}, False),
        ("mssql", {"statement_timeout_seconds": 1}, True),
        ("clickhouse", {"lock_timeout_seconds": 1}, True),
        ("clickhouse", {"statement_timeout_seconds": 1}, True),
        ("bigquery", {"lock_timeout_seconds": 1}, True),
        ("bigquery", {"statement_timeout_seconds": 1}, True),
    ),
)
def test_timeout_capability_matrix_is_explicit(dialect: str, option: dict[str, int], blocked: bool) -> None:
    governed = OnlineSchemaPlanner().plan(
        schema_plan=_schema_plan("add_column"),
        dialect=dialect,
        table="landing.orders",
        policy=DdlGovernancePolicy(**option),
    )

    assert bool(governed.blockers) is blocked
    if blocked:
        assert "unsupported_option" in governed.blockers[0]


def test_mssql_target_budget_probe_uses_exact_three_part_count_big() -> None:
    class Connector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def get_records(self, query: str):
            self.queries.append(query)
            return [(1,)]

    connector = Connector()
    config = _load_config({})
    config.target_database = "DWH_Dev"

    assert MssqlTargetMetrics(connector).row_count(config) == 1
    assert connector.queries == ["SELECT COUNT_BIG(*) FROM [DWH_Dev].[landing].[orders]"]
