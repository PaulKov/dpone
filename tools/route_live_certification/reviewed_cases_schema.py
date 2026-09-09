"""Reviewed schema-evolution case authority for PostgreSQL→MSSQL."""

from __future__ import annotations

from itertools import product

from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy

from .reviewed_case import ReviewedCase, ReviewedSuite, case

_TABLE_MODES = ("evolve", "freeze", "ignore")
_COLUMN_MODES = ("evolve", "freeze", "ignore", "quarantine")
_DATA_TYPE_MODES = ("widen", "variant_column", "freeze", "quarantine")
_DDL_MODES = ("online", "safe_window", "plan_only", "manual_approval")
_CHANGE_BEHAVIORS = ("apply", "notify", "fail", "disable_pipeline")
_CHANGE_FAMILIES = ("add_column", "add_generated_column", "type_widen")


def schema_evolution_suite() -> ReviewedSuite:
    """Return the exhaustive governed DDL product plus runtime boundaries."""

    suite_id = "schema_evolution"
    cases = [*_existing_table_cases(suite_id), *_missing_table_cases(suite_id)]
    cases.extend(_runtime_boundary_cases(suite_id))
    cases.extend(_generated_column_cases(suite_id))
    cases.extend(_temporal_catalog_cases(suite_id))
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.schema_evolution.v1", cases)


def _existing_table_cases(suite_id: str) -> list[ReviewedCase]:
    output: list[ReviewedCase] = []
    for family in _CHANGE_FAMILIES:
        for tables, columns, data_type, ddl_mode, behavior, apply_safe in product(
            _TABLE_MODES,
            _COLUMN_MODES,
            _DATA_TYPE_MODES,
            _DDL_MODES,
            _CHANGE_BEHAVIORS,
            (False, True),
        ):
            decision = _expected_decision(
                family=family,
                tables=tables,
                columns=columns,
                data_type=data_type,
                ddl_mode=ddl_mode,
                behavior=behavior,
                apply_safe=apply_safe,
            )
            parameters = {
                "target_state": "existing",
                "change_family": family,
                "tables": tables,
                "columns": columns,
                "data_type": data_type,
                "ddl_mode": ddl_mode,
                "on_schema_change": behavior,
                "apply_safe": apply_safe,
            }
            case_id = _id("existing", parameters)
            output.append(
                case(
                    suite_id,
                    case_id,
                    parameters,
                    action="fenced_schema_ddl_and_standard_etl" if decision == "apply" else "schema_preflight",
                    outcome="catalog_change_applied" if decision == "apply" else f"typed_block_{decision}",
                    mutation=decision == "apply",
                )
            )
    return output


def _missing_table_cases(suite_id: str) -> list[ReviewedCase]:
    output: list[ReviewedCase] = []
    for tables, ddl_mode, behavior, apply_safe in product(
        _TABLE_MODES,
        _DDL_MODES,
        _CHANGE_BEHAVIORS,
        (False, True),
    ):
        may_create = tables == "evolve" and ddl_mode in {"online", "safe_window"} and behavior == "apply" and apply_safe
        parameters = {
            "target_state": "missing",
            "tables": tables,
            "ddl_mode": ddl_mode,
            "on_schema_change": behavior,
            "apply_safe": apply_safe,
        }
        output.append(
            case(
                suite_id,
                _id("missing", parameters),
                parameters,
                action="fenced_create_and_standard_etl" if may_create else "schema_preflight",
                outcome="created_and_loaded" if may_create else "typed_reject_before_source_copy",
                mutation=may_create,
            )
        )
    return output


def _runtime_boundary_cases(suite_id: str) -> list[ReviewedCase]:
    definitions = (
        ("table_size_budget_exceeded", {"max_table_size_for_inline_ddl": 0}, False, "table_size_budget"),
        ("table_size_budget_equal", {"max_table_size_for_inline_ddl": 1}, True, "catalog_change_applied"),
        (
            "table_size_probe_missing",
            {"max_table_size_for_inline_ddl": 1, "probe": "missing"},
            False,
            "table_size_unknown",
        ),
        (
            "table_size_probe_failure",
            {"max_table_size_for_inline_ddl": 1, "probe": "failure"},
            False,
            "table_size_unavailable",
        ),
        (
            "table_size_probe_invalid",
            {"max_table_size_for_inline_ddl": 1, "probe": "negative"},
            False,
            "table_size_invalid",
        ),
        ("mssql_statement_timeout", {"statement_timeout_seconds": 1}, False, "unsupported_statement_timeout"),
        ("mssql_lock_timeout", {"lock_timeout_seconds": 1}, True, "catalog_change_applied"),
    )
    return [
        case(
            suite_id,
            f"runtime__{name}",
            parameters,
            action="fenced_schema_ddl" if applies else "schema_preflight",
            outcome=outcome,
            mutation=applies,
        )
        for name, parameters, applies, outcome in definitions
    ]


def _generated_column_cases(suite_id: str) -> list[ReviewedCase]:
    base = {
        "change_family": "add_generated_column",
        "on_type_change": "new_column",
        "retained_predecessor": "amount:int:null:catalog_collation",
        "managed_companion": "__dpone__nc__amount:nvarchar(max):null:catalog_collation",
    }
    return [
        case(
            suite_id,
            "generated_column__first_standard_etl",
            {**base, "run": "first"},
            action="fenced_schema_ddl_and_standard_etl",
            outcome="retained_predecessor_and_companion_loaded",
            mutation=True,
        ),
        case(
            suite_id,
            "generated_column__idempotent_rerun",
            {**base, "run": "identical_rerun"},
            action="standard_etl_rerun",
            outcome="zero_business_dml_catalog_unchanged",
            mutation=False,
        ),
        case(
            suite_id,
            "generated_column__unrelated_extra_rejected",
            {**base, "catalog_drift": "unrelated_extra_column"},
            action="schema_preflight",
            outcome="typed_reject_before_source_copy",
            mutation=False,
        ),
    ]


def _temporal_catalog_cases(suite_id: str) -> list[ReviewedCase]:
    output: list[ReviewedCase] = []
    for target_type in ("time", "datetime2", "datetimeoffset"):
        for scale in range(8):
            for operation in ("add", "alter"):
                parameters = {
                    "operation": operation,
                    "target_type": f"{target_type}({scale})",
                    "assertion": "predicted_expected_after_equals_sys_columns",
                }
                output.append(
                    case(
                        suite_id,
                        f"temporal_catalog__{operation}__{target_type}_{scale}",
                        parameters,
                        action="fenced_schema_ddl",
                        outcome="exact_sys_catalog_match",
                        mutation=True,
                    )
                )
    return output


def _expected_decision(
    *,
    family: str,
    tables: str,
    columns: str,
    data_type: str,
    ddl_mode: str,
    behavior: str,
    apply_safe: bool,
) -> str:
    governed = OnlineSchemaPlanner().plan(
        schema_plan=_isolated_family_plan(family),
        dialect="mssql",
        table="dpone_it.route_live_schema_case",
        policy=DdlGovernancePolicy(
            tables=tables,
            columns=columns,
            data_type=data_type,
            ddl_mode=ddl_mode,
            on_schema_change=behavior,
        ),
        table_row_count=1,
    )
    if len(governed.actions) != 1:
        raise AssertionError("reviewed schema family must resolve exactly one action")
    decision = governed.actions[0].decision
    if decision == "apply" and not apply_safe:
        return "apply_safe_block"
    return decision


def _isolated_family_plan(family: str):
    if family == "add_column":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False), ColumnDef("new_value", "nvarchar(max)")],
            target=[ColumnDef("id", "bigint", False)],
        )
    if family == "add_generated_column":
        return SchemaComparator(SchemaEvolutionPolicy(on_type_change="new_column")).compare(
            source=[ColumnDef("amount", "nvarchar(max)")],
            target=[ColumnDef("amount", "int")],
        )
    if family == "type_widen":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False)],
            target=[ColumnDef("id", "int", False)],
        )
    raise AssertionError(f"unsupported schema family: {family}")


def _id(prefix: str, parameters: dict[str, object]) -> str:
    fields = "__".join(f"{key}-{str(value).lower()}" for key, value in parameters.items())
    return f"{prefix}__{fields}"


__all__ = ["schema_evolution_suite"]
