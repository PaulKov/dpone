"""Reviewed SQL Server physical-design capability matrix."""

from __future__ import annotations

from itertools import product

from .reviewed_case import ReviewedCase, ReviewedSuite, case

_COMPRESSIONS = ("none", "row", "page")
_FILLFACTORS = (None, 1, 80, 100)
_MODES = ("auto", "explicit", "off")
_APPLY_MODES = ("online", "safe_window", "plan_only", "manual_approval")
_RECONCILIATION_MODES = ("block", "auto_safe", "plan_only", "safe_window")
_MIGRATION_MODES = ("block", "online_safe", "shadow")


def physical_design_suite() -> ReviewedSuite:
    """Return accepted, rejected, reconciliation, and cross-contract cells."""

    suite_id = "physical_design"
    cases = [
        *_rowstore_cases(suite_id),
        *_placement_and_cci_cases(suite_id),
        *_invalid_cases(suite_id),
        *_policy_cases(suite_id),
        *_compression_reconciliation_cases(suite_id),
        *_key_boundary_cases(suite_id),
        *_catalog_state_cases(suite_id),
        *_strategy_cross_cases(suite_id),
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.physical_design.v1", cases)


def _rowstore_cases(suite_id: str) -> list[ReviewedCase]:
    output: list[ReviewedCase] = []
    for compression, has_primary in product(_COMPRESSIONS, (False, True)):
        fillfactors = _FILLFACTORS if has_primary else (None,)
        for fillfactor in fillfactors:
            parameters = {
                "enabled": True,
                "mode": "explicit",
                "apply": "online",
                "compression": compression,
                "primary_key": ["id"] if has_primary else [],
                "fillfactor": fillfactor,
            }
            output.append(
                case(
                    suite_id,
                    _id("rowstore", parameters),
                    parameters,
                    action="fenced_physical_create",
                    outcome="exact_sys_catalog_match",
                    mutation=True,
                )
            )
    return output


def _placement_and_cci_cases(suite_id: str) -> list[ReviewedCase]:
    definitions = (
        (
            "rowstore_filegroup_textimage",
            {
                "compression": "row",
                "filegroup": "PRIMARY",
                "textimage_filegroup": "PRIMARY",
                "primary_key": ["id"],
                "fillfactor": 80,
            },
        ),
        (
            "clustered_columnstore",
            {"compression": "none", "clustered_columnstore": True, "primary_key": []},
        ),
    )
    return [
        case(
            suite_id,
            f"placement__{name}",
            parameters,
            action="fenced_physical_create",
            outcome="exact_sys_catalog_match",
            mutation=True,
        )
        for name, parameters in definitions
    ]


def _invalid_cases(suite_id: str) -> list[ReviewedCase]:
    definitions: tuple[tuple[str, dict[str, object]], ...] = (
        ("unknown_compression", {"compression": "invalid"}),
        ("row_compression_with_cci", {"compression": "row", "clustered_columnstore": True}),
        ("page_compression_with_cci", {"compression": "page", "clustered_columnstore": True}),
        ("primary_key_with_cci", {"primary_key": ["id"], "clustered_columnstore": True}),
        ("textimage_without_filegroup", {"textimage_filegroup": "PRIMARY"}),
        ("textimage_wrong_filegroup", {"filegroup": "PRIMARY", "textimage_filegroup": "MISSING"}),
        ("filegroup_missing", {"filegroup": "MISSING"}),
        ("fillfactor_without_primary_key", {"fillfactor": 80}),
        ("fillfactor_zero", {"primary_key": ["id"], "fillfactor": 0}),
        ("fillfactor_101", {"primary_key": ["id"], "fillfactor": 101}),
        ("fillfactor_conflicting_alias", {"primary_key": ["id"], "fillfactor": 80, "index_fillfactor": 90}),
        ("filegroup_injection", {"filegroup": "PRIMARY];DROP TABLE sentinel;--"}),
        ("target_type_injection", {"target_type": "nvarchar(10));DROP TABLE sentinel;--"}),
        ("unknown_storage_option", {"unsupported_setting": True}),
        ("unknown_index_option", {"unsupported_index": ["id"]}),
        ("unsupported_partitioning", {"partitioning": {"column": "id"}}),
    )
    return [
        case(
            suite_id,
            f"reject__{name}",
            parameters,
            action="physical_preflight",
            outcome="typed_reject_before_source_copy",
            mutation=False,
        )
        for name, parameters in definitions
    ] + [
        case(
            suite_id,
            "runtime_activation__enabled",
            {"apply_runtime": True, "apply": "online"},
            action="fenced_physical_create",
            outcome="exact_sys_catalog_match",
            mutation=True,
        ),
        case(
            suite_id,
            "runtime_activation__disabled_with_online_apply",
            {"apply_runtime": False, "apply": "online"},
            action="physical_plan_only",
            outcome="zero_vendor_physical_mutation",
            mutation=False,
        ),
    ]


def _policy_cases(suite_id: str) -> list[ReviewedCase]:
    output: list[ReviewedCase] = []
    for mode, apply, reconciliation, migration in product(
        _MODES,
        _APPLY_MODES,
        _RECONCILIATION_MODES,
        _MIGRATION_MODES,
    ):
        should_apply = mode in {"auto", "explicit"} and apply in {"online", "safe_window"}
        parameters = {
            "mode": mode,
            "apply": apply,
            "reconciliation": reconciliation,
            "migration": migration,
            "target_state": "missing",
        }
        output.append(
            case(
                suite_id,
                _id("policy", parameters),
                parameters,
                action="fenced_physical_create" if should_apply else "physical_plan_only",
                outcome="exact_sys_catalog_match" if should_apply else "zero_vendor_mutation",
                mutation=should_apply,
            )
        )
    return output


def _compression_reconciliation_cases(suite_id: str) -> list[ReviewedCase]:
    approvals = (
        ("block", "absent", False),
        ("auto_safe", "absent", False),
        ("plan_only", "absent", False),
        ("safe_window", "absent", False),
        ("safe_window", "exact_live", True),
        ("safe_window", "wrong_table", False),
        ("safe_window", "expired", False),
        ("safe_window", "wrong_risk", False),
    )
    output: list[ReviewedCase] = []
    for actual, desired, (mode, approval, exact) in product(_COMPRESSIONS, _COMPRESSIONS, approvals):
        has_drift = actual != desired
        applies = has_drift and mode == "safe_window" and exact
        parameters = {
            "actual": actual,
            "desired": desired,
            "reconciliation": mode,
            "approval": approval,
        }
        output.append(
            case(
                suite_id,
                _id("compression_reconcile", parameters),
                parameters,
                action="fenced_physical_rebuild" if applies else "physical_reconciliation",
                outcome=(
                    "catalog_changed_rows_preserved"
                    if applies
                    else ("exact_no_drift" if not has_drift else "blocked_rows_and_catalog_preserved")
                ),
                mutation=applies,
            )
        )
    return output


def _key_boundary_cases(suite_id: str) -> list[ReviewedCase]:
    definitions = (
        ("absent", None, True),
        ("single", ["id"], True),
        ("composite_16", [f"k{i:02d}" for i in range(16)], True),
        ("composite_17", [f"k{i:02d}" for i in range(17)], False),
        ("duplicate", ["id", "id"], False),
        ("missing_column", ["not_present"], False),
        ("nullable_source_column_becomes_not_null", ["nullable_key"], True),
        ("clustered_bytes_equal_900", ["nvarchar_450"], True),
        ("clustered_bytes_902", ["nvarchar_451"], False),
        ("max_type", ["nvarchar_max"], False),
    )
    return [
        case(
            suite_id,
            f"primary_key__{name}",
            {"primary_key": columns, "equivalence_boundary": name},
            action="fenced_physical_create" if accepted else "physical_preflight",
            outcome="exact_primary_key_catalog" if accepted else "typed_reject_before_source_copy",
            mutation=accepted,
        )
        for name, columns, accepted in definitions
    ]


def _catalog_state_cases(suite_id: str) -> list[ReviewedCase]:
    definitions = (
        ("exact_rerun", "exact", False, "zero_vendor_mutation"),
        ("disabled_filtered", "disabled_or_filtered_index", False, "typed_catalog_drift_reject"),
        ("hypothetical", "hypothetical_index", False, "typed_catalog_drift_reject"),
        ("partitioned", "partitioned_index", False, "typed_catalog_drift_reject"),
        ("wrong_order", "key_order_drift", False, "typed_catalog_drift_reject"),
        ("wrong_compression", "compression_drift", False, "typed_catalog_drift_reject"),
        ("ignore_dup_key", "ignore_dup_key_on", False, "typed_catalog_drift_reject"),
        ("filtered_unique", "ordinary_filtered_unique", False, "typed_catalog_drift_reject"),
        ("included_columns", "unique_with_include", False, "authority_accepted_physical_drift_separate"),
    )
    return [
        case(
            suite_id,
            f"catalog__{name}",
            {"existing_catalog_state": state},
            action="physical_reconciliation",
            outcome=outcome,
            mutation=mutation,
        )
        for name, state, mutation, outcome in definitions
    ]


def _strategy_cross_cases(suite_id: str) -> list[ReviewedCase]:
    output = [
        case(
            suite_id,
            _id("strategy_cross", parameters),
            parameters,
            action="fenced_strategy_aware_create_and_load",
            outcome="exact_catalog_and_strategy_authority",
            mutation=True,
        )
        for parameters in reviewed_strategy_cross_parameters()
    ]
    output.append(
        case(
            suite_id,
            "strategy_cross__scd2_business_key_primary_key_reject",
            {
                "strategy": "scd2",
                "unique_key": ["id"],
                "primary_key": ["id"],
                "source_boundary": "complete_relation_snapshot",
                "certification_scope": "governed_mssql_sink_physical_interaction",
            },
            action="cross_contract_preflight",
            outcome="typed_reject_before_source_copy",
            mutation=False,
        )
    )
    return output


def reviewed_strategy_cross_parameters() -> tuple[dict[str, object], ...]:
    """Return the exact governed sink/physical combinations executed live.

    The complete-snapshot DI boundary isolates MSSQL sink semantics.  It does
    not claim that PostgreSQL column-cursor append/merge is a production-safe
    checkpoint protocol; that route remains independently fail-closed.
    """

    output: list[dict[str, object]] = []
    definitions = (
        ("incremental_append", "only_new"),
        ("incremental_merge", "update_insert"),
        ("snapshot_diff", "row_hash"),
        ("scd2", "expire"),
    )
    for (strategy, submode), target_state in product(definitions, ("missing", "existing")):
        output.append(
            {
                "strategy": strategy,
                "submode": submode,
                "target_state": target_state,
                "source_boundary": "complete_relation_snapshot",
                "certification_scope": "governed_mssql_sink_physical_interaction",
                "text_key_collation": "Latin1_General_100_BIN2",
                "compression": "row",
                "primary_key": [] if strategy == "scd2" else ["id"],
            }
        )
    return tuple(output)


def _id(prefix: str, parameters: dict[str, object]) -> str:
    fields = "__".join(f"{key}-{_token(value)}" for key, value in parameters.items())
    return f"{prefix}__{fields}"


def _token(value: object) -> str:
    if isinstance(value, list):
        return "none" if not value else "_".join(str(item) for item in value)
    return str(value).lower()


__all__ = ["physical_design_suite", "reviewed_strategy_cross_parameters"]
