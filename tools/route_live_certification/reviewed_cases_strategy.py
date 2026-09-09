"""Reviewed load-strategy and wide-route case authority."""

from __future__ import annotations

from itertools import product

from .reviewed_case import ReviewedCase, ReviewedSuite, case

_STRATEGIES = (
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "replace",
    "partition_replace",
    "snapshot_diff",
    "scd2",
    "backfill",
)


def strategy_capability_suite() -> ReviewedSuite:
    """Every supported submode, empty-input behavior, and typed rejection."""

    suite_id = "strategy_capability"
    cases = [*_supported_strategy_cases(suite_id), *_empty_strategy_cases(suite_id)]
    cases.extend(_strategy_rejection_cases(suite_id))
    cases.extend(_production_route_rejection_cases(suite_id))
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.strategy_capability.v1", cases)


def text_key_lifecycle_suite() -> ReviewedSuite:
    """Binary text-key identity across every keyed generic strategy."""

    suite_id = "text_key_lifecycle"
    definitions = (
        ("append_only_new", "incremental_append", "only_new"),
        ("merge_update_insert", "incremental_merge", "update_insert"),
        ("merge_delete_insert", "incremental_merge", "delete_insert"),
        ("snapshot_soft_delete", "snapshot_diff", "soft_delete"),
        ("scd2_expire", "scd2", "expire"),
    )
    cases = [
        case(
            suite_id,
            case_id,
            {
                "strategy": strategy,
                "submode": submode,
                "collation": "Latin1_General_100_BIN2",
                "source_boundary": "complete_relation_snapshot",
                "keys": ["A", "a", "accented", "supplementary", "control_marker"],
                "phases": ["baseline", "rerun", "delete_or_update", "reactivate_or_history"],
            },
            action="standard_etl_text_key_lifecycle",
            outcome="exact_binary_key_identity_and_clean_staging",
            mutation=True,
        )
        for case_id, strategy, submode in definitions
    ]
    cases.append(
        case(
            suite_id,
            "reject_trailing_space_sql_equivalence",
            {
                "strategy": "incremental_merge",
                "keys": ["trail", "trail "],
                "collation": "Latin1_General_100_BIN2",
                "source_boundary": "complete_relation_snapshot",
            },
            action="text_key_preflight",
            outcome="typed_reject_before_target_transaction",
            mutation=False,
        )
    )
    cases.append(
        case(
            suite_id,
            "reject_shadow_swap_physical_preservation",
            {
                "strategy": "incremental_merge",
                "merge_policy": "shadow_swap",
                "source_boundary": "complete_relation_snapshot",
            },
            action="text_key_strategy_preflight",
            outcome="typed_physical_preservation_reject_before_target_transaction",
            mutation=False,
        )
    )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.text_key_lifecycle.v1", cases)


def wide_strategy_suite() -> ReviewedSuite:
    """All implemented strategies over the authoritative 128-plus-column fixture."""

    suite_id = "wide_strategy"
    fail_closed = {
        "incremental_append": "DPONE_POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE",
        "incremental_merge": "DPONE_POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE",
        "replace": "mssql.strategy.replace.cross_dialect_raw_predicate",
        "backfill": "mssql.strategy.backfill.replace_cross_dialect_raw_predicate",
    }
    cases = [
        case(
            suite_id,
            strategy,
            {
                "strategy": strategy,
                "minimum_column_count": 128,
                "correctness_oracle": "exact_keyset_plus_128_type_catalog_and_full_boundary_sentinels",
                "rows": ["boundary_row", "null_row", "change_row"],
                "assertions": (
                    ["target_absent", "pre_source_reject", "staging_cleanup"]
                    if strategy in fail_closed
                    else ["catalog", "values", "row_count", "rerun", "staging_cleanup"]
                ),
                "blocker": fail_closed.get(strategy),
            },
            action=(
                "standard_etl_route_preflight" if strategy in fail_closed else "standard_etl_wide_strategy_lifecycle"
            ),
            outcome=(
                "typed_unsupported_route_reject_before_io"
                if strategy in fail_closed
                else "exact_wide_catalog_and_value_roundtrip"
            ),
            mutation=strategy not in fail_closed,
        )
        for strategy in _STRATEGIES
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.wide_strategy.v1", cases)


def lineage_parity_suite() -> ReviewedSuite:
    """Lineage parity for strategy, artifact, and terminal-consumption paths."""

    suite_id = "lineage_parity"
    cases: list[ReviewedCase] = []
    for strategy, transport in product(
        _STRATEGIES,
        ("python_rows", "file_stream", "postgres_copy_mssql_bcp"),
    ):
        cases.append(
            case(
                suite_id,
                f"{strategy}__{transport}",
                {
                    "strategy": strategy,
                    "transport": transport,
                    "source_boundary": "postgres_complete_relation_snapshot_test_adapter",
                    "production_column_cursor_claim": False,
                    "required_phases": ["extract", "stage", "mutate", "commit", "checkpoint"],
                    "required_identity": ["run", "operation", "source", "target", "artifact"],
                },
                action="standard_etl_lineage_projection",
                outcome="identical_semantic_lineage_and_receipt",
                mutation=True,
            )
        )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.lineage_parity.v1", cases)


def wide_performance_soak_suite() -> ReviewedSuite:
    """Seven-run warm soak for every strategy on the governed wide fixture."""

    suite_id = "wide_performance_soak"
    cases = [
        case(
            suite_id,
            f"{strategy}__run_{run_number:02d}",
            {
                "strategy": strategy,
                "run_number": run_number,
                "source_boundary": "postgres_complete_relation_snapshot_test_adapter",
                "production_column_cursor_claim": False,
                "execution_surface": (
                    "governed_backfill_single_invocation_sink_delegate"
                    if strategy == "backfill"
                    else "governed_standard_etl"
                ),
                "backfill_orchestration_claim": False,
                "minimum_rows": 10_000,
                "minimum_column_count": 128,
                "warmup_runs": 1,
                "measured_runs": 7,
                "failure_rate_maximum": 0.0,
                "correctness_required": True,
                "correctness_oracle": ("exact_keyset_plus_128_type_catalog_and_full_boundary_sentinels"),
            },
            action="standard_etl_wide_soak",
            outcome="correctness_pass_and_measured_runtime",
            mutation=True,
        )
        for strategy, run_number in product(_STRATEGIES, range(1, 8))
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.wide_performance_soak.v1", cases)


def _supported_strategy_cases(suite_id: str) -> list[ReviewedCase]:
    definitions: list[tuple[str, dict[str, object]]] = [
        ("full__truncate_insert", {"strategy": "full_refresh", "overwrite": "truncate_insert"}),
        ("append__all", {"strategy": "incremental_append", "only_new_rows": False}),
        (
            "append__only_new",
            {"strategy": "incremental_append", "only_new_rows": True, "unique_key": ["id"]},
        ),
        ("replace__predicate", {"strategy": "replace", "predicate": "partition_id = 1"}),
    ]
    definitions.extend(
        (f"merge__{policy}", {"strategy": "incremental_merge", "merge_policy": policy})
        for policy in ("auto", "update_insert", "delete_insert")
    )
    definitions.extend(
        (
            f"partition__native_{str(native).lower()}__{mode}",
            {
                "strategy": "partition_replace",
                "native": native,
                "native_mode": mode,
                "require_native": False,
            },
        )
        for native, mode in product((False, True), ("auto", "fallback"))
    )
    definitions.extend(
        (
            f"snapshot__{compare}__{delete_policy}",
            {"strategy": "snapshot_diff", "compare": compare, "delete_policy": delete_policy},
        )
        for compare, delete_policy in product(
            ("row_hash", "all_columns"),
            ("ignore", "hard_delete", "soft_delete"),
        )
    )
    definitions.extend(
        (f"scd2__{policy}", {"strategy": "scd2", "delete_policy": policy}) for policy in ("expire", "ignore")
    )
    definitions.extend(
        (
            f"backfill__{inner_mode}__authored_workers_{workers}__single_invocation",
            {
                "strategy": "backfill",
                "inner_mode": inner_mode,
                "authored_parallel_workers": workers,
                "certification_scope": "single_sink_invocation_inner_mode_only",
            },
        )
        for inner_mode, workers in product(
            ("partition_replace", "replace", "incremental_merge"),
            (1, 2),
        )
    )
    definitions.extend(
        (
            case_id,
            {"strategy": "partition_replace", "max_partitions_per_run": maximum},
        )
        for case_id, maximum in (
            ("partition__limit_equality", 2),
            ("partition__limit_large_4096", 4096),
        )
    )
    return [
        case(
            suite_id,
            case_id,
            {**parameters, "source_boundary": "complete_relation_snapshot"},
            action="standard_etl_strategy_lifecycle",
            outcome="exact_rows_metrics_catalog_and_cleanup",
            mutation=case_id != "partition__limit_equality",
        )
        for case_id, parameters in definitions
    ]


def _empty_strategy_cases(suite_id: str) -> list[ReviewedCase]:
    definitions = (
        ("empty__full", "full_refresh", True),
        ("empty__append", "incremental_append", False),
        ("empty__merge", "incremental_merge", False),
        ("empty__replace", "replace", True),
        ("empty__partition", "partition_replace", False),
        ("empty__snapshot", "snapshot_diff", False),
        ("empty__scd2", "scd2", False),
        ("empty__backfill", "backfill", True),
    )
    return [
        case(
            suite_id,
            case_id,
            {
                "strategy": strategy,
                "source_rows": 0,
                "source_boundary": "complete_relation_snapshot",
            },
            action=(
                "destructive_empty_snapshot_precommit_guard"
                if case_id in {"empty__snapshot", "empty__scd2"}
                else "standard_etl_empty_input"
            ),
            outcome=(
                "typed_reject_target_preserved_and_clean_staging"
                if case_id in {"empty__snapshot", "empty__scd2"}
                else "exact_empty_input_semantics_and_clean_staging"
            ),
            mutation=mutation,
        )
        for case_id, strategy, mutation in definitions
    ]


def _strategy_rejection_cases(suite_id: str) -> list[ReviewedCase]:
    case_ids = (
        "reject__full__exchange",
        "reject__full__unknown",
        "reject__append__missing_key",
        "reject__merge__missing_key",
        "reject__merge__duplicate_policy",
        "reject__merge__duplicate_key_declaration",
        "reject__merge__clickhouse_override",
        "reject__merge__mutations_sync",
        "merge__shadow_swap",
        "reject__replace__missing_predicate",
        "reject__partition__values_external",
        "reject__partition__expression",
        "reject__partition__required",
        "reject__partition__limit_zero",
        "reject__partition__missing_column",
        "reject__partition__unknown_native_mode",
        "reject__snapshot__missing_key",
        "reject__snapshot__compare",
        "reject__snapshot__delete",
        "reject__scd2__missing_key",
        "reject__scd2__hard_marker",
        "reject__scd2__custom_hash",
        "reject__backfill__full_refresh",
        "reject__backfill__workers_zero",
        "reject__backfill__partition_missing_column",
        "reject__backfill__replace_missing_predicate",
        "reject__backfill__merge_missing_key",
        "reject__backfill__unknown_inner",
        "reject__cdc",
        "reject__irrelevant_diff",
        "data_reject__append_duplicate",
        "data_reject__partition_limit",
        "data_reject__partition_null",
    )
    generated = [
        *(
            f"reject__merge__{policy}"
            for policy in ("lightweight_delete_insert", "mutation_delete_insert", "event_upsert", "unknown")
        ),
        *(
            f"data_reject__merge_duplicate__{policy}"
            for policy in ("auto", "update_insert", "delete_insert", "shadow_swap")
        ),
        *(
            f"reject__partition_hint__{hint}"
            for hint in ("partition_function", "switch_out_schema", "switch_out_table_template")
        ),
        *(f"reject__scd2__custom_{field}" for field in ("valid_from_column", "valid_to_column", "current_flag_column")),
    ]
    generated.extend(
        f"reject__partition_product__native_{str(native).lower()}__{mode}__require_{str(required).lower()}"
        for native, mode, required in product((False, True), ("auto", "fallback", "required"), (False, True))
        if mode == "required" or required
    )
    return [
        case(
            suite_id,
            case_id,
            {
                "authored_case_id": case_id,
                "expected_stage": "preflight_or_staged_data_guard",
                "source_boundary": "complete_relation_snapshot",
            },
            action="strategy_capability_guard",
            outcome="typed_reject_and_target_preserved",
            mutation=False,
        )
        for case_id in (*case_ids, *generated)
    ]


def _production_route_rejection_cases(suite_id: str) -> list[ReviewedCase]:
    return [
        case(
            suite_id,
            f"route_reject__{mode}__xmin_generic_atomicity",
            {
                "strategy": f"incremental_{mode}",
                "source_boundary": "postgres_xmin",
                "transaction_boundary": "generic_mssql_target_atomic",
            },
            action="standard_etl_production_route_admission",
            outcome="typed_xmin_generic_atomicity_reject_before_source_or_target_io",
            mutation=False,
        )
        for mode in ("append", "merge")
    ]


__all__ = [
    "lineage_parity_suite",
    "strategy_capability_suite",
    "text_key_lifecycle_suite",
    "wide_performance_soak_suite",
    "wide_strategy_suite",
]
