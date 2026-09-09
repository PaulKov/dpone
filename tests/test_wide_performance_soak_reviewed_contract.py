"""Reviewed semantics for the finite PostgreSQL→MSSQL wide soak."""

from __future__ import annotations

import json

from tools.route_live_certification.reviewed_cases_strategy import wide_performance_soak_suite


def test_wide_soak_has_exact_strategy_run_and_boundary_authority() -> None:
    suite = wide_performance_soak_suite()

    assert suite.suite_id == "wide_performance_soak"
    assert len(suite.cases) == 56
    assert len({case.case_id for case in suite.cases}) == 56
    for case in suite.cases:
        parameters = json.loads(case.config_json)["parameters"]
        assert parameters["source_boundary"] == "postgres_complete_relation_snapshot_test_adapter"
        assert parameters["production_column_cursor_claim"] is False
        assert parameters["backfill_orchestration_claim"] is False
        assert parameters["minimum_rows"] == 10_000
        assert parameters["minimum_column_count"] == 128
        assert parameters["correctness_oracle"] == ("exact_keyset_plus_128_type_catalog_and_full_boundary_sentinels")
        assert parameters["warmup_runs"] == 1
        assert parameters["measured_runs"] == 7
        assert parameters["failure_rate_maximum"] == 0.0
        assert parameters["correctness_required"] is True
        if parameters["strategy"] == "backfill":
            assert parameters["execution_surface"] == "governed_backfill_single_invocation_sink_delegate"
        else:
            assert parameters["execution_surface"] == "governed_standard_etl"
        assert case.action_class == "standard_etl_wide_soak"
        assert case.outcome_class == "correctness_pass_and_measured_runtime"
        assert case.expected_mutation is True
