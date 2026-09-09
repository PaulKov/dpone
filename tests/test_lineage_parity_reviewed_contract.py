"""Closed authoring assertions for the sink-side lineage live matrix."""

from __future__ import annotations

import json

from tools.route_live_certification.reviewed_cases_strategy import lineage_parity_suite


def test_lineage_parity_reviewed_contract_is_exact_sink_side_matrix() -> None:
    suite = lineage_parity_suite()

    assert len(suite.cases) == 24
    for reviewed in suite.cases:
        parameters = json.loads(reviewed.config_json)["parameters"]
        assert parameters["source_boundary"] == "postgres_complete_relation_snapshot_test_adapter"
        assert parameters["production_column_cursor_claim"] is False
        assert parameters["required_phases"] == ["extract", "stage", "mutate", "commit", "checkpoint"]
        assert parameters["required_identity"] == ["run", "operation", "source", "target", "artifact"]
        assert reviewed.action_class == "standard_etl_lineage_projection"
        assert reviewed.outcome_class == "identical_semantic_lineage_and_receipt"
        assert reviewed.expected_mutation is True
