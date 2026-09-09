"""Reviewed authority for the real SQL Server target-behaviour matrix."""

from __future__ import annotations

import json

from tools.route_live_certification.reviewed_cases_runtime import target_behavior_suite


def test_target_behavior_inventory_distinguishes_linux_filetable_boundary() -> None:
    suite = target_behavior_suite()
    assert len(suite.cases) == 36
    filetable = [
        case for case in suite.cases if json.loads(case.config_json)["parameters"]["target_behavior"] == "filetable"
    ]
    assert len(filetable) == 3
    assert {case.action_class for case in filetable} == {"pinned_vendor_capability_preflight"}
    assert {case.outcome_class for case in filetable} == {"pinned_sqlserver_linux_filetable_unavailable"}
    assert all(not case.expected_mutation for case in filetable)


def test_target_behavior_inventory_keeps_five_real_mutation_cells() -> None:
    suite = target_behavior_suite()
    mutable = [case.case_id for case in suite.cases if case.expected_mutation]
    assert mutable == [
        "append_key_preserving__inbound_fk_no_action",
        "append_key_preserving__ordinary_table",
        "destructive_replace__ordinary_table",
        "merge_key_preserving__inbound_fk_no_action",
        "merge_key_preserving__ordinary_table",
    ]
