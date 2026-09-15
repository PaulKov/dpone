"""Pure navigation keeps legacy commands and uses the existing id resolver."""

import pytest


@pytest.mark.parametrize("mode,expected", [("apply", "next"), ("plan", "next  # after --apply")])
def test_workload_mode_hint_unchanged(mode, expected):
    from dpone.commands.init_navigation import _next_command

    assert (
        _next_command(
            "dpone workload init",
            {"passed": True, "mode": mode, "next_commands": ["next"]},
            pipeline_id_from_target=lambda value: value,
        )
        == expected
    )


def test_pipeline_uses_injected_canonical_id_resolver():
    from dpone.commands.init_navigation import _next_command

    observed = []

    def resolve(value):
        observed.append(value)
        return "orders"

    assert (
        _next_command(
            "dpone init pipeline", {"passed": True, "pipeline_path": "x/pipeline.yaml"}, pipeline_id_from_target=resolve
        )
        == "dpone check orders"
    )
    assert observed == ["x/pipeline.yaml"]


def test_failed_result_has_no_next_command_and_conflict_uses_rerun():
    from dpone.commands.init_navigation import _init_json_command, _next_command

    assert (
        _next_command(
            "dpone init dbt", {"passed": False, "next_command": "unsafe"}, pipeline_id_from_target=lambda value: value
        )
        == ""
    )
    assert (
        _init_json_command("dpone init dbt", {"rerun_command": "dpone init dbt demo"})
        == "dpone init dbt demo --format json"
    )
