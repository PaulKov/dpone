from __future__ import annotations

from dpone.commands.run_safe_sample_rendering import safe_sample_markdown, safe_sample_text


def test_live_copy_text_reports_runtime_without_platform_handoff_commands() -> None:
    output = safe_sample_text(
        errors=[],
        runtime_run=_runtime_run(),
        runtime_handoff=_handoff(),
        execution_mode="live_copy",
    )

    assert output.startswith("dpone safe sample live copy\n")
    assert "OK: safe sample run passed" in output
    assert "runtime evidence: .dpone-cache/safe-sample-runtime-execution.json" in output
    assert "runtime status:" not in output
    assert "data outcome:" not in output
    assert "runtime handoff:" not in output
    assert "live copy handoff:" not in output


def test_blocked_markdown_reports_error_without_handoff_commands() -> None:
    output = safe_sample_markdown(
        errors=[
            {
                "code": "DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE",
                "message": "Deployment-scoped live inputs are incomplete.",
            }
        ],
        runtime_run=None,
        runtime_handoff=_handoff(),
        execution_mode="blocked",
    )

    assert output.startswith("# dpone safe sample run blocked\n")
    assert "DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE" in output
    assert "runtime handoff:" not in output
    assert "live copy handoff:" not in output


def test_local_handoff_keeps_platform_commands_out_of_human_output() -> None:
    output = safe_sample_text(
        errors=[{"code": "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED", "message": "Copy is unavailable."}],
        runtime_run=_runtime_run(status="failed"),
        runtime_handoff=_handoff(),
        execution_mode="local_handoff",
    )

    assert output.startswith("dpone safe sample handoff prepared\n")
    assert "platform prepares the signed authorization overlay; rerun the same dpone run command" in output
    assert "runtime handoff:" not in output
    assert "live copy handoff:" not in output


def test_legacy_renderer_call_without_mode_preserves_handoff_commands() -> None:
    output = safe_sample_text(
        errors=[],
        runtime_run=_runtime_run(),
        runtime_handoff=_handoff(),
    )

    assert "runtime handoff: dpone ops safe-sample-runtime-run --plan-json plan.json" in output
    assert "live copy handoff: dpone ops safe-sample-runtime-run --enable-live-copy" in output


def _runtime_run(*, status: str = "succeeded") -> dict[str, object]:
    return {
        "execution_status": status,
        "data_outcome": "passed" if status == "succeeded" else "unknown",
        "evidence_write": {"path": ".dpone-cache/safe-sample-runtime-execution.json"},
    }


def _handoff() -> dict[str, str]:
    return {
        "command": "dpone ops safe-sample-runtime-run --plan-json plan.json",
        "live_copy_command": "dpone ops safe-sample-runtime-run --enable-live-copy",
    }
