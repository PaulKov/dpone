"""Quality outcome redaction regressions for the public ``dpone run`` boundary."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.commands import run_cmd
from dpone.contracts.quality_failure import QualityGateFailureOutcome
from dpone.governance.quality import QualityGateReport, QualityGateResult
from dpone.runtime.governance.service import QualityGateFailure
from dpone.security_redaction import REDACTION_TOKEN


@pytest.mark.parametrize("status", ["success", "warning"])
@pytest.mark.parametrize("output_format", ["json", "text", "md"])
def test_success_and_warning_quality_cli_output_is_redacted(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: str,
    output_format: str,
) -> None:
    report = _PublicReport(status=status)
    _patch_run_dependencies(monkeypatch, report)

    exit_code = run_cmd.cmd_run(
        _args(output_format=output_format),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    output = capsys.readouterr().out
    if output_format == "json":
        json.loads(output)
    assert exit_code == 0
    assert REDACTION_TOKEN in output
    assert "quality-password" not in output
    assert "quality-token" not in output
    assert "/Users/operator" not in output


def test_enriched_quality_failure_json_preserves_actual_safe_outcome_and_redaction(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    report = QualityGateReport(
        results=(
            QualityGateResult(
                gate_id="target_minimum",
                type="min_rows",
                status="failed",
                severity="error",
                metrics={"row_count": 11, "password": "quality-password"},
                message="token=quality-token at /Users/operator/private/report.json",
            ),
        )
    )
    outcome = QualityGateFailureOutcome(
        failure_boundary="post_commit",
        target_state="mutation_returned_success",
        checkpoint_state="committed",
        source_state="not_advanced",
        retry_classification="retry_via_native_resume",
        inserted_rows=7,
        updated_rows=2,
        final_rows=11,
        extracted_rows=9,
        attempts=1,
    )
    args = _args(output_format="json")
    args.path = tmp_path / "pipelines" / "orders.yaml"

    run_cmd._write_run_failure(args, QualityGateFailure(report, outcome=outcome))

    payload = json.loads(capsys.readouterr().out)
    result = payload["result"]
    assert payload["passed"] is False
    assert payload["attempts"] == 1
    assert result["inserted_rows"] == 7
    assert result["updated_rows"] == 2
    assert result["final_rows"] == 11
    assert result["extracted_rows"] == 9
    assert result["error_code"] == "DPONE_QUALITY_GATES_FAILED"
    assert result["failure_context"] == {
        "failure_boundary": "post_commit",
        "target_state": "mutation_returned_success",
        "checkpoint_state": "committed",
        "source_state": "not_advanced",
        "retry_classification": "retry_via_native_resume",
    }
    assert result["quality_gates"]["results"][0]["metrics"]["password"] == REDACTION_TOKEN
    serialized = json.dumps(payload)
    assert "quality-password" not in serialized
    assert "quality-token" not in serialized
    assert "/Users/operator" not in serialized
    assert str(tmp_path) not in serialized


def test_old_quality_failure_constructor_keeps_legacy_zero_outcome_shape(
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = QualityGateReport(results=())

    run_cmd._write_run_failure(_args(output_format="json"), QualityGateFailure(report))

    payload = json.loads(capsys.readouterr().out)
    assert payload["attempts"] == 0
    assert payload["result"]["inserted_rows"] == 0
    assert payload["result"]["updated_rows"] == 0
    assert payload["result"]["final_rows"] == 0
    assert payload["result"]["extracted_rows"] == 0
    assert "failure_context" not in payload["result"]


class _PublicReport:
    passed = True

    def __init__(self, *, status: str) -> None:
        self._status = status

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": True,
            "result": {
                "status": self._status,
                "quality_gates": {
                    "passed": True,
                    "results": [
                        {
                            "status": self._status,
                            "metrics": {"password": "quality-password"},
                            "message": "token=quality-token at /Users/operator/private/report.json",
                        }
                    ],
                },
            },
        }

    def to_text(self) -> str:
        return (
            "dpone run\n"
            f"- status: {self._status}\n"
            "- password: quality-password\n"
            "- message: token=quality-token at /Users/operator/private/report.json\n"
        )

    def to_markdown(self) -> str:
        return (
            "# dpone run\n\n"
            f"- Status: `{self._status}`\n"
            "- password: `quality-password`\n"
            "- message: `token=quality-token at /Users/operator/private/report.json`\n"
        )


def _patch_run_dependencies(monkeypatch: pytest.MonkeyPatch, report: _PublicReport) -> None:
    class _InvocationService:
        def __init__(self, **dependencies):  # noqa: ANN003
            del dependencies

        def resolve(self, **kwargs):  # noqa: ANN003
            del kwargs
            return SimpleNamespace(
                dag_id=None,
                execution_date=None,
                load_config_mutator=lambda load_config: load_config,
                run_context_config={},
            )

    class _RunService:
        def run(self, **kwargs):  # noqa: ANN001
            del kwargs
            return report

    monkeypatch.setattr(run_cmd, "RunInvocationContextService", _InvocationService)
    monkeypatch.setattr(run_cmd, "build_manifest_context", lambda args, ctx: object())
    monkeypatch.setattr(run_cmd, "RunManifestService", _RunService)


def _args(*, output_format: str) -> argparse.Namespace:
    return argparse.Namespace(
        path="manifest.yaml",
        selector=None,
        run_id="run_1",
        dag_id=None,
        retry_attempts=0,
        retry_backoff_seconds=0.0,
        format=output_format,
        sample=None,
        target=None,
        select=[],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=10,
    )
