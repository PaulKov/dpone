from __future__ import annotations

import argparse
import json
import logging
import shlex
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.cli import main as cli_main
from dpone.commands import run_cmd, run_safe_sample_cmd
from dpone.commands.run_safe_sample_cmd import SafeSampleCommandResult
from dpone.commands.run_safe_sample_rendering import safe_sample_markdown, safe_sample_text
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.security_redaction import REDACTION_TOKEN
from dpone.services.safe_sample_cli_arguments import (
    selected_safe_sample_fix_command,
    validate_safe_sample_cli_arguments,
)

_INCOMPATIBLE_OPTION_CASES = (
    ("--registry", "registry-value-$(touch should-not-run)"),
    ("--dag-id", "dag-value-marker"),
    ("--execution-date", "2042-03-04T05:06:07Z"),
    ("--interval-start", "2042-03-04T05:00:00Z"),
    ("--interval-end", "2042-03-04T06:00:00Z"),
    ("--retry-attempts", "7"),
    ("--retry-backoff-seconds", "19.75"),
)


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context_free_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = _LoggerStub()
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda *, logger: SimpleNamespace(logger=logger)),
    )


def _run_cli(
    argv: list[str],
    *,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    _patch_context_free_cli(monkeypatch)
    with pytest.raises(SystemExit) as exc:
        cli_main.main(argv)
    captured = capsys.readouterr()
    return int(exc.value.code), captured.out, captured.err


def _unexpected_effect(*args: object, **kwargs: object) -> Any:
    del args, kwargs
    raise AssertionError("safe-sample argument validation must precede this effect")


@pytest.mark.parametrize(("option", "value"), _INCOMPATIBLE_OPTION_CASES)
def test_safe_sample_rejects_each_explicit_ordinary_run_option_before_effects(
    option: str,
    value: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_safe_sample_cmd, "_execute_safe_sample", _unexpected_effect)
    monkeypatch.setattr(run_cmd, "ProjectSelectionService", _unexpected_effect)

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders",
            "--sample",
            "1000",
            "--target",
            "temporary",
            option,
            value,
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    payload = json.loads(stdout)
    error = payload["result"]["errors"][0]
    assert code == 2
    assert stderr == ""
    assert error["code"] == "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID"
    assert error["exit_code"] == 2
    assert error["incompatible_options"] == [option]
    assert option in error["message"]
    assert value not in stdout
    assert value not in stderr
    assert list(tmp_path.iterdir()) == []


def test_selected_safe_sample_incompatible_fix_preserves_bounded_scope_and_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "run",
            ".",
            "--select",
            "id:orders_daily",
            "--sample",
            "42",
            "--dag-id",
            "legacy-dag",
            "--environment",
            "production",
            "--run-id",
            "audit-42",
            "--max-selected",
            "1",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    error = json.loads(stdout)["errors"][0]
    assert shlex.split(error["fixes"][0]["command"]) == [
        "dpone",
        "run",
        ".",
        "--select",
        "id:orders_daily",
        "--max-selected",
        "1",
        "--sample",
        "42",
        "--target",
        "temporary",
        "--environment",
        "production",
        "--run-id",
        "audit-42",
    ]


def test_safe_sample_explicit_default_is_rejected_but_parser_default_is_not_explicit() -> None:
    parser = cli_main.build_parser()

    implicit = parser.parse_args(["run", "pipelines/orders", "--sample", "1000", "--target", "temporary"])
    explicit = parser.parse_args(
        [
            "run",
            "pipelines/orders",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--retry-attempts",
            "0",
            "--retry-backoff-seconds",
            "0",
        ]
    )

    assert run_cmd.safe_sample_incompatible_options(implicit) == ()
    assert run_cmd.safe_sample_incompatible_options(explicit) == (
        "--retry-attempts",
        "--retry-backoff-seconds",
    )


def test_safe_sample_reports_all_explicit_ordinary_options_once_in_stable_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_safe_sample_cmd, "_execute_safe_sample", _unexpected_effect)
    argv = [
        "run",
        "pipelines/orders",
        "--sample",
        "1000",
        "--target",
        "temporary",
    ]
    for option, value in reversed(_INCOMPATIBLE_OPTION_CASES):
        argv.extend((option, value))
    argv.extend(("--format", "json"))

    code, stdout, stderr = _run_cli(
        argv,
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    payload = json.loads(stdout)
    errors = payload["result"]["errors"]
    assert code == 2
    assert stderr == ""
    assert len(errors) == 1
    assert errors[0]["incompatible_options"] == [option for option, _value in _INCOMPATIBLE_OPTION_CASES]
    for _option, value in _INCOMPATIBLE_OPTION_CASES:
        assert value not in stdout
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("output_format", ["text", "md"])
def test_safe_sample_human_argument_failures_are_redacted_and_traceback_free(
    output_format: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_safe_sample_cmd, "_execute_safe_sample", _unexpected_effect)
    ordinary_value = "registry-value-$(touch should-not-run)"

    code, stdout, stderr = _run_cli(
        [
            "run",
            "pipelines/orders",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--registry",
            ordinary_value,
            "--format",
            output_format,
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    assert "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID" in stdout
    assert "--registry" in stdout
    assert ordinary_value not in stdout
    assert "Traceback" not in stdout
    assert list(tmp_path.iterdir()) == []


def test_selected_safe_sample_argument_failure_precedes_selection_and_has_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_cmd, "ProjectSelectionService", _unexpected_effect)
    ordinary_value = "registry-value-must-not-appear"

    code, stdout, stderr = _run_cli(
        [
            "run",
            ".",
            "--select",
            "id:orders",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--registry",
            ordinary_value,
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    payload = json.loads(stdout)
    assert code == 2
    assert stderr == ""
    assert payload["schema"] == "dpone.selected-safe-sample-report.v1"
    assert payload["passed"] is False
    assert payload["exit_code"] == 2
    assert payload["errors"][0]["incompatible_options"] == ["--registry"]
    assert ordinary_value not in stdout
    assert list(tmp_path.iterdir()) == []
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.selected-safe-sample-report.v1",
        )
        == ()
    )


@pytest.mark.parametrize("output_format", ["text", "md"])
def test_selected_safe_sample_human_failure_includes_redacted_recovery(
    output_format: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_cmd, "ProjectSelectionService", _unexpected_effect)
    ordinary_value = "registry-value-must-not-appear"

    code, stdout, stderr = _run_cli(
        [
            "run",
            ".",
            "--select",
            "id:orders",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--registry",
            ordinary_value,
            "--format",
            output_format,
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    assert "- recovery:" in stdout
    assert "dpone run . --select id:orders --sample 1000 --target temporary" in stdout
    assert ordinary_value not in stdout
    assert "Traceback" not in stdout
    assert list(tmp_path.iterdir()) == []


def test_selected_safe_sample_success_payload_has_top_level_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = SimpleNamespace(
        selected=(SimpleNamespace(node=SimpleNamespace(node_id="orders")),),
        to_jsonable=lambda: {
            "schema": "dpone.selection-report.v1",
            "selection_fingerprint": "sha256:" + ("a" * 64),
        },
    )
    outcome = SimpleNamespace(
        report=report,
        checked_sources={"orders": SimpleNamespace(source_label="pipelines/orders/pipeline.yaml")},
        consumed_files={},
    )

    class _SelectionService:
        def select(self, **kwargs: object) -> object:
            del kwargs
            return outcome

        def verify_consumed_files(self, consumed_files: dict[str, object]) -> None:
            assert consumed_files == {}

    child_payload: dict[str, object] = {
        "run_id": "selected-orders",
        "passed": True,
        "result": {"status": "success", "errors": []},
        "safe_sample": {
            "runtime_run": {"evidence_write": {"path": ".dpone-cache/safe-sample-runs/orders/evidence.json"}}
        },
    }
    monkeypatch.setattr(run_cmd, "ProjectSelectionService", lambda *, root: _SelectionService())
    monkeypatch.setattr(
        run_cmd,
        "build_safe_sample_result",
        lambda args: SafeSampleCommandResult(
            exit_code=0,
            payload=child_payload | {"run_id": args.run_id},
            text="passed\n",
            markdown="# passed\n",
        ),
    )

    code = run_cmd.cmd_run(
        argparse.Namespace(
            path=".",
            selector=None,
            select=["id:orders"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=10,
            sample=1000,
            target="temporary",
            run_id="selected",
            environment="development",
            format="json",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["passed"] is True
    assert payload["exit_code"] == 0
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.selected-safe-sample-report.v1",
        )
        == ()
    )


def test_selected_safe_sample_failure_payload_has_top_level_exit_code(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = run_cmd.cmd_run(
        argparse.Namespace(
            path=".",
            selector=None,
            select=["id:orders"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=10,
            sample=None,
            target=None,
            run_id=None,
            environment="development",
            format="json",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 4
    assert payload["passed"] is False
    assert payload["exit_code"] == 4
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.selected-safe-sample-report.v1",
        )
        == ()
    )


@pytest.mark.parametrize("output_format", ["json", "text", "md"])
def test_selected_report_recursively_redacts_paths_tokens_and_uri_credentials(
    output_format: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    physical_path = tmp_path / "private" / "pipelines" / "orders" / "pipeline.yaml"
    evidence_path = tmp_path / "private" / "evidence" / "safe-sample.json"
    token = "selection-token-must-not-leak"
    uri_user = "selection-user"
    uri_password = "selection-password"
    query_token = "query-token-must-not-leak"
    secret_uri = f"postgresql://{uri_user}:{uri_password}@db.internal/dwh?token={query_token}"
    error = {
        "schema": "dpone.error.v1",
        "code": "DPONE_SELECTION_CONTEXT_INVALID",
        "stage": "selected_safe_sample_selection",
        "severity": "error",
        "message": f"Selection failed at {physical_path} via {secret_uri} token={token}.",
        "path": str(physical_path),
        "token": token,
        "registry_uri": secret_uri,
        "fixes": [
            {
                "id": "retry_selection",
                "safety": "safe",
                "command": f"dpone run {physical_path} --token {token}",
            }
        ],
    }
    payload: dict[str, object] = {
        "schema": "dpone.selected-safe-sample-report.v1",
        "passed": False,
        "exit_code": 2,
        "selection": None,
        "results": [
            {
                "workload_id": "orders",
                "source": str(physical_path),
                "run_id": "orders-run",
                "passed": False,
                "status": "blocked",
                "exit_code": 2,
                "errors": [error],
                "evidence_path": str(evidence_path),
            }
        ],
        "unscheduled": [],
        "errors": [error],
    }

    run_cmd._write_selected_report(  # noqa: SLF001 - focused output-boundary regression.
        argparse.Namespace(format=output_format),
        payload,
    )

    stdout = capsys.readouterr().out
    for private_value in (
        tmp_path.as_posix(),
        token,
        uri_user,
        uri_password,
        query_token,
    ):
        assert private_value not in stdout
    assert REDACTION_TOKEN in stdout
    assert "Traceback" not in stdout
    if output_format == "json":
        public_payload = json.loads(stdout)
        assert public_payload["results"][0]["source"] == "$ABSOLUTE_PATH"
        assert public_payload["results"][0]["evidence_path"] == "$ABSOLUTE_PATH"
        assert public_payload["errors"][0]["path"] == "$ABSOLUTE_PATH"
        assert public_payload["errors"][0]["token"] == REDACTION_TOKEN
        assert (
            GitOpsSchemaValidator().validate(
                public_payload,
                expected_kind="dpone.selected-safe-sample-report.v1",
            )
            == ()
        )


def test_safe_sample_fix_command_round_trips_shell_metacharacters_as_one_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    pipeline = str(tmp_path / "pipelines" / "orders $(touch should-not-run); 'quoted'")
    public_pipeline = "pipelines/orders $(touch should-not-run); 'quoted'"

    error = validate_safe_sample_cli_arguments(
        path=pipeline,
        sample=1000,
        target=None,
    )

    assert error is not None
    command = error["fixes"][0]["command"]
    assert shlex.split(command) == [
        "dpone",
        "run",
        public_pipeline,
        "--sample",
        "1000",
        "--target",
        "temporary",
    ]
    assert not (tmp_path / "should-not-run").exists()


def test_selected_safe_sample_fix_command_quotes_selection_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    pipeline = str(tmp_path / "pipelines" / "orders with spaces")
    selection = "tag:finance;$(touch should-not-run)"

    command = selected_safe_sample_fix_command(
        argparse.Namespace(
            path=pipeline,
            select=[selection],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            sample=None,
        )
    )

    assert shlex.split(command) == [
        "dpone",
        "run",
        "pipelines/orders with spaces",
        "--select",
        selection,
        "--sample",
        "1000",
        "--target",
        "temporary",
    ]
    assert not (tmp_path / "should-not-run").exists()


@pytest.mark.parametrize(
    ("renderer", "evidence_fragment"),
    (
        (
            safe_sample_text,
            "- evidence: .dpone-cache/safe-sample-runs/orders/evidence.json",
        ),
        (
            safe_sample_markdown,
            "- evidence: `.dpone-cache/safe-sample-runs/orders/evidence.json`",
        ),
    ),
)
def test_planned_human_output_includes_project_relative_evidence(
    renderer: Any,
    evidence_fragment: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    plan_path = tmp_path / ".dpone-cache" / "safe-sample-runs" / "orders" / "plan.json"
    evidence_path = tmp_path / ".dpone-cache" / "safe-sample-runs" / "orders" / "evidence.json"

    output = renderer(
        errors=[],
        runtime_run={"evidence_write": {"path": str(evidence_path)}},
        runtime_handoff={"plan_path": str(plan_path)},
        execution_mode="local_handoff",
    )

    assert evidence_fragment in output
    assert tmp_path.as_posix() not in output


@pytest.mark.parametrize(
    ("renderer", "evidence_fragment"),
    (
        (
            safe_sample_text,
            "- runtime evidence: .dpone-cache/safe-sample-runs/orders/evidence.json",
        ),
        (
            safe_sample_markdown,
            "- runtime evidence: `.dpone-cache/safe-sample-runs/orders/evidence.json`",
        ),
    ),
)
def test_completed_human_output_includes_project_relative_evidence(
    renderer: Any,
    evidence_fragment: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    evidence_path = tmp_path / ".dpone-cache" / "safe-sample-runs" / "orders" / "evidence.json"

    output = renderer(
        errors=[],
        runtime_run={"evidence_write": {"path": str(evidence_path)}},
        runtime_handoff=None,
        execution_mode="live_copy",
    )

    assert evidence_fragment in output
    assert tmp_path.as_posix() not in output


def test_blocked_human_output_includes_copyable_recovery() -> None:
    pipeline = "pipelines/orders $(touch should-not-run); 'quoted'"
    error = validate_safe_sample_cli_arguments(
        path=pipeline,
        sample=1000,
        target=None,
    )
    assert error is not None

    output = safe_sample_text(
        errors=[error],
        runtime_run=None,
        runtime_handoff=None,
        execution_mode="blocked",
    )

    recovery = next(
        line.removeprefix("- recovery: ") for line in output.splitlines() if line.startswith("- recovery: ")
    )
    assert shlex.split(recovery) == [
        "dpone",
        "run",
        pipeline,
        "--sample",
        "1000",
        "--target",
        "temporary",
    ]
    assert "Traceback" not in output


def test_blocked_human_output_falls_back_to_error_documentation() -> None:
    output = safe_sample_text(
        errors=[
            {
                "code": "DPONE_SAFE_SAMPLE_TEST_BLOCKED",
                "message": "The safe sample is blocked.",
                "fixes": [],
            }
        ],
        runtime_run=None,
        runtime_handoff=None,
        execution_mode="blocked",
    )

    assert "- recovery: https://paulkov.github.io/dpone/errors/DPONE_SAFE_SAMPLE_TEST_BLOCKED/" in output
