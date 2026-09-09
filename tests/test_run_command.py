from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands import run_cmd
from dpone.commands.registry import get_commands
from dpone.contracts.configuration_errors import ETLConfigurationError
from dpone.contracts.process_types import ProcessResult
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.services.run_manifest import RunManifestResult, RunManifestService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_run_command_is_registered() -> None:
    assert "run" in {command.name for command in get_commands()}


def test_run_manifest_service_executes_selected_process_with_run_context(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    class Process:
        def __init__(self, config, config_path=None):
            self.config = config
            self.config_path = config_path

        def run(self, context=None, dag_id=None, execution_date=None):
            calls.append(
                {
                    "config": self.config,
                    "config_path": self.config_path,
                    "run_id": context.run_id,
                    "dag_id": dag_id,
                    "execution_date": execution_date,
                }
            )
            return ProcessResult(
                status="success",
                inserted_rows=10,
                updated_rows=2,
                final_rows=12,
                extracted_rows=12,
                duration_seconds=1.5,
                errors=[],
            )

    spec = SimpleNamespace(name="orders_to_landing", selector="orders", config="cfg", config_path=tmp_path / "m.yml")
    manifest = SimpleNamespace(processes=(spec,), path=tmp_path / "m.yml")

    class Loader:
        def load(self, path, *, metadata_only=True):
            assert metadata_only is False
            assert path == tmp_path / "m.yml"
            return manifest

    result = RunManifestService(process_factory=Process).run(
        path=tmp_path / "m.yml",
        manifest_ctx=SimpleNamespace(loader=Loader()),
        selector="orders",
        run_id="run_01",
        dag_id="dag_01",
        execution_date="2026-06-05",
    )

    assert result.passed is True
    assert result.process == "orders_to_landing"
    assert result.selector == "orders"
    assert result.result.inserted_rows == 10
    assert calls == [
        {
            "config": "cfg",
            "config_path": str(tmp_path / "m.yml"),
            "run_id": "run_01",
            "dag_id": "dag_01",
            "execution_date": "2026-06-05",
        }
    ]


def test_run_manifest_service_defaults_run_id_and_marks_errors_as_failed(tmp_path: Path) -> None:
    class Process:
        def __init__(self, config, config_path=None):
            del config, config_path

        def run(self, context=None, dag_id=None, execution_date=None):
            assert context.run_id == "manual-invocation"
            assert dag_id is None
            assert execution_date is None
            return ProcessResult(
                status="failed",
                inserted_rows=0,
                updated_rows=0,
                final_rows=0,
                extracted_rows=5,
                duration_seconds=0.25,
                errors=["source timeout"],
            )

    spec = SimpleNamespace(name="orders_to_landing", selector=None, config="cfg", config_path=tmp_path / "m.yml")
    manifest = SimpleNamespace(processes=(spec,), path=tmp_path / "m.yml")

    class Loader:
        def load(self, path, *, metadata_only=True):
            del path, metadata_only
            return manifest

    result = RunManifestService(
        process_factory=Process,
        invocation_id_factory=lambda: "manual-invocation",
    ).run(
        path=tmp_path / "m.yml",
        manifest_ctx=SimpleNamespace(loader=Loader()),
    )

    assert result.passed is False
    assert result.run_id == "manual-invocation"
    assert result.result.errors == ["source timeout"]


def test_run_manifest_service_retries_failed_result_until_success(tmp_path: Path) -> None:
    attempts: list[int] = []
    sleeps: list[float] = []

    class Process:
        def __init__(self, config, config_path=None):
            del config, config_path

        def run(self, context=None, dag_id=None, execution_date=None):
            del context, dag_id, execution_date
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                return ProcessResult(
                    status="failed",
                    inserted_rows=0,
                    updated_rows=0,
                    final_rows=0,
                    extracted_rows=0,
                    duration_seconds=0.1,
                    errors=["temporary timeout"],
                )
            return ProcessResult(
                status="success",
                inserted_rows=10,
                updated_rows=0,
                final_rows=10,
                extracted_rows=10,
                duration_seconds=0.2,
                errors=[],
            )

    spec = SimpleNamespace(name="orders_to_landing", selector=None, config="cfg", config_path=tmp_path / "m.yml")
    manifest = SimpleNamespace(processes=(spec,), path=tmp_path / "m.yml")

    class Loader:
        def load(self, path, *, metadata_only=True):
            del path, metadata_only
            return manifest

    result = RunManifestService(process_factory=Process, sleep=sleeps.append).run(
        path=tmp_path / "m.yml",
        manifest_ctx=SimpleNamespace(loader=Loader()),
        retry_attempts=2,
        retry_backoff_seconds=1.5,
    )

    assert result.passed is True
    assert result.attempts == 2
    assert result.max_attempts == 3
    assert attempts == [1, 2]
    assert sleeps == [1.5]
    assert json.loads(result.to_json())["attempts"] == 2
    assert "attempts: 2" in result.to_text()


def test_run_manifest_service_marks_execution_before_process_construction(tmp_path: Path) -> None:
    spec = SimpleNamespace(name="orders_to_landing", selector=None, config="cfg", config_path=tmp_path / "m.yml")
    manifest = SimpleNamespace(processes=(spec,), path=tmp_path / "m.yml")
    phases: list[str] = []

    class Loader:
        def load(self, path, *, metadata_only=True):
            del path, metadata_only
            return manifest

    def fail_factory(**kwargs):
        del kwargs
        raise RuntimeConfigurationError("runtime bootstrap failed")

    with pytest.raises(RuntimeConfigurationError, match="runtime bootstrap failed"):
        RunManifestService(process_factory=fail_factory).run(
            path=tmp_path / "m.yml",
            manifest_ctx=SimpleNamespace(loader=Loader()),
            on_execution_started=lambda: phases.append("started"),
        )

    assert phases == ["started"]


@pytest.mark.parametrize(
    ("retry_attempts", "retry_backoff_seconds", "message"),
    (
        (-1, 0.0, "retry attempts"),
        (0, -0.1, "retry backoff seconds"),
        (0, float("nan"), "retry backoff seconds"),
        (True, 0.0, "retry attempts"),
    ),
)
def test_run_manifest_service_rejects_invalid_retry_policy_before_loading(
    tmp_path: Path,
    retry_attempts: object,
    retry_backoff_seconds: object,
    message: str,
) -> None:
    class Loader:
        def load(self, *_args, **_kwargs):
            pytest.fail("manifest must not be loaded for invalid retry policy")

    with pytest.raises(ValueError, match=message):
        RunManifestService().run(
            path=tmp_path / "missing.yml",
            manifest_ctx=SimpleNamespace(loader=Loader()),
            retry_attempts=retry_attempts,  # type: ignore[arg-type]
            retry_backoff_seconds=retry_backoff_seconds,  # type: ignore[arg-type]
        )


def test_run_manifest_result_renders_all_public_formats(tmp_path: Path) -> None:
    result = RunManifestResult(
        manifest=str(tmp_path / "manifest.yaml"),
        process="orders_to_landing",
        selector="orders",
        run_id="run_01",
        passed=True,
        result=ProcessResult(
            status="success",
            inserted_rows=10,
            updated_rows=2,
            final_rows=12,
            extracted_rows=12,
            duration_seconds=1.5,
            errors=[],
        ),
    )

    payload = result.to_dict()

    assert payload["passed"] is True
    assert payload["result"]["inserted_rows"] == 10
    assert json.loads(result.to_json())["run_id"] == "run_01"
    assert "inserted_rows: 10" in result.to_text()
    assert "| `inserted_rows` | `10` |" in result.to_markdown()


def test_dpone_run_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)

    class Service:
        def run(self, **kwargs):
            assert kwargs["path"] == tmp_path / "manifest.yaml"
            assert kwargs["selector"] == "orders"
            assert kwargs["retry_attempts"] == 2
            assert kwargs["retry_backoff_seconds"] == 1.25
            return SimpleNamespace(
                passed=True,
                to_dict=lambda: {
                    "manifest": str(tmp_path / "manifest.yaml"),
                    "process": "orders_to_landing",
                    "selector": "orders",
                    "run_id": "run_01",
                    "passed": True,
                    "result": {"status": "success", "inserted_rows": 10},
                },
                to_markdown=lambda: "# dpone run\n",
                to_text=lambda: "dpone run\n",
            )

    import dpone.commands.run_cmd as run_cmd

    monkeypatch.setattr(run_cmd, "RunManifestService", Service)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "run",
                str(tmp_path / "manifest.yaml"),
                "--selector",
                "orders",
                "--run-id",
                "run_01",
                "--retry-attempts",
                "2",
                "--retry-backoff-seconds",
                "1.25",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["process"] == "orders_to_landing"
    assert payload["passed"] is True


def test_dpone_run_cli_returns_one_for_failed_execution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)

    class Service:
        def run(self, **kwargs):
            del kwargs
            return SimpleNamespace(
                passed=False,
                to_dict=lambda: {"passed": False},
                to_markdown=lambda: "# dpone run\n",
                to_text=lambda: "dpone run\n- passed: False\n",
            )

    import dpone.commands.run_cmd as run_cmd

    monkeypatch.setattr(run_cmd, "RunManifestService", Service)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["run", str(tmp_path / "manifest.yaml")])

    assert exc.value.code == 1


def test_dpone_run_cli_missing_manifest_returns_two_without_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    missing_manifest = tmp_path / "missing.yaml"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["run", str(missing_manifest), "--format", "json"])

    assert exc.value.code == 2
    assert list(tmp_path.iterdir()) == []
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["passed"] is False
    assert payload["result"]["inserted_rows"] == 0


@pytest.mark.parametrize("output_format", ("text", "md", "json"))
def test_dpone_run_cli_returns_two_for_typed_configuration_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output_format: str,
) -> None:
    class Service:
        def run(self, **kwargs):
            del kwargs
            raise ETLConfigurationError("invalid manifest token=secret")

    monkeypatch.setattr(run_cmd, "RunManifestService", Service)
    monkeypatch.setattr(run_cmd, "build_manifest_context", lambda args, ctx: object())

    code = run_cmd.cmd_run(
        _run_args(output_format=output_format),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    output = captured.out
    assert code == 2
    assert "secret" not in output
    if output_format == "json":
        assert json.loads(output)["passed"] is False


@pytest.mark.parametrize(
    "error",
    (
        RuntimeError("runtime token=secret"),
        RuntimeConfigurationError("runtime configuration token=secret"),
    ),
)
def test_dpone_run_cli_returns_one_for_executed_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    class Service:
        def run(self, **kwargs):
            kwargs["on_execution_started"]()
            raise error

    monkeypatch.setattr(run_cmd, "RunManifestService", Service)
    monkeypatch.setattr(run_cmd, "build_manifest_context", lambda args, ctx: object())

    code = run_cmd.cmd_run(
        _run_args(output_format="json"),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    assert code == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    output = captured.out
    assert "secret" not in output
    assert error.__class__.__name__ in output
    assert json.loads(output)["passed"] is False


def test_python_api_run_uses_same_manifest_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import dpone.api as api

    calls: list[dict[str, object]] = []

    class Service:
        def run(self, **kwargs):
            calls.append(kwargs)
            return "result"

    monkeypatch.setattr(api, "RunManifestService", Service)

    result = api.run(
        tmp_path / "manifest.yaml",
        selector="orders",
        run_id="run_01",
        registry_paths=(tmp_path / "registry.yaml",),
        dag_id="dag_01",
        execution_date="2026-06-05",
        retry_attempts=3,
        retry_backoff_seconds=2.0,
    )

    assert result == "result"
    assert calls[0]["path"] == tmp_path / "manifest.yaml"
    assert calls[0]["selector"] == "orders"
    assert calls[0]["run_id"] == "run_01"
    assert calls[0]["dag_id"] == "dag_01"
    assert calls[0]["execution_date"] == "2026-06-05"
    assert calls[0]["retry_attempts"] == 3
    assert calls[0]["retry_backoff_seconds"] == 2.0
    assert calls[0]["manifest_ctx"].registry_paths == (tmp_path / "registry.yaml",)


def test_top_level_dpone_run_is_lazy_public_api() -> None:
    import dpone
    from dpone.api import run

    assert dpone.run is run


def _run_args(*, output_format: str) -> SimpleNamespace:
    return SimpleNamespace(
        path="manifest.yaml",
        selector=None,
        run_id=None,
        dag_id=None,
        execution_date=None,
        interval_start=None,
        interval_end=None,
        retry_attempts=0,
        retry_backoff_seconds=0.0,
        format=output_format,
        registry=[],
        sample=None,
        target=None,
        select=[],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=10,
    )
