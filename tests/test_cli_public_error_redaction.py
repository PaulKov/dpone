from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.cli import main as cli_main
from dpone.commands import airflow_self_service_cmd, airflow_self_service_output
from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.security_redaction import REDACTION_TOKEN, redact_public_value

_INTERNAL_CHECK_CODE = "DPONE_INTERNAL_CHECK_FAILED"
_TRACE_ID_RE = re.compile(r"\b[0-9a-f]{32}\b")


def _run_cli(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc_info:
        cli_main.main(argv)
    captured = capsys.readouterr()
    return int(exc_info.value.code or 0), captured.out, captured.err


def _configure_cli_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> logging.Logger:
    logger = logging.getLogger("dpone.tests.public_cli_error_boundary")
    logger.handlers.clear()
    logger.propagate = True
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda logger: SimpleNamespace(logger=logger)),
    )
    return logger


def test_recursive_public_redactor_preserves_shape_and_is_idempotent() -> None:
    source = {
        "password": "mapping-secret",
        "diagnostics": [
            "token=inline-secret failed at /Users/alice/private/adapter.py",
            (
                "postgresql://user:uri-secret@db.internal/orders",
                {
                    "private_key": ("-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----"),
                    "windows_path": "C:\\work\\private\\config.yaml",
                    "unc_path": "\\\\server\\share\\private\\config.yaml",
                    "file_uri": "file:///Users/alice/private/config.yaml",
                    "spaced_posix_path": "/Users/alice/My Project/private.py",
                    "labeled_posix_path": "path:/Users/alice/private.py",
                    "spaced_windows_path": "C:\\Users\\alice\\My Project\\private.py",
                    "logical_path": "pipelines/orders/pipeline.yaml",
                },
            ),
        ],
        "count": 7,
        "enabled": True,
        "optional": None,
        "connection_ref": "warehouse",
        "mount_path": "/run/secrets/dpone/airflow-connections/orders",
        "host_mount_path": "/Users/alice/private/airflow-connections/orders",
        "secret_key": "AIRFLOW_CONN_ORDERS",
        "secret_name": "dpone-airflow-connection-bridge",
        "credentials": {
            "resolver": "vault_kv",
            "fields": {"username": "user", "password": "passwd"},
        },
        "deleted_secret_refs": ["sha256:" + "a" * 64],
        "safe_presence": {"secrets": False, "planned_secrets": True},
        "unsafe_secret_fields": {
            "secrets": "plural-secret",
            "planned_secret": "planned-secret",
            "planned_secrets": ["planned-list-secret"],
            "clientSecret": "camel-case-secret",
        },
    }

    redacted = redact_public_value(source)

    assert isinstance(redacted, dict)
    assert isinstance(redacted["diagnostics"], list)
    assert isinstance(redacted["diagnostics"][1], tuple)
    assert redacted["password"] == REDACTION_TOKEN
    assert redacted["diagnostics"][0] == f"token={REDACTION_TOKEN} failed at $ABSOLUTE_PATH"
    uri, nested = redacted["diagnostics"][1]
    assert uri == f"postgresql://{REDACTION_TOKEN}@db.internal/orders"
    assert nested["private_key"] == REDACTION_TOKEN
    assert nested["windows_path"] == "$ABSOLUTE_PATH"
    assert nested["unc_path"] == "$ABSOLUTE_PATH"
    assert nested["file_uri"] == "$ABSOLUTE_PATH"
    assert nested["spaced_posix_path"] == "$ABSOLUTE_PATH"
    assert nested["labeled_posix_path"] == "path:$ABSOLUTE_PATH"
    assert nested["spaced_windows_path"] == "$ABSOLUTE_PATH"
    assert nested["logical_path"] == "pipelines/orders/pipeline.yaml"
    assert redacted["count"] == 7
    assert redacted["enabled"] is True
    assert redacted["optional"] is None
    assert redacted["connection_ref"] == "warehouse"
    assert redacted["mount_path"] == "/run/secrets/dpone/airflow-connections/orders"
    assert redacted["host_mount_path"] == "$ABSOLUTE_PATH"
    assert redacted["secret_key"] == "AIRFLOW_CONN_ORDERS"
    assert redacted["secret_name"] == "dpone-airflow-connection-bridge"
    assert redacted["credentials"]["fields"] == {
        "username": "user",
        "password": "passwd",
    }
    assert redacted["deleted_secret_refs"] == ["sha256:" + "a" * 64]
    assert redacted["safe_presence"] == {"secrets": False, "planned_secrets": True}
    assert redacted["unsafe_secret_fields"] == {
        "secrets": REDACTION_TOKEN,
        "planned_secret": REDACTION_TOKEN,
        "planned_secrets": REDACTION_TOKEN,
        "clientSecret": REDACTION_TOKEN,
    }
    assert redact_public_value(redacted) == redacted


def test_recursive_public_redactor_sanitizes_mapping_keys_without_collisions() -> None:
    source = {
        1: "numeric-key",
        "1": "string-key",
        "/Users/alice/private/key.pem": "first-path-key",
        "/opt/dpone/private/key.pem": "second-path-key",
        "$ABSOLUTE_PATH": "literal-public-key",
        "postgresql://user:key-secret@db.internal/orders": "uri-key",
    }

    redacted = redact_public_value(source)

    assert len(redacted) == len(source)
    assert redacted[1] == "numeric-key"
    assert redacted["1"] == "string-key"
    assert redacted["$ABSOLUTE_PATH"] == "first-path-key"
    assert redacted["$ABSOLUTE_PATH~2"] == "second-path-key"
    assert redacted["$ABSOLUTE_PATH~3"] == "literal-public-key"
    assert redacted[f"postgresql://{REDACTION_TOKEN}@db.internal/orders"] == "uri-key"
    rendered = repr(redacted)
    assert "/Users/alice" not in rendered
    assert "key-secret" not in rendered
    assert redact_public_value(redacted) == redacted


def test_recursive_public_redactor_keeps_uri_key_collisions_idempotent() -> None:
    source = {
        "postgresql://user:first@db.internal/orders": "first",
        "postgresql://user:second@db.internal/orders": "second",
    }

    redacted = redact_public_value(source)

    assert redacted == {
        f"postgresql://{REDACTION_TOKEN}@db.internal/orders": "first",
        f"postgresql://{REDACTION_TOKEN}@db.internal/orders~2": "second",
    }
    assert redact_public_value(redacted) == redacted


@pytest.mark.parametrize(
    "key",
    [
        "passwords",
        "apiKeys",
        "accessKeys",
        "privateKeys",
        "authTokens",
        "vaultTokens",
        "clientSecrets",
        "connectionStrings",
        "config[password]",
        "vault/password",
        "config:apiKey",
    ],
)
def test_recursive_public_redactor_hides_plural_and_qualified_sensitive_keys(key: str) -> None:
    assert redact_public_value({key: "qualified-secret"}) == {key: REDACTION_TOKEN}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('{"apiKeys":"serialized-secret"}', f'{{"apiKeys":"{REDACTION_TOKEN}"}}'),
        (
            '{"config[password]":"serialized-secret"}',
            f'{{"config[password]":"{REDACTION_TOKEN}"}}',
        ),
        (
            "https://api.example.test/run?clientSecrets=query-secret&view=compact",
            f"https://api.example.test/run?clientSecrets={REDACTION_TOKEN}&view=compact",
        ),
    ],
)
def test_recursive_public_redactor_hides_sensitive_keys_inside_strings(
    value: str,
    expected: str,
) -> None:
    assert redact_public_value(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Authorization: Basic basic-secret", f"Authorization: {REDACTION_TOKEN}"),
        ("Authorization: ApiKey api-secret", f"Authorization: {REDACTION_TOKEN}"),
        ("refreshToken=refresh-secret", f"refreshToken={REDACTION_TOKEN}"),
        (
            "https://api.example.test/run?accessToken=access-secret&view=compact",
            f"https://api.example.test/run?accessToken={REDACTION_TOKEN}&view=compact",
        ),
        ('password="true"', f'password="{REDACTION_TOKEN}"'),
        ('{"password":"prefix\\"TAIL"}', f'{{"password":"{REDACTION_TOKEN}"}}'),
        ("password: correct horse battery staple", f"password: {REDACTION_TOKEN}"),
    ],
)
def test_recursive_public_redactor_hides_complete_credential_values(
    value: str,
    expected: str,
) -> None:
    redacted = redact_public_value(value)

    assert redacted == expected
    assert redact_public_value(redacted) == redacted


@pytest.mark.parametrize(
    "value",
    [
        "-----BEGIN PRIVATE KEY-----\ntruncated-private-material",
        ("-----BEGIN PGP PRIVATE KEY BLOCK-----\npgp-private-material\n-----END PGP PRIVATE KEY BLOCK-----"),
    ],
)
def test_recursive_public_redactor_hides_complete_or_truncated_private_keys(value: str) -> None:
    assert redact_public_value(value) == REDACTION_TOKEN


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "https://docs.example.test/run?local=/Users/alice/My Project/private.py&view=compact",
            "https://docs.example.test/run?local=$ABSOLUTE_PATH&view=compact",
        ),
        (
            "uri=https://docs.example.test/run;path=/Users/alice/private.py",
            "uri=https://docs.example.test/run;path=$ABSOLUTE_PATH",
        ),
        (
            "https://docs.example.test/run?local=C:\\Users\\alice\\My Project\\private.py&view=compact",
            "https://docs.example.test/run?local=$ABSOLUTE_PATH&view=compact",
        ),
    ],
)
def test_recursive_public_redactor_hides_local_paths_inside_remote_uri_values(
    value: str,
    expected: str,
) -> None:
    assert redact_public_value(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "remote=https://docs.example.test/run,local=/Users/alice/private.py",
            "remote=https://docs.example.test/run,local=$ABSOLUTE_PATH",
        ),
        (
            "remote=https://docs.example.test/run|local=C:\\Users\\alice\\private.py",
            "remote=https://docs.example.test/run|local=$ABSOLUTE_PATH",
        ),
        (
            "failed at /Users/alice/private.py after validation",
            "failed at $ABSOLUTE_PATH after validation",
        ),
    ],
)
def test_recursive_public_redactor_preserves_safe_text_around_labeled_paths(
    value: str,
    expected: str,
) -> None:
    assert redact_public_value(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (r"path:\\server\share\private.py", "path:$ABSOLUTE_PATH"),
        (r"failed at \Users\alice\private.py", "failed at $ABSOLUTE_PATH"),
        ("sqlite+pysqlite:////Users/alice/private.db", "$ABSOLUTE_PATH"),
        ("vscode://file/Users/alice/private.py:42", "$ABSOLUTE_PATH"),
        ("zip:///Users/alice/private.zip", "$ABSOLUTE_PATH"),
        (
            "https://docs.example.test/run?local=%2FUsers%2Falice%2Fprivate.py",
            ("https://docs.example.test/run?local=$ABSOLUTE_PATH"),
        ),
    ],
)
def test_recursive_public_redactor_hides_wrapped_and_windows_root_paths(
    value: str,
    expected: str,
) -> None:
    redacted = redact_public_value(value)

    assert redacted == expected
    assert redact_public_value(redacted) == redacted


def test_self_service_renderer_never_reads_unredacted_result_after_mapping(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class OneShotResult:
        calls = 0

        @property
        def passed(self) -> bool:
            raise AssertionError("renderer must not read the original result")

        def to_dict(self) -> dict[str, object]:
            self.calls += 1
            return {
                "passed": False,
                "changes": [{"action": "inspect", "path": "/Users/alice/private/pipeline.yaml"}],
                "errors": [
                    {
                        "code": "DPONE_TEST_FAILURE",
                        "message": "password=output-secret at C:\\work\\private\\runtime.py",
                    }
                ],
            }

    result = OneShotResult()

    emit_self_service_result(result, "text", command="test")  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert result.calls == 1
    assert "output-secret" not in output
    assert "/Users/alice" not in output
    assert "C:\\work" not in output
    assert REDACTION_TOKEN in output
    assert "$ABSOLUTE_PATH" in output


@pytest.mark.parametrize(
    "failure_at",
    ["construction", "execution", "serialization", "redaction", "redaction_contract", "rendering"],
)
@pytest.mark.parametrize("output_format", ["text", "json"])
def test_unexpected_check_failure_has_one_safe_public_and_logging_contract(
    failure_at: str,
    output_format: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.chdir(tmp_path)
    logger = _configure_cli_logger(monkeypatch)
    secret = "unexpected-check-secret"
    private_path = tmp_path / "private" / "check.py"
    failure = RuntimeError(
        f"password={secret} at {private_path} via "
        "postgresql://user:uri-secret@db.internal/orders\n"
        "-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----"
    )

    class BrokenCheckService:
        def __init__(self) -> None:
            if failure_at == "construction":
                raise failure

        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            if failure_at == "serialization":
                return BrokenResult()  # type: ignore[return-value]
            if failure_at == "execution":
                raise failure
            return SelfServiceResult(passed=True)

    class BrokenResult:
        passed = False
        exit_code = None

        @staticmethod
        def to_dict() -> dict[str, object]:
            raise failure

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", BrokenCheckService)
    if failure_at == "redaction":
        monkeypatch.setattr(
            airflow_self_service_output,
            "redact_public_value",
            lambda _value: (_ for _ in ()).throw(failure),
        )
    if failure_at == "redaction_contract":
        monkeypatch.setattr(airflow_self_service_output, "redact_public_value", lambda _value: None)
    if failure_at == "rendering":
        monkeypatch.setattr(
            airflow_self_service_cmd,
            "_emit",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
        )

    with caplog.at_level(logging.ERROR, logger=logger.name):
        exit_code, stdout, stderr = _run_cli(
            ["check", "pipeline.yaml", "--format", output_format],
            capsys,
        )

    logger_messages = [record.getMessage() for record in caplog.records if record.name == logger.name]
    assert exit_code == 5
    assert stderr == ""
    assert len(logger_messages) == 1
    assert _INTERNAL_CHECK_CODE in stdout
    assert "Check failed unexpectedly. Use the trace id with platform support." in stdout
    assert "Traceback (most recent call last)" not in stdout
    assert "RuntimeError" not in stdout

    if output_format == "json":
        payload = json.loads(stdout)
        assert payload["passed"] is False
        assert payload["exit_code"] == 5
        assert len(payload["errors"]) == 1
        error = payload["errors"][0]
        assert error["schema"] == "dpone.error.v1"
        assert error["code"] == _INTERNAL_CHECK_CODE
        assert error["stage"] == "check"
        assert error["severity"] == "error"
        trace_id = error["trace_id"]
    else:
        trace_ids = _TRACE_ID_RE.findall(stdout)
        assert len(trace_ids) == 1
        trace_id = trace_ids[0]
        assert "correct the target or initialize it" not in stdout

    assert logger_messages == [f"{_INTERNAL_CHECK_CODE} trace_id={trace_id}"]
    public_channels = "\n".join([stdout, stderr, *logger_messages])
    for forbidden in (
        secret,
        "uri-secret",
        "private-material",
        private_path.as_posix(),
        tmp_path.as_posix(),
        "-----BEGIN PRIVATE KEY-----",
    ):
        assert forbidden not in public_channels
    assert tuple(tmp_path.iterdir()) == ()


def test_unexpected_check_failure_survives_trace_id_generation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _configure_cli_logger(monkeypatch)

    class BrokenCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            raise RuntimeError("check failed")

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", BrokenCheckService)
    monkeypatch.setattr(
        airflow_self_service_cmd,
        "uuid4",
        lambda: (_ for _ in ()).throw(RuntimeError("uuid-secret at /Users/alice/private/uuid.py")),
    )

    exit_code, stdout, stderr = _run_cli(
        ["check", "pipeline.yaml", "--format", "json"],
        capsys,
    )

    payload = json.loads(stdout)
    assert exit_code == 5
    assert stderr == ""
    assert _TRACE_ID_RE.fullmatch(payload["errors"][0]["trace_id"])
    assert "uuid-secret" not in stdout
    assert "/Users/alice" not in stdout


def test_unexpected_check_failure_survives_secondary_logger_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    logger = _configure_cli_logger(monkeypatch)
    secret = "secondary-logger-secret"

    class BrokenCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            raise RuntimeError("check failed")

    def broken_log(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError(f"password={secret} at {tmp_path / 'private' / 'logger.py'}")

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", BrokenCheckService)
    monkeypatch.setattr(logger, "error", broken_log)

    exit_code, stdout, stderr = _run_cli(
        ["check", "pipeline.yaml", "--format", "json"],
        capsys,
    )

    payload = json.loads(stdout)
    assert exit_code == 5
    assert stderr == ""
    assert payload["exit_code"] == 5
    assert payload["errors"][0]["code"] == _INTERNAL_CHECK_CODE
    assert _TRACE_ID_RE.fullmatch(payload["errors"][0]["trace_id"])
    assert secret not in stdout
    assert tmp_path.as_posix() not in stdout
    assert tuple(tmp_path.iterdir()) == ()


def test_check_json_recovery_is_atomic_on_non_seekable_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NonSeekableOutput:
        def __init__(self) -> None:
            self.value = ""

        def write(self, value: str) -> int:
            self.value += value
            return len(value)

        def seek(self, *_args: object) -> None:
            raise OSError("stream is not seekable")

        def truncate(self, *_args: object) -> None:
            raise OSError("stream is not seekable")

    class PassingCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            return SelfServiceResult(passed=True)

    def broken_write_json(_payload: object) -> None:
        sys.stdout.write('{"partial":"safe"}')
        raise RuntimeError("password=writer-secret at /Users/alice/private/writer.py")

    output = NonSeekableOutput()
    logger = logging.Logger("dpone.tests.atomic-public-check")
    logger.addHandler(logging.NullHandler())
    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", PassingCheckService)
    monkeypatch.setattr(airflow_self_service_output, "write_json", broken_write_json)
    monkeypatch.setattr(sys, "stdout", output)

    exit_code = airflow_self_service_cmd.cmd_check(
        SimpleNamespace(
            target="pipeline.yaml",
            live=False,
            connections=False,
            format="json",
            select=(),
            exclude=(),
            state=None,
            selectors=None,
            max_selected=1000,
            environment="dev",
        ),
        ctx=object(),
        logger=logger,
    )

    payload = json.loads(output.value)
    assert exit_code == 5
    assert payload["errors"][0]["code"] == _INTERNAL_CHECK_CODE
    assert "partial" not in output.value
    assert "writer-secret" not in output.value
    assert "/Users/alice" not in output.value


@pytest.mark.parametrize("partial_write", [False, True])
def test_check_json_writer_failure_uses_independent_emergency_document(
    partial_write: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.chdir(tmp_path)
    logger = _configure_cli_logger(monkeypatch)
    secret = "json-writer-secret"
    failure = RuntimeError(f"password={secret} at {tmp_path / 'private' / 'writer.py'}")

    class PassingCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            return SelfServiceResult(passed=True)

    def broken_write_json(_payload: object) -> None:
        if partial_write:
            sys.stdout.write('{"partial":"safe"}')
        raise failure

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", PassingCheckService)
    monkeypatch.setattr(airflow_self_service_output, "write_json", broken_write_json)

    with caplog.at_level(logging.ERROR, logger=logger.name):
        exit_code, stdout, stderr = _run_cli(
            ["check", "pipeline.yaml", "--format", "json"],
            capsys,
        )

    payload = json.loads(stdout)
    trace_id = payload["errors"][0]["trace_id"]
    logger_messages = [record.getMessage() for record in caplog.records if record.name == logger.name]
    assert exit_code == 5
    assert stderr == ""
    assert payload["exit_code"] == 5
    assert payload["errors"][0]["code"] == _INTERNAL_CHECK_CODE
    assert logger_messages == [f"{_INTERNAL_CHECK_CODE} trace_id={trace_id}"]
    assert secret not in stdout
    assert tmp_path.as_posix() not in stdout


def test_check_boundary_does_not_raise_when_static_emergency_output_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            raise RuntimeError("primary check failure")

    failure = RuntimeError("password=fallback-secret at /Users/alice/private/fallback.py")
    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", BrokenCheckService)
    monkeypatch.setattr(
        airflow_self_service_cmd,
        "_emit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(
        airflow_self_service_cmd,
        "_emit_internal_check_failure",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
    )
    logger = logging.Logger("dpone.tests.broken-emergency-check")
    logger.addHandler(logging.NullHandler())

    exit_code = airflow_self_service_cmd.cmd_check(
        SimpleNamespace(
            target="pipeline.yaml",
            live=False,
            connections=False,
            format="json",
            select=(),
            exclude=(),
            state=None,
            selectors=None,
            max_selected=1000,
            environment="dev",
        ),
        ctx=object(),
        logger=logger,
    )

    assert exit_code == 5


def test_static_emergency_output_does_not_truncate_caller_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sys.stdout.write("caller-owned-output\n")

    airflow_self_service_output.emit_internal_check_failure("text", trace_id="a" * 32)

    stdout = capsys.readouterr().out
    assert stdout.startswith("caller-owned-output\n")
    assert _INTERNAL_CHECK_CODE in stdout


@pytest.mark.parametrize("expected_exit_code", [1, 3, 4])
def test_check_preserves_expected_service_exit_codes(
    expected_exit_code: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_cli_logger(monkeypatch)
    expected_result = SelfServiceResult(
        passed=False,
        errors=(
            {
                "schema": "dpone.error.v1",
                "code": "DPONE_EXPECTED_CHECK_FAILURE",
                "stage": "check",
                "severity": "error",
                "message": "Expected check failure.",
                "fixes": [],
            },
        ),
        exit_code=expected_exit_code,
    )

    class ExpectedCheckService:
        def check(self, *_args: object, **_kwargs: object) -> SelfServiceResult:
            return expected_result

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", ExpectedCheckService)

    exit_code, stdout, stderr = _run_cli(
        ["check", "pipeline.yaml", "--format", "json"],
        capsys,
    )

    assert exit_code == expected_exit_code
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_EXPECTED_CHECK_FAILURE"
    assert stderr == ""


def test_check_rejects_markdown_before_service_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    class ForbiddenService:
        def __init__(self) -> None:
            raise AssertionError("service must not be constructed")

    monkeypatch.setattr(airflow_self_service_cmd, "build_airflow_self_service_service", ForbiddenService)

    exit_code, stdout, stderr = _run_cli(
        ["check", "pipeline.yaml", "--format", "md"],
        capsys,
    )

    assert exit_code == 2
    assert stdout == ""
    assert "invalid choice" in stderr
    assert tuple(tmp_path.iterdir()) == ()


class _RecordingETLLogger:
    def __init__(self) -> None:
        self.starts: list[dict[str, Any]] = []
        self.errors: list[tuple[str, dict[str, str]]] = []
        self.ends: list[dict[str, Any]] = []
        self.warnings: list[tuple[str, tuple[object, ...]]] = []

    def log_etl_start(self, payload: dict[str, Any]) -> None:
        self.starts.append(payload)

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        del event, payload

    def log_etl_error(self, message: str, payload: dict[str, str]) -> None:
        self.errors.append((message, payload))

    def log_etl_end(self, payload: dict[str, Any]) -> None:
        self.ends.append(payload)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        del message, args, kwargs

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        del kwargs
        self.warnings.append((message, args))


class _FailingSource:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    def extract(self, load_config: LoadConfig, state: object) -> object:
        del load_config, state
        raise self.failure


class _UnprintableFailure(RuntimeError):
    def __str__(self) -> str:
        raise RuntimeError("stringification-secret at /Users/alice/private/stringify.py")


class _HostileETLLogger(_RecordingETLLogger):
    def log_etl_error(self, message: str, payload: dict[str, str]) -> None:
        del message, payload
        raise RuntimeError("error-logger-secret at /Users/alice/private/error_logger.py")

    def log_etl_end(self, payload: dict[str, Any]) -> None:
        del payload
        raise RuntimeError("end-logger-secret at /Users/alice/private/end_logger.py")

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        del message, args, kwargs
        raise RuntimeError("warning-secret at /Users/alice/private/warning.py")


class _RecordingAuditStorage:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.failed: list[Any] = []

    def record_load_started(self, _record: object) -> None:
        return

    def record_load_staged(self, _record: object) -> None:
        return

    def record_load_committed(self, _record: object) -> None:
        return

    def record_load_failed(self, record: object) -> None:
        self.failed.append(record)
        raise self.failure


class _RecordingRunStateStorage:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.failed: list[Any] = []

    def save_run_state(self, _state: object) -> None:
        return

    def update_run_state(self, state: object) -> None:
        self.failed.append(state)
        raise self.failure


def test_etl_processor_reraises_original_with_only_redacted_bounded_logging() -> None:
    exception_secret = "processor-exception-secret"
    config_secret = "load-config-secret"
    private_path = "/Users/alice/private/source.py"
    failure = RuntimeError(
        f"password={exception_secret} failed at {private_path} via postgresql://user:uri-secret@db.internal/orders"
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False, "password": config_secret},
    )
    logger = _RecordingETLLogger()
    persistence_secret = "persistence-secondary-secret"
    persistence_failure = RuntimeError(f"password={persistence_secret} at /Users/alice/private/audit.py")
    audit_storage = _RecordingAuditStorage(persistence_failure)
    run_state_storage = _RecordingRunStateStorage(persistence_failure)
    processor = ETLProcessor(
        _FailingSource(failure),
        object(),
        etl_logger=logger,  # type: ignore[arg-type]
        load_identity_service=LoadIdentityService(audit_storage=audit_storage),
        run_state_storage=run_state_storage,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError) as exc_info:
        processor.run(
            config,
            dag_id="orders-daily",
            execution_date=datetime(2026, 7, 19, tzinfo=UTC),
        )

    assert exc_info.value is failure
    assert len(logger.errors) == 1
    safe_error, context = logger.errors[0]
    assert safe_error == (
        f"password={REDACTION_TOKEN} failed at $ABSOLUTE_PATH via postgresql://{REDACTION_TOKEN}@db.internal/orders"
    )
    assert set(context) == {"process_name", "run_id"}
    assert context["process_name"] == "orders-daily"
    assert context["run_id"]
    assert len(logger.ends) == 1
    assert logger.ends[0]["errors"] == [safe_error]
    assert logger.ends[0]["run_id"] == context["run_id"]
    assert [record.error_message for record in audit_storage.failed] == [safe_error]
    assert [state.error_message for state in run_state_storage.failed] == [safe_error]
    assert len(logger.warnings) == 2

    public_logging = repr([logger.starts, logger.errors, logger.ends, logger.warnings])
    for forbidden in (
        exception_secret,
        config_secret,
        persistence_secret,
        "uri-secret",
        private_path,
        str(config),
    ):
        assert forbidden not in public_logging


def test_etl_processor_preserves_unprintable_primary_when_all_secondary_callbacks_fail() -> None:
    failure = _UnprintableFailure()
    persistence_failure = RuntimeError("password=persistence-secret at /Users/alice/private/persistence.py")
    audit_storage = _RecordingAuditStorage(persistence_failure)
    run_state_storage = _RecordingRunStateStorage(persistence_failure)
    processor = ETLProcessor(
        _FailingSource(failure),
        object(),
        etl_logger=_HostileETLLogger(),  # type: ignore[arg-type]
        load_identity_service=LoadIdentityService(audit_storage=audit_storage),
        run_state_storage=run_state_storage,  # type: ignore[arg-type]
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False},
    )

    with pytest.raises(_UnprintableFailure) as exc_info:
        processor.run(
            config,
            dag_id="orders-daily",
            execution_date=datetime(2026, 7, 19, tzinfo=UTC),
        )

    assert exc_info.value is failure
    expected = "Runtime failed; error details unavailable."
    assert [record.error_message for record in audit_storage.failed] == [expected]
    assert [state.error_message for state in run_state_storage.failed] == [expected]


def test_etl_processor_removes_embedded_traceback_frames_from_public_logging() -> None:
    secret = "traceback-message-secret"
    failure = RuntimeError(
        "Traceback (most recent call last):\n"
        '  File "/Users/alice/private/source.py", line 7, in extract\n'
        "    raise RuntimeError(...)\n"
        f"RuntimeError: password={secret}"
    )
    logger = _RecordingETLLogger()
    processor = ETLProcessor(
        _FailingSource(failure),
        object(),
        etl_logger=logger,  # type: ignore[arg-type]
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False},
    )

    with pytest.raises(RuntimeError) as exc_info:
        processor.run(config, dag_id="orders-daily")

    assert exc_info.value is failure
    assert logger.errors == [
        (
            f"RuntimeError: password={REDACTION_TOKEN}",
            {"process_name": "orders-daily", "run_id": logger.ends[0]["run_id"]},
        )
    ]
    public_logging = repr([logger.errors, logger.ends])
    assert "Traceback (most recent call last)" not in public_logging
    assert 'File "' not in public_logging
    assert "line 7" not in public_logging
    assert secret not in public_logging
    assert "/Users/alice" not in public_logging
