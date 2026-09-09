from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands import release_summary_cmd


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


@dataclass(frozen=True)
class _Report:
    passed: bool
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return self.payload

    def to_markdown(self) -> str:
        return "# fake release verification\n"


class _Service:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] = {}

    def verify(self, **kwargs: object) -> _Report:
        self.kwargs.update(kwargs)
        return _Report(
            passed=True,
            payload={
                "schema_version": "dpone.release_verification.v1",
                "passed": True,
                "release": "v0.23.1",
                "checks": [{"name": "pypi", "passed": True}],
            },
        )


def test_ops_release_verify_cli_delegates_artifact_checks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    service = _Service()
    monkeypatch.setattr(release_summary_cmd.ReleaseVerificationService, "default", staticmethod(lambda: service))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-verify",
                "--release",
                "v0.23.1",
                "--github-repository",
                "PaulKov/dpone",
                "--runtime-image",
                "ghcr.io/paulkov/dpone-runtime",
                "--install-smoke",
                "--install-extra",
                "full",
                "--install-python",
                "3.11",
                "--timeout-seconds",
                "0",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["schema_version"] == "dpone.release_verification.v1"
    assert service.kwargs["release"] == "v0.23.1"
    assert service.kwargs["github_repository"] == "PaulKov/dpone"
    assert service.kwargs["runtime_image"] == "ghcr.io/paulkov/dpone-runtime"
    assert service.kwargs["install_smoke"] is True
    assert service.kwargs["install_extra"] == "full"
    assert service.kwargs["install_python"] == "3.11"
