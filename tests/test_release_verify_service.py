from __future__ import annotations

from collections.abc import Mapping, Sequence

from dpone.ops.release_verify import CommandResult, HttpResponse, ReleaseVerificationService


def test_release_verify_service_checks_publish_surfaces_without_secrets() -> None:
    urls: list[str] = []
    commands: list[tuple[str, ...]] = []

    def http_get(url: str, headers: Mapping[str, str], timeout_seconds: int) -> HttpResponse:
        assert headers["Cache-Control"] == "no-cache"
        assert timeout_seconds == 60
        urls.append(url)
        if "/simple/dpone/" in url:
            return HttpResponse(status=200, body="dpone-0.23.1-py3-none-any.whl")
        return HttpResponse(status=200, body="{}")

    def command_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
        assert timeout_seconds == 9
        commands.append(tuple(command))
        return CommandResult(returncode=0, stdout="dpone 0.23.1\n", stderr="")

    service = ReleaseVerificationService(http_get=http_get, command_runner=command_runner)
    report = service.verify(
        release="0.23.1",
        install_smoke=True,
        timeout_seconds=0,
        command_timeout_seconds=9,
    )

    assert report.passed is True
    assert report.release == "v0.23.1"
    assert "https://api.github.com/repos/PaulKov/dpone/releases/tags/v0.23.1" in urls
    assert "https://pypi.org/pypi/dpone/0.23.1/json" in urls
    assert "https://pypi.org/simple/dpone/" in urls
    assert ("uvx", "--from", "dpone==0.23.1", "dpone", "--version") in commands
    assert ("docker", "manifest", "inspect", "ghcr.io/paulkov/dpone-runtime:0.23.1") in commands
    assert report.to_dict()["blockers"] == []


def test_release_verify_service_can_pin_python_and_extra_for_install_smoke() -> None:
    commands: list[tuple[str, ...]] = []

    def http_get(url: str, headers: Mapping[str, str], timeout_seconds: int) -> HttpResponse:
        del headers, timeout_seconds
        if "/simple/dpone/" in url:
            return HttpResponse(status=200, body="dpone-0.23.1-py3-none-any.whl")
        return HttpResponse(status=200, body="{}")

    def command_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
        del timeout_seconds
        commands.append(tuple(command))
        return CommandResult(returncode=0, stdout="dpone 0.23.1\n", stderr="")

    report = ReleaseVerificationService(http_get=http_get, command_runner=command_runner).verify(
        release="v0.23.1",
        install_smoke=True,
        install_extra="full",
        install_python="3.11",
        timeout_seconds=0,
    )

    assert report.passed is True
    assert (
        "uvx",
        "--python",
        "3.11",
        "--from",
        "dpone[full]==0.23.1",
        "dpone",
        "--version",
    ) in commands
    install_check = next(check for check in report.checks if check.name == "pypi_install_smoke")
    assert install_check.details["python"] == "3.11"


def test_release_verify_service_reports_pypi_resolver_blocker() -> None:
    def http_get(url: str, headers: Mapping[str, str], timeout_seconds: int) -> HttpResponse:
        del headers, timeout_seconds
        if "/simple/dpone/" in url:
            return HttpResponse(status=200, body="dpone-0.23.0-py3-none-any.whl")
        return HttpResponse(status=200, body="{}")

    service = ReleaseVerificationService(
        http_get=http_get,
        command_runner=lambda command, timeout_seconds: CommandResult(returncode=0, stdout="", stderr=""),
    )
    report = service.verify(
        release="v0.23.1",
        check_github_release=False,
        check_runtime_image=False,
        timeout_seconds=0,
    )

    assert report.passed is False
    assert report.to_dict()["blockers"] == ["pypi_resolver_not_visible"]


def test_release_verify_service_blocks_empty_check_selection() -> None:
    service = ReleaseVerificationService(
        http_get=lambda url, headers, timeout_seconds: HttpResponse(status=200, body="{}"),
        command_runner=lambda command, timeout_seconds: CommandResult(returncode=0, stdout="", stderr=""),
    )

    report = service.verify(
        release="v0.23.1",
        check_github_release=False,
        check_pypi=False,
        check_runtime_image=False,
        install_smoke=False,
        timeout_seconds=0,
    )

    assert report.passed is False
    assert report.to_dict()["blockers"] == ["release_verify_no_checks_selected"]
