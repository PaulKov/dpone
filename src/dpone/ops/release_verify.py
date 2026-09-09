"""Release artifact verification service for post-tag checks."""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: str


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class ReleaseCheck:
    name: str
    passed: bool
    code: str
    summary: str
    details: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "passed": self.passed,
            "code": self.code,
            "summary": self.summary,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class ReleaseVerificationReport:
    release: str
    version: str
    checks: tuple[ReleaseCheck, ...]
    attempts: int

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "dpone.release_verification.v1",
            "passed": self.passed,
            "release": self.release,
            "version": self.version,
            "attempts": self.attempts,
            "checks": [check.to_dict() for check in self.checks],
            "blockers": [check.code for check in self.checks if not check.passed],
        }

    def to_markdown(self) -> str:
        lines = [
            "# dpone release verification",
            "",
            f"- Release: `{self.release}`",
            f"- Version: `{self.version}`",
            f"- Passed: `{self.passed}`",
            f"- Attempts: `{self.attempts}`",
            "",
            "| check | passed | code | summary |",
            "|---|---:|---|---|",
        ]
        for check in self.checks:
            lines.append(f"| `{check.name}` | `{check.passed}` | `{check.code}` | {check.summary} |")
        return "\n".join(lines) + "\n"


HttpGetter = Callable[[str, Mapping[str, str], int], HttpResponse]
CommandRunner = Callable[[Sequence[str], int], CommandResult]


class ReleaseVerificationService:
    """Verify externally published release artifacts with injectable IO ports."""

    def __init__(
        self,
        *,
        http_get: HttpGetter | None = None,
        command_runner: CommandRunner | None = None,
        sleeper: Callable[[float], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._http_get = http_get or _http_get
        self._command_runner = command_runner or _run_command
        self._sleep = sleeper or time.sleep
        self._clock = clock or time.monotonic

    @classmethod
    def default(cls) -> ReleaseVerificationService:
        return cls()

    def verify(
        self,
        *,
        release: str,
        package: str = "dpone",
        github_repository: str = "PaulKov/dpone",
        runtime_image: str = "ghcr.io/paulkov/dpone-runtime",
        check_github_release: bool = True,
        check_pypi: bool = True,
        check_runtime_image: bool = True,
        install_smoke: bool = False,
        install_extra: str | None = None,
        install_python: str | None = None,
        timeout_seconds: int = 0,
        poll_interval_seconds: int = 30,
        command_timeout_seconds: int = 300,
    ) -> ReleaseVerificationReport:
        tag, version = _normalize_release(release)
        deadline = self._clock() + max(timeout_seconds, 0)
        attempts = 0
        while True:
            attempts += 1
            report = self._verify_once(
                tag=tag,
                version=version,
                package=package,
                github_repository=github_repository,
                runtime_image=runtime_image,
                check_github_release=check_github_release,
                check_pypi=check_pypi,
                check_runtime_image=check_runtime_image,
                install_smoke=install_smoke,
                install_extra=install_extra,
                install_python=install_python,
                command_timeout_seconds=command_timeout_seconds,
                attempts=attempts,
            )
            if report.passed or self._clock() >= deadline:
                return report
            self._sleep(max(min(poll_interval_seconds, deadline - self._clock()), 0))

    def _verify_once(self, **kwargs: object) -> ReleaseVerificationReport:
        tag = str(kwargs["tag"])
        version = str(kwargs["version"])
        checks: list[ReleaseCheck] = []
        if bool(kwargs["check_github_release"]):
            checks.append(self._check_github_release(str(kwargs["github_repository"]), tag))
        if bool(kwargs["check_pypi"]):
            checks.append(self._check_pypi(str(kwargs["package"]), version))
        if bool(kwargs["install_smoke"]):
            checks.append(
                self._check_install_smoke(
                    package=str(kwargs["package"]),
                    version=version,
                    extra=kwargs["install_extra"],
                    python=kwargs["install_python"],
                    timeout_seconds=int(kwargs["command_timeout_seconds"]),
                )
            )
        if bool(kwargs["check_runtime_image"]):
            checks.append(
                self._check_runtime_image(
                    runtime_image=str(kwargs["runtime_image"]),
                    version=version,
                    timeout_seconds=int(kwargs["command_timeout_seconds"]),
                )
            )
        if not checks:
            checks.append(
                ReleaseCheck(
                    name="release_verify",
                    passed=False,
                    code="release_verify_no_checks_selected",
                    summary="No release verification checks were selected",
                    details={},
                )
            )
        return ReleaseVerificationReport(
            release=tag, version=version, checks=tuple(checks), attempts=int(kwargs["attempts"])
        )

    def _check_github_release(self, repository: str, tag: str) -> ReleaseCheck:
        url = f"https://api.github.com/repos/{repository}/releases/tags/{tag}"
        response = self._safe_http_get(url)
        passed = response.status == 200
        return ReleaseCheck(
            name="github_release",
            passed=passed,
            code="github_release_found" if passed else "github_release_missing",
            summary=f"GitHub release `{tag}` {'exists' if passed else 'is not visible'}",
            details={"repository": repository, "status": response.status},
        )

    def _check_pypi(self, package: str, version: str) -> ReleaseCheck:
        json_response = self._safe_http_get(f"https://pypi.org/pypi/{package}/{version}/json")
        simple_response = self._safe_http_get(f"https://pypi.org/simple/{package}/")
        version_visible = version in simple_response.body
        passed = json_response.status == 200 and simple_response.status == 200 and version_visible
        return ReleaseCheck(
            name="pypi_resolver",
            passed=passed,
            code="pypi_resolver_visible" if passed else "pypi_resolver_not_visible",
            summary=f"PyPI package `{package}=={version}` {'is' if passed else 'is not'} resolver-visible",
            details={
                "package": package,
                "json_status": json_response.status,
                "simple_status": simple_response.status,
                "version_in_simple_index": version_visible,
            },
        )

    def _check_install_smoke(
        self,
        *,
        package: str,
        version: str,
        extra: object,
        python: object,
        timeout_seconds: int,
    ) -> ReleaseCheck:
        spec = f"{package}[{extra}]=={version}" if isinstance(extra, str) and extra else f"{package}=={version}"
        command = _install_smoke_command(spec=spec, python=python)
        result = self._command_runner(command, timeout_seconds)
        passed = result.returncode == 0 and version in result.stdout + result.stderr
        return ReleaseCheck(
            name="pypi_install_smoke",
            passed=passed,
            code="pypi_install_smoke_passed" if passed else "pypi_install_smoke_failed",
            summary=f"`{' '.join(command)}` {'passed' if passed else 'failed'}",
            details={
                "package_spec": spec,
                "python": python if isinstance(python, str) and python else None,
                "returncode": result.returncode,
            },
        )

    def _check_runtime_image(self, *, runtime_image: str, version: str, timeout_seconds: int) -> ReleaseCheck:
        image_ref = f"{runtime_image}:{version}"
        result = self._command_runner(("docker", "manifest", "inspect", image_ref), timeout_seconds)
        passed = result.returncode == 0
        return ReleaseCheck(
            name="runtime_image",
            passed=passed,
            code="runtime_image_manifest_found" if passed else "runtime_image_manifest_missing",
            summary=f"Runtime image `{image_ref}` {'is' if passed else 'is not'} visible",
            details={"image": image_ref, "returncode": result.returncode},
        )

    def _safe_http_get(self, url: str) -> HttpResponse:
        try:
            return self._http_get(url, _NO_CACHE_HEADERS, 60)
        except Exception as exc:  # pragma: no cover - defensive boundary around external IO
            return HttpResponse(status=0, body=json.dumps({"error": type(exc).__name__}))


_NO_CACHE_HEADERS = {
    "Accept": "application/json, text/html;q=0.9, */*;q=0.1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "User-Agent": "dpone-release-verify/1",
}


def _normalize_release(release: str) -> tuple[str, str]:
    value = release.strip()
    if value.startswith("v"):
        return value, value[1:]
    return f"v{value}", value


def _install_smoke_command(*, spec: str, python: object) -> tuple[str, ...]:
    if isinstance(python, str) and python:
        return ("uvx", "--python", python, "--from", spec, "dpone", "--version")
    return ("uvx", "--from", spec, "dpone", "--version")


def _http_get(url: str, headers: Mapping[str, str], timeout_seconds: int) -> HttpResponse:
    request = Request(url, headers=dict(headers))
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            return HttpResponse(status=int(response.status), body=response.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        return HttpResponse(status=int(exc.code), body=exc.read().decode("utf-8", errors="replace"))
    except URLError as exc:
        return HttpResponse(status=0, body=str(exc.reason))


def _run_command(command: Sequence[str], timeout_seconds: int) -> CommandResult:
    try:
        result = subprocess.run(  # noqa: S603
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return CommandResult(returncode=127, stdout="", stderr=str(exc))
    return CommandResult(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)


__all__ = [
    "CommandResult",
    "HttpResponse",
    "ReleaseCheck",
    "ReleaseVerificationReport",
    "ReleaseVerificationService",
]
