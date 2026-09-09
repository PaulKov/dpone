#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

FetchText = Callable[[str], str]
CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
CommandResolver = Callable[[str], str | None]


@dataclass(frozen=True, slots=True)
class EndpointStatus:
    name: str
    url: str
    passed: bool
    details: str

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "url": self.url, "passed": self.passed, "details": self.details}


@dataclass(frozen=True, slots=True, order=True)
class CandidateArtifact:
    filename: str
    sha256: str


@dataclass(frozen=True, slots=True)
class PyPIReleaseSmokeReport:
    package: str
    version: str
    passed: bool
    attempts: int
    endpoint_statuses: tuple[EndpointStatus, ...]
    install_status: EndpointStatus | None = None
    blockers: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "package": self.package,
            "version": self.version,
            "passed": self.passed,
            "attempts": self.attempts,
            "endpoint_statuses": [status.to_dict() for status in self.endpoint_statuses],
            "install_status": self.install_status.to_dict() if self.install_status else None,
            "blockers": list(self.blockers),
        }

    def to_markdown(self) -> str:
        lines = [
            f"# PyPI release smoke: `{self.package}=={self.version}`",
            "",
            f"- Status: `{'passed' if self.passed else 'failed'}`",
            f"- Attempts: `{self.attempts}`",
            "",
            "## Endpoint visibility",
            "",
            "| Check | Passed | Details |",
            "| --- | --- | --- |",
        ]
        for status in self.endpoint_statuses:
            lines.append(f"| `{status.name}` | `{status.passed}` | {status.details} |")
        if self.install_status:
            lines.extend(
                [
                    "",
                    "## Resolver install smoke",
                    "",
                    f"- Passed: `{self.install_status.passed}`",
                    f"- Details: {self.install_status.details}",
                ]
            )
        if self.blockers:
            lines.extend(["", "## Blockers", ""])
            lines.extend(f"- {blocker}" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class PyPIClient:
    def __init__(self, *, fetch_text: FetchText | None = None, base_url: str = "https://pypi.org") -> None:
        self._fetch_text = fetch_text or _default_fetch_text
        self._base_url = base_url.rstrip("/")

    def simple_index(self, package: str) -> str:
        return self._fetch_text(f"{self._base_url}/simple/{package}/")

    def project_json(self, package: str) -> dict[str, Any]:
        return json.loads(self._fetch_text(f"{self._base_url}/pypi/{package}/json"))

    def version_json(self, package: str, version: str) -> dict[str, Any]:
        return json.loads(self._fetch_text(f"{self._base_url}/pypi/{package}/{version}/json"))


def _default_fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache", "User-Agent": "dpone-pypi-release-smoke/1"},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", "replace")


def _normalize_version(value: str) -> str:
    return value[1:] if value.startswith("v") else value


def _distribution_filename_prefix(package: str) -> str:
    return re.sub(r"[-_.]+", "_", package).lower()


def _status(name: str, url: str, passed: bool, details: str) -> EndpointStatus:
    return EndpointStatus(name=name, url=url, passed=passed, details=details.replace("\n", " ")[:500])


def _artifact_identity_statuses(
    payload: dict[str, Any],
    candidates: Sequence[CandidateArtifact],
    *,
    version_url: str,
) -> tuple[EndpointStatus, ...]:
    published_items = [
        item for item in payload.get("urls", []) if isinstance(item, dict) and isinstance(item.get("filename"), str)
    ]
    published_files = {item["filename"]: item for item in published_items}
    statuses: list[EndpointStatus] = []
    if candidates:
        expected_filenames = sorted(candidate.filename for candidate in candidates)
        published_filenames = sorted(str(item["filename"]) for item in published_items)
        if published_filenames != expected_filenames:
            details = f"PYPI_ARTIFACT_SET_MISMATCH: expected={expected_filenames} published={published_filenames}"
            statuses.append(_status("artifact_set_identity", version_url, False, details))
    for candidate in candidates:
        expected_sha256 = candidate.sha256.lower()
        published = published_files.get(candidate.filename)
        if published is None:
            details = f"PYPI_ARTIFACT_FILENAME_MISSING: filename={candidate.filename} expected_sha256={expected_sha256}"
            statuses.append(_status("artifact_identity", version_url, False, details))
            continue
        if bool(published.get("yanked")):
            details = f"PYPI_ARTIFACT_YANKED: filename={candidate.filename}"
            statuses.append(_status("artifact_identity", version_url, False, details))
            continue
        digests = published.get("digests")
        published_sha256 = digests.get("sha256") if isinstance(digests, dict) else None
        if not isinstance(published_sha256, str) or re.fullmatch(r"[0-9a-fA-F]{64}", published_sha256) is None:
            details = (
                f"PYPI_ARTIFACT_SHA256_UNAVAILABLE: filename={candidate.filename} expected_sha256={expected_sha256}"
            )
            statuses.append(_status("artifact_identity", version_url, False, details))
            continue
        actual_sha256 = published_sha256.lower()
        if actual_sha256 != expected_sha256:
            details = (
                f"PYPI_ARTIFACT_SHA256_MISMATCH: filename={candidate.filename} "
                f"expected_sha256={expected_sha256} published_sha256={actual_sha256}"
            )
            statuses.append(_status("artifact_identity", version_url, False, details))
            continue
        details = f"PYPI_ARTIFACT_MATCH: filename={candidate.filename} sha256={expected_sha256}"
        statuses.append(_status("artifact_identity", version_url, True, details))
    return tuple(statuses)


def check_endpoint_visibility(
    client: PyPIClient,
    *,
    package: str,
    version: str,
    candidate_artifacts: Sequence[CandidateArtifact] = (),
) -> tuple[EndpointStatus, ...]:
    statuses: list[EndpointStatus] = []
    simple_url = f"https://pypi.org/simple/{package}/"
    project_url = f"https://pypi.org/pypi/{package}/json"
    version_url = f"https://pypi.org/pypi/{package}/{version}/json"

    try:
        body = client.simple_index(package)
        filename_prefix = _distribution_filename_prefix(package)
        versions = sorted(set(re.findall(rf"{re.escape(filename_prefix)}-(\d+\.\d+\.\d+)", body, flags=re.IGNORECASE)))
        statuses.append(
            _status(
                "simple_index",
                simple_url,
                _normalize_version(version) in versions,
                f"visible_versions_tail={versions[-8:]}",
            )
        )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        statuses.append(_status("simple_index", simple_url, False, f"{type(exc).__name__}: {exc}"))

    try:
        payload = client.project_json(package)
        files = [item.get("filename", "") for item in payload.get("releases", {}).get(version, [])]
        statuses.append(
            _status(
                "project_json",
                project_url,
                bool(files),
                f"info_version={payload.get('info', {}).get('version')} files={files}",
            )
        )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        statuses.append(_status("project_json", project_url, False, f"{type(exc).__name__}: {exc}"))

    try:
        payload = client.version_json(package, version)
        files = [item.get("filename", "") for item in payload.get("urls", [])]
        statuses.append(
            _status(
                "version_json",
                version_url,
                bool(files) and payload.get("info", {}).get("version") == version,
                f"info_version={payload.get('info', {}).get('version')} files={files}",
            )
        )
        statuses.extend(_artifact_identity_statuses(payload, candidate_artifacts, version_url=version_url))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        statuses.append(_status("version_json", version_url, False, f"{type(exc).__name__}: {exc}"))

    return tuple(statuses)


def run_install_smoke(
    *,
    package: str,
    version: str,
    extra: str | None = None,
    command_runner: CommandRunner | None = None,
    command_resolver: CommandResolver = shutil.which,
    uv_command: str = "uv",
) -> EndpointStatus:
    runner = command_runner or _run_command
    spec = f"{package}[{extra}]=={version}" if extra else f"{package}=={version}"
    if not command_resolver(uv_command):
        return _status("resolver_install", "uv pip install", False, f"{uv_command!r} is not available")
    with tempfile.TemporaryDirectory(prefix="dpone-pypi-smoke-") as raw_dir:
        venv = Path(raw_dir) / "venv"
        create = runner([uv_command, "venv", str(venv)])
        if create.returncode != 0:
            return _status("resolver_install", "uv venv", False, _combined_output(create))
        install = runner([uv_command, "pip", "install", "--python", str(venv / "bin" / "python"), "--no-cache", spec])
        if install.returncode != 0:
            return _status("resolver_install", "uv pip install", False, _combined_output(install))
        version_check = runner(
            [
                str(venv / "bin" / "python"),
                "-c",
                f"import importlib.metadata as metadata; print(metadata.version({package!r}))",
            ]
        )
        return _status(
            "resolver_install",
            "uv pip install",
            _installed_version_matches(version_check, expected=version),
            _combined_output(version_check) or _combined_output(install),
        )


def _run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(command), capture_output=True, text=True, check=False)


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stdout + "\n" + result.stderr).strip()[:1000]


def _installed_version_matches(
    result: subprocess.CompletedProcess[str],
    *,
    expected: str,
) -> bool:
    if result.returncode != 0:
        return False
    reported = result.stdout.strip()
    return bool(reported) and reported == _normalize_version(expected)


def build_report(
    *,
    package: str,
    version: str,
    client: PyPIClient,
    attempts: int,
    install_smoke: bool,
    install_extra: str | None,
    command_runner: CommandRunner | None = None,
    candidate_artifacts: Sequence[CandidateArtifact] = (),
) -> PyPIReleaseSmokeReport:
    endpoint_statuses = check_endpoint_visibility(
        client,
        package=package,
        version=version,
        candidate_artifacts=candidate_artifacts,
    )
    blockers = [
        (
            status.details
            if status.name == "artifact_identity"
            else f"{status.name} did not expose {package}=={version}: {status.details}"
        )
        for status in endpoint_statuses
        if not status.passed
    ]
    install_status: EndpointStatus | None = None
    if install_smoke and not blockers:
        install_status = run_install_smoke(
            package=package,
            version=version,
            extra=install_extra,
            command_runner=command_runner,
        )
        if not install_status.passed:
            blockers.append(f"resolver install failed: {install_status.details}")
    return PyPIReleaseSmokeReport(
        package=package,
        version=version,
        passed=not blockers,
        attempts=attempts,
        endpoint_statuses=endpoint_statuses,
        install_status=install_status,
        blockers=tuple(blockers),
    )


def wait_for_release(
    *,
    package: str,
    version: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    install_smoke: bool,
    install_extra: str | None,
    client: PyPIClient,
    candidate_artifacts: Sequence[CandidateArtifact] = (),
    emit_progress: bool = True,
) -> PyPIReleaseSmokeReport:
    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    latest: PyPIReleaseSmokeReport | None = None
    while True:
        attempts += 1
        latest = build_report(
            package=package,
            version=version,
            client=client,
            attempts=attempts,
            install_smoke=install_smoke,
            install_extra=install_extra,
            candidate_artifacts=candidate_artifacts,
        )
        if emit_progress:
            print(latest.to_markdown(), flush=True)
        if latest.passed or time.monotonic() >= deadline:
            return latest
        time.sleep(poll_interval_seconds)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify that a PyPI release is visible to normal installers.")
    parser.add_argument("--package", default="dpone")
    parser.add_argument("--version", required=True, help="Version such as 0.7.5 or tag v0.7.5")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--install-smoke", action="store_true", help="Also run uv pip install package==version")
    parser.add_argument("--install-extra", default=None, help="Optional extra to install, for example full")
    parser.add_argument("--format", choices=("text", "json", "md"), default="text")
    args = parser.parse_args(argv)

    report = wait_for_release(
        package=args.package,
        version=_normalize_version(args.version),
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        install_smoke=args.install_smoke,
        install_extra=args.install_extra,
        client=PyPIClient(),
        emit_progress=args.format == "text",
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    elif args.format == "md":
        print(report.to_markdown())
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
