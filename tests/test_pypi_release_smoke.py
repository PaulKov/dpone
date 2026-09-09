from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_module() -> ModuleType:
    path = ROOT / "tools" / "pypi_release_smoke.py"
    spec = importlib.util.spec_from_file_location("pypi_release_smoke", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_endpoint_visibility_detects_fresh_pypi_indexes() -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return '<a href="x">dpone-0.7.5-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone/json"):
            return '{"info":{"version":"0.7.5"},"releases":{"0.7.5":[{"filename":"dpone-0.7.5.tar.gz"}]}}'
        return '{"info":{"version":"0.7.5"},"urls":[{"filename":"dpone-0.7.5-py3-none-any.whl"}]}'

    statuses = module.check_endpoint_visibility(module.PyPIClient(fetch_text=fetch), package="dpone", version="0.7.5")
    assert all(status.passed for status in statuses)


def test_endpoint_visibility_accepts_normalized_distribution_filenames() -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone-native-accel/"):
            return '<a href="x">dpone_native_accel-0.29.0-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone-native-accel/json"):
            return (
                '{"info":{"version":"0.29.0"},"releases":{"0.29.0":[{"filename":"dpone_native_accel-0.29.0.tar.gz"}]}}'
            )
        return '{"info":{"version":"0.29.0"},"urls":[{"filename":"dpone_native_accel-0.29.0-py3-none-any.whl"}]}'

    statuses = module.check_endpoint_visibility(
        module.PyPIClient(fetch_text=fetch),
        package="dpone-native-accel",
        version="0.29.0",
    )
    assert all(status.passed for status in statuses)


def test_endpoint_visibility_rejects_version_prefix_collision() -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return '<a href="x">dpone-0.73.22-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone/json"):
            return '{"info":{"version":"0.73.2"},"releases":{"0.73.2":[{"filename":"dpone-0.73.2.tar.gz"}]}}'
        return '{"info":{"version":"0.73.2"},"urls":[{"filename":"dpone-0.73.2-py3-none-any.whl"}]}'

    statuses = module.check_endpoint_visibility(
        module.PyPIClient(fetch_text=fetch),
        package="dpone",
        version="0.73.2",
    )

    simple_status = next(status for status in statuses if status.name == "simple_index")
    assert simple_status.passed is False
    assert "0.73.22" in simple_status.details


def test_endpoint_visibility_flags_stale_simple_and_global_json() -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return '<a href="x">dpone-0.7.1-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone/json"):
            return '{"info":{"version":"0.7.1"},"releases":{"0.7.1":[{"filename":"dpone-0.7.1.tar.gz"}]}}'
        return '{"info":{"version":"0.7.5"},"urls":[{"filename":"dpone-0.7.5-py3-none-any.whl"}]}'

    report = module.build_report(
        package="dpone",
        version="0.7.5",
        client=module.PyPIClient(fetch_text=fetch),
        attempts=1,
        install_smoke=False,
        install_extra=None,
    )
    assert report.passed is False
    assert any("simple_index" in blocker for blocker in report.blockers)
    assert any("project_json" in blocker for blocker in report.blockers)
    assert not any("version_json" in blocker for blocker in report.blockers)


def test_release_report_verifies_exact_candidate_artifact_identity() -> None:
    module = _load_module()
    digest = "a" * 64
    filename = "dpone-0.7.5-py3-none-any.whl"

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return f'<a href="x">{filename}</a>'
        if url.endswith("/pypi/dpone/json"):
            return f'{{"info":{{"version":"0.7.5"}},"releases":{{"0.7.5":[{{"filename":"{filename}"}}]}}}}'
        return (
            f'{{"info":{{"version":"0.7.5"}},"urls":[{{"filename":"{filename}","digests":{{"sha256":"{digest}"}}}}]}}'
        )

    report = module.build_report(
        package="dpone",
        version="0.7.5",
        client=module.PyPIClient(fetch_text=fetch),
        attempts=1,
        install_smoke=False,
        install_extra=None,
        candidate_artifacts=(module.CandidateArtifact(filename=filename, sha256=digest),),
    )

    identity_status = next(status for status in report.endpoint_statuses if status.name == "artifact_identity")
    assert report.passed is True
    assert identity_status.passed is True
    assert identity_status.details == f"PYPI_ARTIFACT_MATCH: filename={filename} sha256={digest}"


@pytest.mark.parametrize(
    ("published_file", "reason"),
    [
        (None, "PYPI_ARTIFACT_FILENAME_MISSING"),
        ({"digests": {}}, "PYPI_ARTIFACT_SHA256_UNAVAILABLE"),
        ({"digests": {"sha256": "b" * 64}}, "PYPI_ARTIFACT_SHA256_MISMATCH"),
    ],
)
def test_release_report_fails_closed_for_unverified_candidate_artifact(
    published_file: dict[str, object] | None,
    reason: str,
) -> None:
    module = _load_module()
    digest = "a" * 64
    filename = "dpone-0.7.5-py3-none-any.whl"

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return f'<a href="x">{filename}</a>'
        if url.endswith("/pypi/dpone/json"):
            return f'{{"info":{{"version":"0.7.5"}},"releases":{{"0.7.5":[{{"filename":"{filename}"}}]}}}}'
        item = {"filename": "different.whl"} if published_file is None else {"filename": filename, **published_file}
        import json

        return json.dumps({"info": {"version": "0.7.5"}, "urls": [item]})

    report = module.build_report(
        package="dpone",
        version="0.7.5",
        client=module.PyPIClient(fetch_text=fetch),
        attempts=1,
        install_smoke=False,
        install_extra=None,
        candidate_artifacts=(module.CandidateArtifact(filename=filename, sha256=digest),),
    )

    assert report.passed is False
    assert any(reason in blocker and filename in blocker for blocker in report.blockers)


@pytest.mark.parametrize(
    ("extra_urls", "reason"),
    [
        ([{"filename": "dpone-0.7.5.tar.gz", "digests": {"sha256": "b" * 64}}], "PYPI_ARTIFACT_SET_MISMATCH"),
        ([], "PYPI_ARTIFACT_YANKED"),
    ],
)
def test_release_report_rejects_unexpected_or_yanked_published_artifacts(
    extra_urls: list[dict[str, object]],
    reason: str,
) -> None:
    module = _load_module()
    digest = "a" * 64
    filename = "dpone-0.7.5-py3-none-any.whl"

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return f'<a href="x">{filename}</a>'
        if url.endswith("/pypi/dpone/json"):
            return f'{{"info":{{"version":"0.7.5"}},"releases":{{"0.7.5":[{{"filename":"{filename}"}}]}}}}'
        import json

        candidate = {
            "filename": filename,
            "digests": {"sha256": digest},
            "yanked": reason == "PYPI_ARTIFACT_YANKED",
        }
        return json.dumps({"info": {"version": "0.7.5"}, "urls": [candidate, *extra_urls]})

    report = module.build_report(
        package="dpone",
        version="0.7.5",
        client=module.PyPIClient(fetch_text=fetch),
        attempts=1,
        install_smoke=False,
        install_extra=None,
        candidate_artifacts=(module.CandidateArtifact(filename=filename, sha256=digest),),
    )

    assert report.passed is False
    assert any(reason in blocker for blocker in report.blockers)


def test_install_smoke_uses_uv_resolver_and_version_command() -> None:
    module = _load_module()
    calls: list[list[str]] = []

    def runner(command):
        calls.append(list(command))
        if any("importlib.metadata" in str(part) for part in command):
            return subprocess.CompletedProcess(command, 0, stdout="0.7.5\n", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    status = module.run_install_smoke(
        package="dpone",
        version="0.7.5",
        command_runner=runner,
        command_resolver=lambda command: command,
        uv_command="uv",
    )
    assert status.passed is True
    assert any(cmd[:3] == ["uv", "pip", "install"] for cmd in calls)
    assert calls[-1][1] == "-c"
    assert "metadata.version('dpone')" in calls[-1][2]


def test_install_smoke_rejects_version_prefix_collision() -> None:
    module = _load_module()

    def runner(command):
        if any("importlib.metadata" in str(part) for part in command):
            return subprocess.CompletedProcess(command, 0, stdout="0.73.22\n", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    status = module.run_install_smoke(
        package="dpone",
        version="0.73.2",
        command_runner=runner,
        command_resolver=lambda command: command,
        uv_command="uv",
    )

    assert status.passed is False
    assert status.details == "0.73.22"


def test_wait_for_release_can_suppress_progress_output(capsys) -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return '<a href="x">dpone-0.7.5-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone/json"):
            return '{"info":{"version":"0.7.5"},"releases":{"0.7.5":[{"filename":"dpone-0.7.5.tar.gz"}]}}'
        return '{"info":{"version":"0.7.5"},"urls":[{"filename":"dpone-0.7.5-py3-none-any.whl"}]}'

    report = module.wait_for_release(
        package="dpone",
        version="0.7.5",
        timeout_seconds=0,
        poll_interval_seconds=0,
        install_smoke=False,
        install_extra=None,
        client=module.PyPIClient(fetch_text=fetch),
        emit_progress=False,
    )

    assert report.passed is True
    assert capsys.readouterr().out == ""


def test_main_markdown_format_prints_single_final_report(monkeypatch, capsys) -> None:
    module = _load_module()

    def fetch(url: str) -> str:
        if url.endswith("/simple/dpone/"):
            return '<a href="x">dpone-0.7.5-py3-none-any.whl</a>'
        if url.endswith("/pypi/dpone/json"):
            return '{"info":{"version":"0.7.5"},"releases":{"0.7.5":[{"filename":"dpone-0.7.5.tar.gz"}]}}'
        return '{"info":{"version":"0.7.5"},"urls":[{"filename":"dpone-0.7.5-py3-none-any.whl"}]}'

    client_cls = module.PyPIClient
    monkeypatch.setattr(module, "PyPIClient", lambda: client_cls(fetch_text=fetch))

    rc = module.main(
        [
            "--package",
            "dpone",
            "--version",
            "0.7.5",
            "--timeout-seconds",
            "0",
            "--poll-interval-seconds",
            "0",
            "--format",
            "md",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert out.count("# PyPI release smoke") == 1
