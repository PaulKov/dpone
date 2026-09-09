from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
VALID_CANDIDATE_NAMES = (
    "dpone-0.73.2-py3-none-any.whl",
    "dpone-0.73.2.tar.gz",
    "dpone_native_accel-0.73.2-py3-none-any.whl",
    "dpone_native_accel-0.73.2.tar.gz",
    "dpone_airflow_pack-0.73.2-py3-none-any.whl",
    "dpone_airflow_pack-0.73.2.tar.gz",
    "apache_airflow_providers_dpone-0.73.2-py3-none-any.whl",
    "apache_airflow_providers_dpone-0.73.2.tar.gz",
)


def _load_module() -> ModuleType:
    path = ROOT / "tools" / "pypi_release_smoke_dist.py"
    spec = importlib.util.spec_from_file_location("pypi_release_smoke_dist", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_candidates(root: Path, names: tuple[str, ...] = VALID_CANDIDATE_NAMES) -> None:
    for name in names:
        (root / name).write_bytes(name.encode())


@pytest.mark.skipif(os.name == "nt", reason="source readiness uses a POSIX runner")
@pytest.mark.parametrize("mutation", ["none", "extra.txt", "subdir", "extra-0.73.2.tar.gz", "changed-bytes"])
def test_source_readiness_revalidates_closed_inventory_after_package_execution(tmp_path: Path, mutation: str) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/source-release-readiness.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["build"]["steps"]
    index = next(
        index
        for index, step in enumerate(steps)
        if step["name"] == "Revalidate closed inventory after package execution"
    )
    assert steps[index - 1]["name"].startswith("Smoke installed candidate")
    assert steps[index + 1]["name"] == "Upload immutable candidate handoff"
    dist = tmp_path / "dist"
    dist.mkdir()
    _write_candidates(dist)
    reports = tmp_path / "test_artifacts/source-readiness-build"
    reports.mkdir(parents=True)
    report = _load_module().evaluate_candidate_inventory(dist, expected_version="0.73.2")
    (reports / "candidate-inventory.json").write_text(json.dumps(report.to_payload(), indent=2, sort_keys=True) + "\n")
    if mutation == "subdir":
        (dist / mutation).mkdir()
    elif mutation == "changed-bytes":
        (dist / VALID_CANDIDATE_NAMES[0]).write_bytes(b"changed after inspection")
    elif mutation != "none":
        (dist / mutation).write_bytes(b"unexpected after smoke")
    command = steps[index]["run"].replace(
        "uv run --frozen python tools/pypi_release_smoke_dist.py",
        f"{shlex.quote(sys.executable)} {shlex.quote(str(ROOT / 'tools/pypi_release_smoke_dist.py'))}",
    )
    result = subprocess.run(
        ["bash", "-c", command],
        cwd=tmp_path,
        env={**os.environ, "RELEASE_VERSION": "0.73.2"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) == (mutation == "none"), result.stdout + result.stderr


def test_discover_distribution_releases_uses_actual_artifact_versions(tmp_path: Path) -> None:
    module = _load_module()
    names = (
        "dpone-0.68.24-py3-none-any.whl",
        "dpone-0.68.24.tar.gz",
        "dpone_native_accel-0.68.20-py3-none-any.whl",
        "dpone_airflow_pack-0.68.24.tar.gz",
    )
    for index, name in enumerate(names):
        (tmp_path / name).write_bytes(f"artifact-{index}".encode())

    releases = module.discover_distribution_releases(tmp_path)

    assert [(release.package, release.version) for release in releases] == [
        ("dpone", "0.68.24"),
        ("dpone-airflow-pack", "0.68.24"),
        ("dpone-native-accel", "0.68.20"),
    ]
    assert [
        (artifact.filename, artifact.sha256) for release in releases for artifact in release.candidate_artifacts
    ] == [
        (
            "dpone-0.68.24-py3-none-any.whl",
            hashlib.sha256(b"artifact-0").hexdigest(),
        ),
        (
            "dpone-0.68.24.tar.gz",
            hashlib.sha256(b"artifact-1").hexdigest(),
        ),
        (
            "dpone_airflow_pack-0.68.24.tar.gz",
            hashlib.sha256(b"artifact-3").hexdigest(),
        ),
        (
            "dpone_native_accel-0.68.20-py3-none-any.whl",
            hashlib.sha256(b"artifact-2").hexdigest(),
        ),
    ]


def test_verify_distribution_releases_passes_dpone_extra_only_to_main_package() -> None:
    module = _load_module()
    calls: list[tuple[str, str, str | None, tuple[object, ...]]] = []

    def waiter(**kwargs):
        calls.append(
            (
                kwargs["package"],
                kwargs["version"],
                kwargs["install_extra"],
                kwargs["candidate_artifacts"],
            )
        )
        return module.PyPIReleaseSmokeReport(
            package=kwargs["package"],
            version=kwargs["version"],
            passed=True,
            attempts=1,
            endpoint_statuses=(),
        )

    reports = module.verify_distribution_releases(
        (
            module.DistributionRelease(
                "dpone",
                "0.68.24",
                (module.CandidateArtifact("dpone-0.68.24-py3-none-any.whl", "a" * 64),),
            ),
            module.DistributionRelease("dpone-airflow-pack", "0.68.24"),
        ),
        timeout_seconds=1,
        poll_interval_seconds=1,
        install_smoke=True,
        dpone_install_extra="accel",
        waiter=waiter,
    )

    assert [report.passed for report in reports] == [True, True]
    assert calls == [
        (
            "dpone",
            "0.68.24",
            "accel",
            (module.CandidateArtifact("dpone-0.68.24-py3-none-any.whl", "a" * 64),),
        ),
        ("dpone-airflow-pack", "0.68.24", None, ()),
    ]


def test_candidate_inventory_requires_all_distributions_at_exact_version(tmp_path: Path) -> None:
    module = _load_module()
    _write_candidates(tmp_path)

    releases = module.discover_distribution_releases(tmp_path)

    assert module.validate_distribution_inventory(releases, expected_version="0.73.2") == ()


def test_candidate_inventory_rejects_missing_package_mixed_version_and_missing_sdist(tmp_path: Path) -> None:
    module = _load_module()
    names = (
        "dpone-0.73.2-py3-none-any.whl",
        "dpone_native_accel-0.73.1-py3-none-any.whl",
        "dpone_native_accel-0.73.1.tar.gz",
        "dpone_airflow_pack-0.73.2-py3-none-any.whl",
        "dpone_airflow_pack-0.73.2.tar.gz",
    )
    for name in names:
        (tmp_path / name).write_bytes(name.encode())

    blockers = module.validate_distribution_inventory(
        module.discover_distribution_releases(tmp_path),
        expected_version="0.73.2",
    )

    assert any(item.startswith("PYPI_CANDIDATE_PACKAGE_SET_MISMATCH:") for item in blockers)
    assert any(item.startswith("PYPI_CANDIDATE_VERSION_MISMATCH: release_ref=sha256:") for item in blockers)
    assert any(item.startswith("PYPI_CANDIDATE_SDIST_MISSING: release_ref=sha256:") for item in blockers)


def test_candidate_inventory_rejects_duplicate_wheel_and_sdist_variants(tmp_path: Path) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    (tmp_path / "dpone-0.73.2-py3-none-macosx_11_0_arm64.whl").write_bytes(b"duplicate-wheel")
    (tmp_path / "dpone-0.73.2.zip").write_bytes(b"duplicate-sdist")

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.status == "failed"
    assert any(item.startswith("PYPI_CANDIDATE_ARTIFACT_COUNT_MISMATCH:") for item in report.blockers)
    assert any(item.startswith("PYPI_CANDIDATE_WHEEL_COUNT_MISMATCH: release_ref=sha256:") for item in report.blockers)
    assert any(item.startswith("PYPI_CANDIDATE_SDIST_FORMAT_UNSUPPORTED:") for item in report.blockers)


def test_candidate_inventory_rejects_unrecognized_and_unsafe_entries(tmp_path: Path) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    (tmp_path / "notes.txt").write_text("not a release artifact", encoding="utf-8")
    (tmp_path / "nested").mkdir()

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.status == "failed"
    assert any(item.startswith("PYPI_CANDIDATE_ARTIFACT_UNRECOGNIZED:") for item in report.blockers)
    assert any(item.startswith("PYPI_CANDIDATE_ENTRY_UNSAFE:") for item in report.blockers)


def test_inventory_only_is_deterministic_and_never_constructs_pypi_client(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)

    def forbidden_client():
        raise AssertionError("inventory-only must not construct a PyPI client")

    monkeypatch.setattr(module, "PyPIClient", forbidden_client)
    arguments = [
        "--dist-dir",
        str(tmp_path),
        "--expected-version",
        "0.73.2",
        "--inventory-only",
        "--format",
        "json",
    ]

    assert module.main(arguments) == 0
    first = capsys.readouterr().out
    assert module.main(arguments) == 0
    second = capsys.readouterr().out

    payload = json.loads(first)
    assert first == second
    assert payload["status"] == "passed"
    assert payload["decision"] == "GO"
    assert payload["summary"] == {
        "artifact_count": 8,
        "distribution_count": 4,
        "expected_artifact_count": 8,
        "expected_distribution_count": 4,
    }
    assert len(payload["artifacts"]) == 8
    assert all(artifact["size_bytes"] > 0 for artifact in payload["artifacts"])


def test_candidate_contract_is_dependency_neutral_and_does_not_mutate_sys_path() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "before_path = tuple(sys.path)\n"
                "before_modules = set(sys.modules)\n"
                "from tools import pypi_candidate_contract as contract\n"
                "assert tuple(sys.path) == before_path\n"
                "added = set(sys.modules) - before_modules\n"
                "assert not any(name == 'packaging' or name.startswith('packaging.') for name in added)\n"
                "assert 'pypi_release_smoke' not in added\n"
                "assert contract.CandidateArtifact('candidate.whl', 'a' * 64).filename == 'candidate.whl'\n"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(os.name == "nt", reason="final no-follow revalidation requires POSIX descriptors")
@pytest.mark.parametrize("mutation", ["replacement", "addition", "removal", "symlink", "file_type"])
def test_main_revalidates_exact_candidate_set_after_network_checks(
    tmp_path: Path,
    monkeypatch,
    capsys,
    mutation: str,
) -> None:
    module = _load_module()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _write_candidates(dist_dir)
    target = dist_dir / VALID_CANDIDATE_NAMES[0]

    def mutate_then_pass(releases, **_kwargs):
        if mutation == "replacement":
            replacement = tmp_path / "replacement.whl"
            replacement.write_bytes(target.read_bytes())
            os.replace(replacement, target)
        elif mutation == "addition":
            (dist_dir / "unexpected.txt").write_bytes(b"unexpected")
            monkeypatch.setattr(
                module.candidate_inventory,
                "_hash_entry",
                lambda *_args, **_kwargs: pytest.fail("entry bound must fail before final hashing"),
            )
        elif mutation == "removal":
            target.unlink()
        elif mutation == "symlink":
            backing = tmp_path / "backing.whl"
            backing.write_bytes(target.read_bytes())
            target.unlink()
            target.symlink_to(backing)
        else:
            target.unlink()
            target.mkdir()
        return tuple(
            module.PyPIReleaseSmokeReport(
                package=release.package,
                version=release.version,
                passed=True,
                attempts=1,
                endpoint_statuses=(),
            )
            for release in releases
        )

    monkeypatch.setattr(module, "verify_distribution_releases", mutate_then_pass)
    monkeypatch.setattr(module, "MAX_CANDIDATE_ENTRIES", len(VALID_CANDIDATE_NAMES))

    return_code = module.main(
        [
            "--dist-dir",
            str(dist_dir),
            "--expected-version",
            "0.73.2",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert return_code == 2
    assert payload["decision"] == "NO-GO"
    assert payload["status"] == "failed"
    assert payload["blockers"][0].startswith("PYPI_CANDIDATE_FINAL_REVALIDATION_FAILED:")


@pytest.mark.skipif(os.name == "nt", reason="directory identity handoff requires POSIX descriptors")
def test_main_rejects_transient_candidate_directory_mutation_after_inventory(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _write_candidates(dist_dir)

    def mutate_then_restore(releases, **_kwargs):
        initial_metadata = dist_dir.stat()
        unexpected = dist_dir / "unexpected.txt"
        unexpected.write_bytes(b"transient")
        unexpected.unlink()
        if dist_dir.stat().st_mtime_ns == initial_metadata.st_mtime_ns:
            os.utime(
                dist_dir,
                ns=(initial_metadata.st_atime_ns, initial_metadata.st_mtime_ns + 1_000_000_000),
            )
        return tuple(
            module.PyPIReleaseSmokeReport(
                package=release.package,
                version=release.version,
                passed=True,
                attempts=1,
                endpoint_statuses=(),
            )
            for release in releases
        )

    monkeypatch.setattr(module, "verify_distribution_releases", mutate_then_restore)

    return_code = module.main(
        [
            "--dist-dir",
            str(dist_dir),
            "--expected-version",
            "0.73.2",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert return_code == 2
    assert payload["decision"] == "NO-GO"
    assert any(blocker.startswith("PYPI_CANDIDATE_FINAL_DIRECTORY_CHANGED:") for blocker in payload["blockers"])


@pytest.mark.skipif(os.name == "nt", reason="directory identity handoff requires POSIX descriptors")
def test_final_revalidation_rejects_transient_directory_mutation_while_hashing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _write_candidates(dist_dir)
    original_sha256 = module.hashlib.sha256

    class MutatingDigest:
        mutated = False

        def __init__(self) -> None:
            self._delegate = original_sha256()

        def update(self, chunk: bytes) -> None:
            self._delegate.update(chunk)
            if self.mutated:
                return
            MutatingDigest.mutated = True
            initial_metadata = dist_dir.stat()
            unexpected = dist_dir / "unexpected.txt"
            unexpected.write_bytes(b"transient")
            unexpected.unlink()
            if dist_dir.stat().st_mtime_ns == initial_metadata.st_mtime_ns:
                os.utime(
                    dist_dir,
                    ns=(initial_metadata.st_atime_ns, initial_metadata.st_mtime_ns + 1_000_000_000),
                )

        def hexdigest(self) -> str:
            return self._delegate.hexdigest()

    def enable_mutating_hash_then_pass(releases, **_kwargs):
        monkeypatch.setattr(module.hashlib, "sha256", MutatingDigest)
        return tuple(
            module.PyPIReleaseSmokeReport(
                package=release.package,
                version=release.version,
                passed=True,
                attempts=1,
                endpoint_statuses=(),
            )
            for release in releases
        )

    monkeypatch.setattr(module, "verify_distribution_releases", enable_mutating_hash_then_pass)

    return_code = module.main(
        [
            "--dist-dir",
            str(dist_dir),
            "--expected-version",
            "0.73.2",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert return_code == 2
    assert payload["decision"] == "NO-GO"
    assert payload["blockers"][0].startswith("PYPI_CANDIDATE_FINAL_REVALIDATION_FAILED:")
    assert any(blocker.startswith("PYPI_CANDIDATE_ROOT_CHANGED:") for blocker in payload["blockers"])


@pytest.mark.parametrize(
    ("unsafe_name", "sensitive_fragment"),
    [
        ("secret-token-\n-\x1b[31m.txt", "secret-token"),
        ("secret_token_private-0.73.2-py3-none-any.whl", "secret-token-private"),
    ],
)
def test_inventory_blockers_redact_control_unsafe_candidate_identifiers(
    tmp_path: Path,
    unsafe_name: str,
    sensitive_fragment: str,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    (tmp_path / unsafe_name).write_bytes(b"unexpected")

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.status == "failed"
    assert unsafe_name not in "\n".join(report.blockers)
    assert sensitive_fragment not in "\n".join(report.blockers)
    assert any(re.search(r"(?:candidate|release)_ref=sha256:[0-9a-f]{16}", blocker) for blocker in report.blockers)
    assert all(len(blocker) <= 256 for blocker in report.blockers)
    assert all(not any(ord(character) < 32 for character in blocker) for blocker in report.blockers)


@pytest.mark.skipif(os.name == "nt", reason="descriptor accounting requires POSIX no-follow support")
def test_final_revalidation_closes_all_opened_descriptors_on_failure(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _write_candidates(dist_dir)
    target = dist_dir / VALID_CANDIDATE_NAMES[0]
    opened: list[int] = []
    closed: list[int] = []
    original_open = module.candidate_inventory.os.open
    original_close = module.candidate_inventory.os.close

    def tracked_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    def tracked_close(descriptor):
        closed.append(descriptor)
        original_close(descriptor)

    def remove_then_pass(releases, **_kwargs):
        target.unlink()
        return tuple(
            module.PyPIReleaseSmokeReport(
                package=release.package,
                version=release.version,
                passed=True,
                attempts=1,
                endpoint_statuses=(),
            )
            for release in releases
        )

    monkeypatch.setattr(module.candidate_inventory.os, "open", tracked_open)
    monkeypatch.setattr(module.candidate_inventory.os, "close", tracked_close)
    monkeypatch.setattr(module, "verify_distribution_releases", remove_then_pass)

    return_code = module.main(
        [
            "--dist-dir",
            str(dist_dir),
            "--expected-version",
            "0.73.2",
            "--format",
            "json",
        ]
    )

    capsys.readouterr()
    assert return_code == 2
    assert opened
    assert Counter(opened) == Counter(closed)


def test_legacy_discovery_fails_closed_on_stray_entries(tmp_path: Path) -> None:
    module = _load_module()
    (tmp_path / "dpone-0.73.2.tar.gz").write_bytes(b"candidate")
    (tmp_path / "release-notes.txt").write_bytes(b"stray")

    with pytest.raises(RuntimeError, match="PYPI_CANDIDATE_ARTIFACT_UNRECOGNIZED"):
        module.discover_distribution_releases(tmp_path)


def test_candidate_inventory_rejects_symlinked_root(tmp_path: Path) -> None:
    module = _load_module()
    candidate_root = tmp_path / "candidate-root"
    candidate_root.mkdir()
    _write_candidates(candidate_root)
    dist_link = tmp_path / "dist"
    try:
        dist_link.symlink_to(candidate_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks are unavailable: {exc}")

    report = module.evaluate_candidate_inventory(dist_link, expected_version="0.73.2")

    assert report.status == "failed"
    assert report.artifacts == ()
    assert report.blockers == (
        "PYPI_CANDIDATE_ROOT_SYMLINK: candidate directory must not be a symlink",
        "PYPI_CANDIDATE_ENTRY_COUNT_MISMATCH: expected=8 actual=0",
        "PYPI_CANDIDATE_ARTIFACT_COUNT_MISMATCH: expected=8 actual=0",
        "PYPI_CANDIDATE_DISTRIBUTION_COUNT_MISMATCH: expected=4 actual=0",
        "PYPI_CANDIDATE_PACKAGE_SET_MISMATCH: "
        "missing=['apache-airflow-providers-dpone', 'dpone', 'dpone-airflow-pack', 'dpone-native-accel'] "
        "unexpected_count=0",
    )


def test_candidate_inventory_enforces_entry_budget_before_hashing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module()
    monkeypatch.setattr(module, "MAX_CANDIDATE_ENTRIES", 2)
    for index in range(3):
        (tmp_path / f"candidate-{index}.tar.gz").write_bytes(b"x")

    def forbidden_hash():
        raise AssertionError("entry budget must fail before hashing")

    monkeypatch.setattr(module.hashlib, "sha256", forbidden_hash)

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.entry_count == 3
    assert report.artifacts == ()
    assert report.blockers[0] == "PYPI_CANDIDATE_ENTRY_LIMIT_EXCEEDED: maximum=2 observed_at_least=3"


def test_candidate_inventory_enforces_per_file_budget_before_hashing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    monkeypatch.setattr(module, "MAX_CANDIDATE_FILE_BYTES", 1)

    def forbidden_hash():
        raise AssertionError("file budget must fail before hashing")

    monkeypatch.setattr(module.hashlib, "sha256", forbidden_hash)

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.artifacts == ()
    assert report.blockers[0].startswith("PYPI_CANDIDATE_FILE_SIZE_LIMIT_EXCEEDED: candidate_ref=sha256:")


def test_candidate_inventory_enforces_aggregate_budget_before_hashing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    total_bytes = sum((tmp_path / name).stat().st_size for name in VALID_CANDIDATE_NAMES)
    monkeypatch.setattr(module, "MAX_CANDIDATE_TOTAL_BYTES", total_bytes - 1)

    def forbidden_hash():
        raise AssertionError("aggregate budget must fail before hashing")

    monkeypatch.setattr(module.hashlib, "sha256", forbidden_hash)

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.artifacts == ()
    assert report.blockers[0] == (
        f"PYPI_CANDIDATE_TOTAL_SIZE_LIMIT_EXCEEDED: maximum_bytes={total_bytes - 1} actual_bytes={total_bytes}"
    )


@pytest.mark.skipif(os.name == "nt", reason="POSIX permits deterministic replacement of an open file")
@pytest.mark.parametrize("mutation", ["identity", "metadata"])
def test_candidate_inventory_rejects_archive_changes_while_hashing(
    tmp_path: Path,
    monkeypatch,
    mutation: str,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)
    target = tmp_path / "apache_airflow_providers_dpone-0.73.2-py3-none-any.whl"
    original_sha256 = module.hashlib.sha256
    changed = False

    class MutatingDigest:
        def __init__(self) -> None:
            self._delegate = original_sha256()

        def update(self, chunk: bytes) -> None:
            nonlocal changed
            self._delegate.update(chunk)
            if changed:
                return
            changed = True
            if mutation == "identity":
                replacement = tmp_path / "replacement.whl"
                replacement.write_bytes(b"replacement archive")
                os.replace(replacement, target)
            else:
                with target.open("ab") as stream:
                    stream.write(b"-changed")

        def hexdigest(self) -> str:
            return self._delegate.hexdigest()

    monkeypatch.setattr(module.hashlib, "sha256", MutatingDigest)

    report = module.evaluate_candidate_inventory(tmp_path, expected_version="0.73.2")

    assert report.status == "failed"
    assert any(blocker.startswith("PYPI_CANDIDATE_ENTRY_CHANGED: candidate_ref=sha256:") for blocker in report.blockers)
    assert target.name not in "\n".join(report.blockers)


@pytest.mark.parametrize(
    ("argument", "limit_name", "blocker_code"),
    [
        (
            "--timeout-seconds",
            "MAX_RELEASE_TIMEOUT_SECONDS",
            "PYPI_RELEASE_TIMEOUT_LIMIT_EXCEEDED",
        ),
        (
            "--poll-interval-seconds",
            "MAX_RELEASE_POLL_INTERVAL_SECONDS",
            "PYPI_RELEASE_POLL_INTERVAL_LIMIT_EXCEEDED",
        ),
    ],
)
def test_cli_rejects_unbounded_polling_before_network_access(
    tmp_path: Path,
    monkeypatch,
    capsys,
    argument: str,
    limit_name: str,
    blocker_code: str,
) -> None:
    module = _load_module()
    _write_candidates(tmp_path)

    def forbidden_client():
        raise AssertionError("invalid polling bounds must fail before network access")

    monkeypatch.setattr(module, "PyPIClient", forbidden_client)
    arguments = [
        "--dist-dir",
        str(tmp_path),
        "--expected-version",
        "0.73.2",
        argument,
        str(getattr(module, limit_name) + 1),
        "--format",
        "json",
    ]

    assert module.main(arguments) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert any(blocker.startswith(f"{blocker_code}:") for blocker in payload["blockers"])
