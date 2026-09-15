"""Synthetic Git inputs validate producer logic, not the shipped package."""

import subprocess
from pathlib import Path

import pytest
import yaml
from tools.dbt_self_service.generate_starter_resources import capture_package_source, check_starter_resources

from tests.test_dbt_starter_resources import PACKAGE_FILES


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def source(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "synthetic"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "Synthetic Test")
    git(repo, "config", "user.email", "synthetic@example.invalid")
    for name in PACKAGE_FILES:
        path = repo / "packages/dbt-dpone" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(("synthetic " + name + "\r\n").encode())
    (repo / "packages/dbt-dpone/dbt_project.yml").write_text("name: dbt_dpone\nversion: '1.0'\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "Synthetic package fixture")
    return repo, git(repo, "rev-parse", "HEAD")


def test_exact_committed_inventory_and_bytes(source):
    repo, revision = source
    captured = capture_package_source(repo, revision)
    assert captured.revision == revision
    assert dict(captured.files) == {name: (repo / "packages/dbt-dpone" / name).read_bytes() for name in PACKAGE_FILES}
    assert len(captured.files) == 14
    assert git(repo, "status", "--porcelain") == ""


@pytest.mark.parametrize("revision", ["HEAD", "main", "a" * 39, "z" * 40, "0" * 40, "--help"])
def test_only_existing_immutable_commit_is_admitted(source, revision):
    with pytest.raises(ValueError):
        capture_package_source(source[0], revision)


@pytest.mark.parametrize("change", ["dirty", "extra", "empty_directory", "missing", "symlink", "invalid_utf8"])
def test_noncanonical_working_package_rejects(source, change):
    repo, revision = source
    root = repo / "packages/dbt-dpone"
    path = root / "INSTALL.md"
    if change == "dirty":
        path.write_text("PRIVATE_SENTINEL")
    elif change == "extra":
        (root / "unexpected.sql").write_text("PRIVATE_SENTINEL")
    elif change == "empty_directory":
        (root / "unexpected").mkdir()
    elif change == "missing":
        path.unlink()
    elif change == "symlink":
        path.unlink()
        path.symlink_to("dbt_project.yml")
    else:
        path.write_bytes(b"\xffPRIVATE_SENTINEL")
    with pytest.raises(ValueError) as caught:
        capture_package_source(repo, revision)
    assert "PRIVATE_SENTINEL" not in str(caught.value)


def test_executable_regular_blob_is_supported(source):
    repo, _ = source
    (repo / "packages/dbt-dpone/INSTALL.md").chmod(0o755)
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "Synthetic executable regular blob")
    assert len(capture_package_source(repo, git(repo, "rev-parse", "HEAD")).files) == 14


def test_unrelated_working_changes_do_not_invalidate_package(source):
    repo, revision = source
    (repo / "unrelated.txt").write_text("unrelated")
    assert capture_package_source(repo, revision).revision == revision


@pytest.mark.parametrize("change", ["missing", "extra", "symlink", "invalid_utf8", "wrong_name", "duplicate_yaml"])
def test_invalid_committed_package_rejects(source, change):
    repo, _ = source
    root = repo / "packages/dbt-dpone"
    if change == "missing":
        (root / "INSTALL.md").unlink()
    elif change == "extra":
        (root / "packages.yml").write_text("packages: []\n")
    elif change == "symlink":
        (root / "INSTALL.md").unlink()
        (root / "INSTALL.md").symlink_to("dbt_project.yml")
    elif change == "invalid_utf8":
        (root / "INSTALL.md").write_bytes(b"\xff")
    elif change == "wrong_name":
        (root / "dbt_project.yml").write_text("name: other_package\n")
    else:
        (root / "dbt_project.yml").write_text("name: other_package\nname: dbt_dpone\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "Synthetic malformed package")
    with pytest.raises(ValueError):
        capture_package_source(repo, git(repo, "rev-parse", "HEAD"))


def test_commit_not_on_current_history_rejects(source):
    repo, revision = source
    git(repo, "checkout", "--orphan", "unrelated")
    git(repo, "commit", "-qm", "Synthetic unrelated history")
    with pytest.raises(ValueError):
        capture_package_source(repo, revision)


def test_read_only_git_does_not_inherit_command_environment(source, monkeypatch):
    repo, revision = source
    monkeypatch.setenv("GIT_DIR", "/nonexistent/synthetic")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "alias.rev-parse")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "!false")
    assert capture_package_source(repo, revision).revision == revision


def test_dirty_index_rejects_even_when_working_bytes_match(source):
    repo, revision = source
    path = repo / "packages/dbt-dpone/INSTALL.md"
    original = path.read_bytes()
    path.write_bytes(b"staged synthetic change")
    git(repo, "add", ".")
    path.write_bytes(original)
    with pytest.raises(ValueError):
        capture_package_source(repo, revision)


def test_dirty_executable_mode_rejects(source):
    repo, revision = source
    (repo / "packages/dbt-dpone/INSTALL.md").chmod(0o755)
    with pytest.raises(ValueError):
        capture_package_source(repo, revision)


@pytest.fixture
def mirrored(source):
    from dpone.runtime.dbt_package_readiness import dbt_package_declaration_sha1
    from tests.test_dbt_starter_resources import STARTER_OUTPUTS

    repo, revision = source
    assets = repo / "src/dpone/_assets"
    for name in PACKAGE_FILES:
        target = assets / "dbt_dpone" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repo / "packages/dbt-dpone" / name).read_bytes())
    starter = assets / "dbt_starter/v4"
    for name in STARTER_OUTPUTS:
        path = starter / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic template\n")
    dependency = {
        "git": "https://github.com/PaulKov/dpone.git",
        "revision": revision,
        "subdirectory": "packages/dbt-dpone",
    }
    declaration = {"packages": [dependency]}
    # Explicit synthetic test lock: never shipped or called actual deps evidence.
    lock = {"packages": [{**dependency, "name": "dbt_dpone"}], "sha1_hash": dbt_package_declaration_sha1(declaration)}
    (starter / "packages.yml").write_text(yaml.safe_dump(declaration))
    (starter / "package-lock.yml").write_text(yaml.safe_dump(lock))
    return repo, revision, starter


def test_offline_check_does_not_write_or_invoke_dbt(mirrored, monkeypatch):
    repo, revision, _ = mirrored
    before = {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file()}
    run = subprocess.run
    commands = []

    def observe(command, **kwargs):
        assert Path(command[0]).name == "git"
        commands.append(command)
        return run(command, **kwargs)

    monkeypatch.setattr(subprocess, "run", observe)
    check_starter_resources(repo, revision)
    assert commands
    assert before == {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file()}


@pytest.mark.parametrize("change", ["revision", "name", "hash", "extra", "missing", "mirror", "template", "duplicate"])
def test_offline_check_rejects_drift(mirrored, change):
    repo, revision, starter = mirrored
    lock_path = starter / "package-lock.yml"
    lock = yaml.safe_load(lock_path.read_text())
    if change in {"revision", "name"}:
        lock["packages"][0][change] = "PRIVATE_SENTINEL"
    elif change == "hash":
        lock["sha1_hash"] = "0" * 40
    elif change == "extra":
        lock["packages"].append(dict(lock["packages"][0]))
    elif change == "missing":
        lock_path.unlink()
    elif change == "mirror":
        (repo / "src/dpone/_assets/dbt_dpone/INSTALL.md").write_text("PRIVATE_SENTINEL")
    elif change == "template":
        (starter / "extra.tmpl").write_text("PRIVATE_SENTINEL")
    elif change == "duplicate":
        lock_path.write_text("packages: []\npackages: []\n")
    if change in {"revision", "name", "hash", "extra"}:
        lock_path.write_text(yaml.safe_dump(lock))
    with pytest.raises(ValueError) as caught:
        check_starter_resources(repo, revision)
    assert "PRIVATE_SENTINEL" not in str(caught.value)
