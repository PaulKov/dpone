"""Synthetic Git inputs validate producer logic, not the shipped package."""

import json
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml
from tools.dbt_self_service.generate_starter_resources import capture_package_source, check_starter_resources

from tests.test_dbt_starter_resources import PACKAGE_FILES


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["--source-repo", "/missing"],
        ["--unknown", "PRIVATE_SENTINEL"],
        ["--source-repo", "/missing", "--revision", "a" * 40, "--recovery-report"],
        ["--source-repo", "/missing", "--check", "--recovery-report"],
    ],
)
def test_cli_invalid_arguments_are_private_json_without_io(arguments, monkeypatch, capsys):
    import tools.dbt_self_service.generate_starter_resources as module

    monkeypatch.setattr(module, "inspect_project_root", lambda *a, **k: pytest.fail("unexpected IO"))
    assert module.main(arguments) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out)["status"] == "INVALID_ARGUMENTS"
    assert "PRIVATE_SENTINEL" not in captured.out + captured.err


def test_cli_help_has_no_io(monkeypatch, capsys):
    import tools.dbt_self_service.generate_starter_resources as module

    monkeypatch.setattr(module, "inspect_project_root", lambda *a, **k: pytest.fail("unexpected IO"))
    assert module.main(["--help"]) == 0
    assert "--recovery-report" in capsys.readouterr().out


def test_cli_offline_check_and_recovery_do_not_resolve_dependencies(mirrored, capsys):
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, _ = mirrored
    before = {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file()}

    def forbidden(source):
        pytest.fail("offline must not resolve dependencies")

    assert main(["--source-repo", str(repo), "--revision", revision, "--check"], generate=forbidden) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    assert main(["--source-repo", str(repo), "--recovery-report"], generate=forbidden) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "CLEAR"
    assert before == {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*") if p.is_file()}


def dependency_fixture(starter):
    from tools.dbt_self_service.starter_dependency_generation import DependencyResources

    return DependencyResources(
        (starter / "packages.yml").read_bytes(),
        b"# synthetic generated lock\n" + (starter / "package-lock.yml").read_bytes(),
    )


def test_cli_composed_generation_changes_resources_then_noop(mirrored, capsys):
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    argv = ["--source-repo", str(repo), "--revision", revision]
    assert main(argv, generate=lambda source: dependencies) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["passed"] and first["changed_paths"]
    assert (starter / "package-lock.yml").read_bytes() == dependencies.package_lock_yml
    assert main(argv, generate=lambda source: dependencies) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "NOOP"


def test_cli_creates_missing_generated_resources(mirrored, capsys):
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    (starter / "package-lock.yml").unlink()
    missing = repo / "src/dpone/_assets/dbt_dpone/INSTALL.md"
    missing.unlink()
    assert main(["--source-repo", str(repo), "--revision", revision], generate=lambda s: dependencies) == 0
    assert json.loads(capsys.readouterr().out)["passed"]
    assert missing.read_bytes() == (repo / "packages/dbt-dpone/INSTALL.md").read_bytes()


@pytest.mark.parametrize("compensation_fails", [False, True])
def test_cli_actual_writer_interruption_reports_outcome(mirrored, monkeypatch, capsys, compensation_fails):
    import tools.dbt_self_service.starter_resource_transaction as writer
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    apply = writer.apply_resource_plan

    def interrupted(*args, **kwargs):
        def hook(phase, path):
            if phase == "after_mutation" or compensation_fails and phase == "before_compensate":
                raise RuntimeError("PRIVATE_SENTINEL")

        return apply(*args, **kwargs, phase_hook=hook)

    monkeypatch.setattr(writer, "apply_resource_plan", interrupted)
    assert main(["--source-repo", str(repo), "--revision", revision], generate=lambda s: dependencies) == (
        3 if compensation_fails else 1
    )
    result = capsys.readouterr().out
    assert "PRIVATE_SENTINEL" not in result
    assert json.loads(result)["recovery_required"] is compensation_fails
    assert main(["--source-repo", str(repo), "--recovery-report"]) == (3 if compensation_fails else 0)
    assert json.loads(capsys.readouterr().out)["pending"] is compensation_fails


def test_cli_retained_dependency_workspace_is_reported(mirrored, tmp_path, capsys):
    from tools.dbt_self_service.generate_starter_resources import main
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError

    repo, revision, _ = mirrored
    workspace = tmp_path / "synthetic-owned-workspace"
    workspace.mkdir()

    def generate(source):
        raise DependencyGenerationError(retained_workspace=workspace)

    assert main(["--source-repo", str(repo), "--revision", revision], generate=generate) == 3
    result = json.loads(capsys.readouterr().out)
    assert result["retained_workspace"] == str(workspace)
    assert workspace.is_dir()


@pytest.mark.parametrize("changed", ["source", "source_inode", "template", "destination", "same_bytes_inode"])
def test_cli_rejects_drift_during_dependencies_without_overwriting_foreign_file(mirrored, capsys, changed):
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    target = (
        repo / "packages/dbt-dpone/INSTALL.md"
        if changed in {"source", "source_inode"}
        else starter / "README.md.tmpl"
        if changed == "template"
        else starter / "packages.yml"
    )
    before_lock = (starter / "package-lock.yml").read_bytes()
    foreign = target.read_bytes() if changed in {"same_bytes_inode", "source_inode"} else b"foreign change\n"

    def generate(source):
        replacement = target.with_name("temporary-replacement")
        replacement.write_bytes(foreign)
        replacement.replace(target)
        return dependencies

    assert main(["--source-repo", str(repo), "--revision", revision], generate=generate) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAILED"
    assert target.read_bytes() == foreign
    assert (starter / "package-lock.yml").read_bytes() == before_lock


def test_cli_under_lock_destination_drift_is_rejected(mirrored, monkeypatch, capsys):
    from contextlib import contextmanager

    from tools.dbt_self_service.generate_starter_resources import main

    import dpone.adapters.project_authoring_lock as locks

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    actual_lock = locks.project_authoring_lock

    @contextmanager
    def lock(root):
        with actual_lock(root):
            target = starter / "packages.yml"
            replacement = target.with_name("replacement")
            replacement.write_bytes(target.read_bytes())
            replacement.replace(target)
            yield

    monkeypatch.setattr(locks, "project_authoring_lock", lock)
    assert main(["--source-repo", str(repo), "--revision", revision], generate=lambda s: dependencies) == 1
    assert not json.loads(capsys.readouterr().out)["passed"]
    assert (starter / "package-lock.yml").read_bytes() != dependencies.package_lock_yml


def test_cli_recovery_pending_blocks_dependency_generation(mirrored, capsys):
    from tools.dbt_self_service.generate_starter_resources import main

    from dpone.manifest.confined_transaction_journal import transaction_journal_name

    repo, revision, starter = mirrored
    (starter / transaction_journal_name("packages.yml")).write_bytes(b"invalid journal")

    def forbidden(source):
        pytest.fail("pending recovery must block generation")

    assert main(["--source-repo", str(repo), "--revision", revision], generate=forbidden) == 3
    result = json.loads(capsys.readouterr().out)
    assert result["pending"] and result["discovery_required"]


@pytest.mark.parametrize("before", [False, True])
def test_cli_unknown_asset_rejects_before_writer(mirrored, capsys, before):
    from tools.dbt_self_service.generate_starter_resources import main

    repo, revision, starter = mirrored
    dependencies = dependency_fixture(starter)
    unexpected = starter / "foreign.sql"
    if before:
        unexpected.write_bytes(b"foreign")

    def generate(source):
        assert not before, "invalid inventory should reject before dependencies"
        unexpected.write_bytes(b"foreign")
        return dependencies

    assert main(["--source-repo", str(repo), "--revision", revision], generate=generate) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAILED"
    assert unexpected.read_bytes() == b"foreign"
    assert not (repo / ".dpone-starter-resource-transactions").exists()


def test_cli_real_module_entrypoint_help_and_argument_error():
    import sys

    command = [sys.executable, "-m", "tools.dbt_self_service.generate_starter_resources"]
    help_result = subprocess.run([*command, "--help"], capture_output=True, text=True, timeout=30)
    assert help_result.returncode == 0 and "--check" in help_result.stdout
    invalid = subprocess.run([*command, "--unknown", "PRIVATE_SENTINEL"], capture_output=True, text=True, timeout=30)
    assert invalid.returncode == 2
    assert json.loads(invalid.stdout)["status"] == "INVALID_ARGUMENTS"
    assert "PRIVATE_SENTINEL" not in invalid.stdout + invalid.stderr


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


def test_source_capture_never_runs_repository_clean_filters(source):
    repo, revision = source
    marker = repo / "unexpected-filter-side-effect"
    (repo / ".gitattributes").write_text("packages/dbt-dpone/INSTALL.md filter=syntheticprobe\n")
    git(repo, "config", "filter.syntheticprobe.clean", "tee " + shlex.quote(str(marker)))
    path = repo / "packages/dbt-dpone/INSTALL.md"
    path.write_bytes(path.read_bytes())
    assert capture_package_source(repo, revision).revision == revision
    assert not marker.exists()


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
