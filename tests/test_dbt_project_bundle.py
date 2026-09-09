from __future__ import annotations

import gzip
import io
import os
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml

from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_project_bundle import DbtProjectBundleLimits
from dpone.runtime.dbt_package_readiness import dbt_package_declaration_sha1
from dpone.runtime.dbt_project_bundle import (
    build_dbt_project_bundle,
    extract_dbt_project_bundle,
    verify_dbt_project_bundle_tree,
)


def _project(root: Path) -> Path:
    root.mkdir()
    (root / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    (root / "models").mkdir()
    (root / "models" / "z.sql").write_text("select 2\n", encoding="utf-8")
    (root / "models" / "a.sql").write_text("select 1\n", encoding="utf-8")
    return root


def _write_package_lock(
    project: Path,
    locked_packages: list[dict[str, object]],
    *,
    declaration_file: str = "packages.yml",
    package_environment: dict[str, str] | None = None,
) -> None:
    declaration = yaml.safe_load((project / declaration_file).read_text(encoding="utf-8"))
    lock = {
        "packages": locked_packages,
        "sha1_hash": dbt_package_declaration_sha1(
            declaration,
            package_environment=package_environment,
        ),
    }
    (project / "package-lock.yml").write_text(
        yaml.safe_dump(lock, sort_keys=False),
        encoding="utf-8",
    )


def _tar_bytes(name: str, body: bytes, *, kind: bytes = tarfile.REGTYPE) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.mode = 0o644
            member.mtime = 0
            member.size = len(body) if kind == tarfile.REGTYPE else 0
            archive.addfile(member, io.BytesIO(body) if kind == tarfile.REGTYPE else None)
    return raw.getvalue()


def _tar_files(count: int) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for index in range(count):
                body = b"name: analytics\n" if index == 0 else b"select 1\n"
                name = "dbt_project.yml" if index == 0 else f"models/model_{index:04d}.sql"
                member = tarfile.TarInfo(name)
                member.mode = 0o644
                member.mtime = 0
                member.size = len(body)
                archive.addfile(member, io.BytesIO(body))
    return raw.getvalue()


def _pax_metadata_bomb() -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            member = tarfile.TarInfo("dbt_project.yml")
            member.mode = 0o644
            member.mtime = 0
            member.size = 8
            member.pax_headers = {"comment": "x" * 1_000_000}
            archive.addfile(member, io.BytesIO(b"name: x\n"))
    return raw.getvalue()


def _canonical_archive(paths: tuple[tuple[str, bytes], ...]) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for name, body in paths:
                member = tarfile.TarInfo(name)
                member.mode = 0o644
                member.mtime = member.uid = member.gid = 0
                member.size = len(body)
                archive.addfile(member, io.BytesIO(body))
    return raw.getvalue()


def _gnu_longname_archive() -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.GNU_FORMAT) as archive:
            body = b"name: analytics\n"
            member = tarfile.TarInfo("x" * 101)
            member.mode = 0o644
            member.mtime = member.uid = member.gid = 0
            member.size = len(body)
            archive.addfile(member, io.BytesIO(body))
    return raw.getvalue()


def test_project_bundle_is_deterministic_normalized_and_secret_free(tmp_path: Path) -> None:
    project = _project(tmp_path / "analytics")
    (project / ".git").mkdir()
    (project / ".git" / "config").write_text("token=not-an-artifact\n", encoding="utf-8")
    (project / "target").mkdir()
    (project / "target" / "manifest.json").write_text("{}\n", encoding="utf-8")
    (project / "logs").mkdir()
    (project / "logs" / "dbt.log").write_text("password=not-an-artifact\n", encoding="utf-8")
    (project / "profiles.yml").write_text("password: not-an-artifact\n", encoding="utf-8")
    (project / ".env").write_text("PASSWORD=not-an-artifact\n", encoding="utf-8")
    (project / ".dpone-ci" / "dbt" / "release-a").mkdir(parents=True)
    (project / ".dpone-ci" / "dbt" / "release-a" / "release-set.json").write_text(
        '{"must_not_recurse": true}\n',
        encoding="utf-8",
    )

    first = build_dbt_project_bundle(project)
    os.chmod(project / "models" / "a.sql", 0o777)
    os.utime(project / "models" / "z.sql", (1_900_000_000, 1_900_000_000))
    second = build_dbt_project_bundle(project)

    assert first.archive == second.archive
    assert first.bundle.to_dict()["schema"] == "dpone.dbt-project-bundle.v1"
    assert [item.path for item in first.bundle.files] == [
        "dbt_project.yml",
        "models/a.sql",
        "models/z.sql",
    ]
    with tarfile.open(fileobj=io.BytesIO(first.archive), mode="r:gz") as archive:
        members = archive.getmembers()
        assert [member.name for member in members] == [
            "dbt_project.yml",
            "models/a.sql",
            "models/z.sql",
        ]
        assert all(member.isfile() for member in members)
        assert all(member.mode == 0o644 for member in members)
        assert all(member.mtime == 0 for member in members)
        assert all(member.uid == member.gid == 0 for member in members)


def test_project_bundle_rejects_pax_metadata_bomb(tmp_path: Path) -> None:
    destination = tmp_path / "destination"

    with pytest.raises(DbtPublishingError, match="archive member is unsafe"):
        extract_dbt_project_bundle(
            _pax_metadata_bomb(),
            destination,
            limits=DbtProjectBundleLimits(max_extracted_bytes=100),
        )


def test_project_bundle_rejects_effective_gnu_longname(tmp_path: Path) -> None:
    with pytest.raises(DbtPublishingError, match="canonical USTAR"):
        extract_dbt_project_bundle(
            _gnu_longname_archive(),
            tmp_path / "destination",
        )


def test_project_bundle_limits_generated_directory_count(tmp_path: Path) -> None:
    archive = _canonical_archive(
        (
            ("dbt_project.yml", b"name: analytics\n"),
            ("models/a/b/c/orders.sql", b"select 1\n"),
        )
    )
    destination = tmp_path / "destination"

    with pytest.raises(DbtPublishingError, match="directory-count limit"):
        extract_dbt_project_bundle(
            archive,
            destination,
            limits=DbtProjectBundleLimits(max_files=2),
        )

    assert not destination.exists() or not tuple(destination.iterdir())


def test_project_bundle_includes_pinned_packages_and_resolved_package_tree(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"name": "dbt_utils", "package": "dbt-labs/dbt_utils@1.3.0"}],
    )
    package = project / "dbt_packages" / "dbt_utils"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text("name: dbt_utils\n", encoding="utf-8")
    (package / "macros.sql").write_text("{% macro stable() %}1{% endmacro %}\n", encoding="utf-8")

    artifact = build_dbt_project_bundle(project)

    assert [item.path for item in artifact.bundle.files] == [
        "dbt_packages/dbt_utils/dbt_project.yml",
        "dbt_packages/dbt_utils/macros.sql",
        "dbt_project.yml",
        "models/a.sql",
        "models/z.sql",
        "package-lock.yml",
        "packages.yml",
    ]


def test_dbt_package_declaration_hash_matches_dbt_core_1_10() -> None:
    declaration = {
        "packages": [
            {
                "package": "dbt-labs/dbt_utils",
                "version": "1.3.0",
            }
        ]
    }

    assert dbt_package_declaration_sha1(declaration) == "226ae69cdfbc9367e2aa2c472b01f99dbce11de0"


def test_dbt_package_declaration_hash_matches_dbt_core_1_10_after_env_rendering() -> None:
    declaration = {
        "packages": [
            {
                "package": "dbt-labs/dbt_utils",
                "version": "{{ env_var('DPONE_DBT_UTILS_VERSION') }}",
            }
        ]
    }

    assert (
        dbt_package_declaration_sha1(
            declaration,
            package_environment={"DPONE_DBT_UTILS_VERSION": "1.3.0"},
        )
        == "f580d118376e6eca062034812235af05f3ab6e05"
    )


def test_project_bundle_accepts_current_env_rendered_package_lock(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    package_environment = {"DPONE_DBT_UTILS_VERSION": "1.3.0"}
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: \"{{ env_var('DPONE_DBT_UTILS_VERSION') }}\"\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"name": "dbt_utils", "package": "dbt-labs/dbt_utils@1.3.0"}],
        package_environment=package_environment,
    )
    package = project / "dbt_packages" / "dbt_utils"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text("name: dbt_utils\n", encoding="utf-8")

    artifact = build_dbt_project_bundle(
        project,
        package_environment=package_environment,
    )

    assert "packages.yml" in {item.path for item in artifact.bundle.files}


def test_project_bundle_rejects_env_rendered_package_lock_without_value(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: \"{{ env_var('DPONE_DBT_UTILS_VERSION') }}\"\n",
        encoding="utf-8",
    )
    (project / "package-lock.yml").write_text(
        "packages: []\nsha1_hash: '0000000000000000000000000000000000000000'\n",
        encoding="utf-8",
    )

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_PACKAGES_NOT_RESOLVED"
    assert "environment value is unavailable" in str(exc.value)


def test_project_bundle_rejects_stale_package_lock_for_changed_version(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    declaration = project / "packages.yml"
    declaration.write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"name": "dbt_utils", "package": "dbt-labs/dbt_utils@1.3.0"}],
    )
    declaration.write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.4.0\n",
        encoding="utf-8",
    )
    package = project / "dbt_packages" / "dbt_utils"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text("name: dbt_utils\n", encoding="utf-8")

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_PACKAGES_NOT_RESOLVED"
    assert "stale" in str(exc.value)


@pytest.mark.parametrize("declaration_file", ("packages.yml", "dependencies.yml"))
def test_project_bundle_rejects_non_file_package_declaration(
    tmp_path: Path,
    declaration_file: str,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / declaration_file).mkdir()

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_BUNDLE_INVALID"


@pytest.mark.parametrize(
    ("with_lock", "expected_code"),
    [
        (False, "DPONE_DBT_PACKAGE_LOCK_REQUIRED"),
        (True, "DPONE_DBT_PACKAGES_NOT_RESOLVED"),
    ],
)
def test_project_bundle_requires_locked_resolved_packages(
    tmp_path: Path,
    with_lock: bool,
    expected_code: str,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "dependencies.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    if with_lock:
        _write_package_lock(project, [], declaration_file="dependencies.yml")
    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == expected_code
    assert "dbt deps && dbt parse" in str(exc.value)


def test_project_bundle_rejects_non_package_content_in_resolved_root(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"name": "dbt_utils", "package": "dbt-labs/dbt_utils@1.3.0"}],
    )
    package_root = project / "dbt_packages"
    package_root.mkdir()
    (package_root / ".sentinel").write_text("not a resolved package\n", encoding="utf-8")

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_PACKAGES_NOT_RESOLVED"
    assert "dbt deps && dbt parse" in str(exc.value)


def test_project_bundle_rejects_package_tree_that_differs_from_lock(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"name": "dbt_utils", "package": "dbt-labs/dbt_utils@1.3.0"}],
    )
    package = project / "dbt_packages" / "unexpected"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text("name: unexpected\n", encoding="utf-8")

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_PACKAGES_NOT_RESOLVED"


def test_project_bundle_preserves_custom_packages_root_after_extraction(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "dbt_project.yml").write_text(
        "name: analytics\npackages-install-path: vendor/dbt\n",
        encoding="utf-8",
    )
    (project / "packages.yml").write_text(
        "packages:\n  - local: vendor-source\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"local": "vendor-source", "name": "local_fixture"}],
    )
    package = project / "vendor" / "dbt" / "local_fixture"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text("name: local_fixture\n", encoding="utf-8")
    (package / "macros.sql").write_text("{% macro stable() %}1{% endmacro %}\n", encoding="utf-8")

    artifact = build_dbt_project_bundle(project)
    extracted = tmp_path / "extracted"
    extract_dbt_project_bundle(artifact.archive, extracted)

    assert (extracted / "vendor" / "dbt" / "local_fixture" / "dbt_project.yml").is_file()
    assert verify_dbt_project_bundle_tree(artifact.archive, extracted) == artifact.bundle


@pytest.mark.skipif(
    not Path(current_environment_dbt_executable()).is_file(),
    reason="dbt CLI is not installed in the active Python environment",
)
def test_extracted_custom_package_bundle_supports_offline_dbt_parse(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / "dbt_project.yml").write_text(
        "\n".join(
            (
                "name: analytics",
                "version: 1.0.0",
                "config-version: 2",
                "profile: analytics",
                "packages-install-path: vendor/dbt",
                "",
            )
        ),
        encoding="utf-8",
    )
    (project / "packages.yml").write_text(
        "packages:\n  - local: vendor-source\n",
        encoding="utf-8",
    )
    _write_package_lock(
        project,
        [{"local": "vendor-source", "name": "local_fixture"}],
    )
    package = project / "vendor" / "dbt" / "local_fixture"
    package.mkdir(parents=True)
    (package / "dbt_project.yml").write_text(
        "name: local_fixture\nversion: 1.0.0\nconfig-version: 2\n",
        encoding="utf-8",
    )
    (package / "macros").mkdir()
    (package / "macros" / "stable.sql").write_text(
        "{% macro stable() %}1{% endmacro %}\n",
        encoding="utf-8",
    )
    artifact = build_dbt_project_bundle(project)
    extracted = tmp_path / "extracted"
    extract_dbt_project_bundle(artifact.archive, extracted)
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "\n".join(
            (
                "analytics:",
                "  target: parse",
                "  outputs:",
                "    parse:",
                "      type: sqlserver",
                "      driver: ODBC Driver 18 for SQL Server",
                "      server: localhost",
                "      port: 1433",
                "      database: analytics",
                "      schema: dbo",
                "      authentication: sql",
                "      user: fixture",
                "      password: fixture",
                "      encrypt: true",
                "      trust_cert: true",
                "      threads: 1",
                "",
            )
        ),
        encoding="utf-8",
    )

    result = subprocess.run(  # noqa: S603 - fixed dbt command and local fixture paths
        (
            current_environment_dbt_executable(),
            "--no-send-anonymous-usage-stats",
            "parse",
            "--no-partial-parse",
            "--project-dir",
            str(extracted),
            "--profiles-dir",
            str(profiles),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ),
        cwd=extracted,
        check=False,
        shell=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=30,
        env={key: value for key, value in os.environ.items() if key in {"HOME", "LANG", "LC_ALL", "PATH"}},
    )

    assert result.returncode == 0
    assert (tmp_path / "target" / "manifest.json").is_file()


@pytest.mark.parametrize(
    "name",
    [
        "production-signing-key.pem",
        "models/client-private.key",
        "models/service-account.json",
    ],
)
def test_project_bundle_rejects_known_credential_and_private_key_files(
    tmp_path: Path,
    name: str,
) -> None:
    project = _project(tmp_path / "analytics")
    path = project / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("must-not-enter-artifacts\n", encoding="utf-8")

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_BUNDLE_INVALID"
    assert "must-not-enter-artifacts" not in str(exc.value)


def test_project_bundle_rejects_secret_like_content_without_echoing_it(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path / "analytics")
    secret = "must-never-enter-release-bytes"
    (project / "models" / "leak.sql").write_text(
        f"select 'password={secret}'\n",
        encoding="utf-8",
    )

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_BUNDLE_INVALID"
    assert "secret-like source content" in str(exc.value)
    assert secret not in str(exc.value)


def test_project_bundle_uses_only_configured_dbt_topology(tmp_path: Path) -> None:
    project = _project(tmp_path / "analytics")
    (project / "notes").mkdir()
    (project / "notes" / "architecture.txt").write_text("not runtime input\n", encoding="utf-8")
    (project / "custom_macros").mkdir()
    (project / "custom_macros" / "publish.sql").write_text(
        "{% macro publish() %}1{% endmacro %}\n",
        encoding="utf-8",
    )
    (project / "dbt_project.yml").write_text(
        "name: analytics\nmacro-paths: [custom_macros]\n",
        encoding="utf-8",
    )

    artifact = build_dbt_project_bundle(project)

    assert [item.path for item in artifact.bundle.files] == [
        "custom_macros/publish.sql",
        "dbt_project.yml",
        "models/a.sql",
        "models/z.sql",
    ]


def test_project_bundle_rejects_symlinks_without_reading_the_target(tmp_path: Path) -> None:
    project = _project(tmp_path / "analytics")
    outside = tmp_path / "outside.sql"
    outside.write_text("select 'must not be bundled'\n", encoding="utf-8")
    (project / "models" / "linked.sql").symlink_to(outside)

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project)

    assert exc.value.code == "DPONE_DBT_BUNDLE_INVALID"
    assert "outside" not in str(exc.value)


@pytest.mark.parametrize(
    ("limits", "extra_name", "extra_body"),
    [
        (DbtProjectBundleLimits(max_files=2), "models/b.sql", b"select 3\n"),
        (DbtProjectBundleLimits(max_file_bytes=4), "models/b.sql", b"select 3\n"),
        (DbtProjectBundleLimits(max_extracted_bytes=20), "models/b.sql", b"select 3\n"),
    ],
)
def test_project_bundle_enforces_configured_source_limits(
    tmp_path: Path,
    limits: DbtProjectBundleLimits,
    extra_name: str,
    extra_body: bytes,
) -> None:
    project = _project(tmp_path / "analytics")
    (project / extra_name).write_bytes(extra_body)

    with pytest.raises(DbtPublishingError) as exc:
        build_dbt_project_bundle(project, limits=limits)

    assert exc.value.code == "DPONE_DBT_BUNDLE_LIMIT_EXCEEDED"


def test_extract_and_verify_use_exact_root_relative_layout(tmp_path: Path) -> None:
    artifact = build_dbt_project_bundle(_project(tmp_path / "analytics"))
    destination = tmp_path / "runtime-project"

    extracted = extract_dbt_project_bundle(artifact.archive, destination)
    verified = verify_dbt_project_bundle_tree(artifact.archive, destination)

    assert extracted == artifact.bundle
    assert verified == artifact.bundle
    assert (destination / "dbt_project.yml").read_text(encoding="utf-8") == "name: analytics\n"
    assert (destination / "models" / "a.sql").read_text(encoding="utf-8") == "select 1\n"
    assert not (destination / "analytics").exists()
    (destination / "models" / "a.sql").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(DbtPublishingError) as exc:
        verify_dbt_project_bundle_tree(artifact.archive, destination)
    assert exc.value.code == "DPONE_DBT_BUNDLE_TREE_MISMATCH"


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param(_tar_bytes("../escape.sql", b"select 1\n"), id="traversal"),
        pytest.param(_tar_bytes("models/link.sql", b"", kind=tarfile.SYMTYPE), id="symlink"),
    ],
)
def test_extract_rejects_unsafe_archive_before_writing(tmp_path: Path, archive: bytes) -> None:
    destination = tmp_path / "runtime-project"

    with pytest.raises(DbtPublishingError) as exc:
        extract_dbt_project_bundle(archive, destination)

    assert exc.value.code == "DPONE_DBT_BUNDLE_INVALID"
    assert destination.is_dir()
    assert list(destination.iterdir()) == []


def test_extract_rejects_symlinked_archive_path_and_destination(tmp_path: Path) -> None:
    artifact = build_dbt_project_bundle(_project(tmp_path / "analytics"))
    archive_path = tmp_path / "bundle.tar.gz"
    archive_path.write_bytes(artifact.archive)
    archive_link = tmp_path / "bundle-link.tar.gz"
    archive_link.symlink_to(archive_path)

    with pytest.raises(DbtPublishingError) as archive_exc:
        extract_dbt_project_bundle(archive_link, tmp_path / "from-link")
    assert archive_exc.value.code == "DPONE_DBT_BUNDLE_INVALID"

    outside = tmp_path / "outside"
    outside.mkdir()
    destination = tmp_path / "runtime-project"
    destination.symlink_to(outside, target_is_directory=True)
    with pytest.raises(DbtPublishingError) as destination_exc:
        extract_dbt_project_bundle(artifact.archive, destination)
    assert destination_exc.value.code == "DPONE_DBT_BUNDLE_INVALID"
    assert list(outside.iterdir()) == []


def test_archive_processing_is_streaming_and_enforces_member_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _tar_files(3)
    monkeypatch.setattr(
        tarfile.TarFile,
        "getmembers",
        lambda _self: pytest.fail("archive members must not be materialized"),
    )

    with pytest.raises(DbtPublishingError) as exc:
        extract_dbt_project_bundle(
            archive,
            tmp_path / "runtime-project",
            limits=DbtProjectBundleLimits(max_files=2),
        )

    assert exc.value.code == "DPONE_DBT_BUNDLE_LIMIT_EXCEEDED"
    assert list((tmp_path / "runtime-project").iterdir()) == []


def test_archive_inspection_and_extraction_stream_without_getmembers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = build_dbt_project_bundle(_project(tmp_path / "analytics"))
    monkeypatch.setattr(
        tarfile.TarFile,
        "getmembers",
        lambda _self: pytest.fail("archive members must not be materialized"),
    )

    extracted = extract_dbt_project_bundle(artifact.archive, tmp_path / "runtime-project")

    assert extracted == artifact.bundle
