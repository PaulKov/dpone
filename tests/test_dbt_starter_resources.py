"""Installed-only starter acquisition; synthetic resources do not certify a wheel."""

from pathlib import Path
from zipfile import Path as ZipPath
from zipfile import ZipFile

import pytest

from dpone.adapters import dbt_starter_resources as resource_module
from dpone.adapters.dbt_starter_resources import InstalledDbtStarterResources

PACKAGE_FILES = (
    "dbt_project.yml",
    "INSTALL.md",
    "macros/dpone_publish.sql",
    "macros/semantic_refresh_restore.sql",
    "macros/semantic_refresh_scope_merge.sql",
    "macros/materializations/mssql_managed_table.sql",
    "macros/physical/mssql_admission.sql",
    "macros/physical/mssql_candidate.sql",
    "macros/physical/mssql_catalog.sql",
    "macros/physical/mssql_receipt.sql",
    "control/sqlserver/physical-v1/schema.sql",
    "control/sqlserver/physical-v1/admission.sql",
    "control/sqlserver/physical-v1/catalog.sql",
    "control/sqlserver/physical-v1/receipt.sql",
    "control/sqlserver/physical-v1/catalog-v2.sql",
)
STARTER_OUTPUTS = {
    "dbt_project.yml.tmpl": "dbt_project.yml",
    "profiles.yml.tmpl": "profiles/profiles.yml",
    "models/orders.sql.tmpl": "models/orders.sql",
    "models/schema.yml.tmpl": "models/schema.yml",
    "README.md.tmpl": "README.md",
    "gitignore.tmpl": ".gitignore",
    "packages.yml": "packages.yml",
    "package-lock.yml": "package-lock.yml",
}


@pytest.fixture
def installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    anchor = tmp_path / "installed" / "dpone"
    for prefix, names in (("dbt_dpone", PACKAGE_FILES), ("dbt_starter/v4", STARTER_OUTPUTS)):
        for name in names:
            path = anchor / "_assets" / prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(("synthetic é " + name + "\r\n").encode())
    monkeypatch.setattr(resource_module.resources, "files", lambda package: anchor if package == "dpone" else None)
    return anchor


def test_complete_inventory_preserves_exact_utf8_bytes(installed: Path) -> None:
    actual = {item.path.as_posix(): item.text.encode() for item in InstalledDbtStarterResources().files()}
    expected = {
        "dbt_packages/dbt_dpone/" + name: (installed / "_assets/dbt_dpone" / name).read_bytes()
        for name in PACKAGE_FILES
    }
    expected.update(
        {out: (installed / "_assets/dbt_starter/v4" / name).read_bytes() for name, out in STARTER_OUTPUTS.items()}
    )
    assert len(actual) == 23
    assert actual == expected
    assert tuple(actual) == tuple(sorted(actual))


@pytest.mark.parametrize("name", PACKAGE_FILES)
def test_every_missing_package_resource_rejects(installed: Path, name: str) -> None:
    (installed / "_assets/dbt_dpone" / name).unlink()
    with pytest.raises(ValueError, match="installed dbt starter"):
        InstalledDbtStarterResources().files()


@pytest.mark.parametrize("name", STARTER_OUTPUTS)
def test_every_missing_starter_resource_rejects(installed: Path, name: str) -> None:
    (installed / "_assets/dbt_starter/v4" / name).unlink()
    with pytest.raises(ValueError):
        InstalledDbtStarterResources().files()


@pytest.mark.parametrize("kind", ["extra_file", "empty_directory", "invalid_utf8", "directory_instead_of_file"])
def test_malformed_package_rejects(installed: Path, kind: str) -> None:
    root = installed / "_assets/dbt_dpone"
    if kind == "extra_file":
        (root / "unexpected.sql").write_text("PRIVATE_SENTINEL")
    elif kind == "empty_directory":
        (root / "unexpected").mkdir()
    elif kind == "invalid_utf8":
        (root / "INSTALL.md").write_bytes(b"PRIVATE_SENTINEL\xff")
    else:
        (root / "INSTALL.md").unlink()
        (root / "INSTALL.md").mkdir()
    with pytest.raises(ValueError) as caught:
        InstalledDbtStarterResources().files()
    assert "PRIVATE_SENTINEL" not in str(caught.value)


@pytest.mark.parametrize(
    "name", ["_assets", "_assets/dbt_dpone", "_assets/dbt_dpone/macros", "_assets/dbt_dpone/INSTALL.md"]
)
def test_symlinked_resource_components_reject(installed: Path, name: str) -> None:
    original = installed / name
    moved = original.with_name(original.name + "_actual")
    original.rename(moved)
    original.symlink_to(moved, target_is_directory=moved.is_dir())
    with pytest.raises(ValueError):
        InstalledDbtStarterResources().files()


def test_constructor_does_not_acquire_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object) -> None:
        raise AssertionError("unexpected resource lookup")

    monkeypatch.setattr(resource_module.resources, "files", fail)
    InstalledDbtStarterResources()


def test_missing_installed_roots_have_no_checkout_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resource_module.resources, "files", lambda package: tmp_path)
    with pytest.raises(ValueError):
        InstalledDbtStarterResources().files()


def test_zip_traversable_without_filesystem_checkout(
    installed: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "resources.zip"
    with ZipFile(archive, "w") as output:
        for path in installed.rglob("*"):
            if path.is_file():
                output.writestr("dpone/" + path.relative_to(installed).as_posix(), path.read_bytes())
    with ZipFile(archive) as package:
        monkeypatch.setattr(resource_module.resources, "files", lambda name: ZipPath(package, "dpone/"))
        files = InstalledDbtStarterResources().files()
    assert len(files) == 23
    assert all("\r\n" in file.text for file in files)


def test_real_templates_without_managed_package_do_not_form_a_complete_distribution(
    tmp_path: Path, monkeypatch
) -> None:
    authored = Path(__file__).parents[1] / "src/dpone/_assets/dbt_starter/v4"
    anchor = tmp_path / "dpone"
    for name in STARTER_OUTPUTS:
        if name.endswith(".tmpl"):
            target = anchor / "_assets/dbt_starter/v4" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((authored / name).read_bytes())
    monkeypatch.setattr(resource_module.resources, "files", lambda package: anchor)
    with pytest.raises(ValueError, match="installed dbt starter"):
        InstalledDbtStarterResources().files()
