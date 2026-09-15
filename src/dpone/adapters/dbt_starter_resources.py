"""Acquire the closed starter inventory from installed package resources.

No source-checkout fallback or dependency resolution occurs here. Package bytes
and dependency locks must be prepared by the explicit resource producer. This
reader checks completeness and UTF-8 integrity, not runtime qualification.
"""

from __future__ import annotations

from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path

from dpone.readiness.airflow_scaffold_apply import ScaffoldFile

_PACKAGE_FILES = (
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
)
_STARTER_FILES = (
    ("dbt_project.yml.tmpl", "dbt_project.yml"),
    ("profiles.yml.tmpl", "profiles/profiles.yml"),
    ("models/orders.sql.tmpl", "models/orders.sql"),
    ("models/schema.yml.tmpl", "models/schema.yml"),
    ("README.md.tmpl", "README.md"),
    ("gitignore.tmpl", ".gitignore"),
    ("packages.yml", "packages.yml"),
    ("package-lock.yml", "package-lock.yml"),
)
_INVALID = "The installed dbt starter resource inventory is incomplete or invalid."


class InstalledDbtStarterResources:
    """Read 22 immutable resources into their intended project-relative paths.

    The six template outputs remain unrendered. The authoring service performs
    explicit placeholder substitution and adds the supplied policy snapshot as
    the twenty-third file. Construction performs no lookup or I/O.
    """

    def files(self) -> tuple[ScaffoldFile, ...]:
        try:
            anchor = resources.files("dpone")
            assets = _directory(anchor, "_assets")
            package = _directory(assets, "dbt_dpone")
            starter = _directory(_directory(assets, "dbt_starter"), "v4")
            package_text = _read_inventory(package, _PACKAGE_FILES)
            starter_text = _read_inventory(starter, tuple(name for name, _ in _STARTER_FILES))
        except (OSError, UnicodeError, ValueError):
            raise ValueError(_INVALID) from None
        result = [ScaffoldFile(Path("dbt_packages/dbt_dpone") / name, text) for name, text in package_text.items()]
        result.extend(ScaffoldFile(Path(output), starter_text[name]) for name, output in _STARTER_FILES)
        return tuple(sorted(result, key=lambda file: file.path.as_posix()))


def _directory(parent: Traversable, name: str) -> Traversable:
    child = parent.joinpath(name)
    _reject_symlink(child)
    if not child.is_dir():
        raise ValueError(_INVALID)
    return child


def _read_inventory(root: Traversable, names: tuple[str, ...]) -> dict[str, str]:
    """Walk only declared directory branches and reject extra entries early."""
    expected = {name.split("/", 1)[0] for name in names}
    observed: set[str] = set()
    for child in root.iterdir():
        if child.name not in expected or child.name in observed:
            raise ValueError(_INVALID)
        observed.add(child.name)
    if observed != expected:
        raise ValueError(_INVALID)
    result: dict[str, str] = {}
    for name in sorted(expected):
        child = root.joinpath(name)
        _reject_symlink(child)
        descendants = tuple(path[len(name) + 1 :] for path in names if path.startswith(name + "/"))
        if descendants:
            nested = _read_inventory(_directory(root, name), descendants)
            result.update((name + "/" + path, text) for path, text in nested.items())
        else:
            if not child.is_file():
                raise ValueError(_INVALID)
            result[name] = child.read_bytes().decode("utf-8")
    return result


def _reject_symlink(resource: Traversable) -> None:
    # Installed directories use Path; archive resources have no filesystem link
    # traversal. Avoid converting a general Traversable into a checkout path.
    if isinstance(resource, Path) and resource.is_symlink():
        raise ValueError(_INVALID)


__all__ = ["InstalledDbtStarterResources"]
