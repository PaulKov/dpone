"""Pure dbt package declaration and lock readiness policy."""

from __future__ import annotations

import ast
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.runtime.dbt_project_bundle_safety import (
    read_root_yaml_mapping,
    resolved_package_names,
    root_regular_file,
)

_DECLARATION_SOURCE_KEYS = frozenset({"git", "local", "package", "private", "tarball"})
_SHA1_HEX_LENGTH = 40


def selected_package_declaration(
    packages: Mapping[str, Any] | None,
    dependencies: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    """Apply dbt's packages.yml/dependencies.yml authority rule."""

    packages = {} if packages is None else packages
    dependencies = {} if dependencies is None else dependencies
    if "packages" in packages and "packages" in dependencies:
        raise _packages_not_resolved("packages are declared in both supported files")
    if "projects" in packages:
        raise _packages_not_resolved("packages.yml cannot contain projects")
    if "packages" in dependencies:
        return {"packages": dependencies["packages"]}
    return packages


def require_current_package_lock(
    declaration: Mapping[str, Any],
    lock: Mapping[str, Any],
    *,
    package_environment: Mapping[str, str],
) -> tuple[str, ...]:
    """Verify dbt 1.10 declaration identity and return locked package names."""

    actual_hash = lock.get("sha1_hash")
    if (
        not isinstance(actual_hash, str)
        or len(actual_hash) != _SHA1_HEX_LENGTH
        or any(character not in "0123456789abcdef" for character in actual_hash)
    ):
        raise _packages_not_resolved("package-lock.yml has no valid declaration hash")
    expected_hash = dbt_package_declaration_sha1(
        declaration,
        package_environment=package_environment,
    )
    if actual_hash != expected_hash:
        raise _packages_not_resolved("package-lock.yml is stale for the current declaration")
    return locked_package_names(lock)


def require_resolved_packages(
    root_descriptor: int,
    package_roots: tuple[PurePosixPath, ...],
    *,
    declaration_files: tuple[str, ...],
    maximum: int,
    package_environment: Mapping[str, str],
) -> None:
    """Verify declaration, dbt lock identity, and resolved package tree."""

    if not declaration_files:
        return
    if not root_regular_file(root_descriptor, "package-lock.yml"):
        raise DbtPublishingError(
            "DPONE_DBT_PACKAGE_LOCK_REQUIRED",
            "dbt package declarations require package-lock.yml; run dbt deps && dbt parse",
        )
    declarations = {
        name: read_root_yaml_mapping(
            root_descriptor,
            name,
            maximum=maximum,
            error_code="DPONE_DBT_PACKAGES_NOT_RESOLVED",
        )
        for name in declaration_files
    }
    lock = read_root_yaml_mapping(
        root_descriptor,
        "package-lock.yml",
        maximum=maximum,
        error_code="DPONE_DBT_PACKAGES_NOT_RESOLVED",
    )
    declaration = selected_package_declaration(
        declarations.get("packages.yml"),
        declarations.get("dependencies.yml"),
    )
    expected = require_current_package_lock(
        declaration,
        lock,
        package_environment=package_environment,
    )
    observed = tuple(
        sorted(
            {name for package_root in package_roots for name in resolved_package_names(root_descriptor, package_root)}
        )
    )
    if not expected or observed != expected:
        raise _packages_not_resolved("dbt packages tree differs from package-lock.yml")


def dbt_package_declaration_sha1(
    declaration: Mapping[str, Any],
    *,
    package_environment: Mapping[str, str] | None = None,
) -> str:
    """Reproduce dbt-core 1.10's package declaration lock hash offline."""

    environment = {} if package_environment is None else package_environment
    raw_packages = declaration.get("packages")
    if not isinstance(raw_packages, Sequence) or isinstance(raw_packages, str | bytes):
        raise _packages_not_resolved("package declaration has no valid package inventory")
    serialized = sorted(
        json.dumps(
            _normalized_package_spec(item, environment),
            ensure_ascii=True,
            sort_keys=True,
        )
        for item in raw_packages
    )
    return hashlib.sha1("\n".join(serialized).encode("utf-8")).hexdigest()  # noqa: S324 - dbt lock compatibility


def locked_package_names(lock: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the exact normalized package names asserted by a dbt lock."""

    raw_packages = lock.get("packages")
    if not isinstance(raw_packages, Sequence) or isinstance(raw_packages, str | bytes):
        raise _packages_not_resolved("package-lock.yml has no valid package inventory")
    names: list[str] = []
    for item in raw_packages:
        name = item.get("name") if isinstance(item, Mapping) else None
        if (
            not isinstance(name, str)
            or not name
            or name != name.strip()
            or "/" in name
            or "\\" in name
            or name in {".", ".."}
        ):
            raise _packages_not_resolved("package-lock.yml contains an invalid package name")
        names.append(name)
    if len(names) != len(set(names)):
        raise _packages_not_resolved("package-lock.yml contains duplicate package names")
    return tuple(sorted(names))


def _normalized_package_spec(
    value: object,
    package_environment: Mapping[str, str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _packages_not_resolved("package declaration contains a non-object entry")
    raw = dict(value)
    source_keys = _DECLARATION_SOURCE_KEYS.intersection(raw)
    if len(source_keys) != 1 or "unrendered" in raw:
        raise _packages_not_resolved("package declaration contains an invalid source")
    source = next(iter(source_keys))
    rendered = _render_package_value(raw, package_environment)
    if not isinstance(rendered, Mapping):
        raise _packages_not_resolved("package declaration contains an invalid package entry")
    normalized = _source_fields(source, rendered)
    normalized["unrendered"] = raw
    name = rendered.get("name")
    if name is not None and source != "tarball":
        normalized["name"] = name
    return normalized


def _render_package_value(
    value: Any,
    package_environment: Mapping[str, str],
) -> Any:
    if isinstance(value, Mapping):
        return {key: _render_package_value(item, package_environment) for key, item in value.items()}
    if isinstance(value, list):
        return [_render_package_value(item, package_environment) for item in value]
    if not isinstance(value, str) or "{{" not in value:
        return value
    stripped = value.strip()
    if not stripped.startswith("{{") or not stripped.endswith("}}"):
        raise _packages_not_resolved("package declaration contains an unsupported template expression")
    try:
        expression = ast.parse(stripped[2:-2].strip(), mode="eval").body
    except (SyntaxError, ValueError) as exc:
        raise _packages_not_resolved("package declaration contains an invalid template expression") from exc
    if (
        not isinstance(expression, ast.Call)
        or not isinstance(expression.func, ast.Name)
        or expression.func.id != "env_var"
        or expression.keywords
        or len(expression.args) not in {1, 2}
        or any(
            not isinstance(argument, ast.Constant) or not isinstance(argument.value, str)
            for argument in expression.args
        )
    ):
        raise _packages_not_resolved("package declaration contains an unsupported template expression")
    name = expression.args[0].value
    if name in package_environment:
        return package_environment[name]
    if len(expression.args) == 2:
        return expression.args[1].value
    raise _packages_not_resolved("package declaration environment value is unavailable")


def _source_fields(source: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    if source == "package":
        return {
            "package": _required_value(raw, "package"),
            "version": _required_value(raw, "version"),
            "install_prerelease": raw.get("install_prerelease", False),
        }
    if source == "local":
        return {"local": _required_value(raw, "local")}
    if source == "tarball":
        return {
            "tarball": _required_value(raw, "tarball"),
            "name": _required_value(raw, "name"),
        }
    if source == "git":
        return {
            "git": _required_value(raw, "git"),
            "revision": raw.get("revision"),
            "warn-unpinned": raw.get("warn-unpinned"),
            "subdirectory": raw.get("subdirectory"),
        }
    return {
        "private": _required_value(raw, "private"),
        "provider": raw.get("provider"),
        "revision": raw.get("revision"),
        "warn-unpinned": raw.get("warn-unpinned"),
        "subdirectory": raw.get("subdirectory"),
    }


def _required_value(raw: Mapping[str, Any], name: str) -> Any:
    value = raw.get(name)
    if value is None or value == "":
        raise _packages_not_resolved(f"package declaration is missing {name}")
    return value


def _packages_not_resolved(message: str) -> DbtPublishingError:
    return DbtPublishingError(
        "DPONE_DBT_PACKAGES_NOT_RESOLVED",
        f"{message}; run dbt deps && dbt parse",
    )


__all__ = [
    "dbt_package_declaration_sha1",
    "locked_package_names",
    "require_current_package_lock",
    "require_resolved_packages",
    "selected_package_declaration",
]
