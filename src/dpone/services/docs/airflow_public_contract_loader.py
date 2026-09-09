"""Bounded loader for the reviewed Airflow self-service contract baseline."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from .airflow_public_contract_models import (
    CliContract,
    LegacyWindow,
    PackageContract,
    ProviderCallable,
    ProviderParameter,
    PublicContractBaseline,
    PythonModuleContract,
    RequiredPositional,
)

BASELINE_SCHEMA = "dpone.airflow-self-service-public-contracts.v1"
_MAX_BASELINE_BYTES = 1024 * 1024
_TOP_LEVEL_KEYS = {
    "schema",
    "target_release",
    "policy",
    "cli",
    "python",
    "schemas",
    "packages",
    "compatibility",
}
_SCHEMA_KIND = re.compile(r"^dpone\.[a-z0-9][a-z0-9_.-]*\.v[1-9][0-9]*$")
_PYTHON_MODULE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$")


def load_public_contract_baseline(path: Path, *, yaml_codec: Any) -> PublicContractBaseline:
    data = path.read_bytes()
    if len(data) > _MAX_BASELINE_BYTES:
        _invalid("baseline exceeds 1 MiB")
    root = _mapping(yaml_codec.load(data.decode("utf-8")), "baseline")
    _exact_keys(root, _TOP_LEVEL_KEYS, "baseline")
    if root.get("schema") != BASELINE_SCHEMA:
        _invalid("unsupported baseline schema")
    policy = _mapping(root["policy"], "policy")
    _exact_keys(policy, {"semver", "additions", "removals", "provider_legacy_window"}, "policy")
    window_policy = _mapping(policy["provider_legacy_window"], "policy.provider_legacy_window")
    _exact_keys(window_policy, {"minimum_minor_releases", "minimum_days"}, "policy.provider_legacy_window")
    python = _mapping(root["python"], "python")
    if (
        not {"namespace", "exports", "callables"}
        <= set(python)
        <= {
            "namespace",
            "exports",
            "callables",
            "modules",
        }
    ):
        _invalid("python fields are invalid")
    schemas = _mapping(root["schemas"], "schemas")
    _exact_keys(schemas, {"required_kinds"}, "schemas")
    schema_kinds = _unique_texts(schemas["required_kinds"], "schemas.required_kinds", maximum=512)
    if any(_SCHEMA_KIND.fullmatch(item) is None for item in schema_kinds):
        _invalid("required schema kinds must use versioned dpone.*.vN identities")
    packages = _load_packages(root["packages"])
    if _text(python["namespace"], "python.namespace") != packages.provider_namespace:
        _invalid("python namespace and provider package namespace disagree")
    compatibility = _mapping(root["compatibility"], "compatibility")
    _exact_keys(compatibility, {"provider_legacy"}, "compatibility")
    legacy = _load_legacy_window(compatibility["provider_legacy"])
    if legacy.namespace != packages.legacy_namespace:
        _invalid("legacy namespace and package contract disagree")
    if legacy.minimum_minor_releases != _positive_int(
        window_policy["minimum_minor_releases"], "minimum releases"
    ) or legacy.minimum_days != _positive_int(window_policy["minimum_days"], "minimum days"):
        _invalid("provider legacy policy and compatibility window disagree")
    return PublicContractBaseline(
        schema=BASELINE_SCHEMA,
        target_release=_text(root["target_release"], "target_release"),
        semver=_text(policy["semver"], "policy.semver"),
        cli=_load_cli(root["cli"]),
        provider_exports=_unique_texts(python["exports"], "python.exports", maximum=128),
        provider_callables=_load_callables(python["callables"]),
        python_modules=_load_python_modules(python.get("modules", [])),
        schema_kinds=schema_kinds,
        packages=packages,
        legacy_window=legacy,
        canonical_payload=json.loads(json.dumps(root, default=str)),
    )


def _load_cli(raw: object) -> tuple[CliContract, ...]:
    result: list[CliContract] = []
    identities: set[str] = set()
    for index, value in enumerate(_list(raw, "cli", maximum=128)):
        item = _mapping(value, f"cli[{index}]")
        required_fields = {
            "id",
            "tier",
            "path",
            "required_positionals",
            "required_options",
            "example_arguments",
        }
        if not required_fields <= set(item) <= required_fields | {"lane"}:
            _invalid(f"cli[{index}] fields are invalid")
        identity = _text(item["id"], "cli.id")
        if identity in identities:
            _invalid(f"duplicate CLI identity: {identity}")
        identities.add(identity)
        positionals = tuple(
            _load_positional(value) for value in _list(item["required_positionals"], "required_positionals", maximum=16)
        )
        result.append(
            CliContract(
                identity=identity,
                tier=_text(item["tier"], "cli.tier"),
                path=_unique_texts(item["path"], f"cli[{index}].path", maximum=8, require_unique=False),
                required_positionals=positionals,
                required_options=_unique_texts(item["required_options"], "required_options", maximum=32),
                example_arguments=_unique_texts(
                    item.get("example_arguments", []),
                    "example_arguments",
                    maximum=32,
                    require_unique=False,
                ),
                lane=_text(item.get("lane", "shared"), "cli.lane"),
            )
        )
    return tuple(result)


def _load_positional(raw: object) -> RequiredPositional:
    item = _mapping(raw, "required_positional")
    if not {"name"} <= set(item) <= {"name", "accepted_value"}:
        _invalid("required positional fields are invalid")
    return RequiredPositional(
        name=_text(item["name"], "positional.name"),
        accepted_value=_optional_text(item.get("accepted_value")),
    )


def _load_callables(raw: object) -> tuple[ProviderCallable, ...]:
    result: list[ProviderCallable] = []
    identities: set[str] = set()
    for value in _list(raw, "python.callables", maximum=64):
        item = _mapping(value, "python.callable")
        _exact_keys(item, {"name", "parameters", "returns"}, "python.callable")
        name = _text(item["name"], "callable.name")
        if name in identities:
            _invalid(f"duplicate provider callable: {name}")
        identities.add(name)
        result.append(
            ProviderCallable(
                qualified_name=name,
                parameters=tuple(
                    _load_parameter(parameter)
                    for parameter in _list(item["parameters"], "callable.parameters", maximum=32)
                ),
                return_annotation=_text(item["returns"], "callable.returns"),
            )
        )
    return tuple(result)


def _load_parameter(raw: object) -> ProviderParameter:
    item = _mapping(raw, "callable.parameter")
    if not {"name", "kind", "required"} <= set(item) <= {"name", "kind", "required", "literal_values"}:
        _invalid("provider parameter fields are invalid")
    return ProviderParameter(
        name=_text(item["name"], "parameter.name"),
        kind=_text(item["kind"], "parameter.kind"),
        required=_bool(item["required"], "parameter.required"),
        literal_values=_unique_texts(item.get("literal_values", []), "parameter.literal_values", maximum=16),
    )


def _load_python_modules(raw: object) -> tuple[PythonModuleContract, ...]:
    result: list[PythonModuleContract] = []
    modules: set[str] = set()
    sources: set[str] = set()
    for index, value in enumerate(_list(raw, "python.modules", maximum=32)):
        item = _mapping(value, f"python.modules[{index}]")
        _exact_keys(item, {"module", "source", "callables"}, f"python.modules[{index}]")
        module = _text(item["module"], "python.module")
        source = _python_source(item["source"])
        if _PYTHON_MODULE.fullmatch(module) is None:
            _invalid("python module must be a dotted identifier")
        if module in modules or source in sources:
            _invalid("python modules contain duplicate module or source identities")
        callables = _load_callables(item["callables"])
        if not callables:
            _invalid("python module must freeze at least one callable")
        modules.add(module)
        sources.add(source)
        result.append(PythonModuleContract(module=module, source=source, callables=callables))
    return tuple(result)


def _python_source(raw: object) -> str:
    source = _text(raw, "python.module.source")
    path = PurePosixPath(source)
    if "\\" in source or path.is_absolute() or ".." in path.parts or path.suffix != ".py":
        _invalid("python module source must be a confined relative .py path")
    return source


def _load_packages(raw: object) -> PackageContract:
    item = _mapping(raw, "packages")
    fields = {
        "core_distribution",
        "reader_distribution",
        "provider_distribution",
        "provider_airflow_requirement",
        "provider_namespace",
        "legacy_namespace",
    }
    _exact_keys(item, fields, "packages")
    return PackageContract(**{field: _text(item[field], f"packages.{field}") for field in fields})


def _load_legacy_window(raw: object) -> LegacyWindow:
    item = _mapping(raw, "compatibility.provider_legacy")
    fields = {"namespace", "announced_release", "announced_date", "minimum_minor_releases", "minimum_days"}
    _exact_keys(item, fields, "compatibility.provider_legacy")
    announced_date = str(item["announced_date"])
    try:
        date.fromisoformat(announced_date)
    except ValueError:
        _invalid("provider legacy announced_date must be ISO YYYY-MM-DD")
    return LegacyWindow(
        namespace=_text(item["namespace"], "legacy.namespace"),
        announced_release=_text(item["announced_release"], "legacy.announced_release"),
        announced_date=announced_date,
        minimum_minor_releases=_positive_int(item["minimum_minor_releases"], "legacy minimum releases"),
        minimum_days=_positive_int(item["minimum_days"], "legacy minimum days"),
    )


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _invalid(f"{label} must be a mapping")
    return value


def _list(value: object, label: str, *, maximum: int) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        _invalid(f"{label} must be a list with at most {maximum} entries")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        _invalid(f"{label} fields are invalid")


def _unique_texts(value: object, label: str, *, maximum: int, require_unique: bool = True) -> tuple[str, ...]:
    values = tuple(_text(item, label) for item in _list(value, label, maximum=maximum))
    if require_unique and len(values) != len(set(values)):
        _invalid(f"{label} contains duplicates")
    return values


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1024:
        _invalid(f"{label} must be bounded non-empty text")
    return value.strip()


def _optional_text(value: object) -> str | None:
    return None if value is None else _text(value, "optional text")


def _positive_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        _invalid(f"{label} must be a positive integer")
    return value


def _bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        _invalid(f"{label} must be boolean")
    return value


def _invalid(message: str) -> NoReturn:
    raise ValueError(f"DPONE_PUBLIC_CONTRACT_BASELINE_INVALID: {message}")


__all__ = ["BASELINE_SCHEMA", "load_public_contract_baseline"]
