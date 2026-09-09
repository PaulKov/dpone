"""Compatibility evaluation for the frozen Airflow self-service contract."""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import tomllib
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

from .airflow_public_contract_models import (
    CliContract,
    ContractCheckCount,
    LegacyWindow,
    ProviderApi,
    ProviderCallable,
    PublicContractBaseline,
    PublicContractIssue,
    PublicContractReport,
    PythonModuleApi,
    PythonModuleContract,
)


def evaluate_public_contracts(
    baseline: PublicContractBaseline,
    *,
    parser: argparse.ArgumentParser,
    provider: ProviderApi,
    python_modules: Sequence[PythonModuleApi] = (),
    schema_kinds: Sequence[str],
    package_root: Path,
    today: date,
) -> PublicContractReport:
    issues: list[PublicContractIssue] = []
    checks = {
        "cli": ContractCheckCount(len(baseline.cli), _check_cli(baseline.cli, parser, issues)),
        "python": ContractCheckCount(len(baseline.provider_exports), _check_provider(baseline, provider, issues)),
        "python_modules": ContractCheckCount(
            sum(len(module.callables) for module in baseline.python_modules),
            _check_python_modules(baseline.python_modules, python_modules, issues),
        ),
        "schemas": ContractCheckCount(
            len(baseline.schema_kinds), _check_schemas(baseline.schema_kinds, schema_kinds, issues)
        ),
        "packages": ContractCheckCount(1, _check_packages(baseline, package_root, today, issues)),
    }
    issues.sort(key=lambda item: (item.code, item.subject))
    return PublicContractReport(
        target_release=baseline.target_release,
        fingerprint=_baseline_fingerprint(baseline),
        checks=checks,
        compatibility=_compatibility_projection(baseline.legacy_window, today=today),
        issues=tuple(issues),
    )


def render_public_contract_report_text(report: PublicContractReport) -> str:
    if report.passed:
        checks = "\n".join(
            f"- {name}: {count.compatible}/{count.required} compatible" for name, count in sorted(report.checks.items())
        )
        return (
            "Airflow public contract: PASSED\n"
            f"- target release: {report.target_release}\n"
            f"- baseline: {report.fingerprint}\n{checks}"
        )
    lines = ["Airflow public contract: FAILED", f"- target release: {report.target_release}"]
    lines.extend(f"- {issue.code}: {issue.subject} - {issue.message}" for issue in report.issues)
    return "\n".join(lines)


def _check_cli(
    contracts: tuple[CliContract, ...], parser: argparse.ArgumentParser, issues: list[PublicContractIssue]
) -> int:
    compatible = 0
    for contract in contracts:
        command_parser = _find_parser(parser, contract.path)
        if command_parser is None:
            issues.append(_issue("DPONE_PUBLIC_CLI_COMMAND_MISSING", contract.identity, "Command path is missing."))
            continue
        current_parser = command_parser
        actions = _parser_actions(current_parser)
        option_actions = dict(actions)
        missing: list[str] = []
        for positional in contract.required_positionals:
            action = actions.get(positional.name)
            if action is None or action.option_strings:
                missing.append(positional.name)
            elif positional.accepted_value is not None and positional.accepted_value not in tuple(action.choices or ()):
                missing.append(f"{positional.name}={positional.accepted_value}")
            elif positional.accepted_value is not None and isinstance(action, argparse._SubParsersAction):
                current_parser = action.choices[positional.accepted_value]
                actions = _parser_actions(current_parser)
                option_actions.update(actions)
        options = {option for action in option_actions.values() for option in action.option_strings}
        missing.extend(option for option in contract.required_options if option not in options)
        baseline_options = set(contract.required_options)
        additional_required = sorted(
            option
            for action in option_actions.values()
            if action.required and action.option_strings
            for option in action.option_strings
            if option.startswith("--") and option not in baseline_options
        )
        if additional_required:
            issues.append(
                _issue(
                    "DPONE_PUBLIC_CLI_NEW_REQUIRED_ARGUMENT",
                    contract.identity,
                    "New required CLI options break baseline invocations: " + ", ".join(additional_required),
                )
            )
        if missing:
            issues.append(
                _issue(
                    "DPONE_PUBLIC_CLI_ARGUMENT_MISSING",
                    contract.identity,
                    "Required CLI surface is missing: " + ", ".join(sorted(missing)),
                )
            )
        elif not additional_required:
            compatible += 1
    return compatible


def _parser_actions(parser: argparse.ArgumentParser) -> dict[str, argparse.Action]:
    return {action.dest: action for action in parser._actions if not isinstance(action, argparse._HelpAction)}


def _check_provider(baseline: PublicContractBaseline, provider: ProviderApi, issues: list[PublicContractIssue]) -> int:
    actual_exports = set(provider.exports)
    compatible = 0
    for name in baseline.provider_exports:
        if name not in actual_exports:
            issues.append(_issue("DPONE_PUBLIC_PROVIDER_EXPORT_MISSING", name, "Provider export is missing."))
        else:
            compatible += 1
    actual_callables = provider.callable_map()
    for expected in baseline.provider_callables:
        actual = actual_callables.get(expected.qualified_name)
        if actual is None or not _callable_compatible(expected, actual):
            issues.append(
                _issue(
                    "DPONE_PUBLIC_PROVIDER_SIGNATURE_INCOMPATIBLE",
                    expected.qualified_name,
                    "Provider callable signature is missing or incompatible.",
                )
            )
    return compatible


def _callable_compatible(expected: ProviderCallable, actual: ProviderCallable) -> bool:
    if expected.return_annotation != actual.return_annotation or len(actual.parameters) < len(expected.parameters):
        return False
    for wanted, current in zip(expected.parameters, actual.parameters, strict=False):
        if (wanted.name, wanted.kind) != (current.name, current.kind):
            return False
        if not wanted.required and current.required:
            return False
        if not set(wanted.literal_values) <= set(current.literal_values):
            return False
    return all(
        not item.required and item.kind == "keyword_only" for item in actual.parameters[len(expected.parameters) :]
    )


def _check_python_modules(
    contracts: tuple[PythonModuleContract, ...],
    actual_modules: Sequence[PythonModuleApi],
    issues: list[PublicContractIssue],
) -> int:
    actual_by_module = {module.module: module for module in actual_modules}
    compatible = 0
    for contract in contracts:
        actual_callables = actual_by_module.get(contract.module)
        callable_map = actual_callables.callable_map() if actual_callables is not None else {}
        for expected in contract.callables:
            actual = callable_map.get(expected.qualified_name)
            if actual is None or not _callable_compatible(expected, actual):
                issues.append(
                    _issue(
                        "DPONE_PUBLIC_PYTHON_SIGNATURE_INCOMPATIBLE",
                        f"{contract.module}:{expected.qualified_name}",
                        "Public Python callable signature is missing or incompatible.",
                    )
                )
            else:
                compatible += 1
    return compatible


def _check_schemas(required: tuple[str, ...], actual: Sequence[str], issues: list[PublicContractIssue]) -> int:
    counts = {item: actual.count(item) for item in set(actual)}
    compatible = 0
    for kind in required:
        if counts.get(kind) != 1:
            issues.append(
                _issue("DPONE_PUBLIC_SCHEMA_KIND_MISSING", kind, "Public schema kind is missing or ambiguous.")
            )
        else:
            compatible += 1
    return compatible


def _check_packages(
    baseline: PublicContractBaseline,
    root: Path,
    today: date,
    issues: list[PublicContractIssue],
) -> int:
    try:
        core = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        reader_path = root / "packages/dpone-airflow-pack"
        reader = tomllib.loads((reader_path / "pyproject.toml").read_text(encoding="utf-8"))
        provider = tomllib.loads(
            (root / "packages/apache-airflow-providers-dpone/pyproject.toml").read_text(encoding="utf-8")
        )
    except (OSError, tomllib.TOMLDecodeError):
        issues.append(_package_issue("Package metadata cannot be inspected."))
        return 0
    packages = baseline.packages
    names = (core["project"].get("name"), reader["project"].get("name"), provider["project"].get("name"))
    versions = (core["project"].get("version"), reader["project"].get("version"), provider["project"].get("version"))
    dependencies = set(provider["project"].get("dependencies", ()))
    entrypoint = provider["project"].get("entry-points", {}).get("apache_airflow_provider", {})
    valid = (
        names == (packages.core_distribution, packages.reader_distribution, packages.provider_distribution)
        and len(set(versions)) == 1
        and packages.provider_airflow_requirement in dependencies
        and f"{packages.reader_distribution}=={versions[0]}" in dependencies
        and provider.get("tool", {}).get("uv", {}).get("build-backend", {}).get("module-name")
        == packages.provider_namespace
        and reader.get("tool", {}).get("uv", {}).get("build-backend", {}).get("module-name")
        == packages.legacy_namespace
        and entrypoint == {"provider_info": f"{packages.provider_namespace}:get_provider_info"}
        and "entry-points" not in reader["project"]
    )
    if not valid:
        issues.append(
            _package_issue("Formal provider, reader, version parity, dependencies or discovery metadata drifted.")
        )
        return 0
    if _legacy_window_active(baseline.legacy_window, today=today, current_release=str(versions[0])):
        exports = _legacy_compat_exports(reader_path / "src/dpone_airflow_pack/__init__.py")
        missing = sorted(set(baseline.provider_exports) - exports)
        if missing:
            issues.append(
                _issue(
                    "DPONE_PUBLIC_DEPRECATION_WINDOW_VIOLATED",
                    baseline.legacy_window.namespace,
                    "Legacy provider exports were removed early: " + ", ".join(missing),
                )
            )
            return 0
    return 1


def _legacy_compat_exports(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return set()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "_PROVIDER_COMPAT_EXPORTS" for target in node.targets):
            continue
        if isinstance(node.value, ast.Dict):
            return {
                key.value for key in node.value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    return set()


def _legacy_window_active(window: LegacyWindow, *, today: date, current_release: str) -> bool:
    earliest_date = date.fromisoformat(window.announced_date) + timedelta(days=window.minimum_days)
    earliest_release = _add_minor_releases(window.announced_release, window.minimum_minor_releases)
    return not (today >= earliest_date and _release_tuple(current_release) >= _release_tuple(earliest_release))


def _compatibility_projection(window: LegacyWindow, *, today: date) -> dict[str, dict[str, object]]:
    announced = date.fromisoformat(window.announced_date)
    return {
        "provider_legacy": {
            "namespace": window.namespace,
            "announced_release": window.announced_release,
            "announced_date": window.announced_date,
            "minimum_minor_releases": window.minimum_minor_releases,
            "minimum_days": window.minimum_days,
            "earliest_removal_release": _add_minor_releases(window.announced_release, window.minimum_minor_releases),
            "earliest_removal_date": (announced + timedelta(days=window.minimum_days)).isoformat(),
            "window_elapsed_by_date": today >= announced + timedelta(days=window.minimum_days),
        }
    }


def _baseline_fingerprint(baseline: PublicContractBaseline) -> str:
    payload = copy.deepcopy(baseline.canonical_payload)
    payload["cli"] = sorted(payload["cli"], key=lambda item: item["id"])
    payload["python"]["exports"] = sorted(payload["python"]["exports"])
    payload["python"]["callables"] = sorted(payload["python"]["callables"], key=lambda item: item["name"])
    if "modules" in payload["python"]:
        payload["python"]["modules"] = sorted(payload["python"]["modules"], key=lambda item: item["module"])
        for module in payload["python"]["modules"]:
            module["callables"] = sorted(module["callables"], key=lambda item: item["name"])
    payload["schemas"]["required_kinds"] = sorted(payload["schemas"]["required_kinds"])
    for item in payload["cli"]:
        item["required_options"] = sorted(item["required_options"])
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _find_parser(parser: argparse.ArgumentParser, path: tuple[str, ...]) -> argparse.ArgumentParser | None:
    current = parser
    for token in path:
        action = next((item for item in current._actions if isinstance(item, argparse._SubParsersAction)), None)
        if action is None or token not in action.choices:
            return None
        current = action.choices[token]
    return current


def _add_minor_releases(release: str, count: int) -> str:
    major, minor, _patch = _release_tuple(release)
    return f"{major}.{minor + count}.0"


def _release_tuple(release: str) -> tuple[int, int, int]:
    parts = release.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise ValueError("DPONE_PUBLIC_CONTRACT_BASELINE_INVALID: release must be MAJOR.MINOR.PATCH")
    return int(parts[0]), int(parts[1]), int(parts[2])


def _package_issue(message: str) -> PublicContractIssue:
    return _issue("DPONE_PUBLIC_PACKAGE_COMPATIBILITY_NARROWED", "provider-distributions", message)


def _issue(code: str, subject: str, message: str) -> PublicContractIssue:
    return PublicContractIssue(code=code, subject=subject, message=message)


__all__ = ["evaluate_public_contracts", "render_public_contract_report_text"]
