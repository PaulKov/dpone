from __future__ import annotations

import argparse
import copy
from dataclasses import replace
from datetime import date
from pathlib import Path

from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.cli_reference_source import build_root_parser
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.gitops.schema_contracts import gitops_schema_contracts
from dpone.services.docs.airflow_public_contract_models import (
    ProviderApi,
    PublicContractBaseline,
    PythonModuleApi,
)
from dpone.services.docs.airflow_public_contract_provider import (
    inspect_provider_api,
    inspect_python_module_api,
)
from dpone.services.docs.airflow_public_contracts import (
    evaluate_public_contracts,
    load_public_contract_baseline,
)

ROOT = Path(__file__).parents[1]
BASELINE = ROOT / "docs" / "airflow-self-service-public-contracts-v1.yaml"
PROVIDER = ROOT / "packages" / "apache-airflow-providers-dpone" / "src" / "airflow" / "providers" / "dpone"
PYTHON_MODULE_SOURCES = {
    "dpone.readiness.airflow_cache_retention": (ROOT / "src" / "dpone" / "readiness" / "airflow_cache_retention.py"),
    "dpone.readiness.airflow_self_service_cache": (
        ROOT / "src" / "dpone" / "readiness" / "airflow_self_service_cache.py"
    ),
    "dpone.readiness.airflow_self_service_application": (
        ROOT / "src" / "dpone" / "readiness" / "airflow_self_service_application.py"
    ),
    "dpone.contracts.airflow_runtime_pod_retention": (
        ROOT / "src" / "dpone" / "contracts" / "airflow_runtime_pod_retention.py"
    ),
    "dpone.services.airflow_runtime_pod_retention": (
        ROOT / "src" / "dpone" / "services" / "airflow_runtime_pod_retention.py"
    ),
}


def _schema_kinds() -> tuple[str, ...]:
    return tuple(sorted({contract.kind for contract in gitops_schema_contracts()} | set(dbt_schema_contracts())))


def _baseline() -> PublicContractBaseline:
    return load_public_contract_baseline(BASELINE, yaml_codec=PyYamlCodec())


def _python_modules() -> tuple[PythonModuleApi, ...]:
    return tuple(inspect_python_module_api(source, module=module) for module, source in PYTHON_MODULE_SOURCES.items())


def _report(
    *,
    baseline: PublicContractBaseline | None = None,
    parser: argparse.ArgumentParser | None = None,
    provider: ProviderApi | None = None,
    python_modules: tuple[PythonModuleApi, ...] | None = None,
):
    return evaluate_public_contracts(
        baseline or _baseline(),
        parser=parser or build_root_parser(),
        provider=provider or inspect_provider_api(PROVIDER / "__init__.py", PROVIDER / "__init__.pyi"),
        python_modules=python_modules if python_modules is not None else _python_modules(),
        schema_kinds=_schema_kinds(),
        package_root=ROOT,
        today=date(2026, 7, 16),
    )


def test_repository_matches_airflow_v1_public_contract_baseline() -> None:
    report = _report()

    assert report.passed is True
    assert report.status == "passed"
    assert report.issues == ()
    assert report.fingerprint.startswith("sha256:")
    assert report.checks["cli"].required >= 5
    assert report.checks["python"].required == 16
    assert report.checks["python_modules"].required == 9
    assert report.checks["schemas"].required >= 8


def test_missing_core_python_callable_is_a_stable_breaking_issue() -> None:
    modules = list(_python_modules())
    contracts_module = next(
        module for module in modules if module.module == "dpone.contracts.airflow_runtime_pod_retention"
    )
    modules[modules.index(contracts_module)] = replace(
        contracts_module,
        callables=tuple(
            callable_
            for callable_ in contracts_module.callables
            if callable_.qualified_name != "AirflowRuntimePodRetentionApplyRequest.require_authorized"
        ),
    )

    report = _report(python_modules=tuple(modules))

    assert report.passed is False
    assert [(issue.code, issue.subject) for issue in report.issues] == [
        (
            "DPONE_PUBLIC_PYTHON_SIGNATURE_INCOMPATIBLE",
            "dpone.contracts.airflow_runtime_pod_retention:AirflowRuntimePodRetentionApplyRequest.require_authorized",
        )
    ]


def test_core_python_signature_drift_is_a_stable_breaking_issue() -> None:
    modules = list(_python_modules())
    service_module = next(
        module for module in modules if module.module == "dpone.services.airflow_runtime_pod_retention"
    )
    plan = next(callable_ for callable_ in service_module.callables if callable_.qualified_name.endswith(".plan"))
    drifted_plan = replace(plan, parameters=(replace(plan.parameters[0], name="renamed_request"),))
    modules[modules.index(service_module)] = replace(
        service_module,
        callables=tuple(drifted_plan if callable_ == plan else callable_ for callable_ in service_module.callables),
    )

    report = _report(python_modules=tuple(modules))

    assert report.passed is False
    assert [(issue.code, issue.subject) for issue in report.issues] == [
        (
            "DPONE_PUBLIC_PYTHON_SIGNATURE_INCOMPATIBLE",
            "dpone.services.airflow_runtime_pod_retention:AirflowRuntimePodRetentionService.plan",
        )
    ]


def test_python_module_inspection_does_not_execute_source(tmp_path: Path) -> None:
    source = tmp_path / "public_api.py"
    source.write_text(
        "raise RuntimeError('must not execute')\n"
        "@dataclass(kw_only=True)\n"
        "class PublicApi:\n"
        "    request: str\n"
        "    ignored: str = field(init=False)\n"
        "    limit: int = field(default=100)\n"
        "    def call(self, request: str) -> dict[str, object]: ...\n",
        encoding="utf-8",
    )

    module = inspect_python_module_api(source, module="example.public_api")

    assert [callable_.qualified_name for callable_ in module.callables] == ["PublicApi.__init__", "PublicApi.call"]
    constructor = module.callable_map()["PublicApi.__init__"]
    assert [(item.name, item.kind, item.required) for item in constructor.parameters] == [
        ("request", "keyword_only", True),
        ("limit", "keyword_only", False),
    ]


def test_provider_only_baseline_remains_compatible(tmp_path: Path) -> None:
    raw = PyYamlCodec().load(BASELINE.read_text(encoding="utf-8"))
    raw["python"].pop("modules")
    path = tmp_path / "provider-only-baseline.yaml"
    path.write_text(PyYamlCodec().dump(raw), encoding="utf-8")

    baseline = load_public_contract_baseline(path, yaml_codec=PyYamlCodec())
    report = _report(baseline=baseline, python_modules=())

    assert report.passed is True
    assert report.checks["python"].required == 16
    assert report.checks["python_modules"].required == 0


def test_missing_golden_path_option_is_a_stable_breaking_issue() -> None:
    baseline = _baseline()
    commands = list(baseline.cli)
    first = commands[0]
    commands[0] = first.with_required_options((*first.required_options, "--removed-option"))
    mutated = baseline.with_cli(tuple(commands))

    report = _report(baseline=mutated)

    assert report.passed is False
    assert [issue.code for issue in report.issues] == ["DPONE_PUBLIC_CLI_ARGUMENT_MISSING"]


def test_new_required_option_is_a_stable_breaking_issue() -> None:
    parser = build_root_parser()
    root_subcommands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    airflow_parser = root_subcommands.choices["airflow"]
    airflow_subcommands = next(
        action for action in airflow_parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    airflow_subcommands.choices["build"].add_argument("--new-required", required=True)

    report = _report(parser=parser)

    assert report.passed is False
    assert [issue.code for issue in report.issues] == ["DPONE_PUBLIC_CLI_NEW_REQUIRED_ARGUMENT"]


def test_missing_provider_export_is_a_stable_breaking_issue() -> None:
    baseline = _baseline().with_provider_exports((*_baseline().provider_exports, "RemovedPublicType"))

    report = _report(baseline=baseline)

    assert report.passed is False
    assert [issue.code for issue in report.issues] == [
        "DPONE_PUBLIC_DEPRECATION_WINDOW_VIOLATED",
        "DPONE_PUBLIC_PROVIDER_EXPORT_MISSING",
    ]


def test_missing_schema_kind_is_a_stable_breaking_issue() -> None:
    baseline = _baseline().with_schema_kinds((*_baseline().schema_kinds, "dpone.removed-contract.v1"))

    report = _report(baseline=baseline)

    assert report.passed is False
    assert [issue.code for issue in report.issues] == ["DPONE_PUBLIC_SCHEMA_KIND_MISSING"]


def test_baseline_accepts_additive_versioned_schema_contracts(tmp_path: Path) -> None:
    raw = PyYamlCodec().load(BASELINE.read_text(encoding="utf-8"))
    raw["schemas"]["required_kinds"].append("dpone.example-contract.v2")
    path = tmp_path / "baseline.yaml"
    path.write_text(PyYamlCodec().dump(raw), encoding="utf-8")

    loaded = load_public_contract_baseline(path, yaml_codec=PyYamlCodec())

    assert "dpone.example-contract.v2" in loaded.schema_kinds


def test_baseline_rejects_duplicate_public_identity(tmp_path: Path) -> None:
    raw = PyYamlCodec().load(BASELINE.read_text(encoding="utf-8"))
    raw["cli"].append(copy.deepcopy(raw["cli"][0]))
    path = tmp_path / "baseline.yaml"
    path.write_text(PyYamlCodec().dump(raw), encoding="utf-8")

    try:
        load_public_contract_baseline(path, yaml_codec=PyYamlCodec())
    except ValueError as exc:
        assert "DPONE_PUBLIC_CONTRACT_BASELINE_INVALID" in str(exc)
    else:
        raise AssertionError("duplicate CLI identity must fail")


def test_provider_legacy_window_is_not_considered_elapsed_early() -> None:
    baseline = _baseline()

    report = evaluate_public_contracts(
        baseline,
        parser=build_root_parser(),
        provider=inspect_provider_api(PROVIDER / "__init__.py", PROVIDER / "__init__.pyi"),
        python_modules=_python_modules(),
        schema_kinds=_schema_kinds(),
        package_root=ROOT,
        today=date(2026, 8, 1),
    )

    assert report.passed is True
    assert report.compatibility["provider_legacy"]["earliest_removal_date"] == "2027-07-13"


def test_provider_legacy_export_cannot_be_removed_before_support_window() -> None:
    baseline = _baseline().with_provider_exports((*_baseline().provider_exports, "RetainedLegacyType"))
    provider = inspect_provider_api(PROVIDER / "__init__.py", PROVIDER / "__init__.pyi")
    provider = ProviderApi(exports=(*provider.exports, "RetainedLegacyType"), callables=provider.callables)

    report = _report(baseline=baseline, provider=provider)

    assert report.passed is False
    assert [issue.code for issue in report.issues] == ["DPONE_PUBLIC_DEPRECATION_WINDOW_VIOLATED"]


def test_baseline_fingerprint_is_independent_of_set_like_order(tmp_path: Path) -> None:
    raw = PyYamlCodec().load(BASELINE.read_text(encoding="utf-8"))
    raw["cli"].reverse()
    raw["python"]["exports"].reverse()
    raw["python"]["callables"].reverse()
    raw["schemas"]["required_kinds"].reverse()
    for command in raw["cli"]:
        command["required_options"].reverse()
    reordered_path = tmp_path / "baseline.yaml"
    reordered_path.write_text(PyYamlCodec().dump(raw), encoding="utf-8")

    reordered = load_public_contract_baseline(reordered_path, yaml_codec=PyYamlCodec())

    assert _report(baseline=reordered).fingerprint == _report().fingerprint
