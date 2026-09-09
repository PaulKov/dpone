"""Pure models for the frozen Airflow self-service public contract."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True, slots=True)
class RequiredPositional:
    name: str
    accepted_value: str | None = None


@dataclass(frozen=True, slots=True)
class CliContract:
    identity: str
    tier: str
    path: tuple[str, ...]
    required_positionals: tuple[RequiredPositional, ...]
    required_options: tuple[str, ...]
    example_arguments: tuple[str, ...]
    lane: str = "shared"

    def with_required_options(self, options: tuple[str, ...]) -> CliContract:
        return replace(self, required_options=options)


@dataclass(frozen=True, slots=True)
class ProviderParameter:
    name: str
    kind: str
    required: bool
    literal_values: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderCallable:
    qualified_name: str
    parameters: tuple[ProviderParameter, ...]
    return_annotation: str


@dataclass(frozen=True, slots=True)
class PythonModuleContract:
    module: str
    source: str
    callables: tuple[ProviderCallable, ...]


@dataclass(frozen=True, slots=True)
class PythonModuleApi:
    module: str
    callables: tuple[ProviderCallable, ...]

    def callable_map(self) -> dict[str, ProviderCallable]:
        return {item.qualified_name: item for item in self.callables}


@dataclass(frozen=True, slots=True)
class LegacyWindow:
    namespace: str
    announced_release: str
    announced_date: str
    minimum_minor_releases: int
    minimum_days: int


@dataclass(frozen=True, slots=True)
class PackageContract:
    core_distribution: str
    reader_distribution: str
    provider_distribution: str
    provider_airflow_requirement: str
    provider_namespace: str
    legacy_namespace: str


@dataclass(frozen=True, slots=True)
class PublicContractBaseline:
    schema: str
    target_release: str
    semver: str
    cli: tuple[CliContract, ...]
    provider_exports: tuple[str, ...]
    provider_callables: tuple[ProviderCallable, ...]
    python_modules: tuple[PythonModuleContract, ...]
    schema_kinds: tuple[str, ...]
    packages: PackageContract
    legacy_window: LegacyWindow
    canonical_payload: dict[str, Any]

    def with_cli(self, cli: tuple[CliContract, ...]) -> PublicContractBaseline:
        return replace(self, cli=cli)

    def with_provider_exports(self, exports: tuple[str, ...]) -> PublicContractBaseline:
        return replace(self, provider_exports=exports)

    def with_python_modules(self, modules: tuple[PythonModuleContract, ...]) -> PublicContractBaseline:
        return replace(self, python_modules=modules)

    def with_schema_kinds(self, kinds: tuple[str, ...]) -> PublicContractBaseline:
        return replace(self, schema_kinds=kinds)


@dataclass(frozen=True, slots=True)
class PublicContractIssue:
    code: str
    subject: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "subject": self.subject, "message": self.message}


@dataclass(frozen=True, slots=True)
class ContractCheckCount:
    required: int
    compatible: int

    def to_dict(self) -> dict[str, int]:
        return {"required": self.required, "compatible": self.compatible}


@dataclass(frozen=True, slots=True)
class PublicContractReport:
    target_release: str
    fingerprint: str
    checks: dict[str, ContractCheckCount]
    compatibility: dict[str, dict[str, object]]
    issues: tuple[PublicContractIssue, ...]

    @property
    def passed(self) -> bool:
        return not self.issues

    @property
    def status(self) -> str:
        return "passed" if self.passed else "failed"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.airflow-public-contract-report.v1",
            "baseline": "dpone.airflow-self-service-public-contracts.v1",
            "target_release": self.target_release,
            "status": self.status,
            "passed": self.passed,
            "fingerprint": self.fingerprint,
            "checks": {name: value.to_dict() for name, value in sorted(self.checks.items())},
            "compatibility": self.compatibility,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True, slots=True)
class ProviderApi:
    exports: tuple[str, ...]
    callables: tuple[ProviderCallable, ...]

    def callable_map(self) -> dict[str, ProviderCallable]:
        return {item.qualified_name: item for item in self.callables}


__all__ = [
    "CliContract",
    "ContractCheckCount",
    "LegacyWindow",
    "PackageContract",
    "ProviderApi",
    "ProviderCallable",
    "ProviderParameter",
    "PublicContractBaseline",
    "PublicContractIssue",
    "PublicContractReport",
    "PythonModuleApi",
    "PythonModuleContract",
    "RequiredPositional",
]
