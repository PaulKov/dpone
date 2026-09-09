"""Application services for the reviewed Airflow self-service contract."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from dpone.services.docs.errors import DocsConfigurationError

from .airflow_public_contract_models import PublicContractIssue, PublicContractReport
from .airflow_public_contract_provider import inspect_provider_api, inspect_python_module_api
from .airflow_public_contracts import (
    evaluate_public_contracts,
    is_airflow_public_contract_reference_in_sync,
    load_public_contract_baseline,
    render_public_contract_report_text,
    sync_airflow_public_contract_reference,
)
from .context import DocsServiceContext


class _AirflowPublicContractService:
    def __init__(self, *, ctx: DocsServiceContext) -> None:
        self.ctx = ctx
        self.log = ctx.logger

    def _resolve_path(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Path argument must not be empty")
        root = self.ctx.settings.repo_root.resolve()
        path = Path(text)
        resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
        if not resolved.is_relative_to(root):
            raise DocsConfigurationError("Public-contract paths must stay inside the repository")
        return resolved


class CheckAirflowPublicContractsService(_AirflowPublicContractService):
    """Compare current source projections with the reviewed v1 baseline."""

    def __init__(
        self,
        *,
        ctx: DocsServiceContext,
        clock: Callable[[], date] | None = None,
    ) -> None:
        super().__init__(ctx=ctx)
        self._clock = clock or date.today

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict[str, object]]:
        root = self.ctx.settings.repo_root.resolve()
        baseline_path = self._resolve_path(
            getattr(args, "baseline", "docs/airflow-self-service-public-contracts-v1.yaml")
        )
        provider_root = self._resolve_path(
            getattr(
                args,
                "provider_root",
                "packages/apache-airflow-providers-dpone/src/airflow/providers/dpone",
            )
        )
        try:
            baseline = load_public_contract_baseline(baseline_path, yaml_codec=self.ctx.yaml)
            provider = inspect_provider_api(provider_root / "__init__.py", provider_root / "__init__.pyi")
            python_modules = tuple(
                inspect_python_module_api(self._resolve_path(contract.source), module=contract.module)
                for contract in baseline.python_modules
            )
            report = evaluate_public_contracts(
                baseline,
                parser=_build_parser(),
                provider=provider,
                python_modules=python_modules,
                schema_kinds=_schema_kinds(),
                package_root=root,
                today=self._clock(),
            )
        except (OSError, SyntaxError, ValueError) as exc:
            report = _invalid_report(str(exc))
        output_format = str(getattr(args, "format", "text") or "text")
        if output_format == "json":
            output: str | dict[str, object] = report.to_dict()
        elif output_format == "text":
            output = render_public_contract_report_text(report)
        else:
            raise DocsConfigurationError("--format must be text or json")
        if report.passed:
            self.log.info("Airflow public contract is compatible")
            return 0, output
        self.log.error("Airflow public contract check failed")
        return 2, output


class UpdateAirflowPublicContractReferenceService(_AirflowPublicContractService):
    """Render the reference page from the reviewed baseline only."""

    def run(self, args: argparse.Namespace) -> int:
        baseline_path = self._resolve_path(
            getattr(args, "baseline", "docs/airflow-self-service-public-contracts-v1.yaml")
        )
        doc_path = self._resolve_path(getattr(args, "doc", "docs/reference/airflow-public-contracts.md"))
        baseline = load_public_contract_baseline(baseline_path, yaml_codec=self.ctx.yaml)
        if bool(getattr(args, "check", False)):
            if is_airflow_public_contract_reference_in_sync(doc_path, baseline=baseline):
                self.log.info("Airflow public-contract reference is up-to-date: %s", doc_path)
                return 0
            self.log.error(
                "Airflow public-contract reference is outdated (%s). Run: "
                "dpone docs update-airflow-public-contract-reference",
                doc_path,
            )
            return 2
        changed, _ = sync_airflow_public_contract_reference(doc_path, baseline=baseline)
        message = "Updated" if changed else "Already current"
        self.log.info("%s Airflow public-contract reference: %s", message, doc_path)
        return 0


def _build_parser() -> Any:
    from dpone.app.cli_reference_source import build_root_parser

    return build_root_parser()


def _schema_kinds() -> tuple[str, ...]:
    from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
    from dpone.gitops.schema_contracts import gitops_schema_contracts

    return tuple(sorted({contract.kind for contract in gitops_schema_contracts()} | set(dbt_schema_contracts())))


def _invalid_report(message: str) -> PublicContractReport:
    safe_message = "The reviewed baseline or source projection is invalid."
    if message.startswith("DPONE_PUBLIC_CONTRACT_BASELINE_INVALID:"):
        safe_message = message[:1024]
    return PublicContractReport(
        target_release="unknown",
        fingerprint="sha256:" + "0" * 64,
        checks={},
        compatibility={},
        issues=(
            PublicContractIssue(
                code="DPONE_PUBLIC_CONTRACT_BASELINE_INVALID",
                subject="airflow-public-contracts",
                message=safe_message,
            ),
        ),
    )


__all__ = [
    "CheckAirflowPublicContractsService",
    "UpdateAirflowPublicContractReferenceService",
]
