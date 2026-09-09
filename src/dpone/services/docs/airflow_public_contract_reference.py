"""Generated reference rendering for the Airflow self-service contract."""

from __future__ import annotations

from pathlib import Path

from .airflow_public_contract_models import CliContract, PublicContractBaseline
from .generated_block import is_generated_doc_in_sync, sync_generated_doc

REFERENCE_START = "<!-- DPONE_AIRFLOW_PUBLIC_CONTRACTS_START -->"
REFERENCE_END = "<!-- DPONE_AIRFLOW_PUBLIC_CONTRACTS_END -->"


def render_airflow_public_contract_reference(baseline: PublicContractBaseline) -> str:
    beginner = [item for item in baseline.cli if item.tier == "beginner"]
    offline = [item for item in beginner if item.identity != "safe-sample"]
    flat = [item for item in offline if item.lane in {"flat", "shared"}]
    domain_first = [item for item in offline if item.lane in {"domain_first", "shared"}]
    optional_sample = [item for item in beginner if item.identity == "safe-sample"]
    lines = [
        REFERENCE_START,
        "_This section is generated from the reviewed Airflow v1 public-contract baseline._",
        "",
        "## Beginner command contract",
        "",
        "### Successful flat offline preview path",
        "",
        *(f"- `{_invocation(item)}`" for item in flat),
        "",
        "### Successful domain-first offline preview path",
        "",
        *(f"- `{_invocation(item)}`" for item in domain_first),
        "",
        "### Optional platform-gated safe sample",
        "",
        *(
            f"- `{_invocation(item)}` — returns success only when the approved sample runtime is configured."
            for item in optional_sample
        ),
        "",
        "## Canonical provider",
        "",
        f"- Distribution: `{baseline.packages.provider_distribution}`",
        f"- Namespace: `{baseline.packages.provider_namespace}`",
        f"- Lightweight reader: `{baseline.packages.reader_distribution}`",
        f"- Required exports: **{len(baseline.provider_exports)}**",
        "",
        "## Public schema majors",
        "",
        f"- Frozen v1 schema kinds: **{len(baseline.schema_kinds)}**",
        "",
        "## Compatibility policy",
        "",
        f"- SemVer: `{baseline.semver}`",
        f"- Provider legacy namespace: `{baseline.legacy_window.namespace}`",
        "- Legacy minimum: "
        f"{baseline.legacy_window.minimum_minor_releases} minor releases and "
        f"{baseline.legacy_window.minimum_days} days, whichever is later.",
        "",
        REFERENCE_END,
    ]
    return "\n".join(lines)


def sync_airflow_public_contract_reference(path: Path, *, baseline: PublicContractBaseline) -> tuple[bool, str]:
    return sync_generated_doc(
        path,
        rendered_block=render_airflow_public_contract_reference(baseline),
        start_marker=REFERENCE_START,
        end_marker=REFERENCE_END,
    )


def is_airflow_public_contract_reference_in_sync(path: Path, *, baseline: PublicContractBaseline) -> bool:
    return is_generated_doc_in_sync(
        path,
        rendered_block=render_airflow_public_contract_reference(baseline),
        start_marker=REFERENCE_START,
        end_marker=REFERENCE_END,
    )


def _invocation(contract: CliContract) -> str:
    suffix = [positional.accepted_value or f"<{positional.name}>" for positional in contract.required_positionals]
    return " ".join(("dpone", *contract.path, *suffix, *contract.example_arguments))


__all__ = [
    "REFERENCE_END",
    "REFERENCE_START",
    "is_airflow_public_contract_reference_in_sync",
    "render_airflow_public_contract_reference",
    "sync_airflow_public_contract_reference",
]
