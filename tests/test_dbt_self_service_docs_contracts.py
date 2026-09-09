from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.commands.dbt_publish_cli_support import public_error_code
from dpone.contracts.runtime_artifact_attestation import parse_runtime_artifact_trust_policy

_ROOT = Path(__file__).resolve().parents[1]
_ERROR_TOKEN = re.compile(r"\bDPONE_DBT_[A-Z0-9_]+\b")
_INTERNAL_ISSUE_CODE = re.compile(
    r"""(?:code\s*=\s*|DbtPublishIssue\(\s*code\s*=\s*|_issue\(\s*[^,]*,\s*)"""
    r"""['\"]([a-z][a-z0-9_]*)['\"]"""
)
_NON_ERROR_CODES = {
    "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_",
    "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
    "DPONE_DBT_DEV_EVIDENCE_VERIFIED",
    "DPONE_DBT_EVIDENCE_BOOTSTRAP_ROOT",
    "DPONE_DBT_EVIDENCE_EXPORT_ROOT",
    "DPONE_DBT_EVIDENCE_SET_ID",
    "DPONE_DBT_EXECUTION_PASSED",
    "DPONE_DBT_MANIFEST_ADAPTER_SCHEMA_EXTENSION",
    "DPONE_DBT_PROMOTION_SOURCE_VERIFIED",
    "DPONE_DBT_PUBLISH_PROFILES",
    "DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF",
    "DPONE_DBT_WORKFLOW_PASSED",
}
_DYNAMIC_CODE_ROOTS = (
    _ROOT / "src" / "dpone" / "commands",
    _ROOT / "src" / "dpone" / "contracts",
    _ROOT / "src" / "dpone" / "dbt_publish",
    _ROOT / "src" / "dpone" / "manifest",
    _ROOT / "src" / "dpone" / "readiness",
    _ROOT / "src" / "dpone" / "runtime",
    _ROOT / "src" / "dpone" / "services",
    _ROOT / "packages" / "dpone-airflow-pack",
)
_ATTESTATION_DOCS = (
    _ROOT / "docs" / "dbt-inline-publishing.md",
    _ROOT / "docs" / "dbt-self-service-platform-workflows.md",
    _ROOT / "docs" / "airflow-pack-provider.md",
    _ROOT / "docs" / "airflow-cache-sync.md",
)


def _json_block_between(document: str, start_marker: str, end_marker: str) -> dict[str, object]:
    start = document.index(start_marker) + len(start_marker)
    end = document.index(end_marker, start)
    fenced = document[start:end]
    json_start = fenced.index("```json") + len("```json")
    json_end = fenced.index("```", json_start)
    payload = json.loads(fenced[json_start:json_end])
    assert isinstance(payload, dict)
    return payload


def test_dbt_error_catalog_covers_all_emitted_stable_error_codes() -> None:
    source_roots = (_ROOT / "src" / "dpone", _ROOT / "packages" / "dpone-airflow-pack")
    emitted = {
        token
        for root in source_roots
        for path in root.rglob("*.py")
        for token in _ERROR_TOKEN.findall(path.read_text(encoding="utf-8"))
        if token not in _NON_ERROR_CODES
    }
    emitted.add("DPONE_DBT_MANIFEST_INVALID_JSON")
    for root in _DYNAMIC_CODE_ROOTS:
        emitted.update(_dynamic_dbt_codes(root))
    catalog = (_ROOT / "docs" / "dbt-self-service-errors.md").read_text(encoding="utf-8")

    missing = sorted(code for code in emitted if f"`{code}`" not in catalog)

    assert missing == []
    assert "`DPONE_ARTIFACT_ATTESTATION_REQUIRED`" in catalog


def _dynamic_dbt_codes(root: Path) -> set[str]:
    emitted: set[str] = set()
    for path in root.rglob("*.py"):
        if "dbt" not in (Path(root.name) / path.relative_to(root)).as_posix():
            continue
        for raw in _INTERNAL_ISSUE_CODE.findall(path.read_text(encoding="utf-8")):
            public = public_error_code(raw)
            if public.startswith("DPONE_DBT_") and public not in _NON_ERROR_CODES:
                emitted.add(public)
    return emitted


def test_dynamic_error_discovery_ignores_worktree_name_but_keeps_nested_dbt_modules(tmp_path: Path) -> None:
    source = tmp_path / "codex-dbt-feature" / "src"
    source.mkdir(parents=True)
    (source / "generic.py").write_text('issue(code="unrelated_failure")', encoding="utf-8")
    (source / "dbt_source.py").write_text('issue(code="source_invalid")', encoding="utf-8")
    (source / "dbt").mkdir()
    (source / "dbt" / "nested.py").write_text('issue(code="nested_invalid")', encoding="utf-8")
    assert _dynamic_dbt_codes(source) == {"DPONE_DBT_SOURCE_INVALID", "DPONE_DBT_NESTED_INVALID"}
    publishing = source / "dbt_publish"
    publishing.mkdir()
    (publishing / "compiler.py").write_text('issue(code="compile_invalid")', encoding="utf-8")
    assert _dynamic_dbt_codes(publishing) == {"DPONE_DBT_COMPILE_INVALID"}


def test_promotion_journey_and_platform_workflow_docs_are_split() -> None:
    promotion = (_ROOT / "docs" / "dbt-self-service-promotion.md").read_text(encoding="utf-8")
    platform = (_ROOT / "docs" / "dbt-self-service-platform-workflows.md").read_text(encoding="utf-8")

    assert "dbt-self-service-platform-workflows.md" in promotion
    assert "DPONE_DBT_TRUSTED_SIGNER_DIGEST" in platform
    assert "uses: PaulKov/dpone/.github/workflows/dbt-self-service-prod.yml" in platform
    assert "uses: PaulKov/dpone/.github/workflows/dbt-self-service-prod.yml" not in promotion
    assert promotion.count("\n") < 220
    assert "Roll back prod" in promotion


def test_attestation_docs_describe_stock_verifier_without_false_certification() -> None:
    documents = {path.name: path.read_text(encoding="utf-8") for path in _ATTESTATION_DOCS}
    combined = "\n".join(documents.values())

    stale_claims = (
        "until the runtime attestation verifier exists",
        "does not ship a concrete production attestation verifier",
        "until the concrete runtime attestation verifier is implemented",
        "BLOCKED: runtime attestation verifier required",
        "future audited CAS current",
    )
    assert [claim for claim in stale_claims if claim in combined] == []
    assert "stock runtime" in combined
    assert "offline verifier" in combined
    assert all("UNVERIFIED" in document for document in documents.values())


def test_documented_v2_trust_policy_passes_schema_and_production_parser() -> None:
    platform = (_ROOT / "docs" / "dbt-self-service-platform-workflows.md").read_text(encoding="utf-8")
    policy = _json_block_between(
        platform,
        "<!-- BEGIN RUNTIME_ARTIFACT_TRUST_POLICY_V2_EXAMPLE -->",
        "<!-- END RUNTIME_ARTIFACT_TRUST_POLICY_V2_EXAMPLE -->",
    )
    schema = json.loads(
        (_ROOT / "docs" / "schemas" / "gitops" / "runtime-artifact-trust-policy-v2.schema.json").read_text(
            encoding="utf-8"
        )
    )

    Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(policy)
    parsed = parse_runtime_artifact_trust_policy(
        policy,
        now=datetime(2026, 7, 27, tzinfo=UTC),
    )

    assert parsed.schema == "dpone.runtime-artifact-trust-policy.v2"
    assert parsed.requires_attestation is True
    assert parsed.verifier is not None
    assert parsed.verifier.signer_digest == "0123456789abcdef0123456789abcdef01234567"


def test_platform_examples_do_not_use_cross_workflow_needs_outputs() -> None:
    platform = (_ROOT / "docs" / "dbt-self-service-platform-workflows.md").read_text(encoding="utf-8")

    assert "needs.dev-release.outputs" not in platform
    assert "needs.dev-evidence.outputs" not in platform
    for job_id in ("dbt-release", "dev-activation", "dev-evidence", "prod-mirror", "promote"):
        assert f"  {job_id}:" in platform


def test_documented_prod_order_matches_reusable_workflow() -> None:
    platform = (_ROOT / "docs" / "dbt-self-service-platform-workflows.md").read_text(encoding="utf-8")
    workflow = (_ROOT / ".github" / "workflows" / "dbt-self-service-prod.yml").read_text(encoding="utf-8")

    production_section = platform[platform.index("Production CI verifies") :]
    normalized_section = " ".join(production_section.split())
    documented = (
        "performs offline verification",
        "builds the environment-specific deployment candidate",
        "parse-smokes its exact index",
        "immutably publish",
        "CAS promotion",
    )
    assert [normalized_section.index(phrase) for phrase in documented] == sorted(
        normalized_section.index(phrase) for phrase in documented
    )

    workflow_steps = (
        "Verify the runtime attestation with the exact offline policy",
        "Build immutable prod deployment",
        "Parse smoke the exact deployment index",
        "Publish immutable release and deployment",
        "CAS-promote current deployment",
    )
    assert [workflow.index(step) for step in workflow_steps] == sorted(workflow.index(step) for step in workflow_steps)
