from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "feature-design-ci-pr-gate-exact-sha-evidence.md"
ADR_0046 = ROOT / "docs" / "adr" / "0046-component-aware-pr-gate-authority.md"
ADR_0048 = ROOT / "docs" / "adr" / "0048-exact-sha-readiness-evidence.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _squash_whitespace(value: str) -> str:
    return " ".join(value.split())


def test_shadow_spec_preserves_authority_boundary() -> None:
    spec = _read(SPEC)
    adr = _read(ADR_0046)

    for text in (spec, adr):
        assert "`PR Gate shadow`" in text
        assert (
            "No Actions workflow may create `PR Gate`" in text or "rejects any Actions job\n   named `PR Gate`" in text
        )
        assert "different from `15368`" in text
        assert "canonical active" in text
        assert "only merge authority" in text

    assert "- Implementation status: IN PROGRESS" in spec
    assert "ruleset `18806829`" in spec
    policy = yaml.safe_load((ROOT / ".agents" / "policy" / "github-branch-protection.yml").read_text(encoding="utf-8"))
    required_contexts = policy["ruleset"]["required_status_checks"]["checks"]
    assert required_contexts
    assert f"same {len(required_contexts)} contexts" in _squash_whitespace(spec)


def test_shadow_spec_closes_trusted_data_only_evidence() -> None:
    spec = _squash_whitespace(_read(SPEC))
    adr = _squash_whitespace(_read(ADR_0048))

    for expected in (
        "archive: false",
        "overwrite: false",
        "runner.temp",
        "skip-decompress: true",
        "digest-mismatch: error",
        "producer_run_id",
        "producer_run_attempt",
        "auditor_revision_sha",
        "producer_head_sha",
        "Duplicate JSON keys",
        "dynamic imports",
        "30-minute grace period",
        "complete stateless interval scan",
        "pr-gate-shadow-evidence-v1.schema.json",
        "pr-gate-shadow-audit-v1.schema.json",
        "/actions/runs/{run_id}/attempts/{attempt}/jobs",
        ".agents/policy/ci-shadow-bundle-v1.yml",
    ):
        assert expected in spec

    normalized_adr = _squash_whitespace(adr)
    assert "never executes, imports, sources" in normalized_adr
    assert "default provenance verifier denies authority" in normalized_adr


def test_read_only_auditor_does_not_claim_pr_head_check_publication() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "The PR-head aggregator publishes that context" in normalized
    assert "auditor never creates or updates a PR check" in normalized
    assert "no secrets, cache, PR checkout, content execution, or check publication" in normalized
    assert "cannot change the already-published producer context" in normalized
    assert "critical false-green condition, not verified evidence" in normalized


def test_auditor_recomputes_plan_bundle_and_attempt_jobs() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "independently recompute canonical plan bytes and digest" in normalized
    assert "recomputes the manifest digest" in normalized
    assert "provider job/case conclusions" in normalized
    assert "producer's bundle digest is only a claim" in normalized
    assert "step-level and job-level `continue-on-error` are forbidden for product work" in normalized
    assert "page its own exact run-attempt Jobs API" in normalized
    assert "`TRUST_CORE` from `SUBJECT_INPUT`" in normalized
    assert "changing only declared product source" in normalized
    assert "observe_stable_current_pr_snapshot" in normalized
    assert "Every observation independently repaginates the complete open-PR set" in normalized
    assert "eligible-set digest" in normalized
    assert "same_eligible_set_pr_ref_and_parents" in normalized
    assert "eligible_prs = api.page_open_pull_requests" not in normalized
    assert "actions/attest" not in normalized
    assert "No PR-head shadow job may receive write, OIDC, attestation" in normalized


def test_producer_binds_stable_workflow_blob_and_exact_subject_checkout() -> None:
    normalized = _squash_whitespace(_read(SPEC))
    adr = _squash_whitespace(_read(ADR_0048))

    for expected in (
        "`subject_tree_oid`",
        "`workflow_blob_oid`",
        '"merge_sha"',
        "`workflow_run.head_sha` also binds `H`",
        "must never be relabelled as the synthetic pull-request merge `M`",
        "diagnostic only and neither its membership nor its SHA fields are attempt authority",
        "`refs/pull/<pr_number>/merge`",
        "without giving the PR workflow OIDC, write permission, or a secret",
        "requires exactly two ordered parents `(B, H)`",
        "provider-owned ref still equals the collector's `GITHUB_SHA=M` claim",
        "final read is the audit linearization point",
        "does not claim that a mutable ref proves a historical event `M`",
        "No job may rely on checkout's default PR merge ref",
        "`persist-credentials: false`",
        "`EXECUTION_WORKFLOW`",
        "`SUBJECT_TRUST_CORE`",
        "require_same_workflow_blob(base_tree, head_tree, merge_tree, run.path)",
        "recompute_subject_closure(workflow_blob, head_tree)",
    ):
        assert expected in normalized
    assert "Current PR association membership and SHA fields are mutable diagnostics" in adr
    assert "requires exactly two ordered parents `(B, H)` plus `run.head_sha == H`" in adr
    assert "workflow path/mode/blob must be identical in `B/H/M`" in adr
    assert "Neither producer nor auditor creates or requires an attestation" in adr
    assert "re-resolves the same ref immediately before receipt persistence" in adr
    assert "same_eligible_set_pr_ref_and_parents" in normalized
    assert "checkout-free, cache-free, and download-free" in normalized
    assert "no `needs` output, product artifact, PR environment" in normalized


def test_auditor_code_catalog_covers_attempt_provenance_and_workflow_source() -> None:
    spec = _read(SPEC)
    auditor_row = next(line for line in spec.splitlines() if line.startswith("| auditor |"))

    assert "`AUDITOR_MERGE_REF_UNVERIFIED`" in auditor_row
    assert "`AUDITOR_WORKFLOW_SOURCE_UNVERIFIED`" in auditor_row
    assert "`AUDITOR_MERGE_REF_UNVERIFIED`" in spec
    assert "`AUDITOR_WORKFLOW_SOURCE_UNVERIFIED`" in spec
    reconciliation_row = next(line for line in spec.splitlines() if line.startswith("| reconciliation |"))
    provenance_row = next(line for line in spec.splitlines() if line.startswith("| provenance |"))
    assert "`RECONCILIATION_RESOURCE_LIMIT`" in reconciliation_row
    assert "`RECONCILIATION_RETENTION_HISTORY_LOST`" in reconciliation_row
    assert "`PROVENANCE_APPROVAL_PENDING`" in provenance_row


def test_current_merge_snapshot_has_closed_eligibility_and_terminal_recovery() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    for expected in (
        "base_repository_id = run.repository.id",
        "head_repository_id = run.head_repository.id",
        "head_ref = run.head_branch",
        "state = open",
        "mergeable = true",
        "Another open PR with the same base/head repository IDs, refs, and SHAs",
        "At most five observations or 30 seconds",
        "final read is the audit linearization point",
        "schema-valid `UNVERIFIED/AUDITOR_MERGE_REF_UNVERIFIED` receipt",
        "fresh PR event/run",
        "old attempt is terminal `UNVERIFIED`",
    ):
        assert expected in normalized


def test_shadow_matrices_are_complete_and_have_closed_mixed_precedence() -> None:
    normalized = _squash_whitespace(_read(SPEC))
    adr = _squash_whitespace(_read(ADR_0048))

    assert "Every evidence matrix sets `strategy.fail-fast: false`" in normalized
    assert "takes precedence over a different case's failure" in normalized
    assert "Product command steps and jobs may not use `continue-on-error`" in normalized
    assert "post-product step on the same mutable runner is never an enforcement boundary" in normalized
    assert "missing/nonterminal/skipped/cancelled/timed-out/action-required/stale required evidence" in normalized
    assert "code ordering cannot downgrade `UNVERIFIED` to `FAIL`" in normalized
    assert "Every evidence matrix disables fail-fast" in adr
    assert "Missing producer JSON is missing proof and remains `UNVERIFIED`" in normalized
    assert "ArtifactBound --> VerifiedFailure" in _read(SPEC)

    job_row = next(line for line in _read(SPEC).splitlines() if line.startswith("| job |"))
    assert job_row == (
        "| job | `JOB_MISSING`, `JOB_NONTERMINAL`, `JOB_SKIPPED`, `JOB_CANCELLED`, "
        "`JOB_TIMED_OUT`, `JOB_ACTION_REQUIRED`, `JOB_STALE`, `JOB_UNEXPECTED`, `JOB_FAILED` | "
        "missing/nonterminal/skipped/cancelled/timed-out/action-required/stale required evidence is "
        "`UNVERIFIED`; with a complete uncertainty-free case set, unexpected or failed work is `FAIL` |"
    )


def test_closed_json_combines_schema_shape_with_streaming_resource_limits() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "JSON Schema enforces the closed shape" in normalized
    assert "streaming parser separately enforces maximum depth 8" in normalized
    assert "schema never claims to enforce global tree budgets" in normalized
    assert "`JSON_LIMIT_EXCEEDED`" in normalized
    assert "`JSON_SCHEMA_INVALID`" in normalized


def test_audit_receipt_is_stage_discriminated_for_early_uncertainty() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "`audit_stage=EVENT_RECEIVED|RUN_AUTHENTICATED|PLAN_RECOMPUTED|JOBS_BOUND|CLAIMS_BOUND`" in normalized
    assert "nullable `producer_identity`" in normalized
    assert "`PASS` requires `CLAIMS_BOUND`" in normalized
    assert "API or permission failure can persist a schema-valid receipt" in normalized
    assert "`MISSING` is allowed only when no unique usable transport artifact" in normalized
    assert "`parse_status=VALID|INVALID`" in normalized
    assert "never relabels already downloaded bytes as missing" in normalized
    assert "Invalid JSON retains `PRESENT/INVALID` raw transport identity" in normalized


def test_existing_merged_closure_publisher_and_candidate_allowlist_are_preserved() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "ADR 0037's existing `merged-closure-check-publisher`" in normalized
    assert "It is not a PR-head execution profile" in normalized
    assert "`.github/workflows/exact-sha-candidate.yml`" in normalized
    assert "`push` on `refs/heads/master`" in normalized
    assert "Tags remain outside this goal" in normalized
    assert "`ADR0037_GOVERNANCE_SOURCE_ATTESTOR`" in normalized
    assert "any change/retirement requires a prior approved ADR 0037 amendment" in normalized


def test_shadow_spec_has_closed_route_and_evidence_acceptance() -> None:
    spec = _read(SPEC)

    canaries = (
        "docs only",
        "`uv.lock` only",
        "core",
        "PostgreSQL",
        "Airflow",
        "control surface",
        "unknown path",
        "deliberate failure",
    )
    canary_section = spec.split("### Eight route canaries", maxsplit=1)[1].split(
        "### Burst and cancellation", maxsplit=1
    )[0]
    assert all(canary in canary_section for canary in canaries)
    assert "within 90 seconds" in spec
    assert "at least two verified distinct-SHA full runs" in spec
    assert "at least 100 comparative distinct-SHA runs" in _squash_whitespace(spec)
    assert "src/dpone/runtime/__init__.py" in spec
    assert "src/dpone/xmin/__init__.py" in spec
    assert "packages/dpone-airflow-pack/src/dpone_airflow_pack/__init__.py" in spec
    assert ".github/dependabot.yml" in spec
    assert "test-only policy mapping" in spec
    assert "`RUN,RUN,RUN,N/A,N/A,N/A,N/A,N/A,N/A`" in spec
    assert "first producer FAIL/audit FAIL" in spec


def test_shadow_spec_closes_ci_hygiene_without_release_changes() -> None:
    spec = _squash_whitespace(_read(SPEC))

    for expected in (
        "uv sync --locked",
        "every Monday with open-PR limit 3",
        "every Wednesday with open-PR limit 2",
        "timezone `Europe/Berlin`",
        "labels `dependencies` and `python:uv`",
        "labels `dependencies` and `github-actions`",
        "`queue: max`",
        "`cancel-in-progress: false`",
        "Airflow matrices use `max-parallel: 2`",
        "shadow wheel smoke uses `1`",
        "nightly full compatibility uses `4`",
        "Release workflow files, release dependency installation, and release concurrency are unchanged",
    ):
        assert expected in spec


def test_slo_is_scoped_to_one_producer_attempt_without_hidden_averaging() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    assert "sampling unit is one exact completed attempt" in normalized
    assert "Legacy workflows, the later `workflow_run` auditor" in normalized
    assert "there is no averaging or percentile" in normalized
    assert "Cancelled attempts still contribute" in normalized
    assert ".total_seconds()) / 60" in normalized


def test_each_child_has_an_executable_contract_gate() -> None:
    spec = _read(SPEC)

    section = spec.split("### Executable child-contract gate", maxsplit=1)[1].split(
        "### Eight route canaries", maxsplit=1
    )[0]
    for child in ("PR 3A", "PR 3B", "PR 4A", "PR 4B", "PR 4C", "PR 5A", "PR 5B", "PR 6", "PR 7"):
        assert child in section
    for expected in ("positive", "negative", "boundary", "retry", "streams/exits"):
        assert expected in section


def test_canonical_packages_own_policy_and_pr6_waits_for_verified_producer() -> None:
    normalized = _squash_whitespace(_read(SPEC))

    for expected in (
        "`dpone.contracts.ci_shadow_*`",
        "`dpone.services.ci.shadow_*`",
        "`dpone.ports.github_ci_shadow`",
        "`dpone.adapters.github_ci_shadow_*`",
        "`.github/workflows/**` and `tools/ci/**` are thin declarative/CLI composition roots only",
        "PR 6B public adapters, migration, merge, and activation require PR 5B merged with authenticated verifier evidence",
        "no planned outage or permanently `UNVERIFIED` production window is accepted",
    ):
        assert expected in normalized


def test_shadow_policy_v2_is_separate_dormant_and_read_only() -> None:
    spec = _read(SPEC)

    assert "`dpone.ci-shadow-governance-policy.v2`" in spec
    normalized = _squash_whitespace(spec)
    assert "not the release-centric `dpone.github-governance-policy.v2`" in normalized
    assert (
        "No `--apply`, restore, write credential, `Administration: write`, other admin-write credential" in normalized
    )
    assert "least-privilege `Administration: read` credential" in normalized
    assert "`final.enabled=false`" in spec
    assert "`app_id=null`" in spec
    assert "`receipt=null`" in spec
    assert "no `.agents/policy` production file" in normalized
    assert "overlay never copies that list" in normalized
    assert "`legacy_expected_provider`" in spec
    assert "Active v1 does not encode provider/App bindings" in normalized
    assert "`app_id=15368`" in spec
    assert "Missing/mixed provider IDs or inability to read them is `UNVERIFIED`" in normalized
    assert "ci_shadow_observer.py" in spec


def test_readiness_transaction_retains_prior_bytes_until_commit_cleanup() -> None:
    normalized = _squash_whitespace(_read(SPEC))
    adr = _squash_whitespace(_read(ADR_0048))

    assert "confined durable prior-byte backup path" in normalized
    assert "Digests are verification data and are never treated as reconstructable prior content" in normalized
    assert "`dpone.manifest.confined_mutations`" in normalized
    assert "While a journal exists, consumers treat the pair as `UNVERIFIED`" in normalized
    assert "restores both members from the journal-bound prior bytes" in normalized
    assert "an absent backup means that exact cleanup step already completed" in normalized
    assert "crash after each COMMITTED backup deletion" in normalized
    assert "actual authenticated bytes" in adr


def test_shadow_adrs_have_exact_accepted_status_and_links() -> None:
    for path in (ADR_0046, ADR_0048):
        text = _read(path)
        assert "## Status\n\nAccepted.\n" in text
        assert "issue #512" in text
        assert "feature-design-ci-pr-gate-exact-sha-evidence.md" in text

    index = _read(ROOT / "docs" / "adr-index.md")
    navigation = _read(ROOT / "mkdocs.yml")
    for relative in (
        "adr/0046-component-aware-pr-gate-authority.md",
        "adr/0048-exact-sha-readiness-evidence.md",
    ):
        assert relative in index
        assert relative in navigation


def test_feature_spec_links_to_overview_authority_and_next_task() -> None:
    spec = _read(SPEC)

    for link in (
        "[CI/CD overview](ci-cd.md)",
        "[GitHub branch protection](github-branch-protection.md)",
        "[ADR 0046](adr/0046-component-aware-pr-gate-authority.md)",
        "[ADR 0048](adr/0048-exact-sha-readiness-evidence.md)",
        "[Agent task contracts](agent-task-contracts.md)",
    ):
        assert link in spec


def test_branch_protection_guide_derives_checks_from_canonical_policy() -> None:
    guide = _read(ROOT / "docs" / "github-branch-protection.md")
    status_section = guide.split("Status checks:", maxsplit=1)[1].split("Required workflows", maxsplit=1)[0]
    normalized_status = _squash_whitespace(status_section)
    policy_path = ROOT / ".agents" / "policy" / "github-branch-protection.yml"
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    checks = tuple(policy["ruleset"]["required_status_checks"]["checks"])

    assert len(checks) == 21
    assert "exact twenty-one contexts resolved from the canonical active policy" in normalized_status
    assert ".agents/policy/github-branch-protection.yml" in normalized_status
    assert "deliberately does not copy the list" in normalized_status
    assert "stores names but does not store App IDs" in normalized_status
    assert "shape/status only" in normalized_status
    assert "required_context_names" in status_section
    assert "hashlib.sha256(raw).hexdigest()" in status_section
    assert "shasum" not in status_section
    assert "Provider binding is separate live evidence" in normalized_status
    assert "no supported bounded, replacement-safe command" in normalized_status
    assert "Do not redirect an ad-hoc `gh api` response" in normalized_status
    assert "gh auth status" in status_section
    assert "descriptor/inode-stable create-only writer" in normalized_status
    assert "App-binding evidence is `UNVERIFIED`" in normalized_status
    assert "write_create_only" not in status_section
    assert "even twenty-one displayed `<context><TAB>15368` rows" in normalized_status
    assert "- `CodeQL`" not in status_section
    assert "Canonical required-check names: PASS — exact twenty-one" in guide
    assert "Ruleset App bindings: UNVERIFIED until the bounded PR 7 observer" in guide
    assert "Branch-protection evidence status: UNVERIFIED" in guide
    assert "Branch protection: PASS" not in guide


def test_pr2_task_contract_binds_real_merged_pr1_base() -> None:
    contract_path = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-closure-pr2-spec.yml"
    payload = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    assert payload["base_commit"] == "66e6b651e93b96a1e8b9b649095d9f2b53e8aa53"
    assert "tests/test_ci_shadow_reconciliation_contracts.py" in payload["owned_paths"]
    assert ".github/workflows" in payload["forbidden_paths"]
    assert ".agents/policy" in payload["forbidden_paths"]
    assert any(
        "does not contain PR 1 merge commit 0fa1b35bfd20c35fa0cb2a8c3dfa966d7afc11d4" in item
        for item in payload["stop_conditions"]
    )
    assert any("Integrator-only delta" in item and "mkdocs.yml" in item for item in payload["dependencies"])
