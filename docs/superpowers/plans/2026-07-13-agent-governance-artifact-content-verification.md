# Agent Governance Artifact Content Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Agent PR receipt` download and validate the contents of `agent-governance-gate`, not only its metadata.

**Architecture:** Add one pure parser/validator module for governance artifact zip content. Keep GitHub HTTP in `pr_receipt_github_api.py`, live evidence assembly in `pr_receipt_github.py`, policy enforcement in `pr_receipt.py`, and compact audit projection in `pr_evidence_chain.py`/`audit_manifest.py`.

**Tech Stack:** Python stdlib (`json`, `zipfile`, `io`, `urllib`), pytest, jsonschema, repository-local agent policy tooling.

---

### Task 1: Specification And Plan

**Files:**
- Create: `docs/feature-design-agent-governance-artifact-content-verification.md`
- Create: `docs/superpowers/plans/2026-07-13-agent-governance-artifact-content-verification.md`

- [x] **Step 1: Write approved feature specification**

Record the user problem, algorithm, GitHub artifact API source, N/A market
comparison rows, test plan, and rollback plan.

- [x] **Step 2: Write implementation plan**

Create this task list with explicit TDD steps and validation commands.

### Task 2: Artifact Content Parser

**Files:**
- Create: `tools/agent_policy/governance_artifact_content.py`
- Create: `tests/agent_policy/test_governance_artifact_content.py`
- Modify: `tools/agent_policy/setup_inventory.py`
- Modify: `tests/agent_policy/test_agent_policy.py`

- [x] **Step 1: Write failing parser tests**

Add tests that build in-memory zip archives containing
`agent_governance_gate.json` and assert:

```python
evidence = governance_artifact_content.content_from_archive_bytes(zip_bytes)
assert evidence.status == "PASS"
assert evidence.control_surface_changed is True
assert evidence.changed_paths == ["tools/agent_policy/pr_receipt.py"]
assert evidence.check_status("changed_control_surface_red_team") == "PASS"
```

Also test missing JSON, duplicate JSON, invalid JSON, and non-object JSON.

- [x] **Step 2: Verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_governance_artifact_content.py -q
```

Expected: FAIL because `tools/agent_policy/governance_artifact_content.py` does
not exist.

- [x] **Step 3: Implement pure parser and validator**

Implement:

```python
@dataclass(frozen=True)
class GovernanceArtifactContentEvidence:
    schema_version: int | None
    status: str | None
    control_surface_changed: bool | None
    changed_paths: list[str]
    head_commit: str | None
    checks: list[dict[str, str | None]]
    errors: list[str] = field(default_factory=list)
```

Add `content_from_archive_bytes()`, `content_from_payload()`, and
`validate_content_for_receipt()`.

- [x] **Step 4: Register parser in inventory**

Add `tools/agent_policy/governance_artifact_content.py` to setup inventory and
expected test inventory.

### Task 3: Live GitHub Download And Receipt Enforcement

**Files:**
- Modify: `tools/agent_policy/pr_receipt_github_api.py`
- Modify: `tools/agent_policy/pr_receipt_github.py`
- Modify: `tools/agent_policy/pr_receipt.py`
- Modify: `tests/agent_policy/test_pr_receipt.py`
- Modify: `tests/agent_policy/test_pr_receipt_evidence_chain.py`

- [x] **Step 1: Write failing receipt tests**

Add tests proving:

```python
result = pr_receipt.validate_pr_receipt(... artifact content with changed_paths=[])
assert result.status == "FAIL"
assert "changed_paths" in " ".join(result.errors)
```

Add a positive test where metadata, content paths, `status: PASS`, and
`changed_control_surface_red_team: PASS` all match.

- [x] **Step 2: Verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_evidence_chain.py -q
```

Expected: FAIL because metadata-only artifacts still pass.

- [x] **Step 3: Add GitHub bytes helper**

Add `github_bytes(path_or_url, token)` to `pr_receipt_github_api.py` using the
same headers and timeout as JSON requests.

- [x] **Step 4: Attach content evidence to artifacts**

Extend `GitHubArtifactEvidence` with nullable `content`. In `_fetch_artifacts`,
download and parse content for matching head artifacts.

- [x] **Step 5: Enforce content in receipt validation**

Pass normalized changed paths to `validate_governance_artifact_evidence()` and
fail when content is missing, invalid, stale, or has `changed_control_surface_red_team`
not equal to `PASS`.

### Task 4: Schemas, Audit Manifest, And Docs

**Files:**
- Modify: `evals/agent/pr-receipt.schema.json`
- Modify: `evals/agent/audit-manifest.schema.json`
- Modify: `tools/agent_policy/pr_evidence_chain.py`
- Modify: `tools/agent_policy/audit_manifest.py`
- Modify: `tests/agent_policy/test_audit_manifest.py`
- Modify: `docs/agent-governance.md`
- Modify: `docs/github-branch-protection.md`
- Modify: `docs/feature-design-agent-pr-receipt-evidence-chain.md`
- Modify: `docs/quality-metrics.md`

- [x] **Step 1: Update schemas and compact projections**

Allow content evidence in raw artifact payloads and copy content status, changed
paths, and check summaries into evidence-chain governance artifact payloads.

- [x] **Step 2: Update audit manifest tests**

Assert `evidence_chain_governance_artifact` includes content summary and still
validates against `evals/agent/audit-manifest.schema.json`.

- [x] **Step 3: Update docs**

Document that `Agent PR receipt` opens the governance artifact and validates
its JSON content against PR changed paths.

- [x] **Step 4: Refresh generated metrics**

Run:

```bash
DPONE_TEST_USE_INSTALLED_PACKAGE=1 UV_NO_SYNC=1 uv run dpone docs update-dev-metrics
```

### Task 5: Validation, PR, Receipt, Merge

**Files:**
- All changed files

- [x] **Step 1: Run focused checks**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_governance_artifact_content.py tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_evidence_chain.py tests/agent_policy/test_audit_manifest.py tests/agent_policy/test_agent_policy.py -q
```

- [x] **Step 2: Run selected and broad gates**

Run `select_checks.py --base-ref origin/master`, ruff, mypy, import/layer/module
gates, strict agent-policy module-size gates, docs checks, and broad non-live
pytest.

- [ ] **Step 3: Open PR with generated PR body**

Use `tools/agent_policy/pr_body.py` for draft and final body checks. Leave owner
attestation unchecked until GitHub checks and `agent-governance-gate` artifact
exist for the final head.

- [ ] **Step 4: Final receipt and merge**

After all required checks are green, update the PR body with artifact evidence,
wait for `Agent PR receipt: PASS`, merge without admin bypass, and verify
post-merge `Agent Governance Drift`.
