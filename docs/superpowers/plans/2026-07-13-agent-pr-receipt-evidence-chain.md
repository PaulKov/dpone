# Agent PR Receipt Evidence Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a compact tamper-evident metadata chain to agent PR receipt artifacts.

**Architecture:** Keep GitHub API fetching in `pr_receipt_github.py`, add compact chain serialization in a new focused `pr_evidence_chain.py`, and let `pr_receipt.py` compose raw evidence, traceability, and chain payloads. The audit manifest reads only receipt JSON and projects compact chain fields.

**Tech Stack:** Python stdlib, JSON Schema 2020-12, pytest, jsonschema, GitHub Actions metadata.

---

### Task 1: Receipt Contract Tests

**Files:**
- Modify: `tests/agent_policy/test_pr_receipt.py`
- Modify: `tests/agent_policy/test_audit_manifest.py`

- [x] **Step 1: Write failing tests**

Add tests that assert:

```python
assert payload["evidence_chain"]["head_sha"] == "abc1234"
assert payload["evidence_chain"]["governance_artifact"]["artifact_id"] == 42
assert payload["evidence_chain"]["governance_artifact"]["digest"].startswith("sha256:")
assert any("SHA-256 digest" in error for error in result.errors)
```

- [x] **Step 2: Run tests to verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_audit_manifest.py -q
```

Expected: FAIL because `evidence_chain`, artifact id, and digest fields do not exist yet.

### Task 2: GitHub Evidence Metadata

**Files:**
- Modify: `tools/agent_policy/pr_receipt_github.py`
- Test: `tests/agent_policy/test_pr_receipt.py`

- [x] **Step 1: Extend evidence dataclasses**

Add nullable check/status id, details URL, and workflow run id fields to check evidence. Add artifact id, digest, size, created timestamp, and expiry timestamp to artifact evidence.

- [x] **Step 2: Populate fields from GitHub payloads**

Read `id`, `details_url`, `html_url`, `archive_download_url`, `digest`, `size_in_bytes`, `created_at`, and `expires_at` from GitHub responses. Parse workflow run ids from Actions URLs containing `/actions/runs/<id>`.

- [x] **Step 3: Fail closed on weak governance artifact identity**

Require matching name, matching head SHA, non-expired artifact, positive artifact id, positive workflow run id, and `sha256:<64 hex>` digest.

- [x] **Step 4: Run focused tests**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py -q
```

Expected: PASS for GitHub metadata tests.

### Task 3: Compact Evidence Chain Module

**Files:**
- Create: `tools/agent_policy/pr_evidence_chain.py`
- Modify: `tools/agent_policy/pr_receipt.py`
- Test: `tests/agent_policy/test_pr_receipt.py`

- [x] **Step 1: Create serializer**

Implement `evidence_chain_payload(evidence, self_check_name, governance_artifact_name)` that returns `None` when no live evidence exists and otherwise returns:

```json
{
  "head_sha": "abc1234",
  "required_checks": [],
  "governance_artifact": null
}
```

- [x] **Step 2: Compose in receipt payload**

Add `evidence_chain` to `PullRequestReceiptResult` and `result_payload()`.

- [x] **Step 3: Run focused receipt tests**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_traceability.py -q
```

Expected: PASS.

### Task 4: Schemas And Audit Manifest

**Files:**
- Modify: `evals/agent/pr-receipt.schema.json`
- Modify: `evals/agent/audit-manifest.schema.json`
- Modify: `tools/agent_policy/audit_manifest.py`
- Modify: `tests/agent_policy/test_audit_manifest.py`

- [x] **Step 1: Update schemas**

Allow enriched raw evidence fields and require `evidence_chain` in PR receipts. Add compact chain fields to the audit manifest schema.

- [x] **Step 2: Project compact chain fields**

Copy `evidence_chain_head_sha`, `evidence_chain_required_checks`, and `evidence_chain_governance_artifact` from receipt JSON into `agent_audit_manifest.json`.

- [x] **Step 3: Run schema tests**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_audit_manifest.py -q
```

Expected: PASS with Draft 2020-12 schema validation.

### Task 5: Documentation And Generated Metrics

**Files:**
- Modify: `docs/agent-governance.md`
- Modify: `docs/agent-security-mapping.md`
- Modify: `docs/github-branch-protection.md`
- Modify: `docs/quality-metrics.md`

- [x] **Step 1: Update governance docs**

Document that `Agent PR receipt` records check ids, workflow run ids, governance artifact id, and artifact digest, and fails closed on stale or digestless governance artifacts.

- [x] **Step 2: Refresh generated metrics**

Run:

```bash
DPONE_TEST_USE_INSTALLED_PACKAGE=1 UV_NO_SYNC=1 uv run dpone docs update-dev-metrics
```

Expected: `docs/quality-metrics.md` is updated only by the producer if metrics changed.

### Task 6: Validation And PR

**Files:**
- All changed files

- [x] **Step 1: Generate validation plan**

Run:

```bash
UV_NO_SYNC=1 uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

- [x] **Step 2: Run focused and broad checks**

Run the selected focused tests, agent-policy guards, docs checks, and broad Python checks required by `AGENTS.md`.

- [ ] **Step 3: Commit and open PR**

Commit with:

```bash
git commit -m "Harden agent PR receipt evidence chain"
```

Open a PR with owner attestation left unchecked until GitHub checks and `agent-governance-gate` artifact are available for the final head.
