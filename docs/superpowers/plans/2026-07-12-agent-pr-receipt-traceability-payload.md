# Agent PR Receipt Traceability Payload Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a structured `traceability` object to `agent_pr_receipt.json` and a compact traceability summary to `agent_audit_manifest.json`.

**Architecture:** Keep Markdown parsing and source classification in `tools/agent_policy/pr_traceability.py`; keep receipt composition in `tools/agent_policy/pr_receipt.py`; keep audit-manifest projection in `tools/agent_policy/audit_manifest.py`. Update JSON schemas and docs in the same PR because this changes the evidence contract.

**Tech Stack:** Python stdlib, pytest, jsonschema, JSON Schema 2020-12, existing dpone docs/gate tooling.

---

### Task 1: Receipt Traceability Payload

**Files:**
- Modify: `tools/agent_policy/pr_traceability.py`
- Modify: `tools/agent_policy/pr_receipt.py`
- Modify: `tests/agent_policy/test_pr_receipt.py`
- Modify: `tests/agent_policy/test_pr_receipt_traceability.py`
- Modify: `evals/agent/pr-receipt.schema.json`

- [x] **Step 1: Write failing payload tests**

Add tests asserting `result_payload(result)["traceability"]` contains:

```python
{
    "approved_source": "#275",
    "approved_source_kind": "issue",
    "validation_statuses": ["PASS"],
    "non_pass_reasons": [],
    "owner_attestation": {
        "owner_review": True,
        "required_checks": True,
        "admin_bypass": True,
        "governance_receipt": True,
    },
    "governance_receipt_referenced": True,
}
```

Add a second test asserting non-agent PRs emit `"traceability": None`.

- [x] **Step 2: Verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_traceability.py -q
```

Expected: FAIL because `traceability` is absent.

- [x] **Step 3: Implement minimal extraction and serialization**

Add a frozen `TraceabilityPayload` data model or plain typed dictionaries in
`pr_traceability.py`, expose `extract_traceability(body)`, attach it to
`PullRequestReceiptResult`, and include it in `result_payload()`.

- [x] **Step 4: Verify GREEN**

Run the same focused pytest command. Expected: PASS.

- [x] **Step 5: Update receipt schema**

Add required `traceability` property to `evals/agent/pr-receipt.schema.json` with
`anyOf: null | object`. Keep `schema_version: 2`.

### Task 2: Audit Manifest Summary

**Files:**
- Modify: `tools/agent_policy/audit_manifest.py`
- Modify: `tests/agent_policy/test_audit_manifest.py`
- Modify: `evals/agent/audit-manifest.schema.json`

- [x] **Step 1: Write failing manifest tests**

Extend the PR receipt manifest test receipt fixture with a `traceability` object
and assert:

```python
assert manifest["traceability_source"] == "#275"
assert manifest["traceability_source_kind"] == "issue"
assert manifest["traceability_statuses"] == ["PASS"]
assert manifest["traceability_non_pass_reasons"] == []
```

- [x] **Step 2: Verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_audit_manifest.py -q
```

Expected: FAIL because compact fields are absent.

- [x] **Step 3: Implement manifest projection**

Read `receipt["traceability"]` as a mapping and project only compact scalar/list
fields into `agent_audit_manifest.json`.

- [x] **Step 4: Verify GREEN**

Run the same manifest pytest command. Expected: PASS.

### Task 3: Docs, Metrics, and Gates

**Files:**
- Modify: `docs/agent-governance.md`
- Modify: `docs/agent-security-mapping.md`
- Modify: `docs/github-branch-protection.md`
- Modify: `docs/quality-metrics.md`

- [x] **Step 1: Update docs**

Document that `agent_pr_receipt.json` now contains structured traceability and
that `agent_audit_manifest.json` contains compact traceability fields.

- [x] **Step 2: Regenerate quality metrics**

Run:

```bash
DPONE_TEST_USE_INSTALLED_PACKAGE=1 UV_NO_SYNC=1 uv run dpone docs update-dev-metrics
```

- [x] **Step 3: Run focused and broad validation**

Run:

```bash
UV_NO_SYNC=1 uv run python tools/agent_policy/select_checks.py --base-ref origin/master
UV_NO_SYNC=1 uv run ruff check .
UV_NO_SYNC=1 uv run ruff format --check .
UV_NO_SYNC=1 uv run mypy --config-file mypy.ini
UV_NO_SYNC=1 uv run dpone docs check-import-rules
UV_NO_SYNC=1 uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
UV_NO_SYNC=1 uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
UV_NO_SYNC=1 uv run pytest tests/agent_policy tests/test_module_size_gate.py -q
UV_NO_SYNC=1 uv run dpone docs check-docs
UV_NO_SYNC=1 uv run pytest tests/test_docs_language_contracts.py -q
UV_NO_SYNC=1 uv run mkdocs build --strict
DPONE_TEST_USE_INSTALLED_PACKAGE=1 UV_NO_SYNC=1 uv run dpone docs update-dev-metrics --check
UV_NO_SYNC=1 uv run pytest -m "not integration_live" -n auto --dist loadfile
```
