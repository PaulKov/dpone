# Agent PR Body Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local generator and preflight checker for agent-control PR body grammar.

**Architecture:** Add one focused policy helper, `tools/agent_policy/pr_body.py`, that reuses `pr_traceability.py`, `pr_receipt.py`, and `control_surface.py`. Keep live GitHub evidence in the existing CI receipt; local preflight validates Markdown shape and final owner-attestation grammar only.

**Tech Stack:** Python stdlib, pytest, repository-local agent policy modules.

---

### Task 1: TDD Contract Tests

**Files:**
- Create: `tests/agent_policy/test_pr_body.py`

- [x] **Step 1: Write failing tests**

Add tests for:

```python
body = pr_body.render_body(approved_source="docs/feature-design-agent-pr-body-preflight.md")
assert "- Approved specification or issue: docs/feature-design-agent-pr-body-preflight.md" in body
assert pr_body.check_body(body=body, changed_paths=["tools/agent_policy/pr_body.py"], phase="draft").status == "PASS"
```

Also add tests that a heading-only approved source fails draft preflight and an
unchecked generated body fails final preflight.

- [x] **Step 2: Verify RED**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_body.py -q
```

Expected: FAIL because `tools/agent_policy/pr_body.py` does not exist.

### Task 2: Implement Preflight Helper

**Files:**
- Create: `tools/agent_policy/pr_body.py`
- Modify: `tools/agent_policy/setup_inventory.py`
- Modify: `tests/agent_policy/test_agent_policy.py`

- [x] **Step 1: Add render and check functions**

Implement:

```python
render_body(...)
check_body(body: str, changed_paths: list[str], phase: Literal["draft", "final"]) -> PreflightResult
```

Draft phase uses `pr_traceability`; final phase delegates to `pr_receipt`.

- [x] **Step 2: Add CLI subcommands**

Add:

```bash
uv run python tools/agent_policy/pr_body.py render --approved-source docs/example.md
uv run python tools/agent_policy/pr_body.py check --phase draft --body-file pr.md --changed-paths tools/agent_policy/pr_body.py
```

- [x] **Step 3: Register the helper in agent inventory**

Add `tools/agent_policy/pr_body.py` to required setup inventory and the
high-risk setup test expectations.

### Task 3: Documentation

**Files:**
- Modify: `docs/agent-governance.md`
- Modify: `docs/github-branch-protection.md`

- [x] **Step 1: Document the local loop**

Describe render -> draft preflight -> PR -> CI -> final body edit -> final
preflight -> `Agent PR receipt`.

- [x] **Step 2: Add troubleshooting guidance**

Mention that grammar failures should be reproduced locally with
`tools/agent_policy/pr_body.py check` before rerunning GitHub CI.

### Task 4: Validation And PR

**Files:**
- All changed files

- [x] **Step 1: Run focused checks**

Run:

```bash
UV_NO_SYNC=1 uv run pytest tests/agent_policy/test_pr_body.py tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_traceability.py tests/agent_policy/test_agent_policy.py -q
```

- [x] **Step 2: Run selected gates**

Run `select_checks.py --base-ref origin/master`, agent-policy strict module-size
guards, docs checks, ruff, mypy, import/layer/module gates, and broad non-live
pytest.

- [x] **Step 3: Commit, push, and open PR**

Use the generated PR body grammar and leave final owner attestation unchecked
until GitHub checks and the `agent-governance-gate` artifact exist for the final
head.

- [x] **Step 4: Fix CI governance changed-path evidence**

After the first green CI pass, verify that the uploaded
`agent-governance-gate` artifact records the reviewed PR changed paths. If it
does not, update CI to pass the pull-request base/head diff explicitly into
`governance_gate.py` before finalizing owner attestation.
