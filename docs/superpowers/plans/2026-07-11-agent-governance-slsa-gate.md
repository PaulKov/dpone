# Agent Governance SLSA Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an executable governance gate and SLSA self-assessment so agent-control and release-policy changes produce one auditable JSON receipt.

**Architecture:** Keep the gate repo-local under `tools/agent_policy/`, reuse existing validators, and have CI upload `test_artifacts/agent-policy/agent_governance_gate.json`. Documentation explains the scoped SLSA posture without making higher-level claims.

**Tech Stack:** Python standard library, pytest, GitHub Actions, MkDocs, JSON Schema, GitHub Artifact Attestations, OpenSSF Scorecard.

---

### Task 1: Governance gate script

**Files:**
- Create: `tools/agent_policy/governance_gate.py`
- Create: `evals/agent/governance-gate.schema.json`
- Test: `tests/agent_policy/test_governance_gate.py`

- [x] **Step 1: Write tests for PASS, N/A, FAIL, and JSON receipt output.**

```python
report = governance_gate.build_report(ROOT, ["docs/agent-governance.md"])
assert report["status"] == "PASS"
```

- [x] **Step 2: Implement the gate.**

```bash
uv run python tools/agent_policy/governance_gate.py \
  --base-ref origin/master \
  --output test_artifacts/agent-policy/agent_governance_gate.json
```

- [x] **Step 3: Run focused tests.**

Expected: `tests/agent_policy/test_governance_gate.py` passes.

### Task 2: Agent policy integration

**Files:**
- Modify: `tools/agent_policy/validate_setup.py`
- Modify: `tools/agent_policy/select_checks.py`
- Modify: `tests/agent_policy/test_agent_policy.py`

- [x] **Step 1: Require the new gate, schema, and SLSA page in setup validation.**
- [x] **Step 2: Add the governance gate command to agent-policy change-aware checks.**
- [x] **Step 3: Run agent policy tests.**

Expected: `tests/agent_policy/test_agent_policy.py` passes.

### Task 3: CI integration

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/test_github_workflow_governance.py`

- [x] **Step 1: Run the governance gate on Python 3.12 CI.**
- [x] **Step 2: Upload `agent-governance-gate` evidence with `if: always()`.**
- [x] **Step 3: Run workflow governance tests.**

Expected: `tests/test_github_workflow_governance.py` passes and all actions remain pinned by SHA.

### Task 4: Documentation and SLSA self-assessment

**Files:**
- Create: `docs/supply-chain-slsa.md`
- Modify: `docs/agent-governance.md`
- Modify: `docs/agent-security-mapping.md`
- Modify: `docs/agent-risk-register.md`
- Modify: `docs/release-evidence.md`
- Modify: `docs/supply-chain.md`
- Modify: `docs/developer-supply-chain.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `mkdocs.yml`
- Modify: `CHANGELOG.md`

- [x] **Step 1: Add scoped SLSA release self-assessment with explicit non-claims.**
- [x] **Step 2: Cross-link the gate from agent, release, and supply-chain docs.**
- [x] **Step 3: Run docs checks.**

Expected: `uv run dpone docs check-docs` and `uv run mkdocs build --strict` pass.

### Task 5: Final validation and PR

**Files:**
- All changed files in this plan.

- [x] **Step 1: Run change-aware validation plan.**

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

- [x] **Step 2: Run focused and broad checks selected for the diff.**

Expected focused checks:

```bash
uv run pytest tests/agent_policy tests/test_github_workflow_governance.py -q
uv run python tools/agent_policy/governance_gate.py --base-ref origin/master --output test_artifacts/agent-policy/agent_governance_gate.json
uv run ruff check tools/agent_policy/governance_gate.py tests/agent_policy/test_governance_gate.py
uv run ruff format --check tools/agent_policy/governance_gate.py tests/agent_policy/test_governance_gate.py
uv run dpone docs check-docs
uv run mkdocs build --strict
```

- [ ] **Step 3: Commit, push, and open a PR.**

Expected PR summary: governance gate, SLSA self-assessment, CI evidence upload, and validation evidence.
