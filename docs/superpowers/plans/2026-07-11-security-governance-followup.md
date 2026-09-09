# Security Governance Follow-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the current high dependency alert, make release supply-chain evidence a CI-enforced release gate, and add executable red-team checks for agent governance controls.

**Architecture:** Keep the changes small and repository-local. Dependency policy stays in `pyproject.toml` and `uv.lock`; release evidence uses the existing `dpone supply-chain attest` service; agent red-team coverage extends `tools/agent_policy` tests without adding an external evaluation framework.

**Tech Stack:** Python 3.11/3.12, pytest, uv, GitHub Actions, MkDocs, dpone supply-chain attestation service.

---

### Task 1: Patch pyarrow Dependency Range

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`

- [ ] **Step 1: Verify the vulnerable range is test-covered**

Run:

```bash
rg -n 'pyarrow>=16,<23' pyproject.toml uv.lock
```

Expected: matches in `columnar`, `full`, and lock metadata.

- [ ] **Step 2: Update dependency constraints**

Change both optional dependency entries from:

```toml
"pyarrow>=16,<23"
```

to:

```toml
"pyarrow>=23.0.1,<24"
```

- [ ] **Step 3: Refresh the lockfile**

Run:

```bash
uv lock
```

Expected: lock resolves `pyarrow` at `23.0.1` or newer within `<24`.

### Task 2: Enforce Release Supply-chain Attestation

**Files:**
- Modify: `.github/workflows/release.yml`
- Modify: `docs/supply-chain.md`
- Modify: `docs/developer-ci-cd.md`
- Test: `tests/test_cli_supply_chain_commands.py`

- [ ] **Step 1: Add a failing CLI contract test**

Add a test that invokes `dpone supply-chain attest` without `--signing-key` and asserts the command exits non-zero with `signature.missing_key`.

- [ ] **Step 2: Run the new test**

Run:

```bash
uv run pytest tests/test_cli_supply_chain_commands.py::test_supply_chain_attest_fails_without_signing_key -q
```

Expected: FAIL because the test does not exist yet; after adding it, PASS if existing behavior is already correct.

- [ ] **Step 3: Wire release workflow evidence generation**

After `twine check`, add a step that runs:

```bash
uv run dpone supply-chain attest \
  --release "${GITHUB_REF_NAME:-manual-${GITHUB_RUN_ID}}" \
  --subject dist/dpone-*.whl \
  --subject dist/dpone-*.tar.gz \
  --repository "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
  --commit-sha "$GITHUB_SHA" \
  --builder-id "github-actions:$GITHUB_RUN_ID" \
  --signing-key "$DPONE_LOCAL_ATTESTATION_KEY" \
  --signing-key-id github-actions \
  --output-dir test_artifacts/supply-chain/current \
  --format json
```

Set `DPONE_LOCAL_ATTESTATION_KEY` from a repository secret. Upload `test_artifacts/supply-chain/current/` with `if: always()`.

### Task 3: Add Agent Red-team Policy Scenarios

**Files:**
- Modify: `tools/agent_policy/validate_setup.py`
- Modify: `tests/agent_policy/test_agent_policy.py`
- Modify: `docs/agent-risk-register.md`

- [ ] **Step 1: Add failing red-team tests**

Add tests that assert agent governance docs mention and validate these scenarios:

```text
prompt injection
secret disclosure
workflow tampering
verification laundering
excessive agency
unbounded consumption
```

- [ ] **Step 2: Add minimal validator support**

Teach `validate_setup.py` to fail when required red-team phrases are missing from the governance/risk/security docs.

- [ ] **Step 3: Run focused agent policy tests**

Run:

```bash
uv run pytest tests/agent_policy/test_agent_policy.py -q
```

Expected: PASS with the new coverage.

### Task 4: Validation and Publishing

**Files:**
- All changed files.

- [ ] **Step 1: Run focused tests**

```bash
uv run pytest tests/agent_policy/test_agent_policy.py tests/test_supply_chain_attestation.py tests/test_cli_supply_chain_commands.py -q
```

- [ ] **Step 2: Run formatting and typing gates**

```bash
uv run ruff check tools/agent_policy tests/agent_policy/test_agent_policy.py tests/test_cli_supply_chain_commands.py
uv run ruff format --check tools/agent_policy tests/agent_policy/test_agent_policy.py tests/test_cli_supply_chain_commands.py
uv run mypy --config-file mypy.ini tools/agent_policy tests/agent_policy/test_agent_policy.py tests/test_cli_supply_chain_commands.py
```

- [ ] **Step 3: Run docs gates**

```bash
uv run dpone docs check-docs
uv run mkdocs build --strict
```

- [ ] **Step 4: Run broad CI-equivalent gates before publishing**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live" -n auto --dist loadfile
```

- [ ] **Step 5: Commit, push, open PR**

Commit the scoped files, push `codex/security-governance-followup`, and open a PR with validation evidence.
