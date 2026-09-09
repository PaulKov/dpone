# Data Product Remediation Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in execution receipts and certificates for v0.71 data product remediation actions.

**Architecture:** Keep `commands -> services -> readiness`. Readiness modules are pure and provider-neutral; the service facade owns file I/O and the local subprocess adapter. Existing v0.71 remediation planning remains unchanged.

**Tech Stack:** Python 3.11/3.12, pytest, argparse command groups, dpone JSON/YAML artifact conventions, stable fingerprints.

---

### Task 1: Red Tests For Execution Contracts

**Files:**
- Create: `tests/test_data_product_remediation_execution_contracts.py`
- Create: `tests/test_data_product_remediation_execution_cli.py`
- Create: `tests/test_data_product_remediation_execution_integration.py`

- [ ] Write failing unit tests for disabled execution, unresolved placeholders, allowlist blocks, dry-run receipts, execute preconditions, fake executor receipts, and certification.
- [ ] Write failing CLI tests for help and `--output` parity.
- [ ] Write failing integration tests for bundle artifact kinds and registry stages.
- [ ] Run focused pytest and verify failure is missing `dpone.readiness.data_product_remediation_execution`.

### Task 2: Readiness Execution Models

**Files:**
- Create: `src/dpone/readiness/data_product_remediation_execution_support.py`
- Create: `src/dpone/readiness/data_product_remediation_execution.py`
- Create: `src/dpone/readiness/data_product_remediation_execution_rendering.py`

- [ ] Implement immutable options from manifest.
- [ ] Implement safe command normalization with parameter substitution, `shlex.split`, unresolved placeholder detection, and command prefix allowlist.
- [ ] Implement execution planner, runner with injected executor protocol, certifier, and renderer.
- [ ] Run focused unit tests to green.

### Task 3: CLI And Facade

**Files:**
- Create: `src/dpone/services/data_product_remediation_execution.py`
- Create: `src/dpone/commands/data_product_remediation_execution_cmd.py`
- Modify: `src/dpone/commands/data_product_remediation_cmd.py`

- [ ] Add thin file-I/O facade and local `dpone` command executor using `subprocess.run(..., shell=False)`.
- [ ] Add `remediation execution plan|run|certify|report`.
- [ ] Keep CLI lazy imports and output parity through existing data product artifact renderer.
- [ ] Run focused CLI tests to green.

### Task 4: Bundle, Registry, Docs, Schemas, Release Metadata

**Files:**
- Modify small bundle/registry helper modules only.
- Add schemas under `docs/schemas/data-product/`.
- Update `docs/data-product-remediation.md`, developer docs, mkdocs, CLI reference, quality metrics, changelog, package versions.

- [ ] Add execution artifact kinds and registry stages.
- [ ] Add user/developer docs and schemas.
- [ ] Bump version to `0.72.0`.
- [ ] Regenerate CLI reference and dev metrics.

### Task 5: Validation, Commit, PR

- [ ] Run focused pytest.
- [ ] Run ruff, format check, mypy.
- [ ] Run import/layer/module-size/architecture docs checks.
- [ ] Run full non-live pytest.
- [ ] Build all packages, twine check, wheel smoke.
- [ ] Mark spec `IMPLEMENTED`, commit, push, and open/update PR.
