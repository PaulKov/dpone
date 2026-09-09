# Airflow pipeline-first UX P0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Progressive disclosure for Airflow self-service docs: simple beginner CJM, separate advanced Data Engineer CJM, no runtime/schema changes.

**Architecture:** Documentation and MkDocs nav only. Machine terms (`workload_id`, URIs, enums) stay; beginner prose uses **pipeline**. Contract tests in `tests/test_docs_language_contracts.py` enforce glossary, What-to-commit, disclaimer marker, and nav demotion.

**Tech Stack:** Markdown, MkDocs, pytest language contracts, `dpone docs check-docs`.

## Global Constraints

- Spec: `docs/feature-design-airflow-pipeline-first-ux-p0.md` status `APPROVED`.
- Forbidden: `src/**`, `packages/**`, `docs/schemas/**` field renames, `docs/airflow-self-service-public-contracts-v1.yaml`, `tests/test_airflow_v1_public_contract_freeze.py`.
- Do not alter golden bash fences on First Airflow DAG (five commands stay identical).
- Disclaimer marker exact substring: `Advanced path (not the beginner journey)`.
- Beginners: do not edit `domains/<domain>.yaml`.
- P0 acceptance: docs contract tests only (no user study).

---

### Task 1: Failing language-contract tests (integrator first / TDD)

**Files:**
- Modify: `tests/test_docs_language_contracts.py`
- Test: same file

**Interfaces:**
- Consumes: APPROVED assert list from spec
- Produces: failing tests that define P0 done

- [ ] **Step 1: Add P0 contract tests**

```python
ADVANCED_MARKER = "Advanced path (not the beginner journey)"

def test_airflow_pipeline_glossary_defines_core_nouns() -> None:
    page = DOCS_ROOT / "getting-started" / "airflow-pipeline-glossary.md"
    content = page.read_text(encoding="utf-8")
    for noun in ("pipeline", "workload", "DAG", "pack"):
        assert noun.lower() in content.lower()

def test_first_airflow_dag_what_to_commit_guides_beginners() -> None:
    content = (DOCS_ROOT / "getting-started" / "first-airflow-dag.md").read_text(
        encoding="utf-8"
    )
    assert "pipelines/" in content
    assert "do not edit" in content.lower()
    assert "domains/" in content

def test_advanced_airflow_pages_carry_beginner_disclaimer() -> None:
    paths = [
        DOCS_ROOT / "airflow-self-service-selectors.md",
        DOCS_ROOT / "gitops-workload-catalog.md",
        DOCS_ROOT / "airflow-self-service-advanced-cjm.md",
        ROOT / "examples" / "airflow-self-service-pilot" / "README.md",
    ]
    for path in paths:
        assert ADVANCED_MARKER in path.read_text(encoding="utf-8"), path

def test_mkdocs_nav_discloses_airflow_advanced_away_from_beginners() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "getting-started/airflow-pipeline-glossary.md" in mkdocs
    assert "airflow-self-service-advanced-cjm.md" in mkdocs
    getting_started = mkdocs.split("Getting started:", 1)[1].split("\n  - ", 1)[0]
    assert "gitops-workload-catalog.md" not in getting_started
    assert "airflow-self-service-selectors.md" not in getting_started
    core_concepts = mkdocs.split("Core concepts:", 1)[1].split("\n  - ", 1)[0]
    assert "gitops-workload-catalog.md" not in core_concepts
    assert "airflow-self-service-selectors.md" not in core_concepts
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_docs_language_contracts.py -q -k "glossary or what_to_commit or beginner_disclaimer or discloses_airflow"`
Expected: FAIL (missing files / markers / nav)

---

### Task 2: Beginner docs (glossary + First DAG + credentials)

**Files:**
- Create: `docs/getting-started/airflow-pipeline-glossary.md`
- Modify: `docs/getting-started/first-airflow-dag.md`
- Modify: `docs/getting-started/credentials-quickstart.md`

**Interfaces:**
- Produces: What-to-commit tables; glossary nouns; `workload_start` glossary link

- [ ] **Step 1: Write glossary** (pipeline / workload / DAG / pack; 1:1:1 beginner; link to advanced CJM)
- [ ] **Step 2: Insert What-to-commit after init project + after init pipeline expected files** (exact rows from APPROVED spec)
- [ ] **Step 3: Link first “workload pack” / “workload-scoped” occurrences to glossary**
- [ ] **Step 4: Credentials quickstart — link glossary at `workload_start`**
- [ ] **Step 5: Re-run golden-path test** — must still PASS

---

### Task 3: Advanced docs (CJM + disclaimers + hub)

**Files:**
- Create: `docs/airflow-self-service-advanced-cjm.md`
- Modify: `docs/airflow-self-service-selectors.md`
- Modify: `docs/gitops-workload-catalog.md`
- Modify: `examples/airflow-self-service-pilot/README.md`
- Modify: `docs/airflow-self-service.md`

**Interfaces:**
- Consumes: `ADVANCED_MARKER` exact text
- Produces: advanced CJM; hub Advanced section; Analyst → advanced CJM; no day-1 selectors

- [ ] **Step 1: Advanced CJM page with marker + when-to-use + links**
- [ ] **Step 2: Prepend marker to selectors + GitOps catalog**
- [ ] **Step 3: Pilot README — include exact marker substring + relative First DAG link**
- [ ] **Step 4: Hub — rewrite Audience Analyst; move selectors into `## Advanced`; keep beginner pointer to First DAG**

---

### Task 4: Integrator (nav, changelog, backlog, freeze proof)

**Files:**
- Modify: `mkdocs.yml`
- Modify: `CHANGELOG.md`
- Modify: `docs/airflow-self-service-backlog.md`
- Keep: `docs/feature-design-airflow-pipeline-first-ux-p0.md` (APPROVED → IMPLEMENTED after gates)
- Forbidden unchanged: freeze YAML + freeze tests

- [ ] **Step 1: Getting started nav adds glossary**
- [ ] **Step 2: Add Advanced Airflow nav cluster; remove catalog from Core concepts; move selectors from Reference into Advanced**
- [ ] **Step 3: CHANGELOG Unreleased docs note**
- [ ] **Step 4: Backlog checkbox for P0 pipeline-first UX**
- [ ] **Step 5: Gates**

```bash
uv run pytest tests/test_docs_language_contracts.py -q
uv run dpone docs check-docs
uv run mkdocs build --strict
git diff --exit-code docs/airflow-self-service-public-contracts-v1.yaml
git diff --exit-code tests/test_airflow_v1_public_contract_freeze.py
```

- [ ] **Step 6: Mark spec IMPLEMENTED; open MR**

## Spec coverage self-check

| Spec item | Task |
|---|---|
| Glossary | 2 |
| Advanced CJM | 3 |
| What-to-commit | 2 |
| Classification / glossary links | 2 |
| Disclaimer marker | 3 |
| Hub roles + demote selectors | 3 |
| mkdocs demotion | 4 |
| Language contracts | 1+4 |
| CHANGELOG / backlog | 4 |
| Freeze untouched | 4 |
