# Route Certify Release Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a release-level route certification automation gate that aggregates first-class `route_certification_bundle.json` artifacts and decides whether a release candidate can be tagged.

**Architecture:** Keep route execution and route certification separate. `route-certify-release` reads immutable route bundle artifacts, normalizes route status, applies a pure fail-closed policy, writes stable JSON/Markdown release artifacts, and exposes a thin CLI plus a manual GitHub workflow hook.

**Tech Stack:** Python dataclasses, existing route `RouteKey`, ops CLI registry, pytest, GitHub Actions, MkDocs.

---

### Task 1: Red Tests

**Files:**
- Create: `tests/test_route_certify_release.py`
- Create: `tests/test_cli_route_certify_release_command.py`
- Create: `tests/test_route_certify_release_docs_contract.py`

- [ ] Test a release with certified Postgres -> MSSQL and MSSQL -> ClickHouse bundles returns `release_ready`.
- [ ] Test missing required route bundle fails closed.
- [ ] Test `vendor_live` release profile requires each bundle to be produced with `profile=vendor_live`.
- [ ] Test CLI JSON output and nonzero exit for blockers.
- [ ] Test docs, CLI reference, source-sink matrix, architecture, CI/CD, and workflow references.

### Task 2: Models, Policy, Service, CLI

**Files:**
- Create: `src/dpone/ops/routes/certify_release_models.py`
- Create: `src/dpone/ops/routes/certify_release_policy.py`
- Create: `src/dpone/ops/route_certify_release.py`
- Modify: ops catalogs, CLI parser/handler registry, route exports.

- [ ] Define schema `dpone.route_certification_release.v1`.
- [ ] Read and checksum bundle artifacts without route-specific service branches.
- [ ] Default required routes to `postgres_to_mssql__incremental_merge` and `mssql_to_clickhouse__incremental_merge`.
- [ ] Implement pure policy with `release_ready`, `warning`, and `blocked`.
- [ ] Register `dpone ops route-certify-release`.

### Task 3: Workflow And Docs

**Files:**
- Create: `.github/workflows/route-certification-release.yml`
- Create: `docs/route-certify-release.md`
- Create: `docs/developer-route-certify-release.md`
- Modify: README, architecture, CI/CD, developer CI/CD, ops CLI, source-sink docs, `route-certify.md`, mkdocs.

- [ ] Add manual workflow that runs the release gate over conventional artifact paths and uploads the release report.
- [ ] Document OSS-safe and vendor-live modes, required artifact paths, outputs, and runbook.
- [ ] Document SOLID boundaries and extension rules.

### Task 4: Verification And PR

**Files:**
- All touched files.

- [ ] Regenerate CLI reference and quality metrics.
- [ ] Run focused tests, static checks, docs checks, MkDocs strict, full non-live pytest.
- [ ] Stage only intentional files, commit, push, and open stacked PR.
