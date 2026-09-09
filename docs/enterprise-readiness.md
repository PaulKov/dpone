# Enterprise readiness

This document defines the gap between the current `dpone` OSS release and a
framework that can credibly be compared with mature ELT/ETL platforms such as
Airbyte, Fivetran, and Pentaho.

## Current status

`dpone` is production-ready as an OSS MVP:

- Public GitHub repository: `PaulKov/dpone`
- Public PyPI distribution: `dpone`
- Public import package and CLI: `dpone`
- Apache-2.0 license
- Core non-live CI gate with `fail_under = 70`
- Fresh PyPI install smoke for core and `full` extras
- GitHub release artifacts for current public version tags

`dpone` is not yet enterprise-grade in the Airbyte/Fivetran/Pentaho sense. That
requires certified connectors, operational SLOs, compatibility guarantees,
observability contracts, and hardening beyond unit coverage.

## Enterprise-grade bar

### 1. Connector certification

Every connector must have a certification level documented in
[Connector certification](connector-certification.md).

Required for a production-certified connector:

- Configuration schema validation.
- Credential loading contract tests.
- Offline unit tests for query/request construction.
- Local integration tests against disposable services when possible.
- Live integration tests behind explicit `integration_live` markers.
- Rate-limit/retry/idempotency behavior.
- Data type mapping and schema drift coverage.
- Incremental checkpoint recovery tests.

### 2. Reliability guarantees

The runtime must provide documented behavior for:

- Retry and backoff.
- Idempotent task reruns.
- Partial failure recovery.
- Checkpoint persistence.
- Duplicate prevention.
- Schema evolution.
- Soft delete/reconciliation semantics.
- State corruption handling.

### 3. Observability

Each run must expose:

- Stable run ID.
- Manifest path and selector.
- Source and sink identifiers.
- Extracted/staged/loaded/final row counts.
- Insert/update/delete/soft-delete counts.
- Retry counts and failure category.
- Structured logs suitable for centralized ingestion.

### 4. Supply-chain security

The repository must keep:

- CodeQL workflow.
- Secret scanning workflow.
- OSSF Scorecard workflow.
- Dependabot for GitHub Actions and Python dependencies.
- PyPI Trusted Publishing for every ordinary tagged release.
- One OIDC-only controller authority for ordinary publishing and recovery.
- Protected release tags.
- Release artifacts built by CI.

### 5. Compatibility and governance

The project must maintain:

- SemVer policy.
- Deprecation policy.
- Changelog discipline.
- Public API boundary.
- Connector interface stability policy.
- Migration guides for breaking changes.

## Coverage policy

Coverage is a signal, not the goal. The current OSS gate measures core non-live
code and excludes live adapters that require external services or vendor
credentials.

Targets:

- OSS MVP: `>=70%` core non-live coverage.
- Production framework: `>=90%` meaningful core non-live coverage.
- Enterprise-grade: `>=90%` core plus certified integration suites for all
  production connectors.

Avoid meaningless 100% coverage. If code is defensive, environment-specific, or
covered by live certification suites, document why it is excluded instead of
writing brittle tests that only satisfy a metric.

## Roadmap to enterprise-grade

1. Establish connector certification docs and issue templates.
2. Add security workflows and dependency automation.
3. Add structured run telemetry contracts.
4. Add retry/backoff/idempotency primitives.
5. Add live integration profiles for Postgres, ClickHouse, BigQuery, Google Ads,
   Google Sheets, and representative HTTP APIs.
6. Enforce release provenance and Trusted Publishing as the only ordinary release path.
7. Raise meaningful core coverage to `90%+`.
8. Publish an operator guide and failure runbook.
