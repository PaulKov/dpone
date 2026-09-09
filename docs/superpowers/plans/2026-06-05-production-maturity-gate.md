# Production maturity gate implementation plan

## Goal

Add a single release-readiness gate that aggregates production evidence across connector certification, CDC replay, performance benchmarks, security, supply-chain, governance, and documentation.

## Design

1. Add a small `dpone.ops.production_maturity` service with immutable report models.
2. Keep specialized gates independent; the maturity gate reads their JSON artifacts and records checksums.
3. Add `dpone ops production-maturity` as a thin CLI wrapper around the service.
4. Add a scheduled/manual GitHub Actions workflow that proves the aggregator and uploads `production-maturity-report`.
5. Document the operator UX, evidence domains, algorithm, and failure runbook.

## Quality gates

- Focused unit and CLI tests.
- CI/CD docs contract test for workflow and docs coverage.
- Existing lint/type/docs/package gates before push.
