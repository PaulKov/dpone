# ADR 0003: Edge resolver as single source of truth

Status: accepted

## Context

DAG rendering, manifest explanation, and dependency reports must agree on why a task depends on another task. Multiple ad-hoc resolvers would produce confusing or conflicting diagnostics.

## Decision

Use `dpone.dag.edge_resolver` as the single source of truth for dependency semantics.

All renderers and explain/report commands must reuse it instead of implementing local dependency logic.

## Consequences

- Dependency behavior is consistent across CLI outputs.
- New dependency semantics are added in one place.
- Tests can compare manifest, DAG, and report output against the same resolver behavior.

## Related docs

- [DAG debugging](../dag-debugging.md)
- [Runbook](../runbook.md)
