# ADR 0025: Quality-gate authority is explicit and process-local

- Status: Accepted
- Date: 2026-07-18

## Context

A policy fingerprint detects semantic mismatch but does not prove which
execution evaluated the policy. The public report and receipt dataclasses can
be constructed or replayed, while mutable load configuration can drift after
initial validation. Arbitrary report diagnostics can also cross the evidence
boundary before validation.

The runtime needs a fail-closed producer boundary without adding a database,
distributed lock, global registry or second quality engine.

## Decision

Create an immutable normalized quality execution snapshot before runtime side
effects. Bind it explicitly to the current run and load through a
`QualityGateExecution` object passed by dependency injection across the ETL
services and selected coordinator.

Only that execution object may evaluate gates and issue an authoritative
receipt. It owns a locked, single-consumer process-local state machine:

```text
OPEN -> ISSUED -> PAYLOAD_ACCEPTED -> STATE_ACCEPTED
```

The selected staged, legacy or resume path determines the one accepted
boundary. Runtime revalidates the original snapshot before target mutation,
finalization, evidence publication and source-state persistence. Receipt
evidence is an allowlisted bounded projection produced only after authority and
contract validation.

No global state, `ContextVar`, hidden configuration mutation or vendor
dependency participates in this decision. Canonical SHA-256 identities detect
semantic drift; an object capability and locked state enforce process-local
producer and replay discipline.

## Consequences

- Non-empty-to-empty policy drift fails closed at the irreversible boundary.
- A publicly constructed, replaced, stale, wrong-path or replayed receipt
  cannot authorize a non-empty policy in the canonical runtime path.
- Invalid or secret-bearing report diagnostics are rejected or projected
  safely before durable evidence.
- Empty-policy and legacy unbound empty-receipt compatibility remain inert.
- The execution object becomes an explicit dependency from `ETLProcessor`
  through payload loading and governance coordinators.

This decision does not authenticate serialized receipts, survive process or pod
restart, provide cross-process replay protection, establish target idempotency,
or defend against malicious code inside the trusted Python process. Those
claims require durable or cryptographic authority and a separate design.

## Related

- [Quality acceptance fail-closed feature design](../feature-design-quality-acceptance-fail-closed-v0731.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0022: Durable target commit journal and fence](0022-target-commit-terminal-failures.md)
