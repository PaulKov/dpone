# ADR 0060: Explicit authority for isolated synthetic composition execution

- Status: Accepted
- Date: 2026-09-10

## Context

The production native compiler and route matrix require genuine production
qualification. Signing a synthetic observation cannot satisfy that requirement.
The composition acceptance campaign needs to execute all native, generated and
ordinary workloads against newly isolated, independently enrolled participants.

## Decision

Use the six strict document contracts and limits in the maintainer-approved
[nonproduction authority specification](../feature-specs/nonproduction-composition-authority.md).
An externally pinned policy authenticates distinct qualification and execution
grants. Actual route qualification is signed and reopened during scoped
compilation. New native release, producer wire, parent and activation-request
families bind the complete scope and execution grant.

Reuse existing payload and transport shapes only where their verified ancestor
hashes cover the new authority. Keep explicit family dispatch and mandatory
artifact verification. Scope never belongs only in excluded provenance, and
the late execution grant never substitutes for runtime-context identity.

Physical guard identity remains independent of authority family, purpose and
campaign. Expired or revoked grants prevent new issuance; they do not release
unknown attempts or undo committed target data. All actual mutation paths still
require protected writer gates, quiescence and outcome reconciliation.
The common protected owner/operation implementation and explicit qualification
handoff are specified in [ADR 0061](0061-shared-composition-physical-ownership.md).

## Consequences

The synthetic campaign has its own qualification and execution status. It does
not certify production readiness or unchanged native-v2 execution. Existing
production readers, signatures, fixture bytes and acceptance remain unchanged.
There is no fallback from production to the new scoped family.

Public scoped factories remain unavailable until complete scope enforcement
exists for the selected backend matrix. Approval of this design is implementation
authorization; actual SQL, signatures, byte limits and worker execution require
their own observed evidence. Publication authority remains governed by the
existing release runbook.
