# ADR 0079: Protected ClickHouse candidate lifecycle and explicit authority v2

- Status: Accepted for staged Python implementation; not production activation
- Date: 2026-09-30
- Approval: maintainer acceptance of the written protected-publication supplement

## Context

[ADR 0078](0078-method-aware-clickhouse-publication.md) defines the unbound
method-aware kernel. The authority v1 foundation and native publisher provide
retained ownership and one-shot publication, but no protected observer or
candidate-writer seal. V1 does not reserve physical candidate names across
operations, and directly composing its non-reentrant publisher lock with the
kernel's held backend lock would conflict.

## Decision

Implement the [approved supplement](../feature-design-clickhouse-protected-publication.md)
through the [reviewable Native plan](../superpowers/plans/2026-09-30-clickhouse-protected-publication.md).

- Use an explicitly provisioned v2 journal for new enrollment only. Preserve
  v1 APIs/defaults and original records. No migration, owner transfer, downgrade
  conversion or replacement store over previously managed names is authorized.
- Atomically reserve target and candidate physical names with enrollment. UUID
  observations do not replace canonical ownership identity.
- Register CREATE and each serialized synchronous INSERT before send; require
  acknowledged send entry and successful native completion. Close admission
  irreversibly and join every accepted request before sealing.
- Bind a seal to protected candidate identity and typed expected/observed
  evidence. Use a closed, versioned observation profile, not caller flags.
- Before PREPARED, restart permits inspection/quarantine only, even after a
  seal. Readback never recreates an invocation capability or permits source/SQL
  replay. At/after PREPARED retain the existing source-free recovery semantics.
- Hold one explicit execution session across kernel observation and publication;
  add session-bound publisher entry points without changing standalone wrappers.

## Consequences

The single-host/direct-node/dpone-only deployment assumptions remain. The new
backend is a one-operation building block: terminal publication still retains
ownership. Production ingress enforcement, controlled release, method-aware
cleanup, checkpoint/evidence finalization and full route certification remain
separate gates. There is no ODBC route or CLI/manifest activation here.

The conservative profile trades availability for no replay after uncertainty.
Unsupported types/designs and ambiguous writes fail closed. Typed hash parity is
probabilistic evidence, not mathematical equality. Docker process/fault checks
do not certify production grants, TLS or power-loss durability.

Rejected alternatives include silent v1 schema extension, separately committed
lifecycle sidecars, recovered-grant reconstruction and generic reentrant locks.
See the supplement for full alternatives, limits and compatibility rationale.
