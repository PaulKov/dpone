# ADR 0066: Distinct dbt SQL Server adapter owns strict physical transport

Status: Proposed

Date: 2026-09-17

## Context

The managed physical publishing path needs a finite protocol between a dbt model
and the platform-owned SQL Server transaction. The ordinary `sqlserver` adapter
does not expose that protocol, and its existing qualification evidence does not
cover the stronger ownership, timeout, result-shape or failure semantics.

Adding platform methods to the macro-only `dbt-dpone` package would make method
availability depend on project package resolution rather than dbt adapter
identity. Relabelling an ordinary `sqlserver` profile would also allow old
toolchain and source evidence to be reused for behavior it never certified.

## Decision

Provide the optional, normally installed `dbt-dpone-sqlserver` Python
distribution. It registers the distinct dbt adapter type `dpone_sqlserver`
through dbt's standard plugin loader and remains separate from the `dbt-dpone`
macro package. Existing `sqlserver` profiles and behavior are unchanged.

The adapter exposes exactly one platform entry point:
`adapter.dpone_physical_protocol_v1(operation, parameters)`. Parsing returns
`None` without I/O. Runtime dispatch is closed over named operations and exact
parameter/result schemas; it is not an arbitrary SQL, connection, transaction or
budget interface. The first preparatory implementation supports only authenticated
attach decoding. Every unfinished operation rejects before cursor creation.

The strict transport uses the already materialized raw DBAPI handle on the same
thread. It sets the finite query timeout on that handle, consumes bounded result
batches, validates closed field names and types, and rejects every additional
result set before returning values to dbt/agate. It does not reconnect, retry,
commit, roll back or infer settlement. Any dispatch, timeout, cancellation,
decode or cleanup failure poisons the managed attempt; it cannot establish a safe
retry or a publication receipt.

The platform passes the private B1 launch packet through one anonymous, read-only,
inherited POSIX descriptor. Only the authenticated launcher may construct the
packet. The environment contains a reserved descriptor reference only for the
Popen boundary, and the parent closes its lease after spawn. The existing
supervised runner remains responsible for process creation, output draining,
cancellation, deadlines and child cleanup. A file path, ambient environment JSON
or caller-produced packet is not an authority source.

Enabling the adapter requires new, exact identities for the toolchain, runtime
image, profile, source snapshot, project selection and recorder/delivery factory.
Legacy `sqlserver` evidence must never be relabelled or used as fallback. The
public method therefore remains unconditionally unavailable until those producers
are composed and the complete installed route is qualified.

## Consequences

- Analysts keep the ordinary dbt workflow and receive a generated profile; they
  do not configure descriptors, handles, internal budgets or transaction controls.
- Platform code gains a narrow, testable custody boundary without changing the
  stock adapter or granting macros general access to the source connection.
- Package build, plugin discovery, parse behavior and anonymous-descriptor tests
  are preparatory evidence only. They do not certify SQL Server driver behavior,
  terminal settlement, release, reuse or the end-to-end managed route.
- Qualification must cover the exact installed wheels and runtime on the selected
  operating system, including late errors, extra result sets, timeout,
  cancellation, process termination and independent durable outcome observation.
- Release stays on hold while architecture fitness is red or the authenticated
  producers and live qualification are incomplete. Thresholds and legacy
  qualification records are not weakened to enable this path.

## Related material

- [Trusted isolated native generation execution](0065-trusted-isolated-native-generation-execution.md)
- [Workspace release authority](0052-dbt-workspace-release-source-authority.md)
- [Package operator and migration notes](../../packages/dbt-dpone-sqlserver/README.md)

