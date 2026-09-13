# ADR 0064: Dispatcher-owned composition capture

Status: Proposed; not authorized for implementation

Date: 2026-09-13

## Context

ADR 0063 assigns ClickHouse credentials and dispatch to a nonroot protected
service. Current snapshot capture requires a root-owned file adapter and direct
source/control/catalog authority. The ordinary worker, nonroot dispatcher and
read-only host probe do not provide a compatible complete placement.

## Proposed decision

Run the complete existing ClickHouse cell inside the protected dispatcher. Add
an explicitly enrolled nonroot service-owned capture storage profile, preserving
the existing root-owned profile and originals. Add a versioned whole-cell RPC so
the worker submits exact scheduler/parent identity and receives independently
verified terminal references; it does not receive administrative credentials,
Docker authority or source rows. Do not make OPEN_GATE implicitly capture data.

The [capture custody specification](../feature-specs/composition-dispatcher-capture-custody.md)
defines the complete proposed contract, algorithm, alternatives and validation.
This changes the trusted producer/storage boundary and needs maintainer approval.
It does not amend accepted behavior merely by being committed as a proposal.

## Consequences and alternative

Source capture, target observation and immutable recovery originals share one
already protected service. Its enrolled nonzero UID/GID and exclusive volume
become an explicit custody requirement; no historical file is adopted or chowned.
The host-facts process remains read-only. Full Linux/provider certification and
architecture budgets remain release requirements.

If root-only file custody is mandatory, use a separate privileged capture
producer with an independently specified protocol and recovery handoff instead.
Neither privileged workers nor a writable host-facts API are acceptable shortcuts.
