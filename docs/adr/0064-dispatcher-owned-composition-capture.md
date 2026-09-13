# ADR 0064: Dispatcher-owned composition capture

Status: Accepted by the maintainer on 2026-09-13

Date: 2026-09-13

## Context

ADR 0063 assigns ClickHouse credentials and dispatch to a nonroot protected
service. Current snapshot capture requires a root-owned file adapter and direct
source/control/catalog authority. The ordinary worker, nonroot dispatcher and
read-only host probe do not provide a compatible complete placement.

## Decision

Run the complete existing ClickHouse cell inside the protected dispatcher. Add
an explicitly enrolled nonroot service-owned capture storage profile, preserving
the existing root-owned profile and originals. Add a versioned whole-cell RPC so
the worker submits exact scheduler/parent identity and receives independently
verified terminal references; it does not receive administrative credentials,
Docker authority or source rows. Do not make OPEN_GATE implicitly capture data.

The [capture custody specification](../feature-specs/composition-dispatcher-capture-custody.md)
defines the complete proposed contract, algorithm, alternatives and validation.
The maintainer explicitly approved this trusted producer/storage boundary change.
Implementation and live certification remain separate requirements.

## Consequences and alternative

Source capture, target observation and immutable recovery originals share one
already protected service. Its enrolled nonzero UID/GID and exclusive volume
become an explicit custody requirement; no historical file is adopted or chowned.
The host-facts process remains read-only. Full Linux/provider certification and
architecture budgets remain release requirements.

If root-only file custody is mandatory, use a separate privileged capture
producer with an independently specified protocol and recovery handoff instead.
Neither privileged workers nor a writable host-facts API are acceptable shortcuts.


## Completion amendment: acyclic startup identity

The completed deployment uses a separately hashed immutable service policy,
versioned policy binding, and administrator-installed protected bootstrap. The
listener starts with admission closed so its actual process/socket can be
enrolled before bootstrap activation. Enrollment and release catalogs never
participate in the policy hash or immutable Docker launch digest.

The specification defines the exact P → registry → runtime → staged context →
listener → enrollment → bootstrap order, one-way startup latch, protected atomic
handoff, disjoint deployment-artifact roots and fresh checks before admission.
Existing binding v1 and service v2 hashes retain their full-byte meaning. This
explicitly changes bootstrap authentication for the new launch profile; it does
not weaken legacy checks or make enrollment a runtime provisioning operation.
