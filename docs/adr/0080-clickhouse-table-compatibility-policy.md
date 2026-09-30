# ADR 0080: Separate ClickHouse configuration identity and method compatibility

- Status: Accepted for staged design; implementation plan awaiting review
- Date: 2026-09-30
- Approval: maintainer approval of the written table-compatibility specification

## Context

The closed profile in [ADR 0079](0079-protected-clickhouse-candidate-lifecycle.md)
rejects server-rendered table SETTINGS, including ordinary default granularity.
Allowing one literal would not establish a coherent compatibility policy.
Ignoring settings would make preservation and recovery unsafe. The historical
selector in [ADR 0078](0078-method-aware-clickhouse-publication.md) also cannot
express a method-compatible pair whose complete configuration identities differ.

## Decision

Implement the [approved specification](../feature-design-clickhouse-table-compatibility.md)
through its [separate implementation plan](../superpowers/plans/2026-09-30-clickhouse-table-compatibility.md).

1. Separate canonical table configuration, per-method compatibility, desired
   target configuration, object identity, provenance and content evidence.
2. Keep one closed typed settings registry. Preserve supported existing settings
   by default; require explicit `design_change="replace"` for intended changes.
   Omitted defaults need verified historical configuration provenance, not just
   the server's current global values. Unknown settings remain blocking.
3. Default `on_unverified_compatibility` to `block`. In explicit `warn` mode an
   unverified preferred method may be excluded in favor of an independently
   verified alternative, with a durable warning. This is not permission to run
   an unverified method, ignore shared unknowns or retry failed mutations.
4. Keep preview advisory and source-free. Persist a resolved plan at enrollment;
   append the selected method and explicit expected post-state atomically with
   PREPARED only after verified candidate sealing. Freeze the policy and mode.
5. Introduce explicit observation v2, guarded-publication v3 and authority v3
   bindings. Preserve all previous readers, defaults, bytes and recovery rules;
   do not reinterpret old records, migrate stores or adopt retained names.
6. Protect actual preflight through publication with one namespace-bound
   execution session. Candidate creation and metadata verification precede
   opening the source. Recovery never reconstructs a source or send capability.

This amends only the new binding's settings, selection and version decisions in
ADRs 0078/0079. Their historical contracts are unchanged.

## Consequences

Supported refreshes require no hand-copied settings or user-selected SQL method.
Some tables still fail closed because omitted defaults cannot be established.
The plan therefore starts with a real controlled-bootstrap provenance probe,
including an existing-table positive case, before dependent implementation.
An injected deployment evidence source is not a claim of universal production
ingress enforcement; the initial concrete certification is owned Docker only.

The unfinished implementation already fails strict graph budgets. An explicit
structural feasibility gate precedes expansion: module counts, moving imports
or adding forwarding modules cannot manufacture architectural compliance. No
budget or historical evidence is weakened to complete this work.

The single-host/direct-node/plain-MergeTree/dpone-only constraints remain. This
is a staged Python building block, not stock ODBC activation, owner release,
cleanup, checkpoint finalization or release authorization. `warn` does not
certify a whole route. Terminal historical records do not certify future writes.

## Rejected alternatives

- Special-case `index_granularity = 8192`: incomplete public policy.
- Ignore or pass through unrecognized settings: unverifiable identity.
- Trust current global defaults for historical tables: missing provenance.
- Implement `warn` as an unsafe force flag or trial-SQL fallback: uncertain
  effects and false recovery confidence.
- Reuse old version labels with new selection rules: changed historical meaning.
- Relax graph limits or hide dependency edges: false quality evidence.
