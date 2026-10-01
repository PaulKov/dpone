# ADR 0080: Separate ClickHouse configuration identity and method compatibility

- Status: Accepted; current-state correction approved, structural prerequisite unmet
- Date: 2026-09-30
- Amended: 2026-10-01
- Approval: maintainer approval of the written table-compatibility specification and explicit approval of the current-state correction

## Context

The closed profile in [ADR 0079](0079-protected-clickhouse-candidate-lifecycle.md)
rejects server-rendered table SETTINGS, including ordinary default granularity.
Allowing one literal would not establish a coherent compatibility policy.
Ignoring settings would make preservation and recovery unsafe. The historical
selector in [ADR 0078](0078-method-aware-clickhouse-publication.md) also cannot
express a method-compatible pair whose complete configuration identities differ.

## Decision

Implement the [approved specification](../feature-design-clickhouse-table-compatibility.md)
through its [implementation plan under revision](../superpowers/plans/2026-09-30-clickhouse-table-compatibility.md).

1. Separate canonical table configuration, per-method compatibility, desired
   target configuration, object identity, provenance and content evidence.
2. Keep one closed typed settings registry. Preserve supported existing settings
   by default; require explicit `design_change="replace"` for intended changes.
   For the exact certified plain-MergeTree 24.8.14.39 profile, resolve omitted
   values from cached current defaults plus complete persisted overrides, bound
   to actual endpoint/topology and a complete bracketed observation on one
   non-reconnecting connection. Current globals alone remain insufficient;
   unknown settings remain blocking. No pre-creation history is required for a
   genuinely unmanaged existing table, including one created manually.
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
7. Distinguish current configuration from historical part compatibility, and
   first-use inspection from recovery. Missing or uncertain original-operation
   authority cannot be replaced by a fresh store or a new current-state snapshot.
   Remove the planned configuration-epoch provider/bootstrap journal, not the
   operation journal or its retained ownership semantics.

This amends only the new binding's settings, selection and version decisions in
ADRs 0078/0079. Their historical contracts are unchanged.

## Consequences

Supported refreshes require no hand-copied settings or user-selected SQL method.
Some tables still fail closed because required current facts, a supported profile
or method compatibility cannot be established. The plan requires a manually
created-table positive case, current-default/override and historical-part tests,
connection/drift rejection and original-operation recovery checks. Raw owned-Docker
characterization exists; production resolver and publisher certification do not.
The old epoch experiments remain historical evidence, not a universal admission
requirement. A new pre-enrollment observation after restart is allowed; an enrolled
operation retains its original frozen state and source-free recovery restrictions.
No automatic target ALTER, server restart/reset or manual history reconstruction
is introduced. This is not universal production ingress enforcement.

The unfinished implementation already fails strict graph budgets. An explicit
structural feasibility gate precedes expansion: module counts, moving imports
or adding forwarding modules cannot manufacture architectural compliance. No
budget or historical evidence is weakened to complete this work.

The independent [native-boundary refactor](../superpowers/plans/2026-10-01-clickhouse-native-boundary.md)
shares only a read-only endpoint shape and the existing pinned peer predicate
within the native-driver integration owner. The public endpoint dataclass stays
at its original path. Publication and candidate transports retain their distinct
connection lifecycle, deadlines, response handling and error boundaries. This
removes the unfinished candidate's concrete publication-adapter dependency; it
does not satisfy the whole-feature graph gate or implement observation v2 or
guarded publication v3. No user setup or migration step changes.

The single-host/direct-node/plain-MergeTree/dpone-only constraints remain. This
is a staged Python building block, not stock ODBC activation, owner release,
cleanup, checkpoint finalization or release authorization. `warn` does not
certify a whole route. Terminal historical records do not certify future writes.

## Rejected alternatives

- Special-case `index_granularity = 8192`: incomplete public policy.
- Ignore or pass through unrecognized settings: unverifiable identity.
- Trust current global defaults alone, without complete overrides and pinned
  protected observations: insufficient current-state evidence.
- Require dpone CREATE/ATTACH history for every existing table: superseded by
  pinned-source and live evidence for the approved current-state resolver.
- Implement `warn` as an unsafe force flag or trial-SQL fallback: uncertain
  effects and false recovery confidence.
- Reuse old version labels with new selection rules: changed historical meaning.
- Relax graph limits or hide dependency edges: false quality evidence.
