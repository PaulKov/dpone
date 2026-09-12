# Source ADR 0069: PostgreSQL source-schema authority may use eight bounded inward layer edges

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

## Status

Accepted on 2026-09-07 for production implementation
`source record 009` and final Evidence V3 head
`source record 010`. Acceptance is limited to the
activation-blocked selected-relation source-schema prerequisite and the exact
decision block below; vendor-live certification remains `UNVERIFIED`.

The decision was proposed on 2026-09-05 after fresh architecture and test review rejected
implementation candidate `source record 011`
and evidence head `source record 012`.

Fresh review later rejected implementation/evidence pairs
`source record 013` /
`source record 014` and
`source record 015` /
`source record 016`. Neither pair is accepted
or architectural evidence. The proposed edge set below is amended by one exact
substitution required by those reviews; its count remains eight.

Fresh architecture, code-fit and documentation review subsequently rejected
V10 code/evidence `source record 017` /
`source record 018`. The pair passed its recorded
graph ceilings but did not preserve exact terminal transaction identity,
legacy extraction compatibility, caller cancellation, closed exception chains
or behaviorally independent composition evidence. It is not architectural
evidence and cannot populate `accepted_commit` or `evidence_head`. A new task,
behavioral RED and four fresh `GO` reviews are required.

Fresh review also rejected V11 code/evidence
`source record 019` /
`source record 020`. Its graph matched the proposed
eight-edge shape and its evidence provenance was valid, but terminal
linearizability, retained-lease validation, cancellation, exact result-row
admission, legacy constructor compatibility, pinned-session cleanup,
SQLSTATE closure and catalog failure precedence were incomplete. The pair is
audit history only and cannot populate `accepted_commit` or `evidence_head`.
The source specification now closes terminal concurrency and the internal
construction seam for a fresh V12 RED/GREEN attempt.

GREEN-v3 later stopped before any production commit when the complete required
graph disproved the former clustering feasibility simulation. At exact task pin
`source record 021`, a diagnostic production worktree
preserving the eight approved inward edges and all anti-tunnel constraints
measured `avg_clustering=0.1914014772898714`,
`runtime_to_contracts_flow=207`, `max_module_ce=24`, zero class findings and
`cross_layer_ratio=0.29996842437638144`, which exceeds the existing ceiling.
The complete dirty source tree is identified by canonical path/content manifest
SHA-256 `6edc61a17fd2fd8112aa109fa89396acc071387c591f17078da43e140f4bb3c0`.
That digest is computed from the compact key-sorted UTF-8 JSON array of
repository-relative path and file-byte SHA-256 records for the sorted union of
tracked and non-ignored untracked `src/dpone` files.
The worktree and its passing focused diagnostics are not committed
implementation or evidence.

Fresh exhaustive edge-removal analysis found no conforming topology at the old
`0.191276270675373` ceiling. Every passing simulation removed an approved/core
dependency or introduced a forbidden metric-only boundary. The smallest
conforming topology restores the honest same-layer runtime-to-lifecycle type
dependency used by `prepare_boundary`; it yields calculated ceilings
`avg_clustering=0.19140574042045186` and
`cross_layer_ratio=0.2999368553988634`. The then-proposed ADR changed only the
infeasible average-clustering ceiling. The accepted implementation restores
that dependency as a real method contract and reproduces both values. The edge
list, raw repository failure, cross-layer ceiling, activation block and
acceptance process are unchanged.

Acceptance required a new exact code commit and exact evidence head after all
correctness, evidence, compatibility and documentation blockers in the amended
source-schema specification passed fresh review. Historical replacement RED task
`2338161f1` and test-only candidate `2c5a78a75` were rejected before capture:
the frozen tests still invoked a superseded private construction seam, and
cancellation, SQLSTATE-phase and AST ownership proofs were incomplete. The
later RED-v5b test commit `source record 022`
and create-only RED evidence commit
`source record 023` authorized only the rejected
GREEN-v4 attempt. Fresh review of candidate
`source record 024` found contradictory terminal-close
oracles, incomplete cancellation preservation and new module-size warning
debt. RED-v5b remains immutable audit history but cannot authorize future
GREEN work. The replacement GREEN-v5 chain completed with exact 320-node RED
identity, 320/320 GREEN behavior and final Evidence V3. Fresh architecture,
code-fit, test/certification and documentation/UX review accepted the exact
implementation and evidence identities above. The eight-edge scope and
ceilings are unchanged, and activation remains blocked.

## Context

The selected-relation source-schema child must prove one source-owned,
same-snapshot relation/type aggregate and carry its owning scope to the exact
whole-file COPY boundary. The corrective decomposition proposed for reapproval
creates eight direct
inward imports from runtime orchestration to immutable contracts:

```text
prepared_source_boundary    → postgres_mssql_source_schema_authority
route_schema_issuer         → source_schema_authority
route_schema_issuer         → source_schema_models
source_schema_projection    → source_schema_authority
source_schema_projection    → type_target_shapes
verified_relation_observation → postgres_source_authority
verified_relation_snapshot  → postgres_source_authority
prepared_source_boundary    → source_schema_models
```

At exact task base `source record 025`,
the layer gate reports maximum cross-layer flow 199 against repository baseline
194 and allowed tolerance 5. Rejected candidate `4db28ae6e` reports
`runtime → contracts = 207`. The global layer command therefore correctly
returns `FAIL`.

The same candidate improves advisory architecture-fitness values from task
base `avg_clustering=0.19137170217275384` and
`cross_layer_ratio=0.3000000000000000` to
`0.19122452130901885` and `0.2999368553988634`; maximum efferent coupling
remains 24 and class findings remain empty. Those improvements do not turn the
layer failure into a pass.

The earlier read-only adjacency simulation against rejected graph
`source record 015` predicted
`avg_clustering=0.191276270675373`. GREEN-v3 proved that calculation omitted
required dependencies, so it is retained only as rejected planning history.
The complete diagnostic topology instead measures
`runtime_to_contracts_flow=207`,
`avg_clustering=0.1914014772898714`,
`cross_layer_ratio=0.29996842437638144`, and `max_module_ce=24`. Restoring the
one honest same-layer runtime-to-lifecycle type dependency adds one intra-layer
edge while leaving 2,850 cross-layer edges unchanged, producing calculated
values `avg_clustering=0.19140574042045186` and
`cross_layer_ratio=0.2999368553988634`. The exact clean candidate must use that
real dependency, be remeasured and not exceed either normative ceiling.

Removing the eight imports without changing responsibility would require one
of the following unsafe shapes:

- pass contract identity as unchecked `Any` or duck-typed callables;
- duplicate canonical validation or construction policy in runtime;
- hide imports behind local dynamic imports, re-export tunnels or facades;
- move I/O-owning runtime policy into contracts;
- edit unrelated existing imports to game the aggregate metric.

Each option weakens auditability, exact-type anti-splice guarantees or the
repository's dependency direction. A larger adapter/port redesign is possible,
but is disproportionate for this activation-blocked prerequisite and would
require a new specification and RED baseline.

## Decision

The schema issuer owns and exact-validates the injected type policy, so the
runtime bundle no longer imports or accepts that policy separately. The
prepared boundary instead imports the concrete route authority it must admit;
module/qualname checks, dynamic imports and re-export tunnels are forbidden.

The source-schema implementation integrates under this visible, time-bounded
exception at the exact accepted commit and evidence head below. The repository
baseline and raw layer-metrics result remain unchanged and visible.

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded requirement **rule**: top cross-layer flow must not exceed repository baseline plus configured tolerance
- Recorded requirement **reason**: eight direct inward runtime-to-contract imports preserve one canonical source-schema authority and concrete exact scope/authority admission; hiding them would duplicate policy or create metric-only indirection
- Recorded requirement **owner**: dpone architecture maintainers
- Recorded requirement **remediation_owner**: PostgreSQL-to-MSSQL R1 integrator
- Recorded requirement **scope / entry 1**: src/dpone/contracts/postgres_source_authority.py
- Recorded requirement **scope / entry 2**: src/dpone/contracts/postgres_mssql_type_authority.py
- Recorded requirement **scope / entry 3**: src/dpone/contracts/postgres_mssql_source_schema_models.py
- Recorded requirement **scope / entry 4**: src/dpone/contracts/postgres_mssql_source_schema_authority.py
- Recorded requirement **scope / entry 5**: src/dpone/contracts/postgres_mssql_type_target_shapes.py
- Recorded requirement **scope / entry 6**: src/dpone/runtime/postgres_mssql_source_schema_runtime.py
- Recorded requirement **scope / entry 7**: src/dpone/runtime/sources/postgres_verified_relation_observation.py
- Recorded requirement **scope / entry 8**: src/dpone/runtime/sources/postgres_verified_relation_snapshot.py
- Recorded requirement **scope / entry 9**: src/dpone/runtime/sources/postgres_mssql_source_schema_observation.py
- Recorded requirement **scope / entry 10**: src/dpone/runtime/sources/postgres_mssql_source_schema_issuer.py
- Recorded requirement **scope / entry 11**: src/dpone/runtime/sources/postgres_mssql_source_schema_projection.py
- Recorded requirement **scope / entry 12**: src/dpone/runtime/sources/strategies/postgres/postgres_prepared_source_boundary.py
- Recorded requirement **allowed_edge_count**: 8
- Recorded requirement **allowed_edges / entry 1**: dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary -> dpone.contracts.postgres_mssql_source_schema_authority
- Recorded requirement **allowed_edges / entry 2**: dpone.runtime.sources.postgres_mssql_source_schema_issuer -> dpone.contracts.postgres_mssql_source_schema_authority
- Recorded requirement **allowed_edges / entry 3**: dpone.runtime.sources.postgres_mssql_source_schema_issuer -> dpone.contracts.postgres_mssql_source_schema_models
- Recorded requirement **allowed_edges / entry 4**: dpone.runtime.sources.postgres_mssql_source_schema_projection -> dpone.contracts.postgres_mssql_source_schema_authority
- Recorded requirement **allowed_edges / entry 5**: dpone.runtime.sources.postgres_mssql_source_schema_projection -> dpone.contracts.postgres_mssql_type_target_shapes
- Recorded requirement **allowed_edges / entry 6**: dpone.runtime.sources.postgres_verified_relation_observation -> dpone.contracts.postgres_source_authority
- Recorded requirement **allowed_edges / entry 7**: dpone.runtime.sources.postgres_verified_relation_snapshot -> dpone.contracts.postgres_source_authority
- Recorded requirement **allowed_edges / entry 8**: dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary -> dpone.contracts.postgres_mssql_source_schema_models
- Historical authority/requirement record **accepted_commit**: source record 009
- Recorded requirement **evidence_head**: source record 010
- Recorded requirement **introduced_at**: 2026-09-05
- Recorded requirement **review_by**: 2026-10-31
- Recorded requirement **risk**: the runtime layer has eight additional direct contract dependencies and the repository top-flow gate remains above its configured tolerance
- Recorded requirement **mitigation**: route activation remains blocked; imports stay direct and acyclic; no Any-based admission, dynamic import, facade, re-export or duplicated authority is permitted; every changed exact candidate is remeasured and freshly reviewed
- Recorded requirement **ceiling / runtime_to_contracts_flow**: 207
- Recorded requirement **ceiling / avg_clustering**: 0.19140574042045186
- Recorded requirement **ceiling / cross_layer_ratio**: 0.2999368553988634
- Recorded requirement **ceiling / max_module_ce**: 24
- Recorded requirement **ceiling / class_findings**: 0
- Recorded requirement **activation_block**: PostgreSQL-to-MSSQL R1 and every dependent route remain activation-blocked; no vendor-live or GA claim is authorized
- Recorded requirement **remediation_plan**: before provider activation, complete an independently approved architecture-remediation task that introduces a genuine capability boundary or reduces existing runtime dependency debt without facade, re-export, dynamic-import or duplicated-policy techniques; remeasure the integrated graph and remove or supersede this exception


Acceptance fails closed if the exact new candidate exceeds any ceiling,
does not contain exactly the eight fully qualified pairs above, substitutes
another contract import for any pair, introduces another runtime-to-contract
edge, cycle, reverse dependency, facade, re-export, dynamic import or class
finding, or widens the scoped paths. A future change to the source module of an
allowed pair requires new measurement and review; this decision cannot
silently cover it. Expiry at `review_by` blocks further implementation
integration until explicit renewal or retirement; it never silently extends.

## Consequences

- The source-schema child is implemented with hermetic `local_pass` evidence;
  dependent provider work still requires independently approved specifications.
- The raw `check-layer-metrics` result remains `FAIL`; completion reports must
  cite this ADR rather than report the gate as `PASS`.
- Binding V2, provider composition, route activation and vendor-live
  certification remain independently blocked.
- Production promotion must remove or supersede this exception before the
  capability can become an activation default.
- A future genuine lower-coupling design must supersede this decision with new
  measurements and review before it replaces the accepted implementation.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Treat 207 as a passing layer result | Manufactures false evidence and hides the configured baseline |
| Raise or regenerate the shared baseline | Conceals feature-attributable growth and weakens unrelated gates |
| Replace exact types with `Any`/duck typing | Reopens the anti-splice and fake-scope admissions this child exists to close |
| Use local imports, re-export tunnels or facades | Changes the metric without changing responsibility |
| Duplicate contract validators in runtime | Creates a second authority and drift risk |
| Remove unrelated runtime imports | Metric gaming and out-of-scope cleanup |

## Related material

- [Current source-schema authority GREEN-v5](../../feature-design-postgres-mssql-r1-source-schema-authority-green-v5.md)
- [Historical source-schema authority V1](../../feature-design-postgres-mssql-r1-source-schema-authority-v1.md)
- [Historical task requirements — Superseded source-schema V1 task contract (not executable)](../../agent-task-history/postgres-mssql-r1-source-schema-authority-v1.md)
- [ADR 0068: source-owned selected-relation schema](0068-postgres-selected-relation-schema-authority.md)
- [Provider implementation map](../../developer-postgres-mssql-r1-v3-provider-implementation.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
