# Feature amendment: consolidate unpublished ClickHouse publication prototypes

- Status: APPROVED for unpublished-prototype consolidation
- Owner: dpone maintainers
- Approval: explicit maintainer authorization on 2026-10-01 in response to the PR #249/#258 consolidation question
- Parent: [self-service table compatibility](feature-design-clickhouse-table-compatibility.md)
- Implementation: revised dependency map and exact path contract required before production edits
- Target release: unassigned; no activation or publication claim

Last verified: 2026-10-01

## Purpose and user outcome

This amendment removes an implementation obligation to retain separate execution
engines for the unpublished PR #249/#258 prototypes. A data engineer still asks
for a safe refresh, not an authority-schema version or a publication SQL method.
The approved current-state resolver, supported-settings preservation, default
`block`, verified `warn` alternative, one-shot publication and source-free
recovery requirements do not change.

One current implementation can serve those requirements without keeping each
experimental engine operational. Serialized version identifiers remain meaningful
for strict historical inspection. Consolidation is not permission to weaken
ownership, silently reinterpret a journal, discard unfinished work or call an
uncertified route production-ready.

## Approval and supersession boundary

The maintainer explicitly authorized consolidation after being asked whether the
unpublished prototypes could be combined while preserving published dpone and all
history/evidence. This is the compatibility exception required by the earlier
specification; it is not inferred from a general request to work faster.

For PR #249/#258-only prototypes, this amendment supersedes requirements to retain
old execution constructors/default provisioning, duplicate candidate/publication
engines, old mutation-capable facades and separate operational v1/v2/v3 stacks.
It does **not** supersede the historical meaning of their serialized records or
the compatibility of any actually published dpone behavior.

The published baseline must include the public `0.88.0` core wheel, not only the
latest GitHub Release `v0.87.4`. The `v0.88.0` tag peels to source commit
`28aea0d9439ab3f24e0fb39800a041a6b6d661ed`; the `v0.87.4` tag peels to
`2818e00ba3c32e4807a2872ccab83d27e585ec2e`. The protected authority/candidate/
publication prototype modules are absent from both tagged sources and the
inspected public `0.88.0` core wheel. Published cluster, full-refresh, marker,
range-evidence and runtime contracts remain protected. A tag alone is not a
complete publication receipt, and this comparison does not certify the partial
`0.88.0` release set.

Public artifact identity: [dpone 0.88.0 on PyPI](https://pypi.org/project/dpone/0.88.0/),
core-wheel SHA-256
`ceb86e7f1f181a8365111067fa7a2be443052bfb6f278fbd7c3371256297547a`.
The read-only reviewer compared the downloaded bytes with PyPI metadata and
the relevant tag files, not the repository working tree. The corresponding
[0.87.4 core wheel](https://pypi.org/project/dpone/0.87.4/) SHA-256 is
`94f84b072ccb831d85f0d48551d73c4330d3783b99a6c41987984d0e9d29c6cf`.
These immutable archive identities support the scoped compatibility inventory;
they do not turn the failed
[0.88.0 controller run](https://github.com/PaulKov/dpone-release-controller/actions/runs/36762814864)
into a completed release certificate. Full source-file inventory and independent
reproduction belong in the implementation evidence before deleting source.

## Scope and non-goals

In scope:

- one current protected operation contract and execution model;
- reuse of genuinely shared durability, native transport and evidence mechanisms;
- explicit strict historical inspection without historical mutation authority;
- cohesive dependency boundaries, current tests and a truthful implementation map;
- preservation of all unfinished source and original historical artifacts.

Not authorized by this amendment:

- changing published CLI/API, defaults, manifest schemas or v1 range evidence;
- automatic migration, owner release, resource deletion or mutation of old stores;
- activation of ODBC parallelism, checkpoint finalization or release publication;
- relaxation of graph/module limits or rewriting prior failed evidence;
- republishing an existing package version or modifying another release's state.

The global delivery still requires the complete range route, durable finalization,
safe successor ownership, a second refresh and exact-commit certification. Those
requirements are not removed merely because this consolidation can be performed
first. Their concrete integration must be reconciled with the
[ODBC v2 design](feature-design-mssql-columnar-range-parallelism-v2.md), rather
than claiming that a retained-owner Python publisher already implements them.

## Current and historical contract

### Current operations

Retain the approved fresh namespace families: authority v3, guarded publication
v3 and observation v2. Version numbers describe wire/profile contracts, not the
number of engines to keep alive. One explicit current composition creates fresh
operations for eligible unmanaged names. It refuses old, conflicting, unknown or
unreadable ownership. A fresh store does not prove that a target is unmanaged.

The current engine preserves this ordering:

1. Acquire continuous canonical namespace exclusion and inspect original ownership.
2. Resolve supported current configuration and desired design before source access.
3. Durably enroll and acknowledge the resolved, content-independent plan.
4. Register and complete candidate CREATE; independently verify actual configuration.
5. Admit bounded source ingress and account for every accepted mutation request.
6. Require normal exhaustion, successful source closure and request settlement.
7. Independently verify typed content/configuration and durably seal the candidate.
8. Select the permitted method; commit selection, warnings and PREPARED atomically.
9. Dispatch only under the acknowledged volatile one-shot claim.
10. Close/drain transport before source-free method-specific outcome classification.

A loaded record, matching digest, preview, old claim or lost acknowledgement never
reconstructs permission. Unknown shared safety facts block in both compatibility
modes. Recovery never changes the frozen mode or retries publication SQL.

### Historical inspection

Keep strict readers for the historical serialized families and their immutable
golden vectors. Readers retain the original validation/selector semantics needed
to interpret those bytes. They must not parse an old record with current policy,
silently discard fields or manufacture a current operation from it.

Historical inspection reports original schema, identity, state and retention
status. It is explicitly read-only and issues no enrollment, request, source,
dispatch, cleanup or successor capability. A historical COMMITTED record describes
that operation's outcome; it does not prove the current table state.

Original files remain untouched. An unexpectedly operational old store is
quarantined for original-version investigation, not migrated or reset. Its
presence continues to block a conflicting new enrollment. Source history and
old artifacts remain recoverable even when superseded prototype source modules
are removed from the current implementation.

### Published compatibility

Do not repurpose published cluster authority or SCD/merge finalization modules.
Their names may resemble this work but their contracts serve different routes.
Preserve published imports, constructors, defaults, field shapes, errors, old
manifest behavior and evidence bytes. The published suspended range capability
remains suspended until its own activation gates pass.

## Architecture and implementation discipline

Configuration facts, compatibility policy, operation records and bounded ingress
records are distinct cohesive responsibilities. Runtime owns lifecycle policy;
adapters own catalog/native I/O and durable transactions behind narrow ports.
Separate native candidate and publication lifecycles where their protocol/error
handling differs. Do not combine unrelated classes just to meet a graph ratio.

Historical readers are inspection boundaries, not a second current composition.
Compatibility facades are retained only for actually published symbols or genuine
unchanged delegation; a facade must not preserve an obsolete mutation engine
merely to reproduce the previous file map.

Before source consolidation, the integrator must record the exact published
inventory, preserve the dirty source snapshot, reconcile the whole dependency
map, measure it with repository producers and replace the old path contract.
Only then may the explicitly owned production paths change. Existing failed
projections remain failed; this approval alone does not make a graph pass.

No new competitor-performance claim follows from this internal consolidation.
The parent's dated primary-source comparison and measurable self-service targets
remain applicable. The engineering outcome is one current engine with the same
approved safety contract and zero regression in published behavior.

## Validation and documentation

Required evidence separates four scopes:

| Scope | Acceptance |
|---|---|
| Published compatibility | Exact published inventory plus old route/API/schema/evidence regression tests |
| Historical integrity | Original golden bytes decode strictly; unknown/malformed versions reject; inspection cannot mutate or issue capabilities |
| Current operation | Full ordering, drift, loss-of-ACK, cross-role ownership and zero-replay matrix under the approved settings policy |
| Quality and live behavior | Actual module/import/graph gates, integrated non-live suite, fresh review and exact-source owned-Docker profile certification |

Update the existing specification, ADR and implementation plan to distinguish
historical inspection from current execution. User documentation describes one
current capability; experimental version choices do not become user setup steps.
The current-state adoption and warning examples remain unchanged in intent.

## Rollout and rollback

Consolidate in the isolated PR #258 worktree, with root as the sole integrator.
Preserve PR #249, commits, stashes, unfinished source and immutable artifacts.
Do not stage unrelated work. New tests and readers precede removal of old code.

On regression, stop new admissions and retain original stores and evidence.
Restore source through Git if necessary; never downgrade a live authority file,
reuse an old identifier for a new schema or bypass retained physical ownership.
No package version, workflow, tag or release is changed by this amendment.

## Approval and execution checklist

- [x] Maintainer explicitly authorizes unpublished-prototype consolidation.
- [x] Published behavior and historical evidence remain protected.
- [x] Existing settings/current-state/block-warn choices are unchanged.
- [ ] Published inventory and dirty-source preservation are recorded for implementation.
- [ ] A feasible whole-map projection and exact production path contract are reviewed.
- [ ] Current implementation, regression matrix and live certification pass.

Approval is recorded; implementation and release readiness are not established.
