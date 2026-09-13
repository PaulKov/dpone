# Complete reviewed composition execution

Status: APPROVED implementation follow-up under the existing approved
[composition activation/execution specification](../../feature-specs/composition-activation-execution.md).
The maintainer authorized planning and implementation on 2026-09-12 after the
independent review of `8dc8d72` through `583e8fc`. This plan completes the existing
three-cell contract; it does not expand connector or load-strategy support.

## Outcome and preserved behavior

An operator can provision protected authority, build and activate a verified v3
parent, execute native dbt and both ordinary full-refresh cells through the
provider/pack-exec path, and reconcile actual rows and durable terminal proofs.
Native-v2 behavior remains unchanged. Unknown commits never become success and
must retain ownership until reconciliation. Existing versions are not rewritten.

## Ordered work

1. Correct transfer terminalization: resolve issued SQL SID from protected
   attempt originals; explicitly close hydrated connections on success/failure
   before quiescence, including partial hydration failures.
2. Replace unsafe publication file CAS in the production root with a protected
   SQL-backed journal. Serialize canonical intent insertion and exact transitions
   under the existing control transaction; verify current parent/owner/epochs,
   preserve immutable history, reject conflicting attempt/subject/query IDs,
   and never replay an exchange after uncertain commit acknowledgement.
3. Supply concrete independent outcome observation. PostgreSQL→MSSQL requires
   exact committed generic receipt plus independently reconciled rows/content.
   ClickHouse requires independently observed typed generation content, schema,
   physical identity, topology and side effects. Sealed expected hashes are
   comparison inputs, never substitutes for observation. Bound all reads.
4. Wire real collaborators through application factories and admit complete
   execution capability before activation reservations or predecessor drain.
   Constructor registration alone is not readiness. Keep missing-proof refusal.
5. Repair SQL component failures and architecture coupling without weakening
   ledger fences, assertions or quality budgets.
6. Correct administrator schema installation, copyable strict build example,
   and pre/post-dispatch recovery guidance. Record supervisor persistence and
   identity lifetime decisions in architecture documentation.
7. Run focused red/green tests, change-aware broad checks, real three-cell
   execution with retry/recovery and actual row reconciliation, then independent
   review of the final commit. Only proven stages can be marked PASS.

## Required concrete ClickHouse completion

The review found no production writer for the release-side snapshot metadata.
Runtime capture must read the MSSQL source once in a bounded consistent
transaction, encode the Native payload once, and preserve immutable original
bytes in the protected supervisor directory. Protected SQL records bind the
attempt, source identity, schema, content version and file digests before any
ClickHouse mutation. A repeated or uncertain capture cannot authorize a second
CREATE. Generation sealing compares independently observed target data against
these captured originals.

Each attempt uses its own deterministic generation name and journaled UUID.
Publication retains the previous target under that name; a later attempt cannot
reuse or silently drop it. Supported physical design must be checked explicitly.

The Kubernetes worker also requires the actual protected dispatcher frontend
specified by ADR 0063. The local Docker observer cannot run in an ordinary pod.
Implement and validate the authenticated closed-operation transport and its
protected deployment before advertising this profile as usable. Workers must
never receive Docker authority or ClickHouse administrative credentials.

## Execution and evidence

The root integrator owns shared factories, schemas/registries, changelog,
workflow/release indexes and final documentation. Parallel writers use isolated
worktrees and disjoint task contracts. Review findings and command evidence are
retained outside the source checkout under the task artifact directory.

Tests must exercise production factory boundaries as well as isolated roots:
invalid/stale issued SID, cleanup on exceptions and partial construction, two
publication claimants, lost acknowledgements, interrupted writes, mismatched
receipts, same-count/different-content results, decimal/GUID/NULL fidelity,
unsupported topology and incomplete HTTP responses. Mocks do not certify live
services. SQL already committed is not rolled back by deployment rollback.

## Completion checklist

- [ ] Transfer identity and resource lifecycle regressions fixed.
- [ ] Protected publication journal wired and race/recovery checked.
- [ ] Both independent ordinary observers wired; real factory success tested.
- [ ] Activation verifies usable required capabilities before mutations.
- [ ] SQL components and required broad CI pass on the reviewed commit.
- [ ] Provisioning/build/recovery documentation is executable and accurate.
- [ ] Full three-cell real execution and reconciliation verified.
- [ ] Fresh independent review resolved; merge/release readiness recorded.

Until the checklist is supported by current evidence, the specification remains
APPROVED, not IMPLEMENTED, and the full journey remains UNVERIFIED.

## External dispatcher implementation addendum

The existing approved ADR 0063 boundary requires a concrete protected service.
The dispatcher owns ClickHouse administrative and issued credentials inside its
private network namespace. A separate root host observation service exposes a
fixed, peer-credential-restricted Unix socket and observes only its preconfigured
enrollment. Workers and dispatcher containers receive no Docker socket or host
process namespace. A fixed host TCP relay forwards only to the dispatcher TLS
frontend; ClickHouse remains namespace-local. Enrollment must explicitly pin the
additional ingress and host-observer configuration rather than accepting drift.

Reuse a signed descriptor property `composition_dispatcher` with schema
`dpone.composition-dispatcher-binding.v1`, canonical `dispatcher_id`, existing
API `connection_ref`, and `service_configuration_sha256`. The referenced binding
resolves its HTTPS numeric endpoint and secret-backed high-entropy token through
the existing resolver. Stage verified runtime contexts under a protected
service directory; request digests select existing contexts, never paths or URLs.
Authenticate in constant time before payload acceptance and separately verify
parent, attempt, target, source originals and enrollment before every mutation.

The closed wire schema supports READINESS, OPEN_GATE, DISPATCH, CLOSE_GATE,
OBSERVE_CATALOG and READ_STATUS. Common fields are schema, random request_id,
dispatcher_id, runtime_authority_sha256, operation and its exact subject. Reject
unknown fields and arbitrary SQL/settings/endpoints. DISPATCH reuses canonical
existing dispatch documents and bounded Native bytes with one exact length;
verify framing, time, concurrency and payload digest before the durable claim.
No transport retries, credential reconstruction, duplicate issuance or automatic
restart recovery are permitted. Responses bind correlation and subject to
canonical evidence references; HTTP success is never outcome proof.

A nonce-bound read-only READINESS observes staged context, service configuration,
host/container/listener facts, SQL schemas, supported capture/publication code and
actual ClickHouse identity/visibility. It allocates no generation, login or SQL
claim. Runtime rechecks dynamic authority. Never perform a nested RPC acquiring
the control lock while holding that lock locally.

Implementation must include actual service startup, units/container and ingress
configuration, positive authenticated delivery, denied raw ClickHouse access,
duplicate/paused/ambiguous dispatch tests and the complete provider campaign.
The architect's recommendation is not an implementation or certification claim.

## Trusted transfer preplan completion

The existing approved independent-outcome requirement includes independent
physical-route and schema-mutation authorization. A worker-supplied 32-byte plan
digest is insufficient. Before SQL registration and extraction, reconstruct the
load configuration from the verified manifest and signed resolved bindings.
Use the actual prepared PostgreSQL snapshot lease to reverify source identity
and reread its projection on that same session. Observe the target registry and
source/target authority independently, recompute the route fingerprint, and run
the existing schema preplanner through a separately owned target connection.
Close only that private target connection; leave the extraction snapshot active.

Retain the independently generated preplan, full observed source projection,
physical identities, route and exact attempt/write/operation originals in an
exclusive fsynced supervisor envelope under a separate preplans directory.
The composition binding v2 must pin this envelope digest. Generic receipt formats
remain unchanged, and historical v1 bindings remain decodable but cannot acquire
new independent-success authority. Recovery reads the original; it never plans
against a catalog already changed by the committed mutation. Recheck the current
physical registry, retained after-expectations, receipt before/after hashes and
captured payload provenance before accepting an outcome.

This is an implementation elaboration of the approved execution specification;
it does not mark the existing SQL5 finding fixed. Shared registration, hydration,
binding and observation seams remain integrator-owned until the complete path
and its regressions are verified.
