<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Source ADR 0056: PostgreSQL Batch/XMin effects use target-local versioned authority

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

## Status

Accepted.

The V2 foundation decision was accepted on 2026-09-04. The same-day provider
closure amendment replaces its unreleased write codec with V3 before production
activation. V1 and experimental V2 remain read/replay-only.

The 2026-09-05 schema-authority amendment adopts portable schema-2 expectation
plus environment-bound observation, independent principal authority,
certificate-signed caller-executed modules and no automatic migration of any
pre-amendment V3 object. Its normative type algebra is in
`feature-design-postgres-mssql-r1-v3-schema-contract-amendment-v1.md`.

The 2026-09-05 physical-authority amendment further chooses a closed generated
descriptor, explicit verified-registration/principal inputs, an immutable live-
identity baseline, 29 shared plus six per-binding signed modules, a contained-
database-user GA1 permission profile and a crash-safe chunk protocol. Concrete
SQL remains gated by the separately approved exact physical descriptor.

The provider-authority clarification assigns non-overlapping ownership. The
core physical descriptor is the sole authority for physical table/module
semantics. Immutable companion contracts add installation security, migration,
binding-instance and statement-order authority without replacing core leaves.
Their provider aggregate is the sole executable composition authority. A SQL
renderer, installer or attestor cannot infer, override or splice either layer.

The 2026-09-05 provider-install receipt amendment separates database/provider
installation from binding/registration control effects. It adds immutable
`dpone_authority.dpone_provider_install_receipt_v3` and versions the internal
security/replay contracts. `dpone_control_receipt_v3` remains exclusively
binding/registration-scoped. The normative amendment is
`feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.

## Context

PostgreSQL → MSSQL Batch and XMin previously had different commit, retry and
checkpoint mechanisms. They did not share a durable target writer generation,
generation-scoped row hashes, pre-source replay or a pre-commit target-quality
boundary. Some paths could mutate target before blocking quality, and a current
catalog observation could not safely prove physical identity after restore,
clone or object recreation.

R1 needs a deliberately small certifiable profile: PostgreSQL 16 Batch full
refresh or XMin current-state polling into one ordinary SQL Server 2022 rowstore
in a standalone database. Target effect and every canonical recovery authority
must commit in one physical session and local database transaction.

The initial V2 implementation established the target-local UoW, but fresh
provider review found that its scalar generation authority, opaque staging
proof and caller-constructed retry epoch could not safely support executable
composition. No production V2 writer or certified route existed, so the write
contract can be versioned without reinterpreting durable production history.

## Decision

### Version and physical authority boundary

New R1 writes use:

```yaml
effect_contract_version: mssql_effect_receipt_v3
request_codec: dpone.mssql-effect-request.v3
quality_codec: dpone-r1-quality-v3
authority_set_codec: dpone-r1-generation-authority-set-v2
```

The version participates in operation/effect keys, sealed request, receipt
digests, control receipts and recovery probes. V1 and V2 bytes use their exact
legacy readers only; they are never decoded or synthesized as V3.

Target data, final staging, registration and active-registration head, writer
head/fence, operation row, receipt header/body, generation-scoped row hash,
XMin checkpoint and one-shot authority consumption reside in the target SQL
Server database. A control database may contain only repairable projections.
One injected ODBC session uses `XACT_ABORT ON`, `SERIALIZABLE`, bounded timeouts,
fully durable commit and database-level delayed durability disabled.

### Signed target registration and physical uniqueness

`MssqlTargetRegistrationPayloadV1` is a domain-separated, length-framed UTF-8
payload with NFC-normalized canonical identifiers. It binds route/profile,
physical server/database/recovery/object identities, object profile, catalog
projection, target binding/object/generation UUIDs, source authority, issued and
expiry times, nonce, revocation revision, action, predecessor and expected active
revision. Its digest and all signature metadata are outside the signed payload.

`MssqlTargetRegistrationVerificationV1` records exact payload bytes/digest,
bundle digest, trusted-root and Cosign-policy digests, verifier version,
verification time and certificate identity/issuer digests. Trust roots and
policy are environment-owned and cannot come from the bundle. Exactly the
registration payload digest and verification-policy digest are transitively
present in source authority, writer head and effect receipts. The separate
verification-receipt digest is included in control receipts. Bundle, signer,
certificate and verifier details remain in the immutable verification record.

There is exactly one active registration and one binding per physical
server/database/object identity. SQL uniqueness constraints cover both physical
identity and binding UUID. Provisioning locks physical identity before binding,
then writes immutable registration, protected object properties, active head,
writer head, grants and control receipt in one transaction.

Rotation is the `registration_action=rotate` provisioning branch. It proves
unchanged physical identity/binding and exact predecessor/revision, inserts an
immutable successor, CAS-advances only the active-registration head, preserves
writer head/admitted operations, and writes `registration_rotate` receipt.
Expiry blocks new admission but does not invalidate an already sealed effect.
Explicit revocation requires manual recovery.

A future revoked operation-bound registration may finish only with a separately
signed one-effect `MssqlRevokedRegistrationCompletionOverrideV1` and a durable
revocation receipt. The current internal payload/import code is not an active
capability: registration retirement/revocation and override resolution remain
blocked until their signed command and receipt contract is separately approved.
It can never permit source reread or belong to the generation-authority set.

### Identity, writer fence and canonical lock order

Three monotonic dimensions remain independent:

- `writer_generation` identifies target content/hash generation;
- `head_revision` orders committed effects inside it;
- `operation_epoch` fences ownership attempts for one stable operation.

The business effect key is stable before source I/O and excludes attempt,
generation, head and XMin positions. Recovery transitions use their own
domain-separated idempotency keys.

All target/data/control transitions use this lock hierarchy:

```text
physical target
→ target binding
→ operation/effect
→ artifacts ordered by kind and UUID
→ registration/authority rows
```

Receipts form an append-only predecessor chain. Current head/checkpoint may
equal or descend from a historical receipt, so later valid effects never hide
replay authority.

### Closed transaction and authority resolution

`MssqlR1TransactionV3` binds one transaction UUID, opaque physical handle and
stable SQL Server session identity. Its factory owns begin, active-state checks,
fully durable commit, rollback, close and binding release. Every V3 adapter in a
UoW receives that wrapper; a different handle or session identity fails before
SQL.

The session identity includes Python handle binding, `@@SPID`, `DB_ID()`,
database GUID/family/recovery fork, registered server identity and a read-only
transaction UUID in `SESSION_CONTEXT`; MARS is disabled and active statements
require `@@TRANCOUNT=1` and `XACT_STATE()=1`. Lifecycle is
`NEW→ACTIVE→COMMIT_DISPATCHED→COMMITTED|OUTCOME_UNKNOWN→CLOSED` or
`ACTIVE→ROLLED_BACK→CLOSED`. After commit dispatch the old session cannot
execute SQL or rollback and is discarded before a fresh proof session.

The sealed runner never receives caller-selected authority rows. Transactional
ports resolve and admit the imported issuance/ordered authority set from the
target database using the exact sealed attempt. Active import and signature
verification use dedicated authority-set-only V3 facades; the broader prototype
interfaces that expose revoked-override methods are not composition
dependencies. No override resolver or override transaction port exists in the
first active profile. Sealed artifacts transition atomically to
`CONSUMED(receipt_id)` after the receipt is appended. Before commit, a distinct
typed candidate proof re-reads and decodes receipt/body, writer head, row-hash
generation, checkpoint, consumed artifacts/authorities and operation state. It
also retains and decodes the exact canonical attempt bytes and binds their
digest, owner and server lease to the target-local operation observation; a
candidate returned by the port is then checked by the UoW through
`proof.assert_for(transaction.binding, attempt, receipt)` against the
independently trusted pure binding and arguments before commit, after the opaque
transaction is re-asserted ACTIVE. This avoids a contracts-to-ports dependency
and prevents a coherent serialized replacement from becoming authority. It is not a
committed-replay proof. Ambiguous commit is classified only through a
fresh physical session and the committed/known-not-committed/unknown replay
union.

### Exact V3 schema and permissions

Schema revision 2 separates a portable `MssqlR1SchemaContractV3` from the live
`MssqlR1SchemaAttestationV3`. The portable contract contains no database-
assigned object/principal IDs. Fresh attestation binds its digest to exact live
schema/object IDs, owner and principal/SID identities, permission observations,
procedure result/templates, persisted SET options and certificate signatures.
Canonical payloads are authoritative; relational columns support uniqueness,
locks, CAS and bounded fresh probes and must match decoded bytes.

The versioned inventory contains separate tables for schema authority,
registration and active-registration head, target generation head, writer
operation and sealed effect, staging artifacts/chunks, generation-authority
issuance/refs, control/OPEN-recovery/effect receipts and typed bodies,
generation-authority consumption, generation row hashes and XMin checkpoints:
nineteen target-local tables in total. SQL checks repeat canonical
all-or-none, positivity, adjacency, predecessor, discriminator and artifact-set
disjointness invariants. Signed/canonical authority payloads and receipts are
immutable. Heads, operations, artifact lifecycle, authority consumption and row
hashes are guarded mutable projections; XMin checkpoints are append-only.

Migration is additive and fail closed. Exact empty V1/V2 objects may coexist
only with no legacy writer grant; any V1/V2 receipt, sealed intent, nonterminal
operation, stage row, ambiguous state or legacy grant blocks before mutation.
Automatic V3 installation otherwise requires V3 inventory to be wholly absent.
Any schema-1, unversioned or pre-
amendment V3 object blocks for manual disposition; legacy descriptors are never
DROP/ALTER authority. V1/V2 rows are never reinterpreted. V3 runtime principals
receive only named procedure grants. Closed stage-owner and attestation modules
execute as caller and obtain elevation only from exact certificate signatures;
schema-wide EXECUTE, `EXECUTE AS OWNER` and legacy V1/V2 write grants are
forbidden. Schema installation takes its own install lock and then follows the
physical-target to binding lock order.

Known pre-amendment V3 staging tables are never replaced/upgraded automatically,
even when empty. Any such object, active row, partial/unreadable inventory or
unknown codec blocks for manual disposition. The R1 backend never drops or
alters that state.
Concrete SQL backend work requires a separate approved schema sub-spec with the
exact first-revision columns, constraints, procedures, properties and grants.

The normative physical descriptor is the single source for physical
table/module semantics and freezes exactly 20 tables, 29 shared procedures and
a six-module per-binding target template. The provider aggregate binds that
core byte-for-byte to install-security, migration, binding-instance and
statement-order companions; it is the executable renderer/installer/attestor
authority. The binding pack contains separate Batch/XMin mutation, row-hash and
bounded-quality modules. Registered target coordinates are compiled into their
definitions. Dynamic stage coordinates are not: modules consume the exact
instantiated scan result through the shared signed `dpone_scan_stage_v3` into a
module-local table variable. The binding certificate has no stage permission.
Direct runtime DML and a generic dynamic-SQL executor remain forbidden.

The twentieth table is the immutable provider-install receipt. It contains the
installation effect key, request digest, canonical payload bytes, raw SHA-256
payload digest and commit time; it has no target-registration foreign key and
no update/delete/TTL path. It is appended after stable attestation in the same
installer transaction. Its V2 effect key includes target database, provider
contract and binding-pack digests, so independent bindings in one database do
not collide. Any observed 19-table pre-amendment V3 installation blocks for
manual disposition; no receipt is synthesized.
The amended physical descriptor literal is
`dpone-mssql-r1-v3-physical-schema-2-r2`; the portable schema contract remains
schema-2, and the prior 19-table descriptor literal is never decoded as R2.

Schema attestation receives the exact verified registration and independently
resolved principal authority set as explicit port arguments. Registration ID
or a target-loaded projection alone is insufficient. The first valid schema
attestation, its verification/principal bytes and `live_identity_digest` are
stored in the immutable schema-authority row. Registration rotation must
reproduce that live identity and the binding pack byte-for-byte. Schema,
certificate or baseline rotation is a future schema-generation capability and
cannot be self-adopted by status/runtime.

GA1 uses four distinct contained database SQL users with no database-role
membership and no server SID. Database-scoped catalogs prove the closed managed
permission projection. Hostile `sysadmin`, server-level impersonation/control
and a malicious provisioner are outside the GA1 threat model; instance,
Windows and external principals require a separate server-authority profile.
The signed registration's server identity remains a physical identity check,
not a claim that a database module proves absence of server administrators.

Every R1 physical connection is non-pooled, `autocommit=true`, MARS-disabled
and establishes the exact schema-2 session option set before `BEGIN`:
`ANSI_NULLS`, `ANSI_PADDING`, `ANSI_WARNINGS`, `ARITHABORT`,
`CONCAT_NULL_YIELDS_NULL`, `QUOTED_IDENTIFIER` and `XACT_ABORT` ON;
`NUMERIC_ROUNDABORT` OFF; `NOCOUNT` ON; SERIALIZABLE isolation. This set is
reproved with session/binding identity before effect statements.

Named procedures have fixed result contracts except `dpone_scan_stage_v3`.
That procedure uses a portable `stage_scan_template`: exact business columns
are instantiated only from the retained open-stage plan, followed by a fixed
system suffix. The adapter verifies the complete `cursor.description` before
reading rows; dynamic catalog shape is never inferred as authority.

### OPEN, sealed staging and actual DML bytes

One certificate-signed, caller-executed SQL transaction creates each stage,
installs protected token/plan properties and commits `PLANNED→OPEN`; durable
`PLANNED` is corruption/UNKNOWN. OPEN itself has no loader DML grant. A begin-
chunk transaction creates one exact LOADING authority and grants object-local
INSERT; after the independent load, complete revokes INSERT, obtains an
exclusive table lock, proves per-row chunk coordinates/count/digest and commits
LOADING→COMPLETE. At most one chunk is LOADING and sequences are adjacent.
Process loss never resumes a LOADING chunk in place; expiry recovery abandons
the complete artifact set. A failed or unproved renewal aborts/reaps the loader.

`R1OpenStagePlanV1` fixes exact DDL and typed/canonical column mappings.
`R1SealedStageManifestV1` binds target/effect, open-plan and DDL digests,
physical object/token, ordered target mapping, type/catalog/permission policies,
observed counts/bytes and per-kind canonical grammar.

Three closed stage kinds exist:

- Batch payload: typed full row plus canonical key/row/hash;
- XMin delta: typed current row plus canonical key/row/hash;
- XMin complete keys: typed key plus canonical key; row length is exactly zero.

Unique SQL constraints use the supported typed integer/UUID key, never
`varbinary(max)`. Sealing and the UoW separately prove canonical-key uniqueness.
The sealing transaction locks the artifact/table, scans typed and canonical
representations together, proves that the registered type policy re-encodes the
typed values to the exact canonical bytes, proves zero loader grant and LOADING
chunks plus a contiguous chunk/row-ordinal coverage set, computes
count/bytes/digests, persists the manifest and changes `OPEN→SEALED`.

R1 correctness tests use a bounded transactional ODBC reference loader. The BCP
subprocess path remains activation-blocked until R2A certifies its exact wire.
For that future path, begin, BCP and complete are separate commit boundaries;
BCP exit/status counts are telemetry and never durable authority.

Rendered target SQL is retained execution evidence, not self-authorizing
authority. The seal composition resolves renderer authority independently from
the signed environment-profile digest, renders and verifies the exact execution
bytes against that authority, and retains only renderer identity and admission
evidence digests with those bytes. A request or renderer result cannot provide
or expand its own trusted-build allowlist. Resume executes the admitted bytes
without re-rendering.

Target DML reads only typed columns whose canonical equivalence was proved.
Manifest digest enters artifact authority, artifact set, sealed intent, exact
mutation plan and receipt.

Every artifact scan orders rows by unsigned lexicographic
`canonical_key_payload`, never SQL Server native integer/UUID order. Artifact
sets use fixed order `batch_payload → xmin_delta → xmin_complete_keys`, omitting
kinds not used by the selected mode.

### Crash recovery before and after sealing

OPEN/STAGING artifacts cannot be taken over. An expired attempt is reusable only
after a serializable target-only transaction proves receipt and sealed intent
absent, proves lease expiry and loader quiescence, revokes load permission,
exclusively locks and abandons old stages, increments epoch, allocates fresh
artifact/open-plan identities, and CAS-transitions the operation back to
`ADMITTED`.

`MssqlOpenStageRecoveryReceiptV1` commits with that transition. It preserves the
business `effect_key` and has a distinct unique `recovery_effect_key` derived
from operation/effect, old artifact set, expected epoch and expected head.
Ambiguous outcome is resolved only by exact receipt plus operation/head/artifact
read-back. PostgreSQL cannot be opened before recovery is proved committed.

After sealing, the semantic effect payload is immutable and separate from its
attempt envelope. A single serializable takeover primitive locks target,
operation and artifacts; proves receipt absent, authority unchanged, operation
`SEALED`, lease expired and artifacts retained/immutable; CAS-increments epoch
and head projection; and returns a server-built attempt envelope. Caller-side
epoch overlay is forbidden. Ambiguous takeover blocks.

Pure manifest/run-ID/profile validation builds the target authority graph first.
Receipt replay, sealed resume and blockers complete before the PostgreSQL factory
is invoked. Only `ADMITTED` may construct/open the source endpoint and snapshot.

### Recoverable mutation and atomic target UoW

The sealed request stores canonical `R1BatchMutationPlanV1` or
`R1XminMutationPlanV1` bytes, not only a digest. Plans bind ordered stage/target
mappings, D/I/U semantics, row-hash changes, checkpoint transition, resource
limits, type/quality identities and artifact references. Unknown codec blocks.

Every effect executes in this order on the same physical SQL Server session:

```text
lock/re-prove target, operation, registration, head and staging
→ exact receipt probe
→ target-local resolve/admit of the exact generation-authority issuance/set
→ target mutation
→ generation-scoped row-hash mutation
→ closed bounded target-local quality
→ immutable V3 header and typed body
→ XMin checkpoint CAS when applicable
→ consume artifacts and all admitted one-shot authorities
→ operation/head CAS
→ typed candidate proof from full same-transaction read-back
→ fully durable COMMIT
```

External calls, PostgreSQL reads, runtime DDL, arbitrary user SQL and unplanned
unbounded validation are forbidden inside the transaction. A lost COMMIT
response discards the session. A fresh target-only probe may report committed
only from exact receipt/head/checkpoint/authority proof, retry only from positive
non-commit proof, and otherwise returns unknown and pauses the route.

### Generation authority

`MssqlGenerationAuthoritySetV2` is an ordered set of at most two purpose-specific
refs: zero or one transition (`initial_cutover` or `rebaseline`), followed by
zero or one supplemental `empty_refresh`. Duplicate, missing, extra, reversed or
wrong-purpose refs fail. Every ref binds effect, snapshot, artifact set, mutation
plan, expected/candidate generation/head and recovery identity. Admit/consume is
all-or-none; every ref stays `ISSUED` or all become `CONSUMED(receipt_id)`.

Batch always advances an adjacent content generation. Initial Batch requires
`initial_cutover`; ordinary non-empty Batch→Batch needs no exceptional authority;
XMin→Batch or explicit rebaseline requires `rebaseline`; every empty Batch also
requires `empty_refresh`.

XMin initial baseline requires `initial_cutover`; Batch→XMin and explicit XMin
rebaseline require `rebaseline`; ordinary XMin polling advances checkpoint
revision in the same generation. If the complete source key set is empty, every
XMin path additionally requires `empty_refresh`, including empty→empty.

### XMin current-state algebra

One PostgreSQL repeatable-read read-only snapshot produces delta current rows
`Δ` and the complete source key set `K`. With target keys `T0` before mutation:

```text
D  = T0 - K
I  = Δ - T0
U  = {k in Δ∩T0 | staged_hash != target_hash}
NΔ = {k in Δ∩T0 | staged_hash == target_hash}
A  = (T0∩K) - Δ
```

The sets are pairwise disjoint where applicable. Apply order is
`DELETE D → UPDATE U → INSERT I`. Empty delta is not empty source. If `K=∅` and
`T0≠∅`, `D=T0` is a full hard-delete.

Quality proves `Δ⊆K`, both final target↔K anti-joins empty, count/cardinality
equations, target/sidecar/stage survivor hash parity, deleted-key absence and
exact checkpoint predecessor. XMin affected scope is `D∪I∪U∪NΔ`; unchanged
count is `|A|+|NΔ|`. Count overflow, timeout, duplicate or incomplete probe rolls
back.

### Control-plane authority

Provision, registration rotation and generation-authority import are
create-only/CAS target-local effects recorded by `MssqlR1ControlReceiptV1`. It
binds operation, physical identity, registration, input and verification
digests, expected/committed registration and writer revisions, schema/grants,
affected authority set and committed timestamp. Its unique control-effect key is
domain-separated over operation, physical identity, input and expected revision.

Exact replay verifies full receipt plus registration, properties, schema,
grants, head and relevant authorities. Presence of a registration row alone
never resolves ambiguous commit. Pause/resume are deferred until their own
durable active-operation contract is approved.

The control receipt binds both expected and observed writer-head revision and
digest. Provision proves the transition from no writer head to an exact
uninitialized revision-1 head; other control operations prove an unchanged
positive revision and exact digest. Fresh ambiguous-outcome recovery uses a
typed writer-head observation and blocks on a missing or different head.

OPEN/STAGING recovery instead uses `MssqlOpenStageRecoveryReceiptV1`. Its
separate unique `recovery_effect_key` is domain-bound to operation/business
effect, old artifact set, expected epoch and the mandatory positive predecessor
operation-projection revision. The committed projection revision is exactly its
successor. Exact recovery replay verifies that receipt plus operation, head,
abandoned artifacts and new open-plan state.

### Public boundary and truthful status

The user manifest remains semantic. Platform-owned environment profiles select
registration, physical topology and providers. `dpone plan` and manifest
validation share negotiation. Low-level ODBC, staging and receipt APIs remain
internal. The default composition is wired only after all exact V3 providers
exist; until then implementation is `absent`, certification `unverified` and
activation `blocked`.

## Consequences

- Batch and XMin share deterministic target/replay authority without DTC.
- Exact replay and sealed resume suppress PostgreSQL business reads and duplicate
  target effects.
- Staging consumes more space and sealing performs an additional bounded pass,
  but the receipt now proves the same logical values used by DML.
- Persistent row hashes consume target storage and require governed baseline at
  cutover.
- Registration, control and recovery schemas are more explicit, but restore,
  clone, stale-owner and ambiguous-commit outcomes fail closed.
- V1/V2 write compatibility is intentionally not preserved. Their durable rows
  remain readable and replayable; dual-write and reinterpretation are forbidden.
- Cross-database state, AG/DTC, large shadow publication and WAL remain separate
  capability decisions.

## Alternatives rejected

- Central receipt database: requires distributed atomicity or creates a gap.
- Catalog-inferred registration: clone/PITR/object ABA can bless itself.
- Scalar authority kind: cannot prove transition plus empty authorization.
- Metadata-only stage seal: does not prove DML business values equal canonical
  bytes.
- Caller-computed epoch overlay: can race the durable owner/head projection.
- Take over an OPEN BCP stage: old loader may continue after epoch change.
- Recover mutation from a digest: a digest cannot reconstruct exact DML.
- Re-read PostgreSQL after durable seal: the same effect can observe new data.
- Count-only XMin reconciliation: equal cardinality does not prove key equality.
- Blocking quality after commit: can report retry after durable target effect.
- Windows AG/DTC as first GA: unnecessary larger failure domain.

## Validation

Acceptance requires hermetic contract/model suites, real SQL Server integration,
real PostgreSQL→SQL Server route tests and exact vendor-live certification.
Fault injection surrounds OPEN/load/seal, intent seal, takeover, DML, hash,
quality, receipt, authority consumption, checkpoint, head and COMMIT response.

The versioned live scenario registry covers all six Batch authority cells,
Batch/XMin replay, sealed resume, lost commit, XMin I/U/D/no-effect and empty
source cases, stage tamper, all-or-none authority rollback, stale epochs,
registration rotation, blocked override activation and OPEN recovery. `PASS` requires the exact
scenario set, zero `FAIL/SKIP/N/A`, real ODBC and signature verifier, current
commit/dependency/environment digests and create-only evidence. Missing live
infrastructure remains `UNVERIFIED`.

## Related decisions and specification

- [PostgreSQL → MSSQL industrial integration V7](../../feature-design-postgres-mssql-industrial-v7.md)
- [PostgreSQL → MSSQL Batch/XMin correctness R1](../../feature-design-postgres-mssql-r1-correctness-v1.md)
- [PostgreSQL → MSSQL R1 provider closure](../../feature-design-postgres-mssql-r1-provider-closure-v1.md)
- [ADR 0021: signed catalog bundles and closed extension conformance](../0021-signed-catalog-bundle-conformance-boundary.md)
- [ADR 0022: durable target commit journal and fence (Proposed)](../0022-target-commit-terminal-failures.md)
- [ADR 0025: process-local quality-gate authority](../0025-process-local-quality-gate-authority.md)
- [ADR 0051: resumable MSSQL shadow initial publication](../0051-resumable-mssql-shadow-initial-publication.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
