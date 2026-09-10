# Nonproduction composition authority

The approved synthetic authority design provides strict policy, scope and
qualification/execution contracts, plus an injectable authenticator that verifies
original grant signatures through the existing offline GitHub adapter. **Grant
authentication does not issue credentials or enable composition execution.**
Public scoped factories remain unavailable until the complete backend and worker
enforcement path is implemented. Pure contract values only compare subjects;
they cannot establish an authentic signature.

See the [approved specification](feature-specs/nonproduction-composition-authority.md)
and [ADR 0060](adr/0060-nonproduction-composition-authority.md) for the complete
six-document design. Existing production/native-v2 readers retain their original
authority and reject the new family.

## Documents and Python interfaces

| Interface | Responsibility |
|---|---|
| `dpone.contracts.nonproduction_scope.NonproductionScope` | Complete campaign, source/fixture/generator/toolchain identity, enrolled physical participants and explicit read/write effects |
| `dpone.contracts.nonproduction_authority.NonproductionAuthorityPolicy.from_bytes(raw, expected_sha256=...)` | Decode canonical policy bytes against an independently pinned digest |
| `dpone.contracts.nonproduction_grants.parse_nonproduction_grant(raw)` | Decode exactly the qualification or execution variant |
| `validate_grant_subject(...)` in the grant module | Compare common policy, signature subject, scope, clock and current revocation epoch |
| `NonproductionQualificationGrant.require_qualification_subject(...)` | Compare the exact qualification run and fixture/qualification plan subjects |
| `dpone.contracts.nonproduction_plan_pair.require_qualification_plan_originals(...)` | Decode both bounded plan originals and compare their full declared scope with the original grant |
| `pair.require_operation(operation, owner=...)` on the returned original pair | Select the exact work item after complete owner/operation comparison |
| `NonproductionExecutionGrant.require_execution_subject(...)` | Compare the qualified set, native/parent releases, deployment, activation and all workload pins |
| `dpone.runtime.nonproduction_authentication.NonproductionGrantAuthenticator` | Authenticate original qualification/execution grants and refresh independent trust after verification |
| `dpone.ports.nonproduction_authentication.NonproductionTrustProvider` | Supply an independently configured policy, verifier-policy pins and current revocation snapshot |
| `dpone.adapters.nonproduction_github_signature.GitHubNonproductionGrantSignatureVerifier` | Bind external signer expectations and pass exact original bytes to the injected `GitHubArtifactAttestationVerifier` |
| `dpone.contracts.nonproduction_activation.NonproductionCompositionActivationRequest` | Encode the distinct full activation request with its mandatory execution-grant digest |
| `dpone.adapters.nonproduction_mssql_trust.MssqlNonproductionTrustProvider` | Reopen independently provisioned SQL trust revisions and compare them inside an existing admission transaction |
| `dpone.adapters.nonproduction_mssql_registration.MssqlNonproductionRegistrationStore` | Persist immutable grant originals and complete execution campaign membership in that transaction |

Documents reject unknown and duplicate members, noncanonical JSON, non-finite
numbers, ambiguous types and unsupported discriminators. New authority document
digests use SHA256 of exact canonical UTF-8 bytes through `document_sha256` in the
scope module. Do not use legacy path-normalizing fingerprints for these documents.
The existing production hashing functions and IDs retain their semantics.

Scope is exactly `synthetic_composition_validation` / `non_production`. The initial
campaign contains both MSSQL-to-ClickHouse and PostgreSQL-to-MSSQL full_refresh
route pairs and all three physical participant engines. Individual route
qualification observations select a route within that complete campaign scope.
Native dbt effects are verified through the native source/payload closure; a
transfer-route label does not establish native authority.

Participant effects are sorted canonical digests. Their presence cannot prove
that a connection reaches the enrolled service, that role grants are exclusive,
or that manifests declare every actual read/write/helper/staging/state effect.
Those facts require independent physical and complete-source verification.

The [qualification plan reference](nonproduction-qualification-plans.md) defines
the closed fixture profiles, work-item effects, source-generation dependencies
and exact original comparisons. These internal plan codecs provide no physical
observation, source-bound reservation, seed or execution permission.

The [finite fixture input reference](nonproduction-fixture-inputs.md) defines
the canonical recipe inventories and expected seed generation for a selected
route. The [scoped execution owner reference](composition-scoped-execution-originals.md)
defines the mandatory complete owner original, its unchanged request/attempt
projection and exact supplied-original comparisons. Neither prerequisite enables
protected execution or replaces actual generation, physical observation and
source-bound evidence.

## Limits, expiry and replay

Every limit is explicit; there are no unlimited or permissive defaults. Effective
limits take the smallest policy, grant and workload ceiling. Signed validity is
at most 24 hours; the initial maxima are 64 workloads, 128 attempts, 100,000 source
rows, 1 GiB cumulative source bytes per route qualification and 3,600 seconds per
attempt. Execution grant validity and known workload membership must fit every
lower workload ceiling too.

Validity uses canonical UTC timestamps and a half-open interval: the exact expiry
rejects new issuance. Common grant validation requires an independent current
clock and revocation epoch. The pure `require_usage` method checks a supplied
counter snapshot; it does not reserve a counter or observe a stream. Runtime must
atomically reserve limits before reads and enforce cumulative consumption while
streaming. HTTP payload bytes and source bytes remain distinct measurements.

The grant consumption subject binds its immutable grant UUID and phase. Changing
other claims cannot obtain a new replay key. Protected storage must bind that
key to the exact original grant bytes and phase-specific subjects. A digest alone
cannot establish that a qualification was consumed or an execution was admitted.
Expiry or revocation never releases an unknown attempt's physical ownership.

The approved implementation algorithm uses campaign/phase pools for workload
and attempt counts, exact route pools for qualification export rows/bytes, and
a campaign-wide execution export pool. Changed grants or activations cannot
reset those pools. Reserve the verified finite immutable export bound before
BCP/COPY starts; separately retain actual complete or incomplete measurements.
This counter and source-seal enforcement is still pending, as described in the
[specification](feature-specs/nonproduction-composition-authority.md).

## Protected SQL trust revisions

An independent platform administrator installs
`render_nonproduction_mssql_schema(control_schema="dpone_control")` from
`dpone.adapters.nonproduction_mssql_schema` into the existing control database.
This one-time renderer creates `composition_nonproduction_trust` and its exact
append trigger. Runtime never installs, repairs, updates or selects trust from a
grant. Reapplying the initial renderer fails rather than adopting existing data.

Provision the original policy/verifier bytes, independently selected digests,
environment UUID, schema version, consecutive revision and revocation epoch.
The trigger serializes one-row appends with the existing transaction-owned
composition lock, rejects UPDATE/DELETE and requires the next revision and a
nondecreasing epoch. Exact original bytes remain VARBINARY; installed SQL module
identity instead uses the renderer's NVARCHAR/UTF-16 hash.

Construct `MssqlNonproductionTrustProvider` with an independently protected
connection factory, `expected_service_id`, `expected_environment_id` and optional
control schema. `read()` preserves the existing authenticator snapshot API.
`read_revision()` returns a documentary revision plus the full original snapshot
from a fresh connection. Both verify pinned service/environment, schema, trigger,
bounded bytes/hashes and policy/epoch consistency. Uncertain reads or commits
return no trusted observation.

`read_revision_in(ledger)` and `require_revision_in(ledger, expected)` inspect an
existing committable transaction holding the exact Exclusive transaction-owned
composition lock. They neither connect nor commit. Protected admission can
privately bracket signature verification with equal revisions, then recheck the
same revision/bytes and clock inside its atomic grant/budget/attempt transaction.
An absent or uncommittable transaction rejects with `trust_ledger_lock` before
reading authority rows. The precondition only inspects the transaction-owned
application lock when a committable transaction exists, because SQL Server can
otherwise reject the lock-inspection function itself.
Each read also retains the actual SQL transaction ID across catalog and original
row observations. A new transaction holding the same lock cannot validate an
earlier read. The enclosing control authority requires schema version 2; the
optional trust and registration documents retain version 1.
The registration component below uses this read/compare capability; the complete
private authentication/admission coordinator remains pending.

The platform must separately establish complete catalog visibility and prevent
provisioners from altering/truncating tables, disabling triggers, impersonating
privileged principals, using bulk-copy paths that omit trigger execution, or
otherwise bypassing append-only enforcement. Catalog
hashes and RLS rejection detect drift; they do not prove effective permissions.
Actual installation, concurrent appends and no-bypass permissions require
isolated SQL qualification. A constructed revision is never an executor permit.

## Immutable grant registration

After installing trust storage, the independent platform administrator installs
`render_nonproduction_mssql_registration_schema(control_schema="dpone_control")`
from `dpone.adapters.nonproduction_mssql_registration_schema`. The renderer adds
append-only grant and membership tables. Installation does not enroll signers,
verify signatures or grant worker permissions. Apply the same no-bypass permission
requirements as for trust storage; runtime does not install or repair these tables.

Construct `MssqlNonproductionRegistrationStore(trust_provider, clock=...)` with
the independently configured SQL trust provider and UTC-aware clock. Its methods
operate on the caller's existing protected ledger and never connect, commit or
roll back:

- `register_execution_in(...)` receives original request and grant bytes, the
  original signature bundle, verified signature-subject bytes and the expected
  current trust revision. It binds the exact request/grant and charges every
  distinct workload in the complete grant to its environment/campaign/execution
  membership pool. Different grants or activations cannot reset that pool.
- `consume_qualification_in(...)` receives the same originals without an execution
  request. It consumes the exact qualification run once. Repetition and binding
  the same run to another grant reject. It grants no seeding, reading, workload,
  attempt or export authority.
- `read_in(ledger, consumption_subject_sha256, expected_revision=...)` reopens
  immutable history. The expected revision is the current storage consistency
  fence; stored originals are separately audited against their original trust
revision and registration time. Later expiry or revocation does not erase history.

One private invocation boundary retains the actual transaction across both clock
calls, all history pages, each append and successful returns, including an empty
read or an identical execution registration. Before each append and return, it
reopens the latest complete trust revision. A newer revision with identical
policy bytes still rejects. Changed authority, lock or transaction returns no
receipt; a failure after an append requires the caller to roll back all its writes.

The trusted coordinator must verify original signatures before these calls,
retain the transaction and exact lock through all admission writes, roll back the
whole transaction on failure, then commit once and independently read back.
An in-transaction return is documentary data, not an acknowledged durable result
or an execution permit. Signature bundles have a separate 8 MiB bound; individual
documents retain their 1 MiB bound and are stored as original binary bytes.

An identical execution registration reopens history without another membership
charge. Changed originals under the same grant/phase key reject. Distinct execution
candidate grants can name the same activation; registration does not select or
replace the protected occurrence's single exact request. Complete membership must
fit the current policy/grant ceiling and every current workload's lower ceiling.
A higher newly signed ceiling can admit additional members while retaining all
earlier charges; a smaller ceiling can block new admission.

Before new inserts, the external connection owner must set `ANSI_NULLS`,
`ANSI_PADDING`, `ANSI_WARNINGS`, `ARITHABORT`, `CONCAT_NULL_YIELDS_NULL` and
`QUOTED_IDENTIFIER` ON, and `NUMERIC_ROUNDABORT` OFF. SQL Server requires these
options for the qualification-only filtered unique index. The renderer sets them
for installation; repository methods check the caller's options and reject
`registration_session` without changing them. Historical reads do not need this
additional DML precondition.

Complete pool audit reads grant history one original record at a time in stable
key order, including each original signature bundle and historical trust revision.
It retains at most 64 distinct membership identities; an extra membership row
rejects. Memory is bounded, but total audit work grows with registration history
while holding the global lock. The 18 isolated registration cases cover
concurrency, rollback, lost acknowledgement, historical originals and permission
enforcement in the [recorded component run](composition-shared-sql-storage.md#observed-sql-component-evidence).
Large-history audit latency and capacity remain **UNVERIFIED**; component success
does not establish a performance limit. Cumulative attempt and source budgets
remain separate implementation work.

## Scoped activation request

`NonproductionCompositionActivationRequest` carries the complete existing request
fields plus mandatory `execution_grant_sha256` under
`dpone.composition-activation-request.nonproduction.v1`. It requires all three
initial execution cells, their native/standalone assignments, at most 64 workloads
and the complete unique physical-write partition. The request digest covers the
grant and exact canonical bytes; physical guards and `runtime_context_sha256`
retain their previous meanings.

Use `to_bytes()` and the explicit `from_bytes(raw)` reader for this family.
Protected persistence must separately compare the independently expected request
digest. The old v1 reader continues rejecting the new document. Existing attempt
and receipt shapes can bind its new request digest without an optional scope
field or legacy-family fallback.

`require_execution_grant(grant)` compares the exact grant digest, parent release,
deployment, activation and complete workload/constituent/pack membership. This
is a pure comparison; it does not verify signatures, native ancestry, physical
scope, time or consumption. Concrete scoped activation persistence and worker factories
remain unavailable.

## Required runtime verification

A trusted composition root must obtain the policy pin and protected enrollment
independently of the release. It then reopens the original grant bytes and verifies
their authentic signature through the selected existing backend. The resulting
`NonproductionSignatureSubject` is a comparison value: constructing it or receiving
a supplied PASS report is not cryptographic verification.

The application constructs `NonproductionGrantAuthenticator` with keyword
capabilities `trust_provider`, `clock` and `verifier`. The provider returns
`NonproductionTrustSnapshot` containing original policy/configuration bytes,
their independently selected digests and the current revocation epoch. It must
not discover those trust pins from the artifact being authenticated. The clock
returns an aware datetime; the service converts it to UTC within its sanitized
error boundary.

Use `authenticate_qualification(...)` with the original grant/bundle, independently
reconstructed scope, qualification run, fixture plan and qualification plan.
Use `authenticate_execution(...)` with the original grant/bundle, independent
scope, qualified set, native and parent releases, deployment, activation and all
workload pins. Each method verifies the selected phase and returns its parsed
grant plus authenticated documentary signature subject. A previous return value
or caller-supplied verification report cannot replace this call.

The initial adapter accepts only GitHub Artifact Attestations. It strictly parses
the existing external `runtime-artifact-trust-policy.v2`, requiring
`non_production` and mandatory `required_for_prod` attestations. The policy's
repository, Actions issuer, public root digest and hosted-runner requirements
must match. The NP signer identity is the exact composite
`https://github.com/{signer_workflow}@{signer_digest}`; the workflow digest is an
independent 40-hex commit pin. This composite expresses verifier selectors and is
not the certificate SAN spelling. Cosign public-key labels remain unsupported
by this adapter because key possession does not verify their separate
issuer/identity claims.

The adapter stages only bounded public grant/bundle bytes in private temporary
files and invokes the existing offline file verifier. It checks the exact
returned digest, a positive verified-attestation count and the supported CLI
version. The service then reloads policy/configuration, revocation and time.
Changed pins or bytes, a backwards clock, grant expiry or stale root material
reject authentication. This refresh never releases an active or unknown attempt.
Verifier errors expose fixed domain reasons, not raw CLI output or artifact data.

Before source access or issuance, runtime must compare both common and
phase-specific subjects, verify complete source/effect membership against actual
participants, consume the proper grant phase under protected fencing, and enforce
the effective counters. It must repeat current scope/grant/epoch checks at actual
worker admission. Qualification never substitutes for an execution grant.

Signed route observations, scoped native/parent envelopes, protected scoped
activation persistence, real signature qualification, atomic counters and the
full current/provider/SQL/reconciliation campaign remain implementation
obligations. Current authenticator tests use injected verifier results; their
PASS is contract evidence, not a successful genuine signature or live campaign.
