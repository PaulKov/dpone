# Nonproduction composition authority

Status: APPROVED by the maintainer on 2026-09-10 after independent source and
architecture review. The approval applies to the six-document amendment and
limits below; implementation and observed live qualification remain separate.
This amends [composition activation and execution](composition-activation-execution.md).

## Decision

Add one explicit nonproduction acceptance path for
`synthetic_composition_validation`. Use a new scoped native release/wire and
composition parent while retaining unchanged execution-pack.v2/source payload
formats and all existing production/native-v2 producer, certification and reader
behavior. This campaign does not prove execution of an unchanged native-v2
release and does not establish production certification or release readiness.

The campaign must actually execute the complete parent: native SQL Server dbt,
every generated MSSQL-to-ClickHouse full_refresh workload, and ordinary
PostgreSQL-to-MSSQL full_refresh. Existing current/provider entrypoints, protected
writer gates, target transactions, collision/retry/unknown-outcome rules and
independent real-row/type reconciliation remain mandatory.

## Problem and current boundaries

The existing compiler and native release reader require production/enterprise
route authority. The route matrix requires a genuinely signed production
deployment. A synthetic CI deployment cannot satisfy that policy by receiving a
valid signature or a route-certified status. Neither environment relabeling nor
disabling certification is an accepted implementation.

Data engineers need reproducible synthetic validation; platform engineers own
independent enrollment and signing policy; operators need distinct qualification,
activation, execution and reconciliation results. Production use retains its
existing authority path.

## Minimal contract delta

The initial larger draft is superseded by this six-document boundary:

| New strict document | Responsibility |
|---|---|
| `dpone.nonproduction-authority-policy.v1` | Externally pinned signer, environment/participant enrollment, purpose, validity/revocation and resource ceilings |
| `dpone.nonproduction-authority.v1` | Signed closed variants `phase=qualification` and `phase=execution`; each has distinct mandatory subjects |
| `dpone.route-qualification.v1` | Signed actual route observations, exact six-dimensional route, compilation intent, scope and original raw evidence descriptors |
| `dpone.dbt-release-set.nonproduction.v1` | New native authority with mandatory hash-covered scope and qualification descriptors; producer wire `dpone.dbt-airflow-self-service.nonproduction.v1` |
| `dpone.release-composition.nonproduction.v1` | Full scoped-native and genuine ordinary union; scope covers every read/write/helper/staging/state effect |
| `dpone.composition-activation-request.nonproduction.v1` | Full existing request data plus mandatory execution-grant digest |

Existing deployment, init-fetch, Airflow identity, attempt and receipt formats
retain their shapes because their hashes bind the complete verified ancestor
chain. Existing native payload formats are reused through explicit separate
family entrypoints and shared structural validation; a fake production wire is
never passed into an existing validator. Old readers continue rejecting the new
family. No optional scope field, fallback or permissive registry is introduced.

Scope must not live only in `provenance`, which the release hash excludes.
Existing `runtime_context_sha256` does not cover a later execution grant and
retains its meaning. The new scoped request explicitly binds that grant.

## Scope, phases and limits

Common signed claims require canonical source repository/commit, fixture and
generator identities, compilation intent, toolchain/image, campaign and protected
environment UUIDs, policy/enrollment digests, exact route dimensions, all read and
write participants, physical subjects/relation scopes, validity and revocation
epoch. Purpose is exactly `synthetic_composition_validation`; trust tier is
`non_production`. Labels, aliases and caller-supplied trust roots are not proof.

Qualification additionally names its one-time run and fixture/qualification
plan. Execution additionally names the qualified set, native and parent releases,
sealed deployment, activation UUID and exact workload/pack membership. Neither
phase grants the other phase's authority. All JSON is bounded and canonical with
unknown/duplicate-field rejection and complete signature/subject verification.

Initial ceilings: validity 24 hours, 64 workloads, 128 attempts, 100,000 source
rows and 1 GiB cumulative source bytes per route qualification, and 3,600 seconds
per attempt. Effective limits are the minimum of policy, grant and workload
limits. Enforce them before reads and while streaming. Source bytes and actual
HTTP payload bytes remain separate measurements. These are campaign limits,
not performance certification.

Only newly isolated, independently enrolled synthetic PostgreSQL, SQL Server
and single-node ClickHouse participants qualify. No production reads, business
fixtures, ambient legacy writers or arbitrary development data are authorized.

## Identity and execution algorithm

1. Verify external policy, signed qualification grant, actual nonproduction
   enrollment and complete read/write scope before seeding, reading or issuing
   credentials. Persist one-time qualification consumption.
2. Run the canonical route producer against real synthetic rows; independently
   reconcile types, rows, bytes and failures. Sign exact observed qualification
   bytes; a supplied PASS projection is not trusted input.
3. Reopen original evidence and signatures during scoped compilation. The
   native authority block and distinct selection formula bind scope and
   qualifications. Complete parent composition checks every ordinary effect too.
4. Build the existing sealed deployment from the exact scoped parent. Issue the
   separate execution grant only after its release/deployment/activation subjects
   are known. This avoids circular hashes.
5. Verify scope/grant and full physical admission, then persist the scoped
   request containing the grant hash. Persist grant bytes and verified signature
   subject in protected control storage. Exact PREPARED/ACTIVE readback includes
   the scoped request identity.
6. Current/provider/init-fetch retain exact existing IDs and mandatory artifact
   verification. Actual workers reacquire the protected ACTIVE occurrence and
   independently verify scope, grant, actual scheduler attempt and all epochs
   before source access or writer issuance.
7. Issued credentials remain confined to the enrolled targets. Every SQL/CH
   mutation path uses the protected gate. Terminalization requires durable
   business outcome and complete closed-gate/quiescence proof.
8. Replay, expiry or revocation blocks new issuance. Unknown outcomes retain
   non-expiring ownership until gate closure and reconciliation. No blind
   ClickHouse EXCHANGE retry, automatic ownership expiry or distributed rollback.

Physical guard IDs remain based on physical domains. Purpose, campaign, principal
and authority family do not create independent locks for the same target.
Qualification can be reread for deterministic compilation of the same intent
inside its valid campaign; execution grants cannot migrate to another activation.

## Implementation scope and rollout

Root owns shared schemas, dispatch, app factories, CLI/provider wiring, workflows,
changelog and navigation. Narrow canonical contracts and shared structural
algorithms avoid duplicating the production runtime. Separate worktrees and
validated disjoint contracts remain mandatory for writers.

An ADR and amended feature acceptance precede implementation. Freeze existing
compatibility fixtures, add strict readers/negative dispatch, repair genuine
qualification prerequisites, then add scoped producers, protected admission and
actual workers. Public scoped factories remain unavailable until the entire
selected backend matrix enforces scope.

The genuine full_refresh candidate and producer/reader stage mismatch still need
correction. Existing CDC evidence requirements and missing route-signing workflow
must be resolved explicitly; no synthetic PASS/N/A is invented to fill gaps.

Rollback closes new scoped admission and drains/reconciles active gates. It does
not rewrite scoped artifacts into v2, remove unknown ledger records, undo SQL or
promote the campaign to production. Production requires a fresh genuinely
qualified production compilation and deployment.

## Required validation and documentation

- Preserve old production/native-v2 fixture bytes, hashes and rejection behavior.
- Prove the full producer→source→parent→deployment→current/provider→worker chain
  with exact scope propagation, unmodified payloads and actual SQL execution.
- Reject missing, changed, refingerprinted or retagged scope at every boundary;
  reject production escalation, wrong target/signer/issuer/root and stale proofs.
- Reject phase swaps, grants for another intent/deployment/activation, duplicate
  running attempts and concurrent budget overrun; retain unknown outcomes.
- Require real closed/reconnect/quiescence tests, vanished/empty snapshots,
  Decimal/numeric/GUID/NULL reconciliation and measured byte limits at N and N+1.
- Keep mandatory nonproduction artifact attestations; diagnostic verification
  reports never substitute for original signatures/evidence.
- Document exact schemas/options, environment/signing enrollment, expiry/recovery,
  migration and separate qualification/execution/production status in a runnable
  synthetic self-service journey once implemented.

Architecture/source design review: PASS. Implementation, signatures and live
campaign for this amendment: SKIP. This specification itself changes no runtime authority.

## Design review and comparison

The independent source review identified the production-only compiler and route
evidence boundaries. The independent architecture review reduced the contract
to six documents while retaining full ancestor verification. The reviewed
decision is recorded in [ADR 0060](../adr/0060-nonproduction-composition-authority.md).

Official comparison inputs checked 2026-09-10: [dlt pipeline isolation](https://dlthub.com/docs/general-usage/pipeline),
[Airbyte scoped roles](https://github.com/airbytehq/airbyte/blob/master/docs/platform/access-management/rbac.md),
and [Sigstore subject/identity verification](https://docs.sigstore.dev/cosign/verifying/verify/).
Adopt explicit isolation and authenticated scope; reject dataset/environment names
as physical permission proof. Other ETL engines are N/A for this narrow authority
amendment. The measurable target is complete real synthetic parent execution and
zero accepted scope/escalation/replay attacks in the defined tests, while old
production/native-v2 fixtures remain unchanged. No superiority claim is made.
