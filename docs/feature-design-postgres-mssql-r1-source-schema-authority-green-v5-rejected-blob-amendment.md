# Feature design: PostgreSQL R1 GREEN-v5 rejected-blob closure amendment

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Historical source contract context: approvals, source bindings and evidence do not transfer to this migration candidate. Current validation remains separately tracked.

- Status: APPROVED
- Owner: PostgreSQL→MSSQL R1 integrator
- Issue: PostgreSQL→MSSQL Industrial Integration Plan V7 / R1 prerequisite
- Target release: R1
- Last verified: 2026-09-06
- Parent: [source-schema authority GREEN-v5](feature-design-postgres-mssql-r1-source-schema-authority-green-v5.md)

## Executive summary

The approved GREEN-v5 specification correctly rejects production blobs from
eleven rejected candidate heads, but its claim that this set is complete is
too narrow. Four intermediate rejected implementation commits contain five
unique `src/dpone` blob OIDs that no longer exist at those heads. A candidate
could therefore copy those exact bytes and pass the original path-independent
blob gate.

This amendment extends the rejected commit set from eleven terminal heads to
eighteen exact rejected refs and requires a 205-OID closure. It changes no source-schema behavior,
public API, manifest, evidence schema, RED artifact, capability status or live
certification claim. The immutable V3 evidence tuple remains bound to approved
specification commit `source record 088`.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| GREEN implementer | Build without copying rejected production bytes | Intermediate rejected blobs are not closed by the eleven-head gate | Exact eighteen-commit/205-OID gate passes |
| Test certifier | Prove candidate provenance | A path-independent check has five false-negative blobs | All five omitted OIDs are explicitly admitted into the rejection closure |
| Maintainer | Issue one deterministic GREEN task | The approved task algorithm and discovered repository history differ | A reviewed amendment is pinned before task issuance |

The maintainer reviews this amendment, approves its exact content, issues and
pins the GREEN-v5 task, then the implementer works only in the approved
28-path scope. Discovery, execution, observation and recovery behavior for
end users do not change because the route remains activation-blocked.

## Scope

### In scope

- Extend the rejected production commit tuple with seven exact intermediate
  or named rejected refs.
- Require the resulting path-independent closure to contain exactly 205
  unique blob OIDs.
- Pin the five previously omitted OIDs as mandatory members.
- Insert this amendment's researched and approved commits into GREEN task
  provenance.

### Non-goals

- Runtime or test implementation changes.
- Replacing or recapturing RED Evidence V3.
- Changing the approved 28-path ownership block or architecture budgets.
- ADR 0069 acceptance, route activation, Binding V2 or vendor-live evidence.

### Assumptions and constraints

- Production-clean base is
  `source record 069`.
- RED Evidence V3 is exact commit
  `source record 089`.
- The stored RED artifact SHA-256 remains
  `f4faad64e0e4ab0b508dc4555961e9f4cf0c99652a78720317dcace7b3143e51`.
- All eighteen rejected commits exist in repository history and must remain
  non-ancestors of the GREEN candidate.

## Public contract

### CLI, Python API and manifest/schema

No change. No command, import, manifest key, default or activation state is
introduced. `migration_required` remains false.

### Artifacts and evidence

No change. Historical V1/V2 compatibility and V3-only new writes remain as
specified by GREEN-v5. The existing RED artifact is neither edited nor
recaptured. Live PostgreSQL remains `UNVERIFIED`.

### Compatibility and migration

This is a stricter implementation-provenance admission rule. It has no user
migration and does not alter legacy/no-runtime full extraction or prepared
boundary compatibility.

## Detailed algorithm

The GREEN task replaces only the rejected tuple and its closure assertion from
the parent specification with the following executable authority:

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

The historical check bound one original base, an ordered sequence of exactly 18 distinct rejected commits, and five required omitted blob identities. It verified the compact JSON-plus-newline, domain-separated SHA-256 digest of that sequence. Each rejected commit had to fail the ancestor query against the examined HEAD.

For each rejected commit, it enumerated production paths changed from the original base, checked whether the path existed in that commit, and collected the corresponding blob identity only when it existed. The union had to contain exactly 205 identities and include all five required omitted identities. It then inspected existing production blobs changed from the same base to HEAD and required no identity to belong to the rejected union. Missing required identities or any reused rejected blob were failures.

**Original source inputs, in recorded order:**

- Historical `base`, entry 1: source record 069.
- Historical `rejected`, entry 2: source record 071.
- Historical `rejected`, entry 3: source record 011.
- Historical `rejected`, entry 4: source record 013.
- Historical `rejected`, entry 5: source record 015.
- Historical `rejected`, entry 6: source record 072.
- Historical `rejected`, entry 7: source record 073.
- Historical `rejected`, entry 8: source record 074.
- Historical `rejected`, entry 9: source record 075.
- Historical `rejected`, entry 10: source record 017.
- Historical `rejected`, entry 11: source record 019.
- Historical `rejected`, entry 12: source record 024.
- Historical `rejected`, entry 13: source record 076.
- Historical `rejected`, entry 14: source record 077.
- Historical `rejected`, entry 15: source record 078.
- Historical `rejected`, entry 16: source record 079.
- Historical `rejected`, entry 17: source record 080.
- Historical `rejected`, entry 18: source record 081.
- Historical `rejected`, entry 19: source record 082.
- Historical `required_omitted_blobs`, entry 20: source record 083.
- Historical `required_omitted_blobs`, entry 21: source record 084.
- Historical `required_omitted_blobs`, entry 22: source record 085.
- Historical `required_omitted_blobs`, entry 23: source record 086.
- Historical `required_omitted_blobs`, entry 24: source record 087.

**Retained historical literal constraints:**

- Historical literal `base`: `'source record 069'`.
- Historical literal `rejected`: `('source record 071', 'source record 011', 'source record 013', 'source record 015', 'source record 072', 'source record 073', 'source record 074', 'source record 075', 'source record 017', 'source record 019', 'source record 024', 'source record 076', 'source record 077', 'source record 078', 'source record 079', 'source record 080', 'source record 081', 'source record 082')`.
- Historical literal `required_omitted_blobs`: `{'source record 083', 'source record 084', 'source record 085', 'source record 086', 'source record 087'}`.
- Historical literal `reused`: `[]`.
- Historical digest/domain constant: `b'dpone-source-schema-green-v5-rejected-commits-v1\x00'`.
- Named historical integrity assertion: **rejected-source sequence**. The original SHA-256 equality check used domain `b'dpone-source-schema-green-v5-rejected-commits-v1\x00'` followed by `rejected_payload` with the framing described above. Its exact expected digest is retained privately with the original assertion; no current candidate equality or certification is claimed.


The ordered commit digest and explicit OID count are independent inventory
alarms. Adding another rejected ref or production blob requires a new reviewed
amendment rather than silently changing either authority.

### State and failure semantics

```text
missing commit or unexpected closure size
→ task gate fails
→ no GREEN writer starts

rejected commit becomes candidate ancestor
→ task gate fails
→ candidate is discarded

candidate blob belongs to 205-OID closure
→ task gate fails
→ no final evidence is emitted
```

### Alternatives and tradeoffs

| Alternative | Decision | Reason |
|---|---|---|
| Keep only eleven terminal candidate heads | Reject | Five intermediate production blobs remain false negatives |
| Hard-code only the five omitted OIDs | Reject | Loses the reproducible relationship to the source commits and their non-ancestor checks |
| Discover every reachable Git commit dynamically | Reject | Makes the inventory unbounded and dependent on later repository history |
| Exact eighteen-ref tuple plus 205-OID closure | Adopt | Separately pins rejected history identity and path-independent byte provenance |

## Architecture and quality-budget impact

No package, port, adapter, composition root or import edge changes. The exact
28-path ownership digest, eight runtime-to-contract edges and all module/graph
ceilings remain inherited from GREEN-v5.

The parent statement that every new module *targets* less than 300 SLOC remains
a design target, not an additional hard gate. The mandatory scoped limits stay
350 warning SLOC and 400 hard SLOC with zero scoped issues.

No new ADR is required. ADR 0069 remains `Proposed` until exact GREEN code and
final V3 evidence are reviewed.

## Market comparison and measurable differentiation

Market systems are `N/A` for this amendment: it changes only repository-local
provenance closure and introduces no connector capability. The parent
specification's official-source comparison remains authoritative.

```yaml
axis: rejected-production provenance closure
scenario: exact bytes or commit identity copied from an intermediate rejected candidate
baseline: eleven-terminal-head closure with 200 blob OIDs
metric: rejected blob OIDs not detected by the gate
target: 0 across the reviewed eighteen-ref set
procedure: build path-independent OID closure and probe candidate changed blobs
artifact: GREEN task check output bound to exact candidate commit
limitations: repository-history proof; vendor-live remains UNVERIFIED
```

## Security, privacy and operations

The gate reads only Git object identities. It performs no network, credential,
database or endpoint access and writes no artifact. Failure is closed before
implementation acceptance.

## Test and certification plan

| Layer | Scenario | Expected result |
|---|---|---|
| Contract | Eighteen exact refs | Ordered identity digest matches and all are non-ancestors of candidate |
| Inventory | Rejected blob closure | Exactly 205 unique OIDs |
| Mutation | Remove or replace any tuple element | Ordered commit identity assertion fails |
| Mutation | Copy one omitted blob to any path | Candidate admission fails |
| Compatibility | Untouched production-base blob | Not rejected solely for matching base |
| Live | PostgreSQL environment | `UNVERIFIED`; not applicable to this governance correction |

## Documentation plan

The GREEN writer does not edit documentation. After accepted code/final
evidence, the integrator records this amendment identity in ADR 0069 and the
maintainer-facing implementation history. Public tutorials and manifests do
not change.

## Rollout and rollback

The amendment changes the original GREEN-v5 issuance edge as follows:

```text
RED evidence d72a9c6e...
→ RESEARCHED amendment commit
→ APPROVED amendment commit
→ GREEN-v5 task content authority
→ deterministic task-only pin
→ GREEN candidate
```

The task must pin both amendment commits and prove each direct-parent edge.
Its V3 evidence authority continues to use original approved specification
commit `source record 088`.

Rollback discards the unaccepted task/candidate. It never removes a rejected
OID or rewrites the RED artifact.

## Agent execution plan

| Role | Owned paths | Read-only paths | Forbidden paths | Dependency |
|---|---|---|---|---|
| Integrator | This amendment, GREEN task | Production, RED tests during issuance | Evidence/prod mutations before task pin | RED evidence review |
| GREEN writer | Exact 28 production paths | Specs, tests and evidence | Docs, tests, evidence and shared surfaces | Approved amendment and task pin |
| Reviewers | None | Amendment, Git history and gates | All writes | Exact researched commit |

The integrator remains the sole shared-file owner.

## Approval checklist

- [x] The five omitted blob OIDs and their source commits are exact.
- [x] Public behavior, evidence tuple and compatibility remain unchanged.
- [x] Failure semantics are closed before candidate acceptance.
- [x] The task lineage override is explicit.
- [x] Test, ownership, documentation and rollback boundaries are defined.
- [x] Four fresh reviewers return `GO` for the exact researched commit.
- [x] Maintainer changes status to `APPROVED` in a status-only commit.
