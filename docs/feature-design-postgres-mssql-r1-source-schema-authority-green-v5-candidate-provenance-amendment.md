# Feature design: PostgreSQL R1 GREEN-v5 candidate provenance amendment

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Historical source contract context: approvals, source bindings and evidence do not transfer to this migration candidate. Current validation remains separately tracked.

- Status: APPROVED
- Owner: PostgreSQL→MSSQL R1 integrator
- Issue: PostgreSQL→MSSQL Industrial Integration Plan V7 / R1 corrective RED V7
- Target release: R1
- Last verified: 2026-09-07
- Parent: [source-schema authority GREEN-v5](feature-design-postgres-mssql-r1-source-schema-authority-green-v5.md)
- Extends: [rejected-blob closure amendment](feature-design-postgres-mssql-r1-source-schema-authority-green-v5-rejected-blob-amendment.md)

## Executive summary

Candidate `source record 054` passed its frozen
tests but failed four fresh reviews. It is rejected because cleanup,
cancellation, terminal publication ordering, exact query authority and import
direction do not satisfy the approved GREEN-v5 contract. Neither that commit
nor any of its four production ancestors may become an ancestor of the next
candidate.

Ten exact production blobs were independently inspected as bounded,
unaffected contract or bootstrap work. This amendment permits only those exact
path-and-OID pairs to be reused from the rejected lineage. Every other changed
production blob in that lineage remains rejected. This is an explicit
provenance exception, not acceptance of the candidate.

No public behavior, manifest, capability, evidence schema, RED node identity,
certification status or activation status changes here. The approved GREEN-v5
behavior remains the sole implementation authority.

| Axis | State |
|---|---|
| Specification amendment | `APPROVED` |
| Corrective implementation | `absent` |
| Certification and live evidence | `UNVERIFIED` |
| Route activation | `blocked` |
| ADR 0069 | `Proposed` |
| Final candidate evidence | `absent` |

## User problem and journey

| Persona | Goal | Risk | Success signal |
|---|---|---|---|
| Maintainer | Preserve reviewed work without laundering a rejected candidate | Cherry-picking silently bypasses rejected-blob closure | Reuse is restricted to exact reviewed path/OID pairs |
| RED writer | Capture newly discovered violations | Old frozen tests passed the rejected implementation | Corrective RED fails only by assertion with stable node IDs |
| GREEN writer | Correct the defects from a production-clean lineage | Rebuilding all unaffected bytes adds noise and review risk | Only allowlisted bytes are reused; implicated paths are reimplemented |
| Reviewer | Audit lineage and implementation provenance | Commit ancestry alone cannot distinguish reviewed from defective bytes | Executable gate reports no unapproved rejected blob |

The maintainer approves this amendment, issues a corrective RED task, captures
create-only evidence, and then issues a replacement GREEN task. The new GREEN
starts from the production-clean authority chain, may materialize the ten
allowlisted blobs, and must independently implement every implicated path.

## Scope

### In scope

- Reject the five exact candidate production commits listed below.
- Permit reuse of exactly ten path, mode, kind and OID entries.
- Require all other candidate-only production bytes to remain excluded.
- Require a corrective RED task V7 and create-only Evidence V3 RED capture
  before another GREEN task.
- Record the old candidate as rejected in all later task/evidence provenance.

### Non-goals

- Changing approved source-schema behavior or public contracts.
- Accepting any part of the failed candidate by commit ancestry.
- Editing immutable RED V3 evidence.
- Activating PostgreSQL→MSSQL, accepting ADR 0069 or claiming live evidence.
- Waiving focused, broad, fresh-review or rejected-blob gates.

## Exact provenance authority

### Rejected candidate lineage

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded historical statement: source record 055
- Recorded historical statement: source record 056
- Recorded historical statement: source record 057
- Recorded historical statement: source record 058
- Recorded historical statement: source record 054


All five commits must be non-ancestors of the corrective RED and GREEN
candidates.

### Exact reusable blobs

| Blob OID | Exact path | Reason independently admissible |
|---|---|---|
| `source record 059` | `src/dpone/contracts/postgres_mssql_source_schema_authority.py` | Bounded immutable authority value objects |
| `source record 060` | `src/dpone/contracts/postgres_mssql_source_schema_models.py` | Bounded source-schema models |
| `source record 061` | `src/dpone/contracts/postgres_mssql_source_schema_primitives.py` | Closed primitives and validation |
| `source record 062` | `src/dpone/contracts/postgres_mssql_type_authority.py` | Route type-authority contract |
| `source record 063` | `src/dpone/contracts/postgres_source_authority.py` | Connector-neutral source authority |
| `source record 064` | `src/dpone/runtime/bootstrap_hydrator.py` | Compatibility-preserving hydration wiring |
| `source record 065` | `src/dpone/runtime/bootstrap_internal_query_binding.py` | Internal query binding without source I/O |
| `source record 066` | `src/dpone/runtime/bootstrap_state_identity.py` | Bootstrap state identity projection |
| `source record 067` | `src/dpone/runtime/sources/postgres_source_authority.py` | Runtime authority adapter |
| `source record 068` | `src/dpone/runtime/sources/strategies/postgres/postgres_base_strategy.py` | Base strategy compatibility delegation |

The exception is conjunctive: both path and blob OID must match. The same blob
at another path, or different bytes at an allowlisted path, are not admitted by
this amendment.

### Implicated paths

The next GREEN must reimplement, rather than copy, candidate blobs for:

```text
src/dpone/contracts/postgres_mssql_value_admission.py
src/dpone/runtime/bootstrap_postgres_source_authority.py
src/dpone/runtime/connectors/postgres.py
src/dpone/runtime/connectors/postgres_copy_file.py
src/dpone/runtime/postgres_mssql_source_schema_runtime.py
src/dpone/runtime/sources/postgres.py
src/dpone/runtime/sources/postgres_mssql_source_schema_issuer.py
src/dpone/runtime/sources/postgres_mssql_source_schema_observation.py
src/dpone/runtime/sources/postgres_mssql_source_schema_projection.py
src/dpone/runtime/sources/postgres_mssql_source_schema_queries.py
src/dpone/runtime/sources/postgres_verified_relation_observation.py
src/dpone/runtime/sources/postgres_verified_relation_snapshot.py
src/dpone/runtime/sources/postgres_verified_relation_snapshot_cleanup.py
src/dpone/runtime/sources/strategies/postgres/postgres_file_export_mixin.py
src/dpone/runtime/sources/strategies/postgres/postgres_full_extract.py
src/dpone/runtime/sources/strategies/postgres/postgres_prepared_source_boundary.py
src/dpone/runtime/sources/strategies/postgres/postgres_whole_file_export_service.py
```

This closed list includes the P0 defects and the adjacent completion-ownership
cleanup so that dependency direction is corrected coherently.

## Executable admission rule

The corrective task pins the exact rejected commits and allowlist above. Its
candidate gate applies this algorithm:

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

The historical check separated the production base from the approved base, the ordered 18 prior rejected commits, five newly rejected commits, five required previously omitted blob identities, ten approved path/mode/kind/blob entries, and seventeen implicated paths. It required uniqueness of both rejected-commit sequences and checked their compact ASCII JSON-plus-newline, domain-separated SHA-256 preimages against the retained digest constants.

For every rejected commit, the ancestor query against the examined HEAD had to return exactly 1. The check enumerated production paths changed from the original production base and read each path's tree mode, object kind and object identity. The prior set had to contain exactly 205 distinct object identities, including every required omitted identity. The new set had to contain 39 distinct entries and 39 identities. All ten approved entries had to belong to the new set, and none of their object identities could occur in the prior set. The sorted approved-entry tuple payload had its own domain-separated SHA-256 check.

The historical candidate's 27 changed production paths had to equal the disjoint union of the ten approved paths and seventeen implicated paths. The production and approved bases had to have an identical production tree. Finally, every production entry changed from the approved base to the examined HEAD was rejected if its identity belonged to the prior set, or belonged to the new set without matching an exact approved path/mode/kind/identity tuple. The violation list had to remain empty. These are historical provenance obligations, not a reusable blanket approval of any path or blob.

**Original source inputs, in recorded order:**

- Historical `production_base`, entry 1: source record 069.
- Historical `approved_base`, entry 2: source record 070.
- Historical `prior_rejected`, entry 3: source record 071.
- Historical `prior_rejected`, entry 4: source record 011.
- Historical `prior_rejected`, entry 5: source record 013.
- Historical `prior_rejected`, entry 6: source record 015.
- Historical `prior_rejected`, entry 7: source record 072.
- Historical `prior_rejected`, entry 8: source record 073.
- Historical `prior_rejected`, entry 9: source record 074.
- Historical `prior_rejected`, entry 10: source record 075.
- Historical `prior_rejected`, entry 11: source record 017.
- Historical `prior_rejected`, entry 12: source record 019.
- Historical `prior_rejected`, entry 13: source record 024.
- Historical `prior_rejected`, entry 14: source record 076.
- Historical `prior_rejected`, entry 15: source record 077.
- Historical `prior_rejected`, entry 16: source record 078.
- Historical `prior_rejected`, entry 17: source record 079.
- Historical `prior_rejected`, entry 18: source record 080.
- Historical `prior_rejected`, entry 19: source record 081.
- Historical `prior_rejected`, entry 20: source record 082.
- Historical `new_rejected`, entry 21: source record 055.
- Historical `new_rejected`, entry 22: source record 056.
- Historical `new_rejected`, entry 23: source record 057.
- Historical `new_rejected`, entry 24: source record 058.
- Historical `new_rejected`, entry 25: source record 054.
- Historical `required_prior_omitted_oids`, entry 26: source record 083.
- Historical `required_prior_omitted_oids`, entry 27: source record 084.
- Historical `required_prior_omitted_oids`, entry 28: source record 085.
- Historical `required_prior_omitted_oids`, entry 29: source record 086.
- Historical `required_prior_omitted_oids`, entry 30: source record 087.
- Historical `allowed`, entry 31: source record 059.
- Historical `allowed`, entry 32: source record 060.
- Historical `allowed`, entry 33: source record 061.
- Historical `allowed`, entry 34: source record 062.
- Historical `allowed`, entry 35: source record 063.
- Historical `allowed`, entry 36: source record 064.
- Historical `allowed`, entry 37: source record 065.
- Historical `allowed`, entry 38: source record 066.
- Historical `allowed`, entry 39: source record 067.
- Historical `allowed`, entry 40: source record 068.
- Historical `candidate_paths`, entry 41: source record 054.

**Retained historical literal constraints:**

- Historical literal `production_base`: `'source record 069'`.
- Historical literal `approved_base`: `'source record 070'`.
- Historical literal `prior_rejected`: `('source record 071', 'source record 011', 'source record 013', 'source record 015', 'source record 072', 'source record 073', 'source record 074', 'source record 075', 'source record 017', 'source record 019', 'source record 024', 'source record 076', 'source record 077', 'source record 078', 'source record 079', 'source record 080', 'source record 081', 'source record 082')`.
- Historical literal `new_rejected`: `('source record 055', 'source record 056', 'source record 057', 'source record 058', 'source record 054')`.
- Historical literal `required_prior_omitted_oids`: `{'source record 083', 'source record 084', 'source record 085', 'source record 086', 'source record 087'}`.
- Historical literal `allowed`: `{('src/dpone/contracts/postgres_mssql_source_schema_authority.py', '100644', 'blob', 'source record 059'), ('src/dpone/contracts/postgres_mssql_source_schema_models.py', '100644', 'blob', 'source record 060'), ('src/dpone/contracts/postgres_mssql_source_schema_primitives.py', '100644', 'blob', 'source record 061'), ('src/dpone/contracts/postgres_mssql_type_authority.py', '100644', 'blob', 'source record 062'), ('src/dpone/contracts/postgres_source_authority.py', '100644', 'blob', 'source record 063'), ('src/dpone/runtime/bootstrap_hydrator.py', '100644', 'blob', 'source record 064'), ('src/dpone/runtime/bootstrap_internal_query_binding.py', '100644', 'blob', 'source record 065'), ('src/dpone/runtime/bootstrap_state_identity.py', '100644', 'blob', 'source record 066'), ('src/dpone/runtime/sources/postgres_source_authority.py', '100644', 'blob', 'source record 067'), ('src/dpone/runtime/sources/strategies/postgres/postgres_base_strategy.py', '100644', 'blob', 'source record 068')}`.
- Historical literal `implicated_paths`: `{'src/dpone/contracts/postgres_mssql_value_admission.py', 'src/dpone/runtime/bootstrap_postgres_source_authority.py', 'src/dpone/runtime/connectors/postgres.py', 'src/dpone/runtime/connectors/postgres_copy_file.py', 'src/dpone/runtime/postgres_mssql_source_schema_runtime.py', 'src/dpone/runtime/sources/postgres.py', 'src/dpone/runtime/sources/postgres_mssql_source_schema_issuer.py', 'src/dpone/runtime/sources/postgres_mssql_source_schema_observation.py', 'src/dpone/runtime/sources/postgres_mssql_source_schema_projection.py', 'src/dpone/runtime/sources/postgres_mssql_source_schema_queries.py', 'src/dpone/runtime/sources/postgres_verified_relation_observation.py', 'src/dpone/runtime/sources/postgres_verified_relation_snapshot.py', 'src/dpone/runtime/sources/postgres_verified_relation_snapshot_cleanup.py', 'src/dpone/runtime/sources/strategies/postgres/postgres_file_export_mixin.py', 'src/dpone/runtime/sources/strategies/postgres/postgres_full_extract.py', 'src/dpone/runtime/sources/strategies/postgres/postgres_prepared_source_boundary.py', 'src/dpone/runtime/sources/strategies/postgres/postgres_whole_file_export_service.py'}`.
- Historical literal `violations`: `[]`.
- Historical digest/domain constant: `b'\n'`.
- Historical digest/domain constant: `b'dpone-source-schema-green-v5-rejected-commits-v1\x00'`.
- Historical digest/domain constant: `b'dpone-source-schema-green-v5-new-rejected-commits-v1\x00'`.
- Historical digest/domain constant: `b'dpone-source-schema-green-v5-approved-reuse-v1\x00'`.
- Named historical integrity assertion: **prior rejected-source sequence**. The original SHA-256 equality check used domain `b'dpone-source-schema-green-v5-rejected-commits-v1\x00'` followed by `prior_payload` with the framing described above. Its exact expected digest is retained privately with the original assertion; no current candidate equality or certification is claimed.
- Named historical integrity assertion: **new rejected-source sequence**. The original SHA-256 equality check used domain `b'dpone-source-schema-green-v5-new-rejected-commits-v1\x00'` followed by `new_payload` with the framing described above. Its exact expected digest is retained privately with the original assertion; no current candidate equality or certification is claimed.
- Named historical integrity assertion: **approved path/mode/kind/source tuple set**. The original SHA-256 equality check used domain `b'dpone-source-schema-green-v5-approved-reuse-v1\x00'` followed by `allowed_payload` with the framing described above. Its exact expected digest is retained privately with the original assertion; no current candidate equality or certification is claimed.


The replacement GREEN task also asserts that each allowlisted pair exists in
the rejected closure and that no implicated candidate pair is allowlisted.

## Corrective RED and GREEN transitions

```text
approved GREEN-v5 authority
→ RESEARCHED provenance amendment
→ four fresh GO reviews
→ APPROVED status-only amendment
→ corrective RED V7 task authority and deterministic pin
→ create-only Evidence V3 RED capture for corrective RED task V7
→ replacement GREEN task authority and deterministic pin
→ corrective implementation from production-clean lineage
→ focused and broad validation
→ four fresh implementation reviews
→ candidate evidence or rejection
```

The separately reviewed corrective RED task must pin its exact files, existing
function bodies and node IDs while retaining the exact 320-node collection.
The current review target is eight bodies in three files, but exact body
digests, failure counts and artifact identity belong only to that later task
contract; this amendment does not grant test-write authority.

## Failure, rollback and compatibility

- Any unapproved rejected pair aborts candidate admission before evidence.
- A missing allowlisted pair does not require reuse; GREEN may implement fresh
  bytes instead.
- Any newly discovered defect rejects the candidate and requires a new bounded
  RED/task lineage; evidence is never edited in place.
- Rollback discards the unaccepted branch. It never rewrites historical RED
  artifacts or removes the rejected provenance record.
- CLI, Python API, manifest/schema, state/checkpoint and legacy extraction
  compatibility are unchanged by this governance amendment.

## Testing and certification

| Check | Required result |
|---|---|
| Exact commit existence and non-ancestry | PASS |
| Exact ten-entry allowlist inventory | PASS |
| Cumulative prior 205-OID closure | PASS |
| Mutation: rejected non-allowlisted pair | Gate fails |
| Mutation: allowlisted pair at wrong path | Gate fails |
| Untouched production-base blob | Not rejected solely for matching base |
| Corrective RED collection | Exactly 320 nodes |
| Live PostgreSQL/MSSQL | UNVERIFIED until explicitly approved environment exists |

No market comparison is repeated: this amendment changes implementation
provenance only, not connector behavior or measurable product claims. The
current official-source comparison remains inherited from the approved parent.

## Documentation and CJM

This file is maintainer-facing audit history. Public tutorials, source/sink
matrix, manifest reference and first-success CJM remain unchanged because the
route is still activation-blocked. Later task contracts and evidence must link
this amendment using repo-relative paths and exact commits.

## Ownership

| Role | Owned paths | Read-only paths | Forbidden paths |
|---|---|---|---|
| Integrator | This amendment and later task contracts | Specs, Git history, tests and production | Runtime/test mutation before task pin |
| RED writer | Exact three test files in the later task | Specs and production | Production, docs, evidence producer/schema |
| GREEN writer | Exact later task production paths | Specs, RED and evidence | Tests, docs, shared semantic files not assigned |
| Reviewers | None | Exact researched/implemented commits | All writes |

## Approval checklist

- [x] Five rejected commits and ten allowlisted entries are exact.
- [x] The prior 18-ref and new 5-ref ordered identity digests are exact.
- [x] The cumulative prior 205-OID closure rejects every copied OID.
- [x] Wrong-path mutation of a newly rejected OID fails.
- [x] No implicated candidate blob is allowlisted.
- [x] Public behavior and immutable V3 evidence are unchanged.
- [x] Corrective RED precedes replacement GREEN.
- [x] Four fresh reviewers return GO for the exact researched commit.
- [x] Maintainer changes only `Status` and checklist state in the approval commit.
