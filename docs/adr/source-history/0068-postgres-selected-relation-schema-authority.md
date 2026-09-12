# Source ADR 0068: PostgreSQL selected-relation schema authority is source-owned

> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

Purpose and audience: this decision tells dpone architecture maintainers and
provider implementers which layer owns same-snapshot PostgreSQL relation,
column and type authority for the activation-blocked R1 route.

- Status: Accepted
- Date: 2026-09-05
- Decision owners: dpone architecture maintainers

## Context

`SelectedPostgresSourceAuthority` proves the routed PostgreSQL relation,
database and principals. `PostgresMssqlTypePolicyAuthorityV1` proves supported
scalar mappings and can issue a source-column reference from caller-provided
name, ordinal, nullability and an owned source shape. A downstream Binding that
accepts those authorities independently cannot prove that the valid column
references were observed on the valid selected relation.

A Binding-local wrapper over the two values remains self-confirming. Adding
column lists to the deployment registry would duplicate live catalog truth and
make self-service drift handling manual. Observing identity and columns in
different sessions leaves a DDL race.

The existing selected-source digest hashes exact UTF-8 JSON produced with
`ensure_ascii=False`, sorted keys and compact separators. It does not normalize
Unicode. Changing that preimage would silently redefine an accepted authority
and could invalidate identifiers that are byte-distinct in PostgreSQL.

## Decision

dpone will introduce
`PostgresMssqlSelectedRelationSchemaAuthorityV1`, issued only by the PostgreSQL
to-MSSQL schema issuer from a generic branded verified-relation context.
Generic source authority selects the signed relation without I/O. The generic
snapshot issuer begins `REPEATABLE READ READ ONLY`, executes `LOCK TABLE ONLY`
before the first snapshot-establishing query, then proves a granted
`AccessShareLock` for the exact signed relation OID on the same backend and
snapshot. It requires an initially idle physical session, pins the exact
`pg_locks.virtualtransaction` transaction-incarnation witness, and remains the
sole scope owner through extraction and artifact sealing. Terminal cleanup is
idempotent; rollback failure quarantines the physical connection and preserves
any primary error. Route-specific type policy never enters the generic
verifier.

The aggregate embeds:

- the exact existing selected-source JSON preimage and digest, without Unicode
  normalization;
- the approved type policy;
- relation kind, persistence and `relhassubclass=false`;
- every live user column in `pg_attribute.attnum` order, including physical
  attribute number, exact name, type OID/namespace/name/kind, typmod,
  nullability, collation, generated and identity flags;
- the exact source shape and policy-issued column reference derived from that
  observation.

Every column leaf binds the exact selected-source digest plus namespace and
relation OIDs. The aggregate requires those values to equal its selected-source
preimage. This proves structural anti-splice for intact canonical authorities.
Canonical bytes alone do not prove external database provenance: an untrusted
transport must first pass the existing authenticated deployment/sealed-intent
boundary.

R1 admits only ordinary permanent tables without inheritance descendants,
base scalar types, no generated columns, NFC/trim-equal business column names
and `1..1024` complete live user columns. Blocking `relhassubclass=true` is
required as a conservative admission rule, and the certified prepared source
read always renders `FROM ONLY` because `ALTER ... INHERIT` can use a parent
lock compatible with `ACCESS SHARE`. A concurrent attachment therefore cannot
widen the current run. Dropped attributes are excluded but physical
attribute-number gaps remain in the authority. The existing selected-relation
JSON bytes, including any NFD relation identifier, remain unchanged; non-NFC
business columns are an unsupported R1 capability rather than silently
normalized.

Binding V2 accepts only this aggregate for source relation/schema input. It
does not accept selected source authority, type policy or column references as
separate factory arguments. The source verifier owns catalog SQL and issuance;
contracts remain SQL- and adapter-free.

Canonical bytes are a restricted durable contract. Public manifest and status
surfaces expose no physical authority. Route activation remains blocked until
dependent provider work and vendor-live certification are separately complete.

Provisioning issues the authority under a source-only snapshot, builds the
portable Binding pack, closes the source transaction, and only then begins
target Migration. Each non-replay execution reissues authority under its
extraction snapshot and requires exact equality to the installed pack before
business reads. First seal retains exact authority bytes; sealed retries use
retained authority and never re-read source under the same effect identity.
The lock remains held through extraction and artifact sealing.

## Consequences

- Relation/column anti-splice becomes enforceable before target I/O.
- Catalog observation is race-safe under the existing snapshot and lock.
- A stale/ended/replaced snapshot or missing exact relation lock invalidates the
  branded context, including after an out-of-band commit; the assigned
  transaction incarnation prevents a same-session replacement from reproducing
  the authority.
- One scope owns cleanup through extraction/sealing; rollback failure preserves
  the primary error, emits a typed cleanup outcome and quarantines the session.
- Inheritance parents are rejected and prepared R1 reads use `FROM ONLY`, so a
  concurrent inheritance attachment cannot include an unauthorized descendant.
- Binding's API is smaller and its source authority is independently
  reproducible.
- An unsupported live column blocks the whole R1 authority; V1 does not omit or
  coerce it.
- Schema changes produce a new digest and require normal schema-generation
  recovery rather than in-place reinterpretation.
- Existing `verify_snapshot()` and public manifests remain compatible.
- The added direct authority dependencies must be measured. Any increase beyond
  repository quality budgets requires a separate exact-commit exception; this
  ADR does not grant one.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Binding-local digest wrapper | It attests caller-spliced values rather than source observation |
| Registry-authored column list | Duplicates live catalog truth and weakens self-service drift handling |
| `information_schema` observation | Omits PostgreSQL physical type/attribute identity needed by the contract |
| Separate identity and schema sessions | Permits DDL races and cannot prove one snapshot |
| Normalize selected-source JSON to NFC | Redefines an accepted digest and can merge byte-distinct relation identifiers |
| Admit non-NFC business columns in R1 | Contradicts the accepted source-reference and canonical-codec contract |
| Allow selected column subsets in V1 | Introduces projection policy and omission semantics outside this closure task |

## Related material

- [Current source-schema authority GREEN-v5](../../feature-design-postgres-mssql-r1-source-schema-authority-green-v5.md)
- [Historical selected-relation schema authority V1](../../feature-design-postgres-mssql-r1-source-schema-authority-v1.md)
- [Binding V2](../../feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md)
- [R1 correctness V1](../../feature-design-postgres-mssql-r1-correctness-v1.md)
- [PostgreSQL `pg_attribute`](https://www.postgresql.org/docs/16/catalog-pg-attribute.html)
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/16/explicit-locking.html)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
