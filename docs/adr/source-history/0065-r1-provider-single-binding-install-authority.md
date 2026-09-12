# Source ADR 0065: R1 provider installation admits one binding pack per target database

> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

## Status

Accepted on 2026-09-05 after fresh architecture and test/certification review
of exact specification commit `5beba5f39`. Acceptance authorizes scoped child
specification and contract work only; no production activation is authorized.

## Context

The implemented Security V2 permission closure validates the complete managed
database permission set as:

```text
shared permission paths + one exact binding permission-path tuple
```

Installing a second binding would make the first binding's permissions appear
as forbidden extras. Supporting multiple binding packs therefore requires a
new security-contract version that closes over the exact set of installed
bindings. The first R1 profile does not have that authority.

The researched migration plan also treated a receipt row as sufficient replay
proof, queried a receipt table even when the whole provider schema was absent,
and copied transaction settings that conflict with the implemented physical
session profile. Those ambiguities could authorize replay over drifted objects
or make a fresh install impossible.

## Decision

### One binding pack is the R1 database boundary

The certified R1 provider admits exactly one binding pack in one target
database. The V2 installation effect key still includes `binding_pack_digest`
to bind replay and prevent a foreign pack from reusing the receipt; it does not
authorize multiple packs.

```text
SHA256(canonical_bytes(
  "dpone-r1-provider-migration-receipt-effect-key-v2\0",
  (
    target_database_identity_digest,
    provider_contract_digest,
    binding_pack_digest,
  ),
))
```

The closed database states are:

| Provider inventory | Requested binding | Replay authority | Disposition |
|---|---|---|---|
| wholly absent | wholly absent | absent-resource arm | full install |
| exact R2 | exact live binding | exact full replay admission | read-only replay |
| exact R2 | absent or foreign | none | block: one-binding profile |
| absent | any live/receipt evidence | none | block: inconsistent state |
| partial, conflicting or unreadable | any | none | block: manual disposition |

Migration V2 owns a typed
`MssqlR1DatabaseBindingInventoryResultV2` containing the complete ordered
database-wide reserved binding namespace: every binding UUID and pack digest,
signer certificate/user, module, signature, permission path, prefix object and
provider-install receipt reference, plus a tuple of unowned/unclassifiable
artifact digests. Entries sort by binding UUID bytes and then canonical leaf
bytes and are duplicate-free. The result is admitted only when it contains zero
bindings for full install or exactly one binding whose UUID, pack digest,
effect key, receipt and complete live inventory equal the request. Any foreign,
second, orphaned, unowned or unclassifiable binding artifact/receipt blocks.

`binding absent` means complete absence of its receipt, reserved-prefix objects,
signer, modules, signatures and permission paths. `exact live binding` proves
the complete current prefix inventory, signer, modules, signatures and
permissions; a receipt alone is insufficient.

Receipt observation has two source arms:

- if the complete shared-inventory observation proves the R2 receipt resource
  absent under the schema lock, return the typed absent-resource result and do
  not execute receipt-table SQL;
- if the receipt resource is exact, execute the keyed typed V2 probe;
- partial, conflicting or unreadable resource state blocks.

`exact full replay admission` exists only when the keyed V2 row, decoded V2
payload, current request/effect key, active R2 descriptor, active binding pack,
stable catalog attestation, exact three-entry certificate replay tuple and
complete live binding inventory satisfy
`MssqlR1ProviderInstallReplayAdmissionV2`.

### Receipt and transaction authority

Full install observes all state before mutation, executes shared and binding
phases, re-attests the complete schema/security/binding closure, then appends
the receipt in one target-local transaction. Replay executes observations and
attestation only; it performs no DDL, grants, signatures or receipt append.

The canonical V2 receipt is inserted in the same target database and explicit
SQL Server transaction as the admitted install effects. `committed_at` is one
catalog-owned SQL Server UTC expression and is excluded from request, effect
and payload identities.

The installer uses a fresh, non-pooled, MARS-disabled connection with driver
autocommit enabled, applies the complete physical session profile and then
dispatches explicit `BEGIN`. No provider statement executes outside that
transaction.

Commit recovery has three outcomes:

| Fresh-session proof | Action |
|---|---|
| committed exact full replay admission | report committed/replayed; never mutate again |
| known not committed, absent receipt and wholly absent provider | a new attempt may repeat the same request/effect key |
| unknown, mismatch, partial or drifted | block for operator recovery; never rerun mutation |

Cancellation before COMMIT dispatch follows rollback proof. Cancellation at or
after dispatch is commit-outcome-unknown. A quarantined/ambiguous session is
discarded and never used as proof.

Concurrent installation is serialized by the descriptor's exact transaction-
owned schema lock followed by its canonical target/binding lock order. A second
candidate observes either exact replay state or a blocking one-binding state.

Registration rotation is external admission evidence. A fresh verified active
registration must reproduce the byte-identical rotation-stable target authority
before the old binding pack and receipt can be admitted.

### Normative supersession and delivery order

This ADR supersedes transaction, receipt-state and replay prose in the
researched migration/renderer/catalog V1 documents. Their active V2 revisions
must use the absent-resource probe arm, the V2 effect key, raw
`SHA256(payload_bytes)`, full replay admission and the exact session profile.
No V1 alias, reinterpretation or dual-read activation is allowed.

Specification, implementation, certification and activation dependencies are
separate:

```text
descriptor R2 + security V2
  -> canonical type/registered-target authority
  -> binding V2 specification and pure contracts
  -> migration V2 specification and pure contracts
  -> renderer V2 algebra
  -> exact SQL-template catalog V2
  -> renderer/catalog implementation
  -> provider aggregate and installer composition
  -> exact vendor-live certification
  -> explicit capability activation
```

Migration and binding never import renderer/catalog types. Binding owns
semantic renderer leaves; migration owns observation, disposition, receipt and
replay admission. Cross-authority validation that would create a reverse
dependency lives in the provider aggregate/application layer.

## Consequences

- R1 is smaller and consistent with implemented Security V2.
- A foreign or second binding fails closed before mutation.
- Fresh install does not query a table that does not yet exist.
- A receipt cannot hide live schema, certificate, permission or binding drift.
- Registration rotation changes attempt evidence but not binding-pack identity.
- Multi-binding requires a future independently approved security/migration
  capability and does not block this profile.
- Renderer/catalog, installer, live SQL and activation remain separately gated.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Admit multiple packs under Security V2 | Complete permission closure would classify other bindings as forbidden extras |
| Treat an exact shared schema as binding-only install authority | Cannot prove global permission closure and expands the first GA |
| Treat receipt equality as replay authority | Does not detect live object/security drift |
| Execute idempotent DDL on every retry | DDL text is not durable replay authority and can hide partial state |
| Put migration policy in renderer | Creates a reverse dependency and lets SQL templates decide durable semantics |

## Required evidence before implementation completion and activation

- model tests cover every state and commit-recovery row;
- two binding-pack digests produce distinct effect keys, while a second pack is
  rejected by the one-binding admission policy;
- absent-resource admission executes no receipt SQL;
- database-global inventory rejects a foreign receipt alone, foreign objects
  without receipt/permissions, two receipts, and requested-exact plus foreign
  partial state;
- replay rejects every receipt, descriptor, pack, attestation, certificate and
  live-prefix splice;
- registration rotation changes attempt evidence and reproduces byte-identical
  stable target/binding authority;
- migration, renderer and catalog session-profile contracts are identical;
- transaction/session behavior remains `UNVERIFIED` until exact SQL Server live
  certification.

## Related material

- [ADR 0056](0056-mssql-same-database-target-authority-v2.md)
- [Provider security authority amendment V2](../../feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md)
- [Provider implementation map](../../developer-postgres-mssql-r1-v3-provider-implementation.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
