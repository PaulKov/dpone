
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider migration contract V2

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Approved by: dpone maintainer implementation authorization, 2026-09-07
- Approval review basis: exact commit
  `source record 138`; fresh architecture,
  test/certification, documentation/UX and release reviews returned `GO`.
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-07
- Successor to: [Migration V1](feature-design-postgres-mssql-r1-v3-provider-migration-contract-v1.md)
- Depends on:
  - physical descriptor R2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - security V2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - type/registered-target authority `source record 002`;
  - Binding V2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - Binding V2 acceptance `PENDING_PUBLIC_COMMIT_BINDING`;
  - ADR 0065 commit `source record 051`,
    content SHA-256 `34ed4726ac24b6e66be916e50b49c99106d27bc9fe5d46dda5ec836a6ceeed32`;
  - Security V2 amendment commit `PENDING_PUBLIC_COMMIT_BINDING`,
    content SHA-256 `937a03c2533f0362f8bfa7e362f16a76d03ffdd1183f68fa5c7737692a9427fc`.

## Outcome and authority boundary

Audience: dpone maintainers implementing and reviewing the internal R1 provider
authority. This is not a user provisioning guide.

Migration V2 is the pure, renderer-independent authority deciding whether one
exact R1 provider plus one exact Binding V2 pack may be installed or replayed
in one SQL Server target database:

```text
active descriptor/security/binding authorities
→ finite observation contract
→ database-global one-binding inventory
→ deterministic disposition
→ admitted semantic statement sequence
→ stable V2 attestation
→ same-database immutable V2 receipt
→ source-free replay or commit-outcome recovery
```

The first R1 profile admits exactly one binding pack. A receipt is necessary
but never sufficient replay evidence. A wholly absent provider uses an explicit
absent-resource arm and never queries a table that does not exist. Partial,
foreign, unreadable or ambiguous state fails closed before mutation.

This specification authorizes pure immutable contracts and algorithms only
after maintainer approval. It does not authorize SQL text, ODBC/catalog I/O,
CLI/manifest changes, live certification or route activation.

## User problem and journey

An operator must be able to retry provider installation after a lost response
without reinstalling over unknown state. A release owner must be able to prove
that a target-local receipt describes the exact live provider and binding, not
merely that some row exists.

```text
resolve exact authorities
→ create immutable request
→ observe under one locked transaction
→ install / replay / block
→ hand semantic plan to a future renderer
→ attest and append receipt in the same transaction
→ recover an unknown commit from a fresh session
```

## Scope and non-goals

In scope:

- V2 request, observation, inventory, decision, receipt and replay models;
- exact one-binding database policy and absent-resource observation;
- renderer-independent statement admission;
- stable V2 attestation and Security V2 replay composition;
- one-session transaction state machine and commit-unknown recovery;
- closed failures, limits, evidence and model tests.

Non-goals:

- multiple binding packs, automatic repair or V1 upgrade;
- SQL rendering/execution, DTC, cross-database receipts or target data DML;
- public API, manifest, CLI or activation changes;
- interpreting V1 bytes as V2 or dual-read activation.

## Normative compatibility

ADR 0065 and Security V2 supersede conflicting Migration/Renderer/Catalog V1
prose. Migration V1 stays historical research and this approved specification
supersedes it. Finite V1 boolean/token result
types, core/error refs, build profile, physical resource/lock refs,
`MssqlR1MigrationTargetRefV1` and
`MssqlR1MigrationInstallRequestAuthorityV1` are the exact closed reuse whitelist;
no other V1 aggregate is admitted. “V1 rejects” always means every V1 aggregate
except these explicitly nested whitelist types. Every aggregate affected by inventory,
receipt, replay, attestation, session or statement admission has a V2 domain.
Other V1 aggregate bytes reject at all V2 boundaries.

## Canonical form and limits

All models are frozen slotted dataclasses using the strict V3 codec. Decode
checks exact domain, field order/count, tags, primitives and limits, reruns all
invariants, requires byte-identical re-encoding, and rejects trailing bytes.

```text
canonical_digest = SHA256(canonical_bytes(EXACT_DOMAIN, ordered_fields))
```

Identifiers are NFC ASCII, 1..64 bytes, `[a-z][a-z0-9_]*`, and exact- and
casefold-unique. Blocker codes match `[a-z][a-z0-9_.]{0,127}`. Digests are 32
bytes; UUIDs non-nil. Times are canonical UTC and excluded from replay identity.
Raw strings never replace enums; booleans are not integers.

```yaml
max_observations: 16
max_result_columns_per_observation: 12
max_token_values_per_column: 32
max_result_vectors_per_observation: 4096
max_complete_decision_vectors: 65536
max_decision_rules: 4096
max_database_bindings: 1
max_unowned_artifact_digests: 256
max_inventory_leaves_per_kind: 4096
max_expanded_statement_admissions: 90112
max_completed_admission_digest_tuple_bytes: 8388608
max_receipt_probe_rows: 2
max_canonical_payload_bytes: 16777216
max_binding_pack_bytes_for_migration_r1: 8388608
max_receipt_payload_bytes: 16777216
max_receipt_probe_result_payload_bytes: 18874368
max_receipt_append_authority_bundle_payload_bytes: 18874368
max_receipt_append_admission_payload_bytes: 20971520
max_admitted_install_envelope_payload_bytes: 39845888
max_prospective_evidence_reservation_payload_bytes: 104857600
max_plan_construction_boundedness_payload_bytes: 155189248
max_migration_cause_context_bundle_payload_bytes: 104857600
max_certificate_replay_mismatch_payload_bytes: 54525952
max_composite_rollback_cause_authority_payload_bytes: 161480704
max_migration_rollback_cause_payload_bytes: 163577856
max_stable_attestation_payload_bytes: 12582912
max_inventory_payload_bytes: 16777216
max_replay_admission_payload_bytes: 67108864
max_evidence_payload_bytes: 201326592
max_outcome_payload_bytes: 198180864
max_migration_request_payload_bytes: 1048576
max_evidence_envelope_metadata_bytes: 2097152
max_evidence_artifact_json_bytes: 16777216
max_uow_transitions: 65536
max_uow_transition_payload_bytes: 8388608
max_decode_depth: 48
max_decode_elements: 1048576
max_utf8_scalar_bytes: 4096
lock_timeout_milliseconds: 30000
statement_timeout_seconds: 60
transaction_timeout_seconds: 300
```

Exceeding a limit is a typed blocker, never truncation or fallback. Migration
R1 explicitly negotiates a narrower Binding V2 subset: a pack above 8 MiB is a
valid Binding artifact but unsupported here and blocks before observation.
SQL binds `DATALENGTH(payload_bytes) <= max_receipt_payload_bytes` before
returning bytes. The adapter reads at most the bounded length, hashes while
reading, and only then invokes the allocation-bounded decoder. Every nested
payload shares the outer depth/element quota; quotas are not reset recursively.
`max_canonical_payload_bytes` is the default model ceiling; the named receipt,
receipt-append admission, install-envelope, attestation, replay and evidence
limits are explicit type-specific overrides. Receipt append is bounded by
`20 MiB`; its dedicated shared authority-bundle ceiling is `18 MiB` so it may
contain a full `16 MiB` receipt plus codec framing. The final install envelope
is bounded by `38 MiB`, the reviewed
exact-codec ceiling for a `16 MiB` pre-receipt plan plus a `20 MiB` append
admission and at most `2 MiB` of envelope/framing overhead. Constructors use
the exact size function and reject before execution when any nested or aggregate
ceiling would be exceeded. The append ABI embeds the receipt-bearing composite
exactly once in its shared authority bundle; semantic leaf and all parameter
values contain only ordinal+digest refs. RED proves the nested inequalities
`receipt composite <=18 MiB`, `append admission <=20 MiB` and
`install envelope <=38 MiB` with exact codec bytes, including limit+1 rejection.
The receipt-probe result has its own `18 MiB` ceiling so a full `16 MiB`
stored receipt plus keyed-result metadata/framing remains decodable for replay
and lost-response recovery. The decided-context reservation has a `100 MiB`
ceiling: five `16 MiB` payloads, one `18 MiB` probe result and `2 MiB` exact
framing. The plan-construction witness has a
`148 MiB` ceiling: seven `16 MiB` context payloads, one `18 MiB` probe result,
one `8 MiB` Binding pack,
one `8 MiB` completed-admission digest tuple and `2 MiB` exact framing. The
closed expansion ceiling is `22 * 4096 = 90,112` admissions; the strict codec's
canonical tuple of that many 32-byte digests, including element framing, must
fit the separately enforced `8 MiB` tuple limit. Composite rollback authorities
share one `100 MiB` context bundle and admit at most a `52 MiB` certificate
mismatch plus `2 MiB` framing, for a `154 MiB` ceiling. A rollback cause has a
`156 MiB` ceiling so it can embed the largest composite plus `2 MiB` framing.
Exact codec tests must prove:

```text
prospective blocked outcome <= 100 + 16 UoW + 8 transitions + 2 framing = 126 MiB
plan blocked outcome        <= 148 + 16 UoW + 8 transitions + 2 framing = 174 MiB
cleanup after any cause     <= 156 cause + 16 rollback + 8 transitions + 2 framing = 182 MiB
largest overflow/cleanup outer envelope <= 182 + 1 request + 2 metadata = 185 MiB < 192 MiB
overall legal outer envelope <= 192 MiB
```

These are type ceilings, not estimates: constructor admission uses the strict
codec size function and RED covers 0/limit/limit+1 for witness, rollback cause,
outcome and outer envelope.
An operation is admitted only when the exact prospective reservation fits:

```text
reservation = canonical_encoded_size(
  evidence domain + actual decided-phase authority payloads
  + maximum eventual-outcome payloads + exact field/tag/length framing)

install before mutation: reserve final install envelope + receipt ref + attestation + UoW set
replay before execution: reserve receipt ref + replay admission + attestation + UoW set
```

`canonical_encoded_size` is the strict V3 codec's allocation-free size function
over the exact field ABI; it includes domain, tuple, union tag and length-prefix
bytes. Reservation occurs exactly once in state `decided`, before any mutation
or post-decision mutation/attestation statement; replay's keyed receipt probe
has already completed before its decision and reservation. No
executing/commit-ready reservation exists. The
reservation embeds the exact decided-phase payloads needed to recompute its
formula, so it never relies on digest-only completed work. RED compares the
formula byte-for-byte with actual encoding at every 0/limit boundary. No
average, JSON size or implementation estimate is used. The final encoded outcome is checked against
`max_outcome_payload_bytes`; request bytes and envelope metadata are checked
against their two disjoint limits, whose sum is exactly the 192 MiB evidence
ceiling. Thus a committed operation can always serialize its reserved evidence;
limits never become a post-commit evidence failure. UoW transition count and
aggregate transition bytes are independently bounded.
The RFC8785 evidence inventory contains identities, digests, node IDs and
statuses only—never base64 operation-evidence payloads—and is independently
bounded by `max_evidence_artifact_json_bytes` before atomic publication.

## Closed database states

```text
ProviderMigrationDispositionV2:
  install | coexist_then_install | read_only_replay_attest | block

ReceiptResourceStateV2:
  absent | exact | partial | conflicting | unreadable

BindingInventoryDispositionV2:
  wholly_absent | exact_requested | foreign_or_second |
  orphaned_or_unowned | partial_or_unreadable
```

| Provider | Requested binding | Receipt authority | Disposition |
|---|---|---|---|
| wholly absent | wholly absent | absent-resource arm | `install` |
| exact R2 | exact requested live binding | exact full replay admission | `read_only_replay_attest` |
| exact R2 | absent, foreign or second | any | `block` |
| absent | any live binding/receipt evidence | any | `block` |
| partial/conflicting/unreadable | any | any | `block` |

`coexist_then_install` remains an unreachable wire literal only because the
approved Security V2 receipt payload admits it. No active R1 decision rule may
produce it. An exact provider with an absent binding blocks under ADR 0065.

Closed blocker codes are:

```text
unsupported_server_build
observation_visibility_failure
receipt_resource_partial
receipt_resource_conflicting
receipt_resource_unreadable
receipt_mismatch
receipt_ambiguous
provider_binding_inconsistent
one_binding_profile_conflict
orphaned_binding_artifact
unclassifiable_binding_artifact
stable_attestation_mismatch
permission_closure_mismatch
certificate_replay_mismatch
registration_rotation_mismatch
boundedness_exceeded
transaction_profile_mismatch
manual_disposition_required
```

## Exact install request and upstream projection

```python
MssqlR1MigrationTargetRefV1(
    identity_contract_version: Literal["dpone-mssql-target-physical-identity-1"],
    target_database_identity_digest: bytes,
)

MssqlR1MigrationInstallRequestAuthorityV1(
    target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    renderer_registry_digest: bytes,
    migration_plan_digest: bytes,
    shared_security_profile_digest: bytes,
    binding_pack_digest: bytes,
)

MssqlR1MigrationInstallRequestV2(
    request_version: Literal["dpone-r1-migration-install-request-2"],
    target_ref: MssqlR1MigrationTargetRefV1,
    physical_schema_descriptor_digest: bytes,
    binding_pack_digest: bytes,
    migration_plan_digest: bytes,
    stable_target_authority_digest: bytes,
)
```

Domain: `dpone-r1-provider-migration-install-request-v2\0`. Every field has a
durable reconstruction equation from the frozen Security V2 receipt payload:

```text
target_ref = receipt.install_request_authority.target_ref
physical_schema_descriptor_digest
  = receipt.install_request_authority.physical_schema_descriptor_digest
binding_pack_digest
  = receipt.install_request_authority.binding_pack_digest
migration_plan_digest
  = receipt.install_request_authority.migration_plan_digest
stable_target_authority_digest
  = derive_stable_target(receipt.stable_catalog_attestation,
                         active_binding_pack)
```

`derive_stable_target` is the existing Binding V2 stable-target validation: the
stored stable attestation, current pack and current verified registration must
reproduce one byte-identical stable target authority. The request factory takes
resolved descriptor, pack, migration plan and stable-target objects; it does not
take digests. Provider/renderer digests remain owned by the nested frozen
Security V2 authority and are finalized only by the future provider aggregate.

The two reused domains are exactly
`dpone-r1-provider-migration-target-ref-v1\0` and
`dpone-r1-provider-migration-install-request-authority-v1\0`, with the field
order above. They are implemented here solely because frozen Security V2 embeds
them; no V1 decision, receipt, evidence or replay aggregate is admitted.

Migration contract is bound by `migration_plan_digest`; physical session by the
descriptor digest. A verified registration rotation must reproduce the same
stable target. The target ref equals the target ref projected by the stable V2
catalog attestation.

```text
installation_effect_key = SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-receipt-effect-key-v2\0",
  (security_request_authority.target_ref.target_database_identity_digest,
   security_request_authority.provider_contract_digest,
   security_request_authority.binding_pack_digest),
))

security_request_digest = SHA256(security_request_authority.canonical_bytes)
migration_request_digest = SHA256(migration_request.canonical_bytes)
```

The target table keeps `security_request_digest`. Replay reconstructs the exact
historical Migration V2 request using the five equations above and compares its
canonical bytes to the current Migration V2 request. No caller-provided
historical outer payload is trusted.

## Exact observation registry and typed result ABI

```python
MssqlR1MigrationContractIdentityV2(
    identity_version: Literal["dpone-r1-migration-contract-identity-2"],
    behavior_policy_version: Literal["dpone-r1-migration-behavior-2"],
    approved_specification_commit: str,
    ordered_upstream_content_sha256: tuple[bytes, bytes],
)
```

Domain is `dpone-r1-migration-contract-identity-v2\0`. The upstream tuple is
exactly ADR 0065 then Security V2 content SHA-256 from the header. Contract
digest is SHA-256 of these four canonical fields. There is deliberately no
second generated behavior-manifest or predicate DSL: the approved Git commit
is the single content authority for every field ABI, equation, table and
invariant in this document.

`approved_specification_commit` is the lowercase 40-hex commit at which a
maintainer changes this document to `APPROVED`. It is supplied as a frozen
implementation constant, must be an ancestor of the implementation commit and
must resolve to a commit whose exact path has status `APPROVED`. The identity
factory reads no Git state; build tooling supplies the reviewed constant and
tests independently verify ancestry/content. There is no self-hash inside the
approved commit and no independently derived payload whose bytes can diverge.

Migration V2 owns the following scalar leaves because Migration V1 was never an
approved or implemented byte authority:

```python
MssqlR1MigrationBooleanResultColumnV1(ordinal: int, name: str)
MssqlR1MigrationTokenResultColumnV1(
    ordinal: int,
    name: str,
    ordered_allowed_tokens: tuple[str, ...],
)
MssqlR1MigrationResultContractV1(
    contract_version: Literal["dpone-r1-migration-result-1"],
    ordered_columns: tuple[BooleanColumn | TokenColumn, ...],
)
MssqlR1MigrationBooleanValueV1(value: bool)
MssqlR1MigrationTokenValueV1(value: str)
MssqlR1MigrationResultVectorV1(
    ordered_values: tuple[BooleanValue | TokenValue, ...],
)
MssqlR1MigrationObservationClassifierV1(
    ordinal: int,
    outcome_id: str,
    match_kind: Literal["match", "no_match"],
    ordered_result_vectors: tuple[MssqlR1MigrationResultVectorV1, ...],
)
```

Their exact domains are respectively
`dpone-r1-provider-migration-{boolean-result-column|token-result-column|result-contract|boolean-value|token-value|result-vector|classifier}-v1\0`.
Columns and classifiers use contiguous one-based ordinals. Values match column
tag/order and token alphabet. Each scalar contract has exactly MATCH and
NO_MATCH classifiers whose nonempty, disjoint vector sets cover the full finite
Cartesian domain exactly once. Token tuples and result vectors are canonical
and duplicate-free. This paragraph is the active byte authority for these
otherwise V1-named leaves.

The active R1 registry has exactly four observations in this order:

| Ordinal / ID | Result ABI | Result alphabet or payload |
|---|---|---|
| 1 `core_inventory_absent` | V1 finite scalar | `match | no_match` for exact descriptor probe `inventory_absent` |
| 2 `provider_inventory` | V2 finite scalar | `wholly_absent | exact_r2 | partial | conflicting | unreadable` |
| 3 `database_binding_inventory` | V2 typed aggregate | exact `MssqlR1DatabaseBindingInventoryResultV2` |
| 4 `receipt_resource` | V2 tagged aggregate | exact absent/exact/blocked receipt-resource arm |

No additional, missing or reordered observation is legal. Ordinal 1 resolves
the sole R2 descriptor migration probe byte-for-byte. Ordinal 2 is the complete
20-table/29-procedure/6-template/shared-security classification. Ordinals 3 and
4 use typed results and never pass through the scalar boolean/token ABI.

```python
MssqlR1MigrationScalarObservationContractV2(
    ordinal: int,
    observation_id: Literal["core_inventory_absent", "provider_inventory"],
    semantic_query_leaf_digest: bytes,
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1 | None,
    result_contract: MssqlR1MigrationResultContractV1,
    ordered_read_resources: tuple[MssqlR1PhysicalResourceRefV1, ...],
    ordered_classifiers: tuple[MssqlR1MigrationObservationClassifierV1, ...],
    visibility_failure_error_ref: MssqlR1CorePhysicalErrorRefV1,
)

MssqlR1MigrationInventoryObservationContractV2(
    ordinal: Literal[3],
    observation_id: Literal["database_binding_inventory"],
    semantic_query_leaf_digest: bytes,
    ordered_read_resources: tuple[MssqlR1PhysicalResourceRefV1, ...],
    result_abi: Literal["dpone-r1-database-binding-inventory-2"],
    accepted_maximum_rows: Literal[4096],
    query_sentinel_rows: Literal[4097],
    ordering: Literal["binding_uuid_then_canonical_leaf"],
    completeness_scope: Literal["complete_database_reserved_binding_namespace"],
)

MssqlR1MigrationReceiptResourceObservationContractV2(
    ordinal: Literal[4],
    observation_id: Literal["receipt_resource"],
    semantic_query_leaf_digest: bytes,
    ordered_read_resources: tuple[MssqlR1PhysicalResourceRefV1, ...],
    result_abi: Literal["dpone-r1-receipt-resource-observation-2"],
    maximum_rows: Literal[2],
)

MssqlR1MigrationObservationContractV2 = (
    MssqlR1MigrationScalarObservationContractV2
    | MssqlR1MigrationInventoryObservationContractV2
    | MssqlR1MigrationReceiptResourceObservationContractV2
)
```

Domains are `dpone-r1-provider-migration-{scalar|inventory|receipt-resource}-observation-contract-v2\0`.
The future Renderer V2 maps semantic leaves to exact SQL. Migration contains no
query bytes or SQL expression. The tagged union is decoded by each arm's exact
domain. Inventory execution is one bounded, non-paginated result under the
schema lock. Up to 4096 rows is accepted; a 4097th sentinel row produces
`boundedness_exceeded` without decoding further rows.

The raw inventory ABI is exact:

```python
MssqlR1BindingInventoryRawRowV2(
    row_ordinal: int,
    leaf_kind: Literal[
        "receipt", "signer_certificate", "signer_user", "module",
        "signature", "permission_path", "prefix_object", "unowned",
        "unclassifiable"
    ],
    target_binding_uuid: UUID | None,
    binding_pack_digest: bytes | None,
    leaf_payload: bytes,
    leaf_digest: bytes,
)
MssqlR1OpaqueInventoryArtifactV2(
    opaque_version: Literal["dpone-r1-opaque-inventory-artifact-2"],
    resource_coordinate_digest: bytes,
    observed_type_code: str,
    bounded_raw_metadata_bytes: bytes,
    raw_metadata_digest: bytes,
)
MssqlR1InventoryCompletenessWitnessV2(
    query_leaf_digest: bytes,
    transaction_session_identity_digest: bytes,
    reserved_namespace_policy_digest: bytes,
    observed_row_count: int,
    sentinel_seen: Literal[False],
    result_exhausted: Literal[True],
)
MssqlR1DatabaseBindingInventoryQueryResultV2(
    ordered_rows: tuple[MssqlR1BindingInventoryRawRowV2, ...],
    completeness_witness: MssqlR1InventoryCompletenessWitnessV2,
)
MssqlR1ReceiptResourceQueryResultV2(
    row_count: Literal[1],
    state: MssqlR1ReceiptResourceStateV2,
    observed_resource_payload: bytes | None,
    observed_resource_digest: bytes | None,
)
```

Domains are `dpone-r1-binding-inventory-{raw-row|completeness-witness|query-result}-v2\0`,
`dpone-r1-opaque-inventory-artifact-v2\0` and
`dpone-r1-receipt-resource-query-result-v2\0`. Row columns occur exactly in
the declared order and are non-NULL except the two ownership columns: both are
required for owned kinds and both NULL for unowned/unclassifiable. For an owned
kind, `leaf_payload` decodes as that kind's exact upstream authority. For
`unowned` or `unclassifiable`, it decodes as the opaque wrapper; the wrapper
binds the catalog coordinate and type code while its bounded raw metadata is
digest checked. Unknown database bytes are never asserted to be a canonical
dpone object. Payload is nonempty, bounded bytes and digest is raw
SHA-256(payload). Rows use
contiguous ordinals and sort by binding UUID (NULL last), leaf kind, leaf digest
and payload bytes. Duplicate leaves reject. Aggregation groups owned rows by
UUID/pack, requires exactly one signer certificate/user, and canonicalizes all
other leaf tuples into the inventory entries. Unowned/unclassifiable rows go
only to their dedicated digest tuples. No row is ignored.

The receipt-resource result is constructed only after exactly one row was read.
Zero or the second sentinel row creates an observation abort and no
`MssqlR1ReceiptResourceQueryResultV2`. The result has payload+digest only for
`exact`; both are NULL for absent/blocked states. Exact payload decodes as the
descriptor resource and re-encodes byte-identically.

Observation starts only after the transaction-owned schema lock. SQL failure
creates an abort over the exact completed prefix and rolls back. Zero/multiple
rows, NULL, wrong type and unknown tokens are typed visibility blockers.

## Database-global one-binding inventory

```python
MssqlR1ObservedBindingInventoryEntryV2(
    target_binding_uuid: UUID,
    binding_pack_digest: bytes,
    installation_effect_key: bytes,
    receipt_ref: MssqlR1MigrationReceiptRefV2 | None,
    signer_certificate_observation_digest: bytes,
    signer_user_identity_digest: bytes,
    ordered_module_digests: tuple[bytes, ...],
    ordered_signature_digests: tuple[bytes, ...],
    ordered_permission_path_digests: tuple[bytes, ...],
    ordered_prefix_object_digests: tuple[bytes, ...],
)

MssqlR1DatabaseBindingInventoryResultV2(
    inventory_version: Literal["dpone-r1-database-binding-inventory-2"],
    target_database_identity_digest: bytes,
    ordered_bindings: tuple[MssqlR1ObservedBindingInventoryEntryV2, ...],
    ordered_unowned_artifact_digests: tuple[bytes, ...],
    ordered_unclassifiable_artifact_digests: tuple[bytes, ...],
)

MssqlR1DatabaseBindingInventoryClassificationV2(
    classification_version: Literal["dpone-r1-database-binding-inventory-classification-2"],
    requested_binding_uuid: UUID,
    requested_binding_pack_digest: bytes,
    requested_installation_effect_key: bytes,
    receipt_resource_observation_digest: bytes,
    inventory_result_payload: bytes,
    inventory_result_digest: bytes,
    disposition: MssqlR1BindingInventoryDispositionV2,
)
```

Domains are `dpone-r1-observed-binding-inventory-entry-v2\0` and
`dpone-r1-database-binding-inventory-{result|classification}-v2\0`. The result
is purely observational and self-validating. Classification is request-bound:
it decodes/re-encodes the embedded result, reproduces its digest, binds the
separate receipt-resource arm digest, and derives disposition from its own
requested UUID/pack/effect fields. Cross-splicing result and receipt-resource
observations therefore rejects.

Entries sort by UUID bytes then canonical leaves; all tuples are canonical and
duplicate-free.

Only zero complete bindings (`wholly_absent`) or one binding exactly matching
the request (`exact_requested`) can be nonblocking. A foreign receipt, signer,
module, signature, permission, prefix object, second binding, unowned or
unclassifiable artifact blocks. Exact receipt equality alone cannot produce
`exact_requested`.

## Receipt-resource arm and keyed probe

```python
MssqlR1ReceiptResourceAbsentV2(
    descriptor_revision: Literal["dpone-mssql-r1-v3-physical-schema-2-r2"],
    shared_inventory_observation_digest: bytes,
)
MssqlR1ReceiptResourceExactV2(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    observed_object_digest: bytes,
)
MssqlR1ReceiptResourceBlockedV2(
    state: Literal["partial", "conflicting", "unreadable"],
    observation_digest: bytes,
)
```

Domains use
`dpone-r1-provider-migration-receipt-resource-{absent|exact|blocked}-v2\0`.

- absent: complete shared inventory proves the R2 table absent; no receipt SQL;
- exact: the exact R2 coordinate exists and enables one keyed probe;
- blocked: no keyed probe, mutation or replay.

The only coordinate is
`dpone_authority.dpone_provider_install_receipt_v3`, the descriptor's immutable
static object with READ+INSERT and without UPDATE/DELETE. Comparison fields are
ordered exactly: `installation_effect_key`, `request_digest`, `payload_bytes`,
`payload_digest`.

Migration V2 owns these previously researched leaf definitions exactly:

```python
MssqlR1MigrationReceiptFieldBindingV1(
    source_field: Literal[
        "effect_key", "request_digest", "payload_bytes", "payload_digest"
    ],
    target_field: MssqlR1ComparisonCoordinateV1,
)
MssqlR1MigrationReceiptStorageOutcomeV1:
    absent | single | ambiguous
MssqlR1MigrationReceiptProbeOutcomeV1:
    absent | exact | mismatch | ambiguous
```

Domains are `dpone-r1-provider-migration-receipt-field-binding-v1\0`,
`dpone-r1-provider-migration-receipt-storage-outcome-v1\0` and
`dpone-r1-provider-migration-receipt-probe-outcome-v1\0`. The four bindings
occur once in the listed order and resolve to the exact four descriptor fields;
no other coordinate or enum value is accepted.

```python
MssqlR1MigrationTargetReceiptBindingV2(
    binding_version: Literal["dpone-r1-migration-target-receipt-binding-2"],
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    payload_codec_version: Literal["dpone-r1-migration-receipt-payload-2"],
    ordered_field_bindings: tuple[MssqlR1MigrationReceiptFieldBindingV1, ...],
    same_database_required: Literal[True],
    same_transaction_required: Literal[True],
)

MssqlR1MigrationReceiptProbeResultV2(
    installation_effect_key: bytes,
    storage_outcome: MssqlR1MigrationReceiptStorageOutcomeV1,
    outcome: MssqlR1MigrationReceiptProbeOutcomeV1,
    row_count: int,
    stored_request_digest: bytes | None,
    stored_payload_bytes: bytes | None,
    stored_payload_digest: bytes | None,
)
```

The probe accepts only the effect key and reads at most two rows:

```text
0 -> absent/absent, stored fields NULL
1 -> single/exact|mismatch
2 -> ambiguous/ambiguous, stored fields NULL
```

Exact requires four equalities: key equality; raw
`stored_payload_digest=SHA256(stored_payload_bytes)`; exact V2 payload decode;
and `stored_request_digest=SHA256(decoded Security V2 install-request-authority
canonical bytes)=current security_request_digest`. It is never compared to the
outer `migration_request_digest`. Partial NULLs, V1/unknown/trailing bytes or coherent foreign
content are mismatch, never compatibility.

## Decision and semantic statement admission

The executable decision input is:

```python
MssqlR1MigrationDecisionVectorV2(
    core_inventory: Literal["match", "no_match"],
    provider_inventory: Literal[
        "wholly_absent", "exact_r2", "partial", "conflicting", "unreadable"
    ],
    binding_inventory: Literal[
        "wholly_absent", "exact_requested", "foreign_or_second",
        "orphaned_or_unowned", "partial_or_unreadable"
    ],
    receipt_resource: Literal[
        "absent", "exact", "partial", "conflicting", "unreadable"
    ],
    receipt_probe: Literal[
        "not_applicable", "absent", "exact", "mismatch", "ambiguous"
    ],
)
```

Domain: `dpone-r1-provider-migration-decision-vector-v2\0`. Probe is
`not_applicable` iff receipt resource is not exact; an exact resource requires
one of the other four values. Therefore the exact valid universe is
`2 × 5 × 5 × (4 + 4) = 400` vectors. Plan construction enumerates all 400.

There are exactly two nonblocking rows:

```text
install:
  match, wholly_absent, wholly_absent, absent, not_applicable

read_only_replay_attest:
  no_match, exact_r2, exact_requested, exact, exact
```

All other 398 vectors block. `coexist_then_install` has no row. Blocker
precedence is exact: observation unreadable/visibility; partial/conflicting
provider; partial/conflicting/unreadable receipt resource; partial/unreadable
binding; orphan/unowned; foreign/second; receipt mismatch/ambiguous; inconsistent
cross-state; default manual disposition. Within one class, observation ordinal
then canonical leaf order applies.

The classifier evaluates these predicates in exact order and returns the first
matching single blocker code:

```text
provider=unreadable -> observation_visibility_failure
receipt_resource=unreadable -> receipt_resource_unreadable
provider IN (partial, conflicting)
  -> provider_binding_inconsistent
receipt_resource=partial -> receipt_resource_partial
receipt_resource=conflicting -> receipt_resource_conflicting
binding=partial_or_unreadable -> unclassifiable_binding_artifact
binding=orphaned_or_unowned -> orphaned_binding_artifact
binding=foreign_or_second -> one_binding_profile_conflict
receipt_probe=mismatch -> receipt_mismatch
receipt_probe=ambiguous -> receipt_ambiguous
every remaining vector other than the two exact rules
  -> provider_binding_inconsistent
```

Thus all 398 blocking vectors have one reproducible blocker. A visibility
failure outside a completed decision vector retains its exact observation
failure code in the abort evidence instead.

Every closed blocker is owned by exactly one producer class:

| Blocker | Exact producer condition |
|---|---|
| `unsupported_server_build` | certified build-profile admission fails |
| `observation_visibility_failure` | provider observation is unreadable or a scalar observation cannot complete |
| `receipt_resource_partial` | partial receipt-resource state |
| `receipt_resource_conflicting` | conflicting receipt-resource state |
| `receipt_resource_unreadable` | unreadable receipt-resource state |
| `receipt_mismatch` | mismatching keyed-probe state |
| `receipt_ambiguous` | ambiguous keyed-probe state |
| `provider_binding_inconsistent` | provider partial/conflicting or classifier default |
| `one_binding_profile_conflict` | foreign/second binding |
| `orphaned_binding_artifact` | orphaned/unowned inventory |
| `unclassifiable_binding_artifact` | partial/unreadable or opaque unclassifiable inventory |
| `permission_closure_mismatch` | missing or forbidden permission closure is nonempty |
| `certificate_replay_mismatch` | any ordered certificate replay entry differs |
| `stable_attestation_mismatch` | a remaining non-permission attestation result differs after the two specific checks pass |
| `registration_rotation_mismatch` | verified rotation does not reproduce stable target authority |
| `boundedness_exceeded` | any named count/byte/depth/element limit or sentinel is exceeded |
| `transaction_profile_mismatch` | session/transaction/lock authority check differs |
| `manual_disposition_required` | fresh recovery proof is insufficient or ambiguous |

The apparent slash-separated names in the table are individual enum literals,
not a wire value. RED includes at least one valid producer fixture for every
literal and rejects any literal produced outside its owning condition.
Specific precedence is certificate replay, then permission closure, then
generic stable attestation. The generic predicate explicitly excludes the
permission result and certificate replay entries, so producer conditions are
disjoint.

```python
MssqlR1MigrationDecisionRuleV2(
    ordinal: Literal[1, 2],
    rule_id: Literal["install_wholly_absent", "replay_exact_requested"],
    required_vector: MssqlR1MigrationDecisionVectorV2,
    disposition: Literal["install", "read_only_replay_attest"],
)

MssqlR1MigrationDecisionV2(
    decision_version: Literal["dpone-r1-migration-decision-2"],
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    decision_vector: MssqlR1MigrationDecisionVectorV2,
    selected_rule_digest: bytes | None,
    disposition: MssqlR1ProviderMigrationDispositionV2,
    ordered_blocker_codes: tuple[str, ...],
)
```

Domains are `dpone-r1-provider-migration-{decision-rule|decision}-v2\0`.
Nonblocking requires one exact rule digest and no blockers. Block requires no
rule digest and a nonempty precedence-derived blocker tuple.

```python
MssqlR1MigrationDecisionPlanV2(
    plan_version: Literal["dpone-r1-migration-decision-plan-2"],
    migration_contract_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    binding_pack_digest: bytes,
    certified_server_build_profile: MssqlR1CertifiedServerBuildProfileV1,
    ordered_observations: tuple[MssqlR1MigrationObservationContractV2, ...],
    ordered_decision_rules: tuple[MssqlR1MigrationDecisionRuleV2, ...],
    default_blocker_code: str,
    attempt_policy_digest: bytes,
    target_receipt_binding: MssqlR1MigrationTargetReceiptBindingV2,
    zero_mutation_before_decision: Literal[True],
)
```

Domain: `dpone-r1-provider-migration-decision-plan-v2\0`. Construction
exhaustively enumerates the exact 400-vector domain, proving both rules
reachable, no overlap, blocker dominance, no weakening of core BLOCK, exactly
two nonblocking vectors and 398 blocking vectors.

The inventory has exactly 22 semantic rule kinds in this dependency-safe phase order:

```text
01 observe: migration_observation
02 receipt precheck: provider_receipt_probe
03 shared schemas: create_schema
04 shared tables: create_table, create_index, create_trigger,
                  add_table_extended_property
05 shared security prepare: shared_create_certificate,
                            shared_create_certificate_user,
06 shared modules: create_procedure, add_procedure_extended_property
07 shared permissions: shared_grant_permission
08 shared security sign: shared_sign_module
09 shared security remove: shared_remove_private_key
10 binding security prepare: binding_create_certificate,
                             binding_create_certificate_user
11 binding modules: binding_module_create
12 binding permissions: binding_permission_grant
13 binding security sign: binding_sign_module
14 binding security remove: binding_remove_private_key
15 attest: attestation_query
16 receipt append: provider_receipt_append
```

Rule-kind names have `_v2` appended in canonical bytes. This is a closed kind
inventory, not 22 physical statements: each kind expands once per exact owning
descriptor, Security V2 or Binding V2 leaf. Expansion order is `(phase rank,
rule-kind rank within the listed phase, owner rank shared-before-binding,
owner canonical coordinate, owner leaf
ordinal)`. Observation expands exactly four times; attestation exactly seven;
receipt probe/append at most once each. Every descriptor table/index/trigger/
property/procedure, shared certificate/user/permission/signature/key-removal and
every Binding V2 module/certificate/user/permission/signature/key-removal effect
is consumed exactly once for install. Omission, extra, duplicate or reordering
blocks plan construction.

The canonical statement-inventory payload contains one
`MssqlR1MigrationExpansionRuleV2` for each kind with exact fields
`(ordinal, rule_kind, owner_path, cardinality_policy, resource_projection,
parameter_projection)`. Its closed registry is:

```python
MssqlR1MigrationExpansionRuleV2(
    ordinal: int,
    rule_kind: str,
    owner_path: str,
    cardinality_policy: str,
    resource_projection: str,
    parameter_projection: str,
)
MssqlR1MigrationCompositeOwnerV2(
    owner_version: Literal["dpone-r1-migration-composite-owner-2"],
    owner_kind: Literal["schema", "parent_leaf", "signature_pair", "receipt_append"],
    parent_authority_payload: bytes | None,
    leaf_authority_payload: bytes,
)
```

Their domains are `dpone-r1-migration-expansion-rule-v2\0` and
`dpone-r1-migration-composite-owner-v2\0`. The six registry strings are closed
tokens listed byte-for-byte in the table, not arbitrary runtime paths.

| Kind | Exact owner path | Cardinality/order | Resource/parameter projection |
|---|---|---|---|
| `migration_observation` | decision plan `ordered_observations` | exactly 4 / tuple | observation resources / owner |
| `provider_receipt_probe` | `MssqlR1ReceiptProbeRequestV2` | 0 absent, 1 exact / singleton | receipt binding / probe request |
| `create_schema` | Migration-owned `(descriptor.digest, expected_schema_contract.{authority_schema_name,stage_schema_name})` schema owner | exactly 2 / UTF-8 schema name | matching schema declaration / owner |
| `create_table` | descriptor `ordered_tables[*]` | every item / tuple | table `lock_resource` / owner |
| `create_index` | descriptor `ordered_tables[*].portable_object.ordered_indexes[*]` | every nested item / table then tuple | parent table declaration / parent+item owner |
| `create_trigger` | descriptor `ordered_tables[*].portable_object.ordered_triggers[*]` | every nested item / table then tuple | parent table declaration / parent+item owner |
| `add_table_extended_property` | descriptor tables, each `portable_object.ordered_extended_properties[*]` | every nested item / owner then tuple | owning table declaration / parent+item owner |
| `create_procedure` | descriptor `ordered_procedures[*]` | every item / tuple | procedure declaration / owner |
| `add_procedure_extended_property` | descriptor procedures, each `portable_object.ordered_extended_properties[*]` | every nested item / owner then tuple | owning procedure declaration / parent+item owner |
| `shared_create_certificate` | shared profile `ordered_shared_signers[*]` | every signer / tuple | signer certificate coordinate / signer |
| `shared_create_certificate_user` | shared profile `ordered_shared_signers[*]` | every signer / tuple | signer user coordinate / signer |
| `shared_grant_permission` | descriptor `expected_schema_contract.ordered_permission_rules[*]` resolved by shared profile closure | every shared rule / canonical bytes | permission target declaration / resolved path |
| `shared_sign_module` | each signer `ordered_module_refs[*]` paired by its exact signature template | every declared pair / signer then tuple | module+certificate declarations / pair owner |
| `shared_remove_private_key` | shared profile `ordered_shared_signers[*]` after all signatures | every signer / tuple | certificate declaration / signer lifecycle owner |
| `binding_create_certificate` | pack `portable_identity.signer_identity` | exactly 1 | binding certificate coordinate / signer identity |
| `binding_create_certificate_user` | pack `portable_identity.signer_identity` | exactly 1 | binding user coordinate / signer identity |
| `binding_module_create` | pack `portable_identity.ordered_modules[*]` | every module / tuple | instantiated module coordinate / module |
| `binding_permission_grant` | pack `portable_identity.permission_contract.ordered_permission_paths[*]` | every path / canonical bytes | permission target / path |
| `binding_sign_module` | pack `portable_identity.permission_contract.ordered_signature_intents[*]` joined one-to-one to module | every intent / ordinal | module+certificate / intent+module owner |
| `binding_remove_private_key` | pack signer lifecycle after all binding signatures | exactly 1 | certificate declaration / signer lifecycle owner |
| `attestation_query` | closed seven-member attestation registry | exactly 7 / registry order | declared observation resources / owner |
| `provider_receipt_append` | final receipt payload plus target receipt binding | exactly 1, envelope only | receipt resource / five-value append authority |

The schema composite's leaf payload has domain
`dpone-r1-migration-schema-install-owner-v2\0` and fields
`(descriptor_digest, schema_ordinal, schema_name)`; ordinals are authority then
stage. A parent+item/signature/receipt owner uses the composite wrapper carrying
the exact upstream payloads, never an untyped pair. Every path is evaluated over the exact
decoded descriptor/profile/pack named in the install request. Expected counts,
owner digests and resource declarations are recomputed; a missing join,
unresolved resource, duplicate or extra leaf blocks construction. This table,
including path and projection literals, is part of contract identity.

Migration owns this exact ABI for every expanded leaf:

```python
MssqlR1MigrationSemanticLeafV2(
    leaf_version: Literal["dpone-r1-migration-semantic-leaf-2"],
    ordinal: int,
    leaf_id: str,
    rule_kind: str,
    phase_rank: int,
    operation: Literal["read", "ddl", "grant", "signature", "remove_key", "insert"],
    ordered_resource_refs: tuple[MssqlR1PhysicalResourceRefV1, ...],
    owner_authority_ordinal: int,
    owner_authority_digest: bytes,
)
MssqlR1MigrationParameterValueV2(
    ordinal: int,
    name: str,
    scalar_kind: Literal["digest", "canonical_payload", "utc_database_expression"],
    source_authority_ordinal: int,
    source_authority_digest: bytes,
)
MssqlR1MigrationParameterAuthorityV2(
    authority_version: Literal["dpone-r1-migration-parameter-authority-2"],
    leaf_id: str,
    semantic_leaf_digest: bytes,
    owner_authority_digest: bytes,
    ordered_values: tuple[MssqlR1MigrationParameterValueV2, ...],
)
MssqlR1MigrationAdmissionAuthorityBundleV2(
    bundle_version: Literal["dpone-r1-migration-admission-authority-bundle-2"],
    ordered_authority_payloads: tuple[bytes, ...],
    ordered_authority_digests: tuple[bytes, ...],
)
MssqlR1ReceiptProbeRequestV2(
    request_version: Literal["dpone-r1-receipt-probe-request-2"],
    receipt_binding: MssqlR1MigrationTargetReceiptBindingV2,
    installation_effect_key: bytes,
)
```

Domains are `dpone-r1-migration-{semantic-leaf|parameter-value|parameter-authority|admission-authority-bundle}-v2\0`
and `dpone-r1-receipt-probe-request-v2\0`.
Factories are private and keyed by the closed 22-kind inventory. They derive
leaf ID, ordinal, rule kind, phase, operation and resources from exact
observation, descriptor, Security V2, Binding V2 and receipt-binding objects.
The admission authority bundle is the sole physical embedding of owner/source
payloads. Its ordinals are contiguous; every digest is raw SHA-256 of the
adjacent payload. Leaf and parameter refs select those ordinals and repeat the
exact digest. Payloads are decoded against the rule's expected concrete
authority, and every bundle member must be referenced at least once; duplicate,
unused or foreign members reject.
`utc_database_expression` occurs only for receipt append's `committed_at` and
contains the renderer-independent expression-policy authority, not SQL bytes.
No public constructor accepts an unbound digest or arbitrary leaf ID.
`leaf_id` is `l_` plus lowercase RFC 4648 base32 without padding of SHA-256 of canonical
`(rule_kind, owner_authority_digest, owner canonical coordinate, owner leaf
ordinal)` under domain `dpone-r1-migration-leaf-id-v2\0`. Parameter authority
must repeat the exact semantic-leaf and owner digests. Admission rejects unless
leaf ID, bundle ordinals and all digests agree byte-for-byte, preventing
same-kind owners from swapping parameters without duplicating authority bytes.

The parameter ABI is also closed. All owner-expanded kinds except receipt
probe and append carry exactly one value
`(1, "owner_authority", "canonical_payload")`. Receipt probe carries exactly
one `(1, "probe_request", "canonical_payload")` whose source is the exact
`MssqlR1ReceiptProbeRequestV2`; this binds both resource projection and
`installation_effect_key` without hidden renderer context.
`provider_receipt_append_v2` has exactly two bundled authorities: ordinal 1 is
the composite final receipt payload plus receipt binding owner, embedded once;
ordinal 2 is the UTC expression-policy authority. It carries exactly five
values in order:

```text
1 installation_effect_key digest
2 security_request_digest digest
3 receipt_payload canonical_payload
4 receipt_payload_digest digest
5 committed_at utc_database_expression
```

Values 1–4 reference bundle ordinal 1 and derive their named projections from
that decoded composite; value 5 references ordinal 2. The semantic leaf owner
also references ordinal 1. No value or leaf embeds either payload again.

No rule kind admits an additional, missing, reordered or differently typed
parameter. The canonical owner payload contains all structured values needed
by the future renderer; Migration does not flatten or invent SQL scalars.

```python
MssqlR1MigrationStatementAdmissionV2(
    ordinal: int,
    phase: Literal["observe", "receipt_probe", "mutate", "attest", "receipt_append"],
    semantic_leaf_digest: bytes,
    parameter_authority_digest: bytes,
    semantic_leaf_payload: bytes,
    parameter_authority_payload: bytes,
    authority_bundle_payload: bytes,
    authority_bundle_digest: bytes,
)
MssqlR1AdmittedInstallPlanV2(
    plan_version: Literal["dpone-r1-admitted-install-plan-2"],
    migration_request_digest: bytes,
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    decision_digest: bytes,
    installer_attempt_uuid: UUID,
    target_ref: MssqlR1MigrationTargetRefV1,
    disposition: MssqlR1ProviderMigrationDispositionV2,
    completed_prefix_count: int,
    ordered_statement_admissions: tuple[MssqlR1MigrationStatementAdmissionV2, ...],
    target_receipt_binding_digest: bytes,
)
MssqlR1AdmittedReplayPlanV2(
    plan_version: Literal["dpone-r1-admitted-replay-plan-2"],
    migration_request_digest: bytes,
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    decision_digest: bytes,
    installer_attempt_uuid: UUID,
    target_ref: MssqlR1MigrationTargetRefV1,
    disposition: Literal["read_only_replay_attest"],
    completed_prefix_count: int,
    ordered_statement_admissions: tuple[MssqlR1MigrationStatementAdmissionV2, ...],
    target_receipt_binding_digest: bytes,
)
MssqlR1AdmittedInstallEnvelopeV2(
    envelope_version: Literal["dpone-r1-admitted-install-envelope-2"],
    pre_receipt_plan_payload: bytes,
    pre_receipt_plan_digest: bytes,
    pre_receipt_rendered_bundle_digest: bytes,
    receipt_payload_digest: bytes,
    receipt_append_admission: MssqlR1MigrationStatementAdmissionV2,
)
```

Every explicitly adjacent digest is SHA-256 of that canonical payload.
Non-adjacent plan/envelope digests are recomputed from their named active
authority or the decoded admission bundle. Factories decode and resolve every
reference against the active owner; raw digest construction is private.

Domains are `dpone-r1-provider-migration-statement-admission-v2\0`,
`dpone-r1-admitted-{install|replay}-plan-v2\0` and
`dpone-r1-admitted-install-envelope-v2\0`. The plan tuple is contiguous and in
execution order; runtime never sorts or merges it. The install plan is
explicitly the pre-receipt plan and never contains
`provider_receipt_append_v2`. Its disposition is exactly `install`; the frozen
but unreachable `coexist_then_install` literal is rejected at Migration V2 plan
construction. The replay plan has exactly `read_only_replay_attest`, one keyed
probe and no mutation or append admission. A decision, execution, recovery or
evidence payload selects the exact plan type from its disposition; an untyped
or cross-disposition `admitted_plan_payload` rejects.

Both plans are attempt-wide immutable admission ledgers built immediately
after decision. Their ordered tuple includes the already executed pre-decision
admissions reconstructed from the exact observation/probe authorities followed
by the post-decision executable suffix. `completed_prefix_count` is derived,
never caller-selected: install equals all phase-01 observation admissions;
replay equals phase-01 observations plus the sole phase-02 keyed probe. Runtime
starts strictly at the first ordinal after that prefix and never re-executes a
prefix statement. A prefix result/digest that cannot reconstruct its admission
byte-identically blocks plan construction.

```text
install plan:    observe -> no keyed probe -> mutate -> attest
install envelope: exact install plan -> receipt append
replay:          observe -> keyed probe -> attest
block:           observe -> rollback; no plan
wholly absent:   observe -> absent-resource arm -> mutate -> attest
                  # no receipt_probe statement
```

Install ledger contains all expanded phase 01 and 03–15 leaves, excluding phase
02 and 16; only phases 03–15 form its executable suffix. The final envelope
adds the single phase-16 append admission. Replay ledger contains phase 01, one
phase-02 probe and all seven phase-15 leaves; only phase 15 forms its executable
suffix.
No other subset/order is legal. Renderer V2 must prove a bijection from semantic leaves/parameters to exact
registry statements. Migration never imports renderer/catalog types;
cross-validation belongs to the future provider aggregate.

## Stable attestation, receipt and replay

Post-install attestation consumes exactly seven results in order: schema,
table, module, V2 certificate, V2 permission, signature, binding-prefix. It
builds `MssqlR1StableCatalogAttestationV2`, validates it against active R2
descriptor and Binding V2, and proves empty missing/forbidden permission closure.

```python
MssqlR1MigrationInstallReceiptPayloadV2(
    receipt_version: Literal["dpone-r1-migration-install-receipt-2"],
    installation_effect_key: bytes,
    install_request_authority: MssqlR1MigrationInstallRequestAuthorityV1,
    observation_set_digest: bytes,
    decision_digest: bytes,
    admitted_install_plan_digest: bytes,
    rendered_bundle_digest: bytes,
    stable_catalog_attestation_payload: bytes,
    stable_catalog_attestation_digest: bytes,
    original_disposition: Literal["install", "coexist_then_install"],
)
MssqlR1MigrationReceiptRefV2(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    installation_effect_key: bytes,
    request_digest: bytes,
    receipt_payload_digest: bytes,
)
```

These use the approved Security V2 domains. Storage is exact:

```text
payload_bytes = payload.canonical_bytes
payload_digest = SHA256(payload_bytes)
request_digest = SHA256(install_request_authority.canonical_bytes)
```

`committed_at` is a SQL Server UTC expression outside identity. Append occurs
after attestation in the same database, session and explicit transaction as all
install effects. UPDATE, DELETE, TTL and GC are forbidden. New payloads can be
created only by `create_for_install(...)`, which resolves/validates active
descriptor and pack; decoding alone grants nothing.

Construction is acyclic and exact:

```text
build admitted pre-receipt plan (no append) -> admitted_install_plan_digest
render only that plan -> rendered_bundle_digest
build receipt from those two frozen digests
build the two-member append authority bundle with the receipt-bearing composite
build the sole receipt-append leaf/parameters as refs to that bundle
build final admitted install envelope
```

The two frozen Security V2 field names mean the pre-receipt plan and its
pre-receipt rendered bundle in Migration V2. The final envelope digest is never embedded in the receipt or in its own append
parameters. The renderer's final execution bundle may bind the envelope digest,
but the receipt stores only the pre-receipt rendered digest. Any renderer that
includes append bytes in that stored digest rejects aggregate validation.
The envelope's receipt digest is verified through the exact path
`append parameter-value ref -> admission authority bundle ordinal 1 ->
receipt-bearing composite -> nested receipt payload`. It must equal raw
SHA-256 of that nested payload; neither parameter authority nor leaf duplicates
the receipt bytes.

Migration does not widen the frozen six-field
`MssqlR1ProviderInstallReplayAdmissionV2`. It introduces a downstream wrapper:

```python
MssqlR1MigrationReplayAdmissionV2(
    admission_version: Literal["dpone-r1-migration-replay-admission-2"],
    migration_request_payload: bytes,
    migration_request_digest: bytes,
    security_replay_admission: MssqlR1ProviderInstallReplayAdmissionV2,
    current_stable_catalog_attestation: MssqlR1StableCatalogAttestationV2,
    inventory_classification: MssqlR1DatabaseBindingInventoryClassificationV2,
    receipt_resource_observation_payload: bytes,
    receipt_resource_observation_digest: bytes,
    active_stable_target_authority_digest: bytes,
)
```

Domain: `dpone-r1-migration-replay-admission-v2\0`. It reconstructs the
historical Migration V2 request from the stored Security V2 receipt, compares
it to the current request, and requires the current receipt-resource observation
to decode as the exact arm. Its raw SHA-256 must equal both the adjacent digest
and the inventory classification's bound resource-observation digest. Full
replay requires this wrapper,
whose nested Security V2 object contains:

- fresh exact keyed V2 probe and decoded V2 payload;
- exact current request/effect authority;
- active R2 descriptor and Binding V2 pack;
- exact three certificate replay entries in order: shared attestor, shared stage
  owner, binding signer instance;

Additionally:

```text
decoded receipt original_disposition = install
inventory=exact_requested and contains exactly one binding
binding UUID/pack/effect/receipt equal active request and fresh probe
all live leaves equal current stable attestation
unowned/unclassifiable tuples are empty
```

The frozen Security V2 decoder continues to accept
`coexist_then_install`, but Migration V2 replay rejects that otherwise-canonical
receipt because no active one-binding decision rule can produce it.

Registration rotation is legal only when fresh verified registration reproduces
byte-identical rotation-stable target authority.

## Session, UoW and recovery

```python
MssqlR1MigrationAttemptPolicyV2(
    policy_version: Literal["dpone-r1-migration-attempt-policy-2"],
    physical_session_profile_digest: bytes,
    engine_profile_digest: bytes,
    fresh_connection_required: Literal[True],
    explicit_begin_required: Literal[True],
    mars_disabled_required: Literal[True],
    pooling_disabled_required: Literal[True],
    session_context_required: Literal[True],
    transaction_count_after_begin: Literal[1],
    xact_state_after_begin: Literal[1],
    delayed_durability_required: Literal["DISABLED"],
    lock_timeout_milliseconds: Literal[30000],
    statement_timeout_seconds: Literal[60],
    transaction_timeout_seconds: Literal[300],
    install_lock_authority_digest: bytes,
)

MssqlR1MigrationInstallLockAuthorityV2(
    authority_version: Literal["dpone-r1-migration-install-lock-authority-2"],
    descriptor_digest: bytes,
    binding_pack_digest: bytes,
    ordered_requirements: tuple[MssqlR1MigrationInstallLockRequirementV2, ...],
)
MssqlR1MigrationInstallLockRequirementV2(
    ordinal: int,
    lock_kind: MssqlR1LockKindV1,
    lock_subrank: int,
    resource: MssqlR1PhysicalResourceRefV1,
    cardinality: MssqlR1LockCardinalityV1,
)
```

Domains are `dpone-r1-provider-migration-attempt-policy-v2\0` and
`dpone-r1-migration-install-lock-authority-v2\0`. The policy does not duplicate
session semantics. The two profile digests are re-derived from the exact active
R2 descriptor. The physical session bytes require SERIALIZABLE and all ANSI,
ARITHABORT, QUOTED_IDENTIFIER, NUMERIC_ROUNDABORT, XACT_ABORT, NOCOUNT and
driver-autocommit values already implemented upstream. Engine bytes require
MARS/pooling false and delayed durability disabled. Lock authority is derived
from the complete expanded effect inventory: resolve every mutated resource to
its exact descriptor declaration and retain its allowed lock kind, subrank and
cardinality; prepend the exact singleton schema
application-lock requirement. Requirements sort by
`(LOCK_RANK[lock_kind], lock_subrank, resource.canonical_bytes)` and receive contiguous ordinals; only
byte-identical duplicates collapse. Every mutated resource occurs exactly once.

Migration deliberately does not invent mechanism/action/mode flags. The future
provider aggregate resolves each requirement to one exact
`MssqlR1PhysicalLockStepV1` from approved Renderer/Catalog authority, then calls
the existing `require_lock_order(steps, declaration_subranks)`. That validator's
key is `(LOCK_RANK, lock_subrank, resource bytes, LOCK_PHASE[mechanism])`.
Migration neither claims nor derives an `instance_selector`; selector authority
belongs to the future executable Catalog mapping. Resolution must be bijective
with requirements and all steps must use the exact
transaction owner, bounded timeout, action, mode, hint flags, cardinality and
`instance_selector` required by the upstream physical type. Until that proof
exists, executable install remains blocked. Renderer and Catalog reference the
same upstream bytes plus this attempt-policy digest.

```text
fresh non-pooled MARS-disabled connection; driver autocommit ON
→ exact physical session profile + stable SESSION_CONTEXT attempt identity
→ explicit BEGIN
→ prove @@TRANCOUNT=1 and XACT_STATE()=1
→ transaction-owned exclusive schema application lock
→ canonical target/binding locks
→ complete observation and decision
→ block: rollback
→ admitted install or replay
→ receipt/attestation verify
→ COMMIT
```

No provider statement runs outside this transaction. Stable connection-handle
identity is captured after configuration and checked before every phase.
Lock/cancellation/timeout
failure is typed. SQL Server documents that transaction-owned `sp_getapplock`
requires a transaction and releases on commit/rollback.

The canonical state enum is:

```text
planned | session_configured | transaction_active | locked | observed |
decided | executing | commit_dispatched | committed | rolled_back | quarantined
```

Allowed transitions are exactly:

| From | Event | To |
|---|---|---|
| planned | configure | session_configured |
| session_configured | begin | transaction_active |
| transaction_active | lock | locked |
| locked | observe | observed |
| observed | decide | decided |
| decided | execute | executing |
| executing | dispatch_commit | commit_dispatched |
| commit_dispatched | commit_confirmed | committed |
| transaction_active | rollback_confirmed | rolled_back |
| locked | rollback_confirmed | rolled_back |
| observed | rollback_confirmed | rolled_back |
| decided | rollback_confirmed | rolled_back |
| executing | rollback_confirmed | rolled_back |
| transaction_active | quarantine | quarantined |
| locked | quarantine | quarantined |
| observed | quarantine | quarantined |
| decided | quarantine | quarantined |
| executing | quarantine | quarantined |
| commit_dispatched | quarantine | quarantined |

A BLOCK decision uses `decided → rolled_back`; install and replay use
`decided → executing`. `execute` starts only the ledger's post-decision suffix;
the immutable completed prefix proves prior observation/probe work and is never
dispatched again. Per-statement progress remains one contiguous ordinal prefix
across the attempt-wide ledger, so there are deliberately no execution
self-loops. The install success path is
exactly `planned, session_configured, transaction_active, locked, observed,
decided, executing, commit_dispatched, committed`; replay is the same state
path with the replay statement subset. There is no alternate state vocabulary.

Failure states: `KNOWN_NOT_COMMITTED`, `CLEANUP_FAILED_OUTCOME_UNKNOWN`,
`COMMIT_OUTCOME_UNKNOWN`, `MANUAL_DISPOSITION_REQUIRED`.

Before COMMIT dispatch, deterministic failure rolls back. Known-not-committed
requires either proof that no transaction became active or rollback proof.
Cancellation at/after dispatch, connection loss or an
ambiguous response is commit-unknown; the session is discarded.

| Fresh-session recovery proof | Action |
|---|---|
| exact full replay admission | committed/replayed; never mutate again |
| absent receipt and wholly absent provider/binding | known not committed; new attempt allowed |
| mismatch, partial, drifted, unreadable or ambiguous | manual disposition; never rerun mutation |

Receipt absence alone is never known-not-committed proof.

## Architecture and ports

Pure contracts live in canonical `dpone.contracts`/`dpone.runtime` packages.
Cohesive pure services are:

```text
MigrationObservationClassifier
DatabaseBindingInventoryClassifier
MigrationDecisionService
MigrationStatementAdmissionService
MigrationReceiptCodec
ProviderInstallReplayAdmissionService
MigrationRecoveryClassifier
```

I/O ports are intentionally deferred to the installer/composition specification;
this pure-contract task must not invent speculative adapter signatures.
Migration imports descriptor/security/target/Binding authorities, never
renderer, catalog, pyodbc, CLI, manifest or adapters.

## Evidence and tests

```python
MssqlR1MigrationObservationSetV2(
    set_version: Literal["dpone-r1-migration-observation-set-2"],
    migration_plan_digest: bytes,
    installer_attempt_uuid: UUID,
    ordered_result_payloads: tuple[bytes, bytes, bytes, bytes],
    observed_at: str,
)
MssqlR1MigrationObservationAbortV2(
    migration_plan_digest: bytes,
    installer_attempt_uuid: UUID,
    ordered_completed_result_payloads: tuple[bytes, ...],
    failed_observation_ordinal: Literal[1, 2, 3, 4],
    failure_kind: Literal[
        "query_error", "zero_rows", "multiple_rows", "null_value",
        "type_mismatch", "out_of_domain", "boundedness_exceeded"
    ],
    recovery_class: Literal["retry_fresh", "manual_disposition"],
    blocker_code: Literal["observation_visibility_failure", "boundedness_exceeded"] | None,
)
MssqlR1MigrationUowFailureV2(
    operation: Literal[
        "session_configuration", "begin", "lock", "statement", "policy",
        "rollback", "commit"
    ],
    phase: Literal[
        "setup", "observe", "receipt_probe", "mutate", "attest",
        "receipt_append", "commit", "rollback"
    ],
    native_error_number: int | None,
    transaction_state: Literal["not_started", "rolled_back", "unknown"],
    commit_dispatched: bool,
    rollback_proven: bool,
    blocker_code: str | None,
    ordered_completed_statement_ordinals: tuple[int, ...],
    recovery_class: Literal["retry_fresh", "probe_fresh", "manual_disposition"],
)
MssqlR1MigrationUowTransitionV2(
    ordinal: int,
    from_state: Literal[
        "planned", "session_configured", "transaction_active", "locked",
        "observed", "decided", "executing", "commit_dispatched"
    ],
    event: Literal[
        "configure", "begin", "lock", "observe", "decide", "execute",
        "dispatch_commit", "commit_confirmed", "rollback_confirmed",
        "quarantine"
    ],
    to_state: Literal[
        "session_configured", "transaction_active", "locked", "observed",
        "decided", "executing", "commit_dispatched", "committed",
        "rolled_back", "quarantined"
    ],
)
MssqlR1MigrationUowTransitionSetV2(
    transition_set_version: Literal["dpone-r1-migration-uow-transition-set-2"],
    ordered_transitions: tuple[MssqlR1MigrationUowTransitionV2, ...],
)

MssqlR1AdmissionUnsupportedBuildV2(
    authority_version: Literal["dpone-r1-admission-unsupported-build-2"],
    certified_profile_payload: bytes,
    certified_profile_digest: bytes,
    observed_product_major_version: int,
    observed_engine_edition: int,
    observed_edition_id: int,
    observed_product_update_level: str,
)
MssqlR1AdmissionBoundednessV2(
    authority_version: Literal["dpone-r1-admission-boundedness-2"],
    subject_kind: Literal["binding_pack"],
    subject_digest: bytes,
    observed_canonical_byte_count: int,
    applicable_maximum_byte_count: int,
)
MssqlR1AdmissionRotationMismatchV2(
    authority_version: Literal["dpone-r1-admission-rotation-mismatch-2"],
    candidate_registration_payload: bytes,
    candidate_registration_digest: bytes,
    expected_stable_target_authority_digest: bytes,
    derived_candidate_stable_target_authority_digest: bytes,
)
MssqlR1ProspectiveEvidenceReservationV2(
    authority_version: Literal["dpone-r1-prospective-evidence-reservation-2"],
    execution_kind: Literal["install", "replay"],
    phase: Literal["decided"],
    limit_kind: Literal["outcome", "envelope"],
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_plan_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
    actual_decided_payload_set_digest: bytes,
    maximum_remaining_payload_bytes: int,
    canonical_framing_bytes: int,
    prospective_total_bytes: int,
    applicable_maximum_bytes: Literal[198180864, 201326592],
)
MssqlR1PlanConstructionBoundednessV2(
    authority_version: Literal["dpone-r1-plan-construction-boundedness-2"],
    execution_kind: Literal["install", "replay"],
    physical_schema_descriptor_payload: bytes,
    physical_schema_descriptor_digest: bytes,
    shared_security_profile_payload: bytes,
    shared_security_profile_digest: bytes,
    binding_pack_payload: bytes,
    binding_pack_digest: bytes,
    migration_decision_plan_payload: bytes,
    migration_decision_plan_digest: bytes,
    observation_set_payload: bytes,
    decision_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
    subject_kind: Literal[
        "semantic_leaf", "parameter_authority", "authority_bundle",
        "statement_admission", "admitted_install_plan", "admitted_replay_plan"
    ],
    failing_leaf_id: str | None,
    ordered_completed_admission_digests: tuple[bytes, ...],
    streamed_subject_digest: bytes,
    observed_canonical_byte_count: int,
    applicable_maximum_byte_count: Literal[16777216],
)
MssqlR1PermissionClosureMismatchV2(
    authority_version: Literal["dpone-r1-permission-closure-mismatch-2"],
    expected_permission_closure_digest: bytes,
    observed_permission_set_digest: bytes,
    ordered_missing_permission_digests: tuple[bytes, ...],
    ordered_forbidden_permission_digests: tuple[bytes, ...],
)
MssqlR1CertificateReplayMismatchV2(
    authority_version: Literal["dpone-r1-certificate-replay-mismatch-2"],
    receipt_probe_result_digest: bytes,
    ordered_security_replay_evidence_payloads: tuple[bytes, bytes, bytes],
)

```

These three tagged arms use domains
`dpone-r1-admission-{unsupported-build|boundedness|rotation-mismatch}-v2\0`.
Unsupported-build decodes and hashes the exact certified profile, binds all
four observed server fields and requires the profile predicate to reject that
tuple. Boundedness requires `observed > applicable maximum`, the maximum equal
the `8 MiB` Binding-pack migration limit and the digest equal the bounded
streaming hash; it does not embed the over-limit payload or invoke an
allocating decoder. `MssqlR1MigrationInstallRequestV2` is a fixed bounded model;
an over-limit or malformed input stream is rejected by its decoder before a
Migration request or operation-evidence attempt exists and therefore is not an
admission-blocked outcome. Rotation-mismatch decodes/hashes the
candidate registration and recomputes the derived stable target digest, which
must differ from expected. It deliberately does not use the existing successful
rotation-transition evidence, whose invariant requires equality. Unknown tags,
equal rotation digests or a supported build reject.
Prospective reservation uses domain
`dpone-r1-prospective-evidence-reservation-v2\0`. It is constructed only in
state `decided`; the six authority payload fields are the complete ordered
input registry. Install requires probe NULL and an exact install plan. Replay
requires an exact probe and replay plan. Each payload decodes/re-encodes,
cross-links to the same request/attempt/observation/decision/effect and is
included in `actual_decided_payload_set_digest` over the closed field order.
`prospective_total = actual final-arm payload sizes + maximum remaining +
canonical framing`. It is valid only when the total exceeds the applicable
evidence maximum and is the required blocker authority for post-decision
`boundedness_exceeded`. Maximum remaining is exactly the sum of final install
envelope, receipt-ref, attestation and UoW-set maxima for install; for replay it
is receipt-ref, replay-admission, attestation and UoW-set maxima. For install,
the admitted plan is an input to the future envelope and is not counted twice
as a final-arm field; for replay it remains an actual final-arm field.
Concretely, install computes the exact committed-arm codec size using actual
observation/decision/inventory/resource payloads and maximum-sized placeholders
for envelope/ref/attestation/transition-set. Replay computes the exact
replayed-arm size using actual observation/decision/replay-plan/inventory/
resource/probe payloads and maximum placeholders for ref/attestation/
replay-admission/transition-set.
The closed values are `77,594,624` bytes for install
(`38+16+12+8 MiB`) and `104,857,600` bytes for replay
(`16+64+12+8 MiB`); framing is calculated separately by the exact codec size
function and cannot be hidden in either value.
`limit_kind=outcome` requires maximum 198180864; `envelope` requires 201326592
and adds actual Migration request plus the fixed-metadata size. The outer
post-decision evidence requires its adjacent decided-phase fields byte-equal to
the reservation fields and recomputes every digest, size, remaining maximum,
framing and total; a missing, extra, cross-phase or digest-only splice rejects.
Plan-construction boundedness uses domain
`dpone-r1-plan-construction-boundedness-v2\0` and is the only evidence authority
for a deterministic pre-execution expansion that cannot construct a legal
16 MiB leaf, parameter authority, ordinary authority bundle, statement
admission or install/replay plan. It embeds the complete resolved descriptor,
Security profile, Binding pack, decision plan and observation/decision context;
all adjacent digests and request/plan/profile/pack links must reproduce. A
size-only streaming factory reruns the closed expansion, hashes/counts the
entire rejected canonical subject including framing, and proves
`observed > applicable maximum` without allocating it. Leaf-owned subjects
require the exact failing leaf ID and the strict completed-admission digest
prefix; plan subjects require leaf ID NULL and the complete admission-digest
tuple. Execution kind derives probe presence and exact plan kind. Receipt append
uses its separate 18/20/38 MiB ceilings and never uses this witness. A partial
stream, caller-selected subject/limit, foreign prefix or digest-only claim
rejects.
`ordered_completed_admission_digests` is a construction prefix, not an SQL
execution claim: it lists canonical admissions successfully built before the
oversized subject, including any reconstructed phase-01/02 admissions. The UoW
failure's completed statement ordinals independently record database work
already executed and equal the count a valid plan would derive as
`completed_prefix_count` (phase 01 for
install; phase 01+02 for replay), even if construction overflow occurs while
reconstructing one of those prefix admissions. No suffix statement has run.
Permission mismatch uses domain
`dpone-r1-permission-closure-mismatch-v2\0`; both tuples are canonical and at
least one is nonempty. Missing digests must be members of expected and absent
from observed; forbidden digests must be observed and absent from expected. An
empty/foreign tuple rejects. Certificate mismatch uses domain
`dpone-r1-certificate-replay-mismatch-v2\0`. The containing outcome or
cause-context bundle provides the exact probe that reconstructs the frozen
Security V2 receipt; the mismatch authority stores only its raw digest and
never duplicates the probe bytes. Its `52 MiB` type ceiling covers three
`16 MiB` replay entries plus `4 MiB` digest/framing overhead. Each ordered payload decodes as
the exact frozen `MssqlR1CertificateReplayEvidenceV2`; its installed identity,
lifecycle authority, signer order and receipt payload digest must cross-link to
that receipt and the active authority. The positions are shared attestor,
shared stage owner and binding signer instance, and at least one decoded
`.state` must be `CONFLICT`. Arbitrary pinned/current observations or three
successful entries reject.

```python

MssqlR1MigrationCommittedInstallEvidenceV2(
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_install_envelope_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_ref_payload: bytes,
    stable_attestation_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationReplayedEvidenceV2(
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_plan_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes,
    receipt_ref_payload: bytes,
    stable_attestation_payload: bytes,
    replay_admission_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationBlockedEvidenceV2(
    observation_set_payload: bytes,
    decision_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationAdmissionBlockedEvidenceV2(
    blocker_code: Literal[
        "unsupported_server_build", "boundedness_exceeded",
        "registration_rotation_mismatch"
    ],
    admission_authority: (
        MssqlR1AdmissionUnsupportedBuildV2
        | MssqlR1AdmissionBoundednessV2
        | MssqlR1AdmissionRotationMismatchV2
    ),
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationPreObservationBlockedEvidenceV2(
    blocker_code: Literal["transaction_profile_mismatch"],
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationPostDecisionBlockedEvidenceV2(
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_plan_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
    blocker_code: Literal[
        "stable_attestation_mismatch", "permission_closure_mismatch",
        "certificate_replay_mismatch"
    ],
    blocker_authority_payload: bytes,
    blocker_authority_digest: bytes,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationProspectiveBoundednessBlockedEvidenceV2(
    blocker_code: Literal["boundedness_exceeded"],
    reservation_payload: bytes,
    reservation_digest: bytes,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationPlanBoundednessBlockedEvidenceV2(
    blocker_code: Literal["boundedness_exceeded"],
    plan_boundedness_payload: bytes,
    plan_boundedness_digest: bytes,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationObservationAbortedEvidenceV2(
    observation_abort_payload: bytes,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationKnownNotCommittedEvidenceV2(
    observation_set_payload: bytes | None,
    decision_payload: bytes | None,
    admitted_plan_payload: bytes | None,
    inventory_classification_payload: bytes | None,
    receipt_resource_result_payload: bytes | None,
    receipt_probe_result_payload: bytes | None,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1TransactionProfileMismatchAuthorityV2(
    authority_version: Literal["dpone-r1-transaction-profile-mismatch-2"],
    attempt_policy_payload: bytes,
    attempt_policy_digest: bytes,
    ordered_mismatched_fields: tuple[str, ...],
    observed_profile_digest: bytes,
)
MssqlR1MigrationCauseContextBundleV2(
    bundle_version: Literal["dpone-r1-migration-cause-context-bundle-2"],
    context_kind: Literal[
        "decision_blocker", "execution_failure", "post_decision_blocker",
        "manual_disposition"
    ],
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_plan_payload: bytes | None,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
)
MssqlR1ManualDispositionCauseAuthorityV2(
    authority_version: Literal["dpone-r1-manual-disposition-cause-2"],
    cause_context_bundle_payload: bytes,
    cause_context_bundle_digest: bytes,
)
MssqlR1DecisionBlockerCauseAuthorityV2(
    authority_version: Literal["dpone-r1-decision-blocker-cause-2"],
    cause_context_bundle_payload: bytes,
    cause_context_bundle_digest: bytes,
)
MssqlR1ExecutionFailureCauseAuthorityV2(
    authority_version: Literal["dpone-r1-execution-failure-cause-2"],
    cause_context_bundle_payload: bytes,
    cause_context_bundle_digest: bytes,
    failed_statement_admission_payload: bytes,
)
MssqlR1PostDecisionBlockCauseAuthorityV2(
    authority_version: Literal["dpone-r1-post-decision-block-cause-2"],
    cause_context_bundle_payload: bytes,
    cause_context_bundle_digest: bytes,
    blocker_code: str,
    blocker_authority_payload: bytes,
    blocker_authority_digest: bytes,
)
MssqlR1MigrationRollbackCauseV2(
    cause_version: Literal["dpone-r1-migration-rollback-cause-2"],
    cause_kind: Literal[
        "pre_observation_failure", "pre_observation_blocker",
        "observation_abort", "decision_blocker", "execution_failure",
        "post_decision_blocker", "prospective_boundedness",
        "plan_boundedness", "manual_disposition"
    ],
    operation: Literal["lock", "statement", "policy"],
    phase: Literal["setup", "observe", "receipt_probe", "mutate", "attest", "receipt_append"],
    blocker_code: str | None,
    native_error_number: int | None,
    cause_authority_payload: bytes,
    cause_authority_digest: bytes,
    ordered_completed_statement_ordinals: tuple[int, ...],
    recovery_class: Literal["retry_fresh", "manual_disposition"],
)
MssqlR1MigrationCleanupUnknownEvidenceV2(
    rollback_cause: MssqlR1MigrationRollbackCauseV2,
    rollback_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationCommitUnknownEvidenceV2(
    observation_set_payload: bytes,
    decision_payload: bytes,
    admitted_plan_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
    stable_attestation_payload: bytes,
    uow_failure_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationManualDispositionEvidenceV2(
    fresh_observation_set_payload: bytes,
    inventory_classification_payload: bytes,
    receipt_resource_result_payload: bytes,
    receipt_probe_result_payload: bytes | None,
    blocker_decision_payload: bytes,
    uow_transition_set_payload: bytes,
)
MssqlR1MigrationEvidenceV2(
    evidence_version: Literal["dpone-r1-migration-evidence-2"],
    implementation_commit: str,  # lowercase 40-hex Git object name
    migration_contract_digest: bytes,
    migration_request_payload: bytes,
    migration_request_digest: bytes,
    installer_attempt_uuid: UUID,
    operation_outcome: Literal[
        "committed", "replayed", "admission_blocked",
        "pre_observation_blocked", "blocked_rolled_back",
        "post_decision_blocked_rolled_back",
        "prospective_boundedness_blocked_rolled_back",
        "plan_boundedness_blocked_rolled_back",
        "observation_aborted_rolled_back",
        "execution_failed_known_not_committed", "cleanup_failed_outcome_unknown",
        "commit_outcome_unknown", "manual_disposition_required"
    ],
    outcome_payload: (
        MssqlR1MigrationCommittedInstallEvidenceV2
        | MssqlR1MigrationReplayedEvidenceV2
        | MssqlR1MigrationBlockedEvidenceV2
        | MssqlR1MigrationAdmissionBlockedEvidenceV2
        | MssqlR1MigrationPreObservationBlockedEvidenceV2
        | MssqlR1MigrationPostDecisionBlockedEvidenceV2
        | MssqlR1MigrationProspectiveBoundednessBlockedEvidenceV2
        | MssqlR1MigrationPlanBoundednessBlockedEvidenceV2
        | MssqlR1MigrationObservationAbortedEvidenceV2
        | MssqlR1MigrationKnownNotCommittedEvidenceV2
        | MssqlR1MigrationCleanupUnknownEvidenceV2
        | MssqlR1MigrationCommitUnknownEvidenceV2
        | MssqlR1MigrationManualDispositionEvidenceV2
    ),
    generated_at: str,
)
```

Domains are
`dpone-r1-provider-migration-{observation-set|observation-abort|uow-failure|uow-transition|evidence}-v2\0`;
the transition-set domain is
`dpone-r1-migration-uow-transition-set-v2\0` and its exact payload is the only
way an evidence arm carries the ordered transition tuple. Its decoder enforces
the `65,536` transition count, `8 MiB` aggregate canonical-byte bound,
contiguous ordinals, initial/adjacent/terminal state rules and byte-identical
re-encoding; no caller hashes a language-native tuple or supplies framing;
each outcome payload has domain
`dpone-r1-provider-migration-evidence-{committed|replayed|admission-blocked|pre-observation-blocked|blocked|post-decision-blocked|prospective-boundedness-blocked|plan-boundedness-blocked|observation-aborted|known-not-committed|cleanup-unknown|commit-unknown|manual}-v2\0`.
Observation-set payloads decode in exact registry order; abort payloads form an
exact contiguous prefix. Completed statement ordinals are contiguous in the
active execution sequence: pre-receipt plan followed by append for install,
and replay plan for replay. `commit_dispatched=true` requires operation COMMIT, transaction
unknown, rollback false and recovery `probe_fresh`. Rollback failure requires
operation ROLLBACK, state unknown, rollback false and manual disposition. The
outcome discriminator selects exactly one matching payload arm.
`prospective_boundedness_blocked_rolled_back` selects only
`MssqlR1MigrationProspectiveBoundednessBlockedEvidenceV2`;
`plan_boundedness_blocked_rolled_back` selects only
`MssqlR1MigrationPlanBoundednessBlockedEvidenceV2`;
`post_decision_blocked_rolled_back` selects only the ordinary
`MssqlR1MigrationPostDecisionBlockedEvidenceV2`, whose blocker union excludes
boundedness. Cross-arm or cross-discriminator encoding rejects.
`migration_request_payload` is always non-NULL, byte-identically decodable as
`MssqlR1MigrationInstallRequestV2`, and bound to the outer request digest.
Observation-abort `boundedness_exceeded` maps to its same-named blocker.
Zero/multiple/null/type/out-of-domain maps to
`observation_visibility_failure`. `query_error` always has blocker NULL and
`retry_fresh`; it is safe to retry only after the enclosing UoW proves rollback,
and no caller-selected transient classifier exists in this contract. Persistent
query errors may retry and fail again but cannot mutate or report success.
No other blocker or recovery combination can inhabit an
observation-abort payload; its blocker equals the nested UoW failure blocker.

Optional presence is not caller-selected. Constructors enforce this complete
matrix before encoding:

| Arm/condition | Obs | Abort | Decision | Plan | Inventory | Resource | Probe |
|---|---:|---:|---:|---:|---:|---:|---:|
| admission-blocked | no | no | no | no | no | no | no |
| pre-observation-blocked | no | no | no | no | no | no | no |
| known-not-committed, setup/begin/lock | no | no | no | no | no | no | no |
| known-not-committed, admitted execution | yes | no | yes | yes | yes | yes | replay only |
| post-decision-blocked | yes | no | yes | yes | yes | yes | replay only |
| prospective-boundedness-blocked | inside reservation | no | inside reservation | inside reservation | inside reservation | inside reservation | by execution kind |
| plan-boundedness-blocked | inside size witness | no | inside size witness | unavailable by definition | inside size witness | inside size witness | by execution kind |
| commit-unknown, install | yes | no | yes | yes | yes | yes | no |
| commit-unknown, replay | yes | no | yes | yes | yes | yes | yes |
| manual disposition | fresh | no | blocker | no | yes | yes | by resource state |

Every non-NULL `admitted_plan_payload` has an exact decoded type. An install
decision requires `MssqlR1AdmittedInstallPlanV2`; a replay decision requires
`MssqlR1AdmittedReplayPlanV2`. Replayed, commit-unknown, known-not-committed,
post-decision, reservation and composite rollback authorities all derive that
selection from their adjacent decision and reject a cross-kind plan. The
committed install arm carries the final install envelope instead; its embedded
pre-receipt plan must decode as the exact install plan. No generic admitted
plan contract exists.

Cleanup-unknown does not use optional context columns. Its rollback cause has
this exact authority mapping:

| Cause kind | `cause_authority_payload` exact type |
|---|---|
| `pre_observation_failure` | `MssqlR1MigrationAttemptPolicyV2` |
| `pre_observation_blocker` | `MssqlR1TransactionProfileMismatchAuthorityV2` |
| `observation_abort` | `MssqlR1MigrationObservationAbortV2` |
| `decision_blocker` | `MssqlR1DecisionBlockerCauseAuthorityV2` |
| `execution_failure` | `MssqlR1ExecutionFailureCauseAuthorityV2` |
| `post_decision_blocker` | `MssqlR1PostDecisionBlockCauseAuthorityV2` |
| `prospective_boundedness` | `MssqlR1ProspectiveEvidenceReservationV2` |
| `plan_boundedness` | `MssqlR1PlanConstructionBoundednessV2` |
| `manual_disposition` | `MssqlR1ManualDispositionCauseAuthorityV2` |

The raw cause digest is adjacent and mandatory. The rollback trigger is a
closed sum with this total field matrix:

| Cause kind | Operation | Phase | Blocker | Native error | Recovery/ordinals |
|---|---|---|---|---|---|
| `pre_observation_failure` | `lock` | `setup` | NULL | integer or NULL | retry; empty |
| `pre_observation_blocker` | `policy` | `setup` | `transaction_profile_mismatch` | NULL | manual; empty |
| `observation_abort`, query error | `statement` | `observe` | NULL | integer or NULL | retry; exact observed prefix |
| `observation_abort`, structural/limit | `policy` | `observe` | nested abort blocker | NULL | manual; exact observed prefix |
| `decision_blocker` | `policy` | `observe` | first decision blocker | NULL | manual; empty statement prefix |
| `execution_failure` | `statement` | failed admission phase | NULL | integer or NULL | retry; strict plan prefix |
| `post_decision_blocker` | `policy` | authority-owning phase | nested blocker | NULL | manual; strict plan prefix |
| `prospective_boundedness` | `policy` | `mutate` install / `attest` replay | `boundedness_exceeded` | NULL | manual; reservation prefix |
| `plan_boundedness` | `policy` | `mutate` install / `attest` replay | `boundedness_exceeded` | NULL | manual; witness prefix |
| `manual_disposition` | `policy` | `observe` | `manual_disposition_required` | NULL | manual; empty statement prefix |

No session-configuration or BEGIN failure can inhabit rollback cause: those
occur before a rollback-requiring active state. Native error numbers are
optional because transport, driver, timeout and cancellation failures may not
provide a SQL Server integer; blocker NULL distinguishes them from policy
causes. Phase, recovery and completed ordinals must reproduce the nested
authority and matrix row. `rollback_failure_payload` separately decodes as UoW failure
`operation=rollback, phase=rollback, state=unknown, dispatch=false,
rollback_proven=false, recovery=manual`. Thus cleanup evidence preserves both
why rollback began and why cleanup outcome is unknown; neither can substitute
for the other.
Transaction-profile mismatch authority uses domain
`dpone-r1-transaction-profile-mismatch-v2\0`, decodes/hashes the attempt policy,
requires a nonempty canonical tuple drawn only from that policy's exact profile
field names, and requires the observed digest to differ from the expected
profile digest. Rollback-cause domain is
`dpone-r1-migration-rollback-cause-v2\0`.
The four composite cause authorities use domains
`dpone-r1-{decision-blocker-cause|execution-failure-cause|post-decision-block-cause|manual-disposition-cause}-v2\0`.
Their shared context uses domain
`dpone-r1-migration-cause-context-bundle-v2\0` and is embedded exactly once;
each wrapper carries only its adjacent raw digest plus cause-specific authority.
Every context payload decodes and re-encodes as its exact named type. The
observation, inventory, receipt-resource and optional receipt-probe payloads
must belong to one request, binding, attempt, target and observation set; the
decision and admitted plan must reproduce those same identities and digests.
Optional probe presence is derived only from the decoded receipt-resource
state. Context kind is exact: decision/manual require a blocking decision and
plan NULL; execution/post-decision require a nonblocking decision and the exact
install/replay plan. Manual context additionally requires fresh observations
and cannot reuse the quarantined attempt. Execution-failure authority requires the failed statement admission to
belong exactly once to the admitted plan and its completed ordinals to be the
strict prefix before that statement. Post-decision authority requires the same
closed blocker-code/type mapping as the outer post-decision evidence; certificate
mismatch's probe digest must equal the single probe payload in this context.
All explicitly adjacent digests are raw SHA-256 of their payload;
the aggregate cause digest protects the remaining embedded payload bytes.
Any cross-attempt, cross-generation, missing, duplicate or digest-only splice
rejects before the cleanup-unknown envelope can be encoded.

Committed and replayed arms require every non-optional field shown in their
model; admission-blocked occurs before SQL observation; observation-aborted
carries only abort/failure/transitions; blocked
always follows the complete four-observation set and therefore always carries
inventory and receipt-resource results. Any other presence combination is
non-canonical and rejects. All adjacent payloads are decoded as their exact
field type and cross-linked by request, plan, observation, decision, attempt,
effect and authority digests. `implementation_commit` must match
`[0-9a-f]{40}`.
Admission code and tagged authority have a strict one-to-one mapping:
`unsupported_server_build` to `MssqlR1AdmissionUnsupportedBuildV2`,
`boundedness_exceeded` to `MssqlR1AdmissionBoundednessV2`, and
`registration_rotation_mismatch` to `MssqlR1AdmissionRotationMismatchV2`.
The nested profile/registration payload digests and bounded subject digest are
validated by their own arm invariants; no adjacent generic digest exists.
Post-decision boundedness uses the prospective reservation as its trigger
authority. With proven rollback, the outer evidence is exactly
`MssqlR1MigrationProspectiveBoundednessBlockedEvidenceV2`. With rollback
failure, the outer evidence is exactly `MssqlR1MigrationCleanupUnknownEvidenceV2`;
its rollback cause kind is `prospective_boundedness` and embeds the same
reservation. The two terminal arms are mutually exclusive and are never
dual-emitted. The reservation payload embeds the complete decided authority set
and its adjacent digest is raw SHA-256. The UoW failure must be `policy/mutate`
at the pre-execution reservation check for install, or `policy/attest` before the
post-decision attestation for replay, with proven rollback. Install carries the
exact completed phase-01 observation-admission prefix; replay carries the exact
phase-01 observation plus phase-02 keyed-probe prefix. No post-decision
statement ordinal is completed. All other
post-decision blockers use the ordinary arm and require
`MssqlR1StableCatalogAttestationV2` for stable mismatch,
`MssqlR1PermissionClosureMismatchV2` for permission
mismatch, or `MssqlR1CertificateReplayMismatchV2` for certificate mismatch.
The adjacent payload digest is raw SHA-256 and the named comparison must fail;
cross-arm/code splices reject. The
plan-boundedness trigger has the same install `policy/mutate` versus replay
`policy/attest` UoW mapping and the same already-completed prefix, but binds the
streaming plan-construction witness instead of a constructed plan or
reservation. The trigger occurs before any suffix effect. Proven rollback emits
only `MssqlR1MigrationPlanBoundednessBlockedEvidenceV2`; rollback failure emits
only `MssqlR1MigrationCleanupUnknownEvidenceV2` with cause kind
`plan_boundedness` embedding that same witness. Dual emission is forbidden. The
pre-observation arm requires a `policy/setup` UoW failure with
`transaction_profile_mismatch`; before BEGIN it proves `not_started`, while
after BEGIN it requires proven rollback.

Transition ordinals are contiguous; the first state is planned; adjacent states
match; only table rows above are legal. Failure before successful configuration
has an empty transition tuple and terminal `planned`; begin failure has the sole
configure transition and terminal `session_configured`. Neither claims rollback.
Terminal mapping is exact:

```text
committed/replayed                         -> committed
admission_blocked                          -> planned
pre_observation_blocked before active tx   -> planned | session_configured
pre_observation_blocked after BEGIN        -> rolled_back
blocked/post_decision_blocked/observation_aborted -> rolled_back
prospective_boundedness_blocked_rolled_back  -> rolled_back
plan_boundedness_blocked_rolled_back         -> rolled_back
known_not_committed with proven rollback   -> rolled_back
known_not_committed before active tx       -> planned | session_configured
cleanup_failed_outcome_unknown             -> quarantined
commit_outcome_unknown                     -> quarantined
manual_disposition_required                -> rolled_back
```

Known-not-committed permits retry only when transaction state is `not_started`
or `rollback_proven=true`. Cleanup/commit unknown never permits retry mutation.

Exact failure mapping:

| Trigger | Phase | Recovery | Outcome |
|---|---|---|---|
| unsupported build or pre-observation Binding-pack bound | setup | manual disposition | admission blocked |
| session/engine profile mismatch before active transaction | setup | manual disposition | pre-observation blocked, not started |
| active transaction/lock profile mismatch with proven rollback | setup | manual disposition | pre-observation blocked rolled back |
| session configuration/begin failure with state `not_started` | setup | retry fresh | execution failed known not committed |
| lock acquisition failure with proven rollback | setup | retry fresh | execution failed known not committed |
| observation query/type/domain failure with proven rollback | observe | retry fresh or manual per abort kind | observation aborted rolled back |
| complete observation vector classifies BLOCK | observe | manual disposition | blocked rolled back |
| deterministic admitted statement failure + proven rollback | owning phase | retry fresh | execution failed known not committed |
| deterministic post-decision policy mismatch + proven rollback | owning phase | manual disposition | post-decision blocked rolled back |
| prospective overflow before install/replay post-decision effect + proven rollback | mutate/install or attest/replay | manual disposition | prospective boundedness blocked rolled back |
| streamed plan/ordinary-bundle overflow before post-decision effect + proven rollback | mutate/install or attest/replay | manual disposition | plan boundedness blocked rolled back |
| rollback failure | rollback | manual disposition | cleanup failed outcome unknown |
| commit response/cancellation ambiguous after dispatch | commit | probe fresh | commit outcome unknown |
| fresh state mismatch/partial/drift | observe | manual disposition | manual disposition required |

The legal `MssqlR1MigrationUowFailureV2` combinations are total and closed:

| Operation | Legal phase | State/flags/recovery |
|---|---|---|
| `session_configuration` | `setup` | `not_started`, dispatch false, rollback false, retry |
| `begin` | `setup` | `not_started`, dispatch false, rollback false, retry |
| `lock` | `setup` | `rolled_back`, dispatch false, rollback true, blocker NULL, retry |
| `statement` | exactly one of `observe,receipt_probe,mutate,attest,receipt_append` | `rolled_back`, dispatch false, rollback true, blocker NULL, retry |
| `policy` before active transaction | `setup` | `not_started`, dispatch false, rollback false, blocker `transaction_profile_mismatch`, manual |
| `policy` after BEGIN | exactly one of `setup,observe,receipt_probe,mutate,attest,receipt_append` | `rolled_back`, dispatch false, rollback true, blocker required and owned by that phase, manual |
| `rollback` | `rollback` | `unknown`, dispatch false, rollback false, manual |
| `commit` | `commit` | `unknown`, dispatch true, rollback false, probe fresh |

No COMMIT failure after dispatch is classified known-not-committed, even if a
driver error text claims rejection. A client-side failure before dispatch is an
`executing` statement/setup failure followed by proven rollback, not operation
`commit`. All other operation/phase/state/flag/recovery combinations reject.
`blocker_code` is NULL for transport/native failures and required for a
deterministic policy blocker; it must belong to the producer table above.
`native_error_number` is non-NULL exactly when `blocker_code` is NULL and the
driver returned an integer SQL Server error; otherwise it is NULL. Setup and
lock failures have no completed statement ordinals. Statement failures carry
the exact contiguous prefix preceding the failed admission. Rollback and commit
failures repeat the prefix already proved by the enclosing outcome arm. These
rules, plus the table, enumerate every field combination; there is no generic
cross-product constructor.

Internal candidate evidence exposes only blocker code, phase and recovery
class; native messages, SQL, object names and credentials are redacted. No
public message/CLI/API contract is defined here.

All payloads are decoded under shared quotas and cross-validated; digest-only
evidence is insufficient. The tagged outcome classes are the exact presence
matrix: an abort arm cannot carry a decision, blocked cannot carry a plan, and
not-started/cleanup-unknown cannot claim rollback.

Operation evidence contains no certification-status field. A separate candidate
inventory artifact may report pure hermetic `PASS`; SQL execution and
durability remain independently `UNVERIFIED`. Evidence producer publication is create-only: stream
canonical JSON to a private temp file, hash, fsync, atomic rename and parent
fsync. The artifact path binds implementation commit; schema and producer
commits, ordered pytest node IDs/counts and case-registry SHA-256 are embedded.
Any existing differing artifact blocks; mocked/live-unavailable never PASS.

Exact candidate artifact contract:

```yaml
schema_path: docs/schemas/evidence/postgres-mssql-r1-v3-migration-v2.schema.json
schema_version: dpone-postgres-mssql-r1-v3-migration-v2-evidence-1
canonical_json: RFC8785-JCS
producer_path: tests/support/postgres_mssql_r1_v3_migration_v2_evidence.py
artifact_root: test_artifacts/postgres-mssql-r1-v3/migration-v2/<implementation_commit>/
artifact_name: migration-inventory.json
required_identity:
  - approved_specification_commit
  - red_task_commit
  - red_test_commit
  - evidence_protocol_commit
  - schema_sha256
  - producer_commit
  - producer_sha256
  - implementation_commit
  - migration_contract_identity_digest
  - case_registry_sha256
  - ordered_nodeids
  - ordered_nodeids_sha256
  - collected_node_count
  - hermetic_status
  - sql_server_live_status
```

The schema and producer are frozen in a separate evidence-protocol commit before
GREEN. `hermetic_status` may be `PASS|FAIL`; `sql_server_live_status` is always
`UNVERIFIED` in this pure task.

Every commit identity matches lowercase `[0-9a-f]{40}`. `schema_sha256` and
`producer_sha256` are hashes of the exact files at `evidence_protocol_commit`;
the producer refuses a worktree or Git-object mismatch.
`ordered_nodeids` is the exact duplicate-free UTF-8 lexical tuple;
`ordered_nodeids_sha256` hashes its RFC8785-JCS array bytes, and
`collected_node_count` equals its length. Case/outcome counts are deliberately absent: the artifact records the
case-registry digest, while the reviewed test module owns parametrization and
pytest owns collected node IDs. No undefined second case-record model exists.
No additional key is admitted. The artifact schema closes every object with
`additionalProperties: false`. It validates inventory identity/status only and
does not duplicate or attempt to serialize the canonical operation-evidence
tagged union.

The future reviewed RED task must cover:

- every closed state and blocker; absent resource admits no probe leaf;
- zero/one/two probe rows and all partial/mismatch forms;
- maximum-size legal receipt followed by lost response, fresh exact probe and
  source-free replay; probe-result 0/limit/limit+1 codec vectors;
- maximum-size probe through decision, execution, post-decision/certificate and
  manual cause-context arms, including rollback failure and cleanup evidence;
- distinct effect keys for two packs but one-binding rejection of the second;
- foreign receipt, objects-without-receipt, two receipts, requested plus foreign
  partial state, orphan and unclassifiable leaves;
- finite vector coverage/reachability/overlap/default;
- exact statement phase/order and renderer independence;
- all V1/unknown/trailing/cross-authority splices;
- all seven stable result projections and security replay splices;
- registration rotation, UoW transitions and commit-loss recovery;
- prospective-boundedness discriminator/ordinary-arm cross-splices;
- prospective-boundedness rollback failure into cleanup-unknown with exact
  reservation cause and install/replay prefix;
- prospective-boundedness proven rollback selecting only the dedicated blocked
  arm, rollback failure selecting only cleanup-unknown, and rejection of dual
  emission or a trigger/cause mismatch;
- plan-construction streaming witness subject/construction-prefix/executed-prefix/
  digest/limit splices, the 90,112-admission/8 MiB digest-tuple maximum and
  cleanup-unknown after rollback failure;
- plan-boundedness proven rollback selecting only the dedicated blocked arm,
  rollback failure selecting only cleanup-unknown, and rejection of dual
  emission or a trigger/cause mismatch;
- install/replay plan-kind splices and decided-phase reservation presence;
- transition-set terminal-state mismatches across every outcome arm;
- exact receipt bundle, receipt-append and install-envelope 0/limit/limit+1
  size vectors plus duplicate/unused/cross-ordinal bundle refs;
- prospective reservation, plan witness, rollback cause, outcome and outer
  envelope 0/limit/limit+1 exact-codec vectors;
- canonical round-trip, `must_reject`, `valid_distinct`, limits and no fallback;
- import, module-size and architecture budgets.

Properties:

```text
never mutate before a complete nonblocking decision
never query an absent receipt resource
never admit two bindings
never treat a receipt alone as replay authority
never append receipt outside the install transaction
never rerun mutation after ambiguous commit
never let V1 bytes enter V2 admission
```

Live SQL Server tests belong to a later installer task.

## Market comparison

Official sources checked 2026-09-07:

| System/version/edition | Relevant pattern | Decision |
|---|---|---|
| dlt current docs, OSS | Destination state can restore a pipeline; first-run logic may skip querying an absent destination. | Adopt explicit absent-resource and destination-local recovery authority, strengthened with typed receipt/full attestation. |
| Fivetran current SaaS docs | Re-sync is explicit; a sync is paused for history-mode migration queries. | Adopt explicit migration/rebaseline boundaries; reject implicit partial-state repair. |
| SQL Server 2022 / Microsoft Learn ver17 | Transaction-owned `sp_getapplock` requires a transaction and releases on commit/rollback. | Use exact transaction-owned lock with bounded timeout. |
| Airbyte, Informatica, Pentaho, SSIS | N/A: their published route/package behavior is not an equivalent target-local provider-install canonical receipt ABI. | Compare in route/runtime releases. |
| gusty, Astronomer Cosmos | N/A: DAG authoring/orchestration does not own target-local SQL provider installation authority. | Compare only if Airflow integration changes. |
| Apache Beam | N/A: processing semantics do not define this SQL Server install ABI. | Compare in Batch/WAL work. |

Sources: [dlt state](https://dlthub.com/docs/general-usage/state),
[dlt pipeline](https://dlthub.com/docs/general-usage/pipeline),
[Fivetran history mode](https://fivetran.com/docs/core-concepts/sync-modes/history-mode),
[SQL Server `sp_getapplock`](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver17).

No superiority claim is made. Measurable record:

```yaml
scenario: one-binding SQL Server provider install/retry after response loss
baseline: prose/idempotent-DDL or receipt-row-only replay admission
metric: classified closed-state vectors and accepted authority splices
target: 400/400 vectors deterministic; 0 accepted foreign splices
procedure: frozen model registry plus must_reject mutation suite
artifact: test_artifacts/postgres-mssql-r1-v3/migration-v2/<commit>/migration-inventory.json
limitations: hermetic proof only; SQL locks/commit durability need vendor-live task
```

## Documentation, rollout and ownership

No public workflow changes. Update only the provider implementation map,
architecture/recovery notes and evidence reference. First-success remains
blocked until Renderer V2, Catalog V2, aggregate, installer and vendor-live
certification exist. Never document manual receipt edits or automatic repair.

```text
RESEARCHED spec → four fresh reviews → maintainer APPROVED
→ reviewed task contract → frozen RED → pure GREEN/evidence
→ fresh implementation reviews → Renderer V2 specification
```

The future task gives one writer new Migration V2 contract/policy modules,
focused tests and immutable evidence. Shared registries, dependencies,
navigation, changelog, fixtures and status maps stay integrator-owned.
Renderer/catalog/adapters are forbidden paths.

This pure-contract slice cannot create a receipt, so its rollback removes only
unused models. Roll-forward-only authority starts only when a future approved
installer writes the first receipt; that rollout contract must require a binary
able to read it or leave the provider paused. No V2→V1 conversion exists.

## Approval checklist and Definition of Done

- [x] Exact upstream authorities pinned; V1 conflict superseded.
- [x] Absent-resource observation executes no receipt SQL.
- [x] One-binding inventory and V2 receipt/replay identities are closed.
- [x] Receipt/stable authority is same-database and same-transaction.
- [x] Session/UoW/recovery and dependency direction are explicit.
- [x] Limits, failure taxonomy, RED matrix and evidence are explicit.
- [x] Architecture, test/certification, docs/UX and release reviews pass against
  exact commit `source record 138`.
- [x] Maintainer changes status to `APPROVED` after those reviews.

Done means every database state has one disposition; foreign/second/orphan state
blocks; effect/request/raw-payload digests reproduce; replay proves request,
descriptor, pack, inventory, attestation, permissions and certificates; unknown
commit never reruns on receipt absence alone; V1 cannot enter V2; hermetic
evidence is exact-commit-bound and live behavior remains `UNVERIFIED`.

Back: [provider implementation map](developer-postgres-mssql-r1-v3-provider-implementation.md).
Next after approval and pure implementation: Renderer V2 specification.
