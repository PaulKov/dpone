<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider attestation foundation V2

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: APPROVED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-07
- Depends on:
  - physical descriptor R2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - Security V2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - Binding V2 implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - Binding V2 acceptance `PENDING_PUBLIC_COMMIT_BINDING`;
  - Security V2 amendment `PENDING_PUBLIC_COMMIT_BINDING`;
  - schema-attestation implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - schema-observation implementation `PENDING_PUBLIC_COMMIT_BINDING`;
  - Migration V2 approved specification `PENDING_PUBLIC_COMMIT_BINDING`.
- Related decision: [ADR 0071](adr/source-history/0071-r1-provider-attestation-foundation.md)

## Outcome and authority boundary

This specification closes an implementation-order and dependency-cycle defect
found before Migration V2 RED. The approved Security V2 amendment describes ten
query/stable-attestation aggregates, but its layouts depend on leaf types that
exist only in historical `RESEARCHED` Migration/Binding V1 prose. Implementing
those leaves inside Security would create forbidden Security → Binding and
Security → Migration dependencies.

The new internal foundation is the only owner of provider catalog observation
and stable-attestation composition:

```text
physical descriptor + Security V2 + Binding V2 + schema attestation/observation
                    ↓
          provider_attestation foundation
                    ↓
               Migration V2
```

It owns pure immutable contracts, canonical codecs, projection and
authority-validation algorithms. It performs no SQL rendering, database I/O,
transaction control, receipt storage, CLI/manifest work or activation. Public
imports do not change and live SQL Server behavior remains `UNVERIFIED`.

This specification supersedes only the provider-observation and stable-
attestation portions of historical Migration/Binding V1 and the incomplete
ownership implied by the Security V2 amendment. All other approved Security V2,
Binding V2 and Migration V2 semantics remain unchanged.

## Personas and journey

| Persona | Problem | Success |
|---|---|---|
| Migration implementer | Stable attestation has no implemented dependency-safe owner | Imports one complete foundation without Security reverse edges |
| Security reviewer | Permission/certificate observations can be spliced | One authority-aware validator consumes exact descriptor and Binding pack |
| Release reviewer | Historical V1 prose is accidentally treated as approved ABI | Every retained leaf has an exact approved field/domain/bound contract |

The internal journey is:

```text
decode exact active authorities
→ build seven typed catalog query results
→ prove one closed query/result registry
→ derive stable schema/security/binding projections
→ prove permission closure and certificate identity
→ return one stable catalog attestation
→ hand immutable bytes to Migration V2
```

## Scope and non-goals

In scope:

- dependency-neutral statement/query reference ABI;
- database, module, signature and binding-prefix observation leaves;
- seven bounded typed query-result arms;
- the ten V2 types introduced by the Security V2 amendment;
- stable schema digest derivation, two-way merge and authority validation;
- exact compatibility rules, failure taxonomy and hermetic tests.

Non-goals:

- SQL rendering/authoring/execution, renderer catalog implementation or ODBC rows;
- Migration decisions, install plans, receipts, transaction runtime or replay;
- Security policy construction or Binding pack construction;
- package-level re-exports, public Python API, CLI, manifest or route activation;
- live SQL Server certification.

## Canonical rules and bounds

All models are exact frozen slotted dataclasses. They use the strict V3 codec;
decode validates exact domain, field order/count, closed tags, primitive types,
bounds and invariants, then requires byte-identical re-encoding. Booleans are
not integers. Text is NFC, contains no NUL, and is bounded in UTF-8 bytes.
Digests are exactly 32 bytes and UUIDs are non-nil.

```text
digest = SHA256(canonical_bytes(EXACT_DOMAIN, ordered_fields))
```

```yaml
max_identifier_bytes: 128
max_product_text_bytes: 128
max_statement_id_bytes: 128
max_query_definition_bytes: 131072
max_normalized_module_definition_bytes: 131072
max_schemas: 8
max_principals: 32
max_role_memberships: 32
max_table_objects: 32
max_core_module_objects: 32
max_binding_modules: 6
max_shared_certificates: 2
max_permission_paths: 256
max_core_signatures: 32
max_binding_signatures: 6
max_binding_prefix_objects: 64
max_statement_results: 7
max_expected_contract_bytes: 524288
max_principal_authority_set_bytes: 524288
max_stable_catalog_payload_bytes: 12582912
max_canonical_leaf_payload_bytes: 262144
max_statement_authority_payload_bytes: 2097152
max_query_result_payload_bytes: 3145728
max_post_install_result_payload_bytes: 4194304
max_statement_registry_payload_bytes: 3145728
max_stable_schema_payload_bytes: 4194304
max_stable_binding_payload_bytes: 4194304
max_crypt_property_bytes: 4096
max_nested_sequence_items_default: 64
max_nested_total_items: 4096
max_nested_depth: 16
```

Every tuple is canonical and unique by its semantic key. Every internal callable
model `from_canonical_bytes` decoder first invokes
`preflight_provider_attestation_canonical_v2(payload, domain, field_count, cap)`.
The iterative scanner rejects non-bytes, zero/oversize payload, invalid domain,
truncated/overflowing frames, unknown/fixed-width tag shapes, sequence depth
above 16 and total sequence members above 4096 before the existing recursive V3
decoder is called. It performs depth-first cursor traversal with one explicit
`(cursor, end, depth)` frame per open `q`; a child is completed before its next
sibling is visited. It never slices value bodies and therefore holds at most
`max_nested_depth + 1` scanner frames, independent of sibling count. For `b/s`
it validates the eight-byte length against the enclosing
frame; UTF-8/NFC remains the decoder's responsibility. For `q` it iteratively
walks four-byte child-length frames, counts every child occurrence and pushes
nested `q` bodies. Fixed tags admit only `n/t/f` length 1, `u` length 17 and `i`
length 9. Structural/tag failures map to `decode`; depth/member overflow maps to
`semantic_bounds`.

Only after preflight does the existing decoder allocate typed nested values;
model validation then enforces per-field counts. This foundation does not
change the shared codec. A 1,500-level, 7,507-byte nested-`q` adversarial vector
must yield typed `attestation_bounds_exceeded` at `semantic_bounds`, never raw
`RecursionError`. Limit+1 input fails closed. No caller supplies a precomputed
own digest, success flag, set-difference result or stable merge result.

| Tuple field | Count | Canonical semantic key |
|---|---:|---|
| schema result schemas | 0..8 | schema observation canonical key |
| schema result principals | 0..32 | principal observation canonical key |
| schema result memberships | 0..32 | role-membership canonical key |
| table result objects | 0..32 | schema-object canonical coordinate |
| module result core objects | 0..32 | schema-object canonical coordinate |
| module result binding modules | exactly 6 | Binding module ordinal/kind |
| certificate result shared certificates | exactly 2 | Security profile signer order |
| permission/closure path tuples | 0..256 each | resolved permission-path canonical bytes |
| signature result core signatures | 0..32 | module signature canonical key |
| signature result binding signatures | exactly 6 | Binding signature-intent order |
| binding-prefix inventory | 0..64 | `(schema, object_type, object_name, catalog_id)` |
| statement registry/results | exactly 7 | ordinal 1..7 |
| each certificate observation module-signature digests | 0..32 | digest bytes |
| each certificate observation permission-edge digests | 0..64 | digest bytes |
| each observed schema object trigger identities | 0..16 | `(portable_trigger_digest, object_id)` |
| each observed schema object signatures | 0..16 | upstream module-signature key |

Imported schema/certificate/permission/Binding leaves retain their already
implemented upstream field and nested-tuple validation. The foundation does
not weaken or duplicate those contracts. In addition, a recursive pre-
construction validator walks every reachable dataclass/tuple/list under an
imported leaf with identity-cycle detection. Every sequence not named in the
table is capped at 64 items, total reachable sequence members at 4096 and depth
at 16. A newly added upstream sequence is therefore bounded automatically and
must be added to the mutation inventory before acceptance; it cannot become an
unbounded hidden arm.

Exact encoded cap assignment:

| Encoded cap | Exact symbols |
|---:|---|
| 256 KiB leaf | `MssqlR1CertifiedServerBuildProfileV1`, `MssqlR1MigrationTargetRefV1`, `MssqlR1ObservedDatabaseIdentityV1`, `MssqlR1MigrationStatementRefV1`, `MssqlR1ProviderAttestationModuleDefinitionAuthorityV2`, `MssqlR1ObservedBindingModuleV1`, `MssqlR1ObservedBindingSignatureV1`, `MssqlR1ObservedBindingPrefixObjectV1`, `MssqlR1ObservedBindingSignerV2`, `MssqlR1ProviderAttestationFailureV2` |
| 2 MiB statement | `MssqlR1ProviderAttestationStatementAuthorityV2` |
| 3 MiB query/intermediate | `MssqlR1SchemaQueryResultV1`, `MssqlR1TableQueryResultV1`, `MssqlR1ModuleQueryResultV1`, `MssqlR1SignatureQueryResultV1`, `MssqlR1BindingPrefixInventoryQueryResultV1`, `MssqlR1CertificateQueryResultV2`, `MssqlR1PermissionQueryResultV2`, `MssqlR1PermissionClosureResultV2`, `MssqlR1BindingLiveCatalogIdentityV2`, `MssqlR1StableSharedSecurityAttestationV2` |
| 4 MiB post-install | `MssqlR1PostInstallStatementResultV2` |
| 3 MiB registry | `MssqlR1ProviderAttestationStatementRegistryV2` |
| 4 MiB stable schema | `MssqlR1StableSchemaAttestationV2` |
| 4 MiB stable binding | `MssqlR1StableBindingAttestationV2` |
| 12 MiB outer | `MssqlR1StableCatalogAttestationV2` |

Enums, the exception wrapper and pure functions have no canonical payload and
therefore no encoded cap. Every internal callable decoder uses exactly the row containing
its symbol. Golden boundary vectors cover 0, cap and cap+1 for each distinct
cap; an exactly-cap-sized malformed vector passes raw-size admission and then
fails decode, proving phase precedence.

`canonical_encoded_size_v1(domain, fields)` mirrors the existing V3 framing
length rules without materializing output. It is internal to the validation
module and is differential-tested against actual encoding for every model and
boundary vector. Factories use it before final allocation. The relevant upper
bound composition is:

```text
module statement ≤ 128 KiB query + 6 × 256 KiB module authority + framing
                 < 2 MiB
registry         ≤ 2 MiB module statement + 6 × 128 KiB query statements
                   + framing
                 < 3 MiB
outer allocation = canonical_encoded_size(actual validated intermediates)
                 ≤ 12 MiB before bytes are materialized
```

Intermediate maxima are independent admission ceilings, not a promise that all
can be saturated simultaneously. A combination whose prospective outer size
exceeds 12 MiB is not a legal aggregate and fails `semantic_bounds` before
outer allocation. Each individual maximum has a golden minimal enclosing
fixture that remains within its parent cap.

## Owned leaf ABI

The historical V1 names below are retained solely because approved Security V2
and Migration V2 already pin them. Their implementation lives in
`provider_attestation_*`, not Migration or Security. Approval of this document
is their first implementation authority.

### Enums and statement/query references

```python
MssqlR1MigrationStatementPhaseV1 = Literal[
    "pre_decision_observe",
    "post_decision_receipt_precheck",
    "post_decision_mutate",
    "post_decision_attest",
    "post_decision_receipt_append",
]

MssqlR1AttestationQueryKindV1 = Literal[
    "schema", "table", "module", "certificate", "permission",
    "signature", "binding_prefix_inventory",
]

MssqlR1AttestationResultAuthorityKindV1 = Literal[
    "schema_inventory", "table_inventory", "module_inventory",
    "certificate_inventory", "permission_inventory",
    "signature_inventory", "binding_prefix_inventory",
]

MssqlR1MigrationStatementRefV1(
    statement_id: str,
    statement_spec_digest: bytes,
    phase: MssqlR1MigrationStatementPhaseV1,
)
```

Domain: `dpone-r1-provider-migration-statement-ref-v1\0`. An attestation
statement always has phase `post_decision_attest`. Exact query-kind/result-kind
pairs are positional and closed:

```text
schema                   ↔ schema_inventory
table                    ↔ table_inventory
module                   ↔ module_inventory
certificate              ↔ certificate_inventory
permission               ↔ permission_inventory
signature                ↔ signature_inventory
binding_prefix_inventory ↔ binding_prefix_inventory
```

### Target and database identity

```python
MssqlR1CertifiedServerBuildProfileV1(
    profile_id: Literal["sqlserver-2022-standalone-r1"],
    product_major_version: Literal[16],
    product_version_prefix: Literal["16."],
    database_compatibility_level: Literal[160],
    ordered_allowed_product_levels: tuple[str, ...],
    ordered_allowed_engine_editions: tuple[int, ...],
)

MssqlR1MigrationTargetRefV1(
    identity_contract_version: Literal["dpone-mssql-target-physical-identity-1"],
    target_database_identity_digest: bytes,
)

MssqlR1ObservedDatabaseIdentityV1(
    server_instance_identity_sha256: bytes,
    database_id: int,
    database_guid: UUID,
    database_family_guid: UUID,
    recovery_fork_guid: UUID,
    database_name: str,
    database_name_digest: bytes,
    collation_name: str,
    compatibility_level: int,
    product_version: str,
    product_level: str,
    engine_edition: int,
    certified_server_build_profile: MssqlR1CertifiedServerBuildProfileV1,
)
```

Domains are respectively
`dpone-r1-certified-server-build-profile-v1\0`,
`dpone-r1-provider-migration-target-ref-v1\0` and
`dpone-r1-observed-database-identity-v1\0`.
Allowed product levels and engine editions are nonempty, canonical and bounded
to 16 members each. The database-name digest is SHA-256 of canonical NFC UTF-8
database name. Compatibility/product/build coordinates must satisfy the
embedded certified profile. The target database identity digest is derived by
the environment-owned target authority and compared. Its exact projection is:

```text
SHA256(canonical_bytes(
  b"dpone-r1-provider-target-database-identity-v1\0",
  (server_instance_identity_sha256, database_guid, database_family_guid,
   recovery_fork_guid, database_name_digest),
))
```

The observed database must reproduce `expected_target_ref` through this
equation; mutable display fields cannot substitute. Database and collation
names, product version/level and every allowed product-level member are 1..128
UTF-8 bytes. Database/catalog IDs and engine-edition members are positive SQL
integers; compatibility level is exactly the trusted build-profile value.

### Binding observation leaves

The obsolete `MssqlR1ObservedModuleOptionsV1` is not introduced. Observed
binding modules reuse the implemented `MssqlR1ModuleOptionsV3`, which is the
canonical schema-2 module-options authority.

```python
MssqlR1ObservedBindingModuleV1(
    module_kind: MssqlR1BindingModuleKindV1,
    schema_name: str,
    object_name: str,
    object_id: int,
    normalized_definition_bytes: bytes,
    persisted_options: MssqlR1ModuleOptionsV3,
)

MssqlR1ObservedBindingSignatureV1(
    signature_intent_digest: bytes,
    module_object_id: int,
    certificate_id: int,
    crypt_property_bytes: bytes,
)

MssqlR1ObservedBindingPrefixObjectV1(
    schema_name: str,
    object_name: str,
    object_type: Literal["procedure", "certificate", "database_principal"],
    catalog_id: int,
)
```

Domains:

```text
dpone-mssql-r1-observed-binding-module-v1\0
dpone-mssql-r1-observed-binding-signature-v1\0
dpone-mssql-r1-observed-binding-prefix-object-v1\0
```

`object_id`, `module_object_id`, `certificate_id` and `catalog_id` are positive.
Normalized definition bytes are nonempty and capped at 128 KiB; crypt-property
bytes are nonempty and capped at 4096 bytes. Schema/object/certificate/user
names use the identifier bound. A module kind/name must resolve to one of the
six semantic modules in the active Binding V2 pack. Its rendered definition
and persisted options are compared with the typed statement authority defined
below, because the portable Binding pack intentionally contains semantic
identity rather than rendered SQL bytes. A signature observation
uses the exact Binding V2 signature-intent digest; this replaces the undefined
historical `MssqlR1BindingSignatureEdgeV1`. Prefix inventory contains every
catalog object in the reserved binding namespace, including foreign objects;
its canonical key is `(schema_name, object_type, object_name, catalog_id)`.

## Query-result ABI

```python
MssqlR1SchemaQueryResultV1(
    database_identity: MssqlR1ObservedDatabaseIdentityV1,
    ordered_schemas: tuple[MssqlR1ObservedSchemaV3, ...],
    ordered_principals: tuple[MssqlR1ObservedPrincipalV3, ...],
    ordered_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...],
)

MssqlR1TableQueryResultV1(
    ordered_table_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
)

MssqlR1ModuleQueryResultV1(
    ordered_core_module_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
    ordered_binding_modules: tuple[MssqlR1ObservedBindingModuleV1, ...],
)

MssqlR1SignatureQueryResultV1(
    ordered_core_signatures: tuple[MssqlR1ModuleSignatureObservationV3, ...],
    ordered_binding_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...],
)

MssqlR1BindingPrefixInventoryQueryResultV1(
    ordered_binding_prefix_inventory: tuple[MssqlR1ObservedBindingPrefixObjectV1, ...],
)
```

Exact domains are:

```text
dpone-r1-schema-query-result-v1\0
dpone-r1-table-query-result-v1\0
dpone-r1-module-query-result-v1\0
dpone-r1-signature-query-result-v1\0
dpone-r1-binding-prefix-query-result-v1\0
```
The results are data-only finite catalog observations. They do not claim
authority without the enclosing V2 stable projection and active-authority
validation.

The Security V2 amendment types retain their exact approved layouts/domains:

```python
MssqlR1ObservedBindingSignerV2(
    target_binding_uuid: UUID,
    certificate_observation: MssqlR1CertificateCatalogObservationV1,
    certificate_id: int,
    certificate_owner_principal_id: int,
    certificate_user_principal_id: int,
)

MssqlR1CertificateQueryResultV2(
    ordered_shared_certificates: tuple[MssqlR1CertificateCatalogObservationV1, ...],
    binding_signer: MssqlR1ObservedBindingSignerV2,
)

MssqlR1PermissionQueryResultV2(
    ordered_observed_permission_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
)

MssqlR1PermissionClosureResultV2(
    observation_policy_digest: bytes,
    ordered_expected_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_observed_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_missing_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_forbidden_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
)
```

Certificate result contains exactly two shared certificates in Security V2
profile order. Permission tuples are capped by `max_permission_paths`.
`missing=expected-observed` and `forbidden=observed-expected` are recomputed;
successful stable attestation requires both empty.

```python
MssqlR1PostInstallStatementResultV2(
    statement_ref: MssqlR1MigrationStatementRefV1,
    attestation_kind: MssqlR1AttestationQueryKindV1,
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1,
    query_definition_digest: bytes,
    typed_result: (
        MssqlR1SchemaQueryResultV1 | MssqlR1TableQueryResultV1 |
        MssqlR1ModuleQueryResultV1 | MssqlR1CertificateQueryResultV2 |
        MssqlR1PermissionQueryResultV2 | MssqlR1SignatureQueryResultV1 |
        MssqlR1BindingPrefixInventoryQueryResultV1
    ),
)
```

Domain: `dpone-r1-post-install-statement-result-v2\0`. The statement phase,
kind pair and union arm must agree. The exact ordered seven-result registry is
`schema, table, module, certificate, permission, signature,
binding_prefix_inventory`; there is exactly one of each. The statement ID,
statement spec digest and query definition digest must equal the future
Renderer V2 registry authority. Until that registry exists this equality is a
required typed input to foundation construction and activation remains blocked.

### Typed statement-registry authority

The foundation owns the neutral handoff; Renderer later constructs it from its
approved closed registry but cannot replace it with a digest or boolean:

```python
MssqlR1ProviderAttestationModuleDefinitionAuthorityV2(
    module_kind: MssqlR1BindingModuleKindV1,
    semantic_module_digest: bytes,
    normalized_definition_bytes: bytes,
    persisted_options: MssqlR1ModuleOptionsV3,
)

MssqlR1ProviderAttestationStatementAuthorityV2(
    contract_version: Literal["dpone-r1-provider-attestation-statement-2"],
    ordinal: int,
    statement_ref: MssqlR1MigrationStatementRefV1,
    attestation_kind: MssqlR1AttestationQueryKindV1,
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1,
    query_definition_utf8: bytes,
    ordered_module_definitions: tuple[
        MssqlR1ProviderAttestationModuleDefinitionAuthorityV2, ...
    ],
)

MssqlR1ProviderAttestationStatementRegistryV2(
    contract_version: Literal["dpone-r1-provider-attestation-registry-2"],
    renderer_registry_digest: bytes,
    ordered_statements: tuple[MssqlR1ProviderAttestationStatementAuthorityV2, ...],
)
```

Domains:

```text
dpone-r1-provider-attestation-module-definition-v2\0
dpone-r1-provider-attestation-statement-v2\0
dpone-r1-provider-attestation-registry-v2\0
```

`query_definition_utf8` is opaque Renderer-produced normalized UTF-8/LF SQL
evidence, nonempty, has no NUL and is capped at 128 KiB. SQL rendering and
normalization grammar remain outside this foundation. Its SHA-256 must equal
each matching result's
query-definition digest. `statement_ref.statement_spec_digest` separately
binds the complete Renderer statement specification and must equal the exact
Renderer registry reference; the two digests are not conflated. The registry has
exact ordinals 1..7 in the query order above. Only the module statement carries
module definitions: exactly six, in Binding V2 module order; all other tuples
are empty. Each definition binds one exact Binding semantic-module digest and
contains the expected normalized definition/options bytes. The foundation
validates these bytes against the observed module result. The registry aggregate
contains exactly seven statements and structurally binds the Renderer registry
digest; stable construction accepts this object and not a second independent
digest. Its own digest is SHA-256 of its canonical bytes. The future provider
aggregate proves that Renderer produced this authority from its closed source.
Until then the embedded Renderer digest is a provisional content reference and
activation remains blocked. The foundation never accepts caller-selected
`renderer_valid=True`.

## Stable projections

`MssqlR1BindingLiveCatalogIdentityV2`,
`MssqlR1StableSchemaAttestationV2`,
`MssqlR1StableSharedSecurityAttestationV2`,
`MssqlR1StableBindingAttestationV2` and
`MssqlR1StableCatalogAttestationV2` retain the exact fields and canonical
domains in the approved Security V2 amendment. This section adds the missing
bounds, owner and construction rules.

The seven results are named `QS, QT, QM, QC, QP, QG, QB`. Construction consumes
every result field exactly once except the deliberate role-membership reuse:

```text
stable schema database identity  = projection(QS.database_identity)
stable schema schemas            = QS.ordered_schemas
stable schema principals         = QS.ordered_principals
stable schema memberships        = QS.ordered_role_memberships
stable schema objects            = stable_merge(QT.tables, QM.core_modules)
core object signatures           = QG.ordered_core_signatures

stable shared certificates       = QC.ordered_shared_certificates
stable binding database          = QS.database_identity
stable binding modules           = QM.ordered_binding_modules
stable binding signer            = QC.binding_signer
stable binding signatures        = QG.ordered_binding_signatures
stable binding prefix inventory  = QB.ordered_binding_prefix_inventory
permission observed paths        = QP.ordered_observed_permission_paths
```

`stable_merge` is a deterministic two-way merge over two independently
canonical, disjoint tuples keyed by the implemented schema-object canonical
coordinate. It rejects duplicate coordinates, wrong object kinds, overlap,
missing/extra objects and output not equal to the expected physical descriptor
core-object set. It never sorts unbounded caller input.

The stable-schema digest equations remain exactly those in the Security V2
amendment, including `projection_revision=1`, SHA-256 of expected contract and
principal authority bytes, schema inventory digest, principal/membership
inventory digest and live identity digest. Construction takes the exact source
`MssqlR1SchemaAttestationV3`; all retained non-time fields must match
byte-for-byte. `observed_at`, V1 permission tuple/security digest and the
time-bearing source attestation digest are excluded.

Authority validation takes objects, never bare digests:

```python
validate_provider_attestation_against_authorities_v2(
    *,
    active_physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
    active_security_profile: MssqlR1SharedInstallSecurityProfileV2,
    active_binding_pack: MssqlR1BindingModulePackV2,
    expected_target_ref: MssqlR1MigrationTargetRefV1,
    expected_build_profile: MssqlR1CertifiedServerBuildProfileV1,
    source_schema_attestation: MssqlR1SchemaAttestationV3,
    expected_statement_registry: MssqlR1ProviderAttestationStatementRegistryV2,
) -> None
```

Exact constructors are:

```python
MssqlR1StableSchemaAttestationV2.create(
    *,
    source_schema_attestation: MssqlR1SchemaAttestationV3,
    schema_query_result: MssqlR1SchemaQueryResultV1,
    table_query_result: MssqlR1TableQueryResultV1,
    module_query_result: MssqlR1ModuleQueryResultV1,
    signature_query_result: MssqlR1SignatureQueryResultV1,
    active_physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> MssqlR1StableSchemaAttestationV2

MssqlR1StableCatalogAttestationV2.create(
    *,
    expected_target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    physical_schema_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
    security_profile: MssqlR1SharedInstallSecurityProfileV2,
    binding_pack: MssqlR1BindingModulePackV2,
    expected_build_profile: MssqlR1CertifiedServerBuildProfileV1,
    statement_registry: MssqlR1ProviderAttestationStatementRegistryV2,
    ordered_statement_results: tuple[MssqlR1PostInstallStatementResultV2, ...],
    source_schema_attestation: MssqlR1SchemaAttestationV3,
) -> MssqlR1StableCatalogAttestationV2
```

Both factories either return the exact aggregate or raise the one typed closed
failure below. Stable construction copies `expected_target_ref` and the typed
registry's embedded Renderer digest; neither is accepted as an independent
second value. The target ref must equal the database-identity projection from
the environment-owned target registration authority at the later Migration/
provider composition boundary; foundation tests use an exact typed oracle and
activation remains blocked until that aggregate exists.

`provider_contract_digest` is likewise a structurally retained provisional
content reference in this foundation. It is not described as validated until
the future provider aggregate supplies and decodes the exact provider contract
object. Anti-splice claims in this child apply only to the exact descriptor,
Security, Binding, build, target-ref, schema-attestation and typed statement-
registry objects present in the factory signature.

For `expected_build_profile`, `expected_target_ref` and the registration fields
inside `source_schema_attestation`, this pure layer proves internal validity,
byte-for-byte reprojection and equality to the supplied typed objects. Their
environment/receipt provenance remains provisional until the future provider
composition authority obtains and validates them through their owning ports.
Caller possession alone never proves external origin.

`from_canonical_bytes` applies raw bounds first: zero or more than the model's
encoded cap is `attestation_bounds_exceeded`; an in-range malformed payload is
`attestation_domain_invalid`; decoded fields then receive semantic-bound
validation. `.create` accepts typed objects, validates counts/field sizes,
computes the exact prospective size, rejects it if above the model cap, then
constructs and encodes once. It asserts the emitted size equals the prospective
size and remains within cap. A zero-byte constructor output is impossible and tested as an
invariant. Every individual field maximum is valid in a minimal aggregate;
simultaneous maxima are additionally constrained by the named intermediate and
12 MiB outer caps.

It proves:

1. descriptor R2, Security V2 and Binding V2 payloads validate each other;
2. the observed database's complete build profile equals the independently
   environment-owned `expected_build_profile`; its product/build coordinates
   satisfy that profile, so observed input cannot choose its own allowlist;
3. all embedded authority digests and target/binding identities match;
4. the exact seven statement refs, definitions, kind pairs and result arms
   match the closed registry;
5. expected permission paths equal the canonical union of descriptor/Security
   projection and Binding V2 resolved paths;
6. observed paths equal `QP`, and closure missing/forbidden tuples are empty;
7. forbidden shared memberships are exactly the active Security predicate over
   `QS` and are empty;
8. QC contains the exact two shared signer observations plus one binding signer
   whose IDs, names, SID and thumbprint resolve in the same result;
9. module semantic identities, signatures and reserved-prefix inventory equal
   the active Binding expected inventory; normalized definition/options equal
   the matching typed statement-authority entries; no foreign/second binding
   artifact exists;
10. every retained stable-schema field reprojects byte-for-byte from the exact
    `source_schema_attestation`, including registration and physical authority;
11. every nested tuple and payload respects the fixed bounds.

Ordinary canonical decode proves only self-contained structure. It cannot
substitute for this authority-aware validation.

## Failures and compatibility

Closed failure carrier:

```python
MssqlR1ProviderAttestationFailureV2(
    reason: MssqlR1ProviderAttestationFailureReasonV2,
    phase: Literal[
        "raw_bounds", "decode", "semantic_bounds", "registry",
        "projection", "authority",
    ],
    recovery_action: MssqlR1ProviderAttestationRecoveryActionV2,
    redacted_message: str,
)

class MssqlR1ProviderAttestationError(MssqlR1V3ContractError):
    failure: MssqlR1ProviderAttestationFailureV2
```

Domain: `dpone-r1-provider-attestation-failure-v2\0`.
`MssqlR1ProviderAttestationError` is the exact raised wrapper; its sole public
attribute is `failure`, and `str(error)` equals `failure.redacted_message`.
Precedence is `raw bounds → decode → semantic bounds → registry → projection →
authority`; within a phase the table order below wins. The message
is the exact literal shown and never includes catalog IDs, names, SQL or input
exception text.

| Reason | Phase | Recovery action | Exact redacted message |
|---|---|---|---|
| `attestation_domain_invalid` | decode | `regenerate_attestation_input` | `Attestation bytes do not match the required contract.` |
| `attestation_bounds_exceeded` | `raw_bounds` when byte length is 0 or above cap | `reduce_observation_scope` | `Attestation input exceeds the certified bound.` |
| `attestation_bounds_exceeded` | `semantic_bounds` for decoded/typed count, field, depth, total-member or prospective-size overflow | `reduce_observation_scope` | `Attestation input exceeds the certified bound.` |
| `attestation_registry_mismatch` | registry | `reconcile_renderer_registry` | `Attestation statement registry does not match the approved provider.` |
| `attestation_result_arm_mismatch` | registry | `regenerate_attestation_input` | `Attestation result kind does not match its statement.` |
| `attestation_projection_mismatch` | projection | `regenerate_attestation_input` | `Stable attestation projection is inconsistent.` |
| `attestation_schema_inventory_mismatch` | projection | `repair_or_rebaseline_provider` | `Observed schema inventory differs from the approved provider.` |
| `attestation_permission_closure_mismatch` | authority | `repair_or_rebaseline_provider` | `Observed permissions differ from the approved provider.` |
| `attestation_certificate_mismatch` | authority | `repair_or_rebaseline_provider` | `Observed certificate identity differs from the approved provider.` |
| `attestation_binding_inventory_mismatch` | authority | `repair_or_rebaseline_provider` | `Observed binding inventory differs from the approved provider.` |
| `attestation_target_identity_mismatch` | authority | `select_supported_target` | `Target identity differs from the approved target.` |
| `attestation_authority_splice` | authority | `regenerate_attestation_input` | `Attestation authorities do not belong to one provider generation.` |

Recovery actions are the closed literals shown in the table. The foundation
does not execute them; Migration maps them to its own blocker/reporting layer.

Closed failure reasons:

```text
attestation_domain_invalid
attestation_bounds_exceeded
attestation_registry_mismatch
attestation_result_arm_mismatch
attestation_projection_mismatch
attestation_authority_splice
attestation_schema_inventory_mismatch
attestation_permission_closure_mismatch
attestation_certificate_mismatch
attestation_binding_inventory_mismatch
attestation_target_identity_mismatch
```

V1 certificate, permission, post-install and stable
aggregate bytes reject at every V2 boundary. Only the explicitly listed leaf
V1 domains are reusable. Their bytes are not interpreted as any V2 aggregate.

Migration V2 imports target ref, build profile, statement ref and all stable
attestation types from this foundation. Migration must remove those symbols
from its planned ownership map. Security and Binding never import this
foundation. Renderer V2 may consume the neutral statement authority but may
not own or reconstruct it.

## Components and dependency direction

| Module | Exact owned symbols | May import |
|---|---|---|
| `mssql_r1_v3_provider_attestation_enums.py` | `MssqlR1MigrationStatementPhaseV1`, `MssqlR1AttestationQueryKindV1`, `MssqlR1AttestationResultAuthorityKindV1`, `MssqlR1ProviderAttestationFailureReasonV2`, `MssqlR1ProviderAttestationRecoveryActionV2` | codec validation only |
| `mssql_r1_v3_provider_attestation_identity.py` | `MssqlR1CertifiedServerBuildProfileV1`, `MssqlR1MigrationTargetRefV1`, `MssqlR1ObservedDatabaseIdentityV1`, `MssqlR1MigrationStatementRefV1`, `MssqlR1ProviderAttestationModuleDefinitionAuthorityV2`, `MssqlR1ProviderAttestationStatementAuthorityV2`, `MssqlR1ProviderAttestationStatementRegistryV2` | enums, schema module options, Binding module-kind enum |
| `mssql_r1_v3_provider_attestation_binding_observation.py` | `MssqlR1ObservedBindingModuleV1`, `MssqlR1ObservedBindingSignatureV1`, `MssqlR1ObservedBindingPrefixObjectV1`, `MssqlR1ObservedBindingSignerV2` | identity, schema observations, Binding/Security leaves |
| `mssql_r1_v3_provider_attestation_query_results.py` | `MssqlR1SchemaQueryResultV1`, `MssqlR1TableQueryResultV1`, `MssqlR1ModuleQueryResultV1`, `MssqlR1SignatureQueryResultV1`, `MssqlR1BindingPrefixInventoryQueryResultV1`, `MssqlR1CertificateQueryResultV2`, `MssqlR1PermissionQueryResultV2`, `MssqlR1PostInstallStatementResultV2` | lower foundation modules plus exact schema/Security leaves |
| `mssql_r1_v3_provider_attestation_stable_schema.py` | `MssqlR1StableSchemaAttestationV2`, its `.create`, `stable_merge_provider_schema_v2` | query results, physical descriptor, schema attestation |
| `mssql_r1_v3_provider_attestation_stable_catalog.py` | `MssqlR1PermissionClosureResultV2`, `MssqlR1BindingLiveCatalogIdentityV2`, `MssqlR1StableSharedSecurityAttestationV2`, `MssqlR1StableBindingAttestationV2`, `MssqlR1StableCatalogAttestationV2` and its `.create` | lower foundation models |
| `mssql_r1_v3_provider_attestation_errors.py` | `MssqlR1ProviderAttestationFailureV2`, `MssqlR1ProviderAttestationError` | enums and codec error base only |
| `mssql_r1_v3_provider_attestation_validation.py` | `preflight_provider_attestation_canonical_v2`, `canonical_encoded_size_v1`, `validate_provider_attestation_against_authorities_v2` | all foundation models plus descriptor/Security/Binding/schema authorities |

Each module is independently cohesive and at most 400 SLOC. Imports point
directly to defining modules. No facade-only module, re-export tunnel, package
`__init__` export, dynamic registry, service locator or import-time I/O is
allowed.

## Red-green implementation plan

1. Approve this specification, the evidence protocol and ADR 0071 on an exact
   reviewed lineage.
2. Create a self-pinned RED-only task contract; production paths are forbidden.
3. The RED task commits its fixtures, oracle, probe, case registry and five
   independently failing production-facing partitions:
   canonical ABI; seven query arms; stable schema; stable catalog authority;
   mutations/bounds/compatibility.
4. A reviewed measurement amendment to the task pins the RED-assets commit,
   case-registry SHA, exact node IDs/counts and expected failing diagnostics.
5. A disjoint evidence-protocol task first commits only its evidence meta-test
   as a collecting RED, then adds only its schema and producer in the direct
   GREEN child commit. Frozen RED assets are read-only throughout. It generates
   the retained create-only RED artifact; the earlier probe output is
   stdout-only and never retained.
6. Implement only the eight exact foundation modules in the ownership table;
   existing Security, Binding,
   schema and Migration modules remain read-only.
7. Generate exact-commit GREEN evidence, run focused and broad checks, then obtain
   fresh architecture, test/certification, docs/UX and release reviews.
8. Re-align Migration V2 dependency pins and recreate its RED authority.

The RED suite owns disjoint test/support paths and contains no general import
gate. Every production-facing partition must fail independently on the pinned
base because its owning implementation behavior is absent.

## Testing and evidence

Required hermetic partitions:

| Partition | Required proof |
|---|---|
| Canonical ABI | exact classes/fields/domains, frozen/slotted, round trip, unknown/trailing/cross-domain rejection |
| Query arms | seven exact pairs, order, occurrence, union arm, signer identity closure |
| Stable schema | all digest equations, revision 1, source projection, merge coverage/disjointness |
| Stable catalog | object-level authority validation, permission algebra, certificates, inventories, anti-splice |
| Mutation/bounds | every field/tag/union arm classified; 0/limit/limit+1; V1 aggregate rejection |

Recursive boundary vectors use root depth `0`; entering any sequence increments
depth by one, while traversing a dataclass field does not. Every sequence-member
occurrence contributes one to the total even if the same immutable object is
referenced twice; an object identity already on the active recursion stack is a
cycle and rejects. Default unnamed-sequence vectors are exactly `63/64/65`,
total-member vectors `4095/4096/4097`, and depth vectors `15/16/17`; the first
two values in each trio admit and the last rejects, provided stricter named caps
also hold.

Model properties:

```text
never validate a stable aggregate from digests alone
never accept a query result under the wrong statement arm
never omit, duplicate or ignore an observed field
never hide a forbidden permission or membership
never accept a second/foreign binding artifact
never introduce Security → Binding/Migration dependency
```

Evidence is exact-commit-bound and create-only. It records specification SHA,
task authority SHA, case-registry SHA, test-tree SHA, ordered node IDs/counts,
outcome counts, failing node IDs for RED, mutation coverage and raw architecture
metrics. `SKIP`, stale, mocked or unavailable checks never become `PASS`.

The separately reviewed
[Provider Attestation V2 evidence protocol](feature-design-postgres-mssql-r1-v3-provider-attestation-evidence-v1.md)
defines the cross-task evidence contract: the RED task owns the case registry
and probe, while the evidence task owns its meta-test, schema and producer
through an independent RED/GREEN lineage. It must be
maintainer-`APPROVED` before a retained RED artifact is generated.

Exact RED test paths are:

```text
tests/support/postgres_mssql_r1_v3_provider_attestation_v2_fixtures.py
tests/support/postgres_mssql_r1_v3_provider_attestation_v2_oracle.py
tests/support/postgres_mssql_r1_v3_provider_attestation_v2_red_probe.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_abi.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_query_results.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_schema.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_catalog.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_mutations.py
```

Focused checks include the five new suites plus existing Security V2, Binding
V2, physical descriptor and schema contracts. Broad checks are Ruff, formatting,
Mypy, import rules, architecture fitness, module-size/layer metrics and all
non-live tests. The live gate is `N/A` for this pure contract task; actual SQL
Server behavior and certification remain `UNVERIFIED`, and activation is blocked.

## Documentation, rollout and rollback

This is an internal contract boundary; end-user manifest, CLI and CJM are
unchanged (`N/A`). Maintainer implementation maps and ADR index are updated.
All paths are repo-relative.

## Market comparison

`N/A`: this child is a pure internal canonical ABI and dependency boundary. It
does not expose a connector behavior, throughput claim or user workflow that
can be meaningfully compared with dlt, Informatica, Airbyte, Fivetran, Pentaho,
SSIS, gusty, Astronomer Cosmos or Apache Beam. Product comparison remains owned by the Industrial
V7 route/release specification; inventing a market advantage for this internal
refactor would be misleading.

Rollout is additive and activation-blocked:

```text
approved foundation
→ RED evidence
→ GREEN pure contracts
→ exact acceptance evidence
→ Migration V2 dependency realignment
```

Rollback removes the unreferenced internal modules before Migration consumes
them. After Migration consumes the canonical bytes, rollback requires a reader-
compatible binary; no V1 aggregate fallback or dual-write is allowed.

## Definition of Done

1. This specification and the evidence protocol are maintainer-`APPROVED`, and
   ADR 0071 is `Accepted`, on an exact reviewed lineage.
2. Every owned leaf and V2 aggregate has an exact field/domain/bound contract.
3. Dependency direction is Descriptor/Security/Binding → Foundation → Migration.
4. All five RED partitions fail independently before production code.
5. All production modules remain within quality budgets and contain no I/O.
6. Stable decode is structural; authority validation requires exact objects.
7. Seven result arms are complete, ordered and consumed without omission.
8. Permission/certificate/binding/schema splices fail closed.
9. Exact-commit evidence and four fresh reviews are `GO`.
10. Public API/CLI/manifest remain unchanged; activation and live remain blocked.

Back: [provider implementation map](developer-postgres-mssql-r1-v3-provider-implementation.md).
Next after accepted GREEN evidence: [Migration V2](feature-design-postgres-mssql-r1-v3-provider-migration-contract-v2.md).
