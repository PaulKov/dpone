<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider binding contract

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: exact core descriptor, shared-security contract, registered-target
  authority and canonical `PostgresMssqlTypePolicy` commits
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.
  The future binding implementation must use the V2 lifecycle and permission-
  closure authority.

## Outcome and boundary

This child specification turns six static binding templates into one exact,
target-specific portable pack and one separate live catalog attestation:

```text
registered target + canonical type policy
→ typed target mapping
→ instantiated stage-scan results
→ module-local buffer plans
→ six module definitions
→ binding signer and minimal permissions
→ portable pack
→ live attestation
```

It never compiles, receives or constructs a dynamic stage object identifier.
Binding modules obtain stage rows only through the shared signed
`dpone_scan_stage_v3` and materialize them into module-local table variables.

In scope are pure canonical contracts and hermetic tests. SQL rendering,
catalog queries, installation, execution and performance certification are
separate. Public CLI, manifest and Python API do not change.

Implementation is blocked until an exact canonical PostgreSQL→MSSQL type-policy
contract exists. A free-form type name or opaque unresolvable policy digest is
not an acceptable substitute.

## Canonical rules

All models are frozen slotted dataclasses with exact enums and tuples.

```text
canonical_bytes = canonical_bytes(EXACT_DOMAIN, ordered_fields)
digest = SHA256(canonical_bytes)
```

Own digests are derived properties, never constructor fields. External digest
references resolve against embedded or aggregate-supplied canonical authority.
Semantic sets are canonical and duplicate-free; ordered collections use
contiguous ordinals. MSSQL names reject exact and casefold collisions.

## Target and type-policy mapping

Closed R1 values:

```text
TargetObjectProfile: ordinary_disk_rowstore
StageArtifactKind: batch_payload | xmin_delta | xmin_complete_keys
StageBufferSymbol:
  batch_payload | xmin_delta | xmin_complete_keys
BindingPermissionProfile: exact_target_and_row_hash_v1
```

```python
MssqlR1RegisteredTargetRefV1(
    target_binding_uuid: UUID,
    target_object_uuid: UUID,
    resource_ref: MssqlR1PhysicalResourceRefV1,
    schema_name: str,
    object_name: str,
    object_profile: MssqlR1TargetObjectProfileV1,
    registered_target_authority_payload: bytes,
    registered_target_authority_digest: bytes,
)
```

Domain: `dpone-mssql-r1-registered-target-ref-v1\0`. The digest is a validated
content digest of the payload; every semantic coordinate byte-reproduces the
registered authority. The target generation is intentionally absent and is
checked by the writer fence at execution.

```python
MssqlR1BusinessColumnMappingV1(
    ordinal: int,
    source_column_ref: PostgresMssqlSourceColumnRefV1,
    target_column_ref: MssqlR1RegisteredTargetColumnRefV1,
    type_decision_id: str,
    key_ordinal: int | None,
)
```

Domain: `dpone-mssql-r1-business-column-mapping-v1\0`. Both refs carry exact
identifier, ordinal, nullability and canonical source/target scalar shape;
target refs additionally carry length, precision, scale and collation where
applicable. They resolve byte-identically to the type-policy decision and
registered-target authority. Source/target names are exact- and
casefold-unique and business target names are casefold-disjoint from the fixed
stage suffix. Key ordinals are either NULL or contiguous one-based values and
may reference only non-null columns.

```python
MssqlR1BindingTargetMappingV1(
    contract_version: Literal["dpone-mssql-r1-binding-target-mapping-1"],
    registered_target: MssqlR1RegisteredTargetRefV1,
    type_policy_authority_payload: bytes,
    type_policy_authority_digest: bytes,
    ordered_columns: tuple[MssqlR1BusinessColumnMappingV1, ...],
)
```

Domain: `dpone-mssql-r1-binding-target-mapping-v1\0`. The type-policy payload
decodes as the exact canonical `PostgresMssqlTypePolicy` version. Every decision
ID resolves exactly once and supplies stage/target SQL shapes, codec,
normalization, loss, equality and hash policy. At least one non-null key column
is required. Mapping cannot restate or weaken a resolved decision.

## Instantiated stage-scan authority

```python
MssqlR1StageScanColumnV1(
    ordinal: int,
    name: str,
    sql_shape: MssqlR1CanonicalSqlScalarShapeV1,
    maximum_length: int,
    precision: int,
    scale: int,
    nullable: bool,
    role: MssqlR1StageScanColumnRoleV1,
    type_decision_id: str | None,
)
```

Roles are `business|system`. Business columns require a type decision; system
columns require NULL. Domain: `dpone-mssql-r1-stage-scan-column-v1\0`.
`MssqlR1CanonicalSqlScalarShapeV1` is a closed type/facet/collation value from
the canonical PostgreSQL→MSSQL type-policy contract; no free-form SQL type
text is accepted.

The exact fixed system suffix is:

```text
__dpone_artifact_id        uniqueidentifier NOT NULL
__dpone_chunk_sequence     int              NOT NULL
__dpone_chunk_key          binary(32)       NOT NULL
__dpone_load_token         uniqueidentifier NOT NULL
__dpone_chunk_row_ordinal  int              NOT NULL
```

```python
MssqlR1InstantiatedStageScanResultV1(
    artifact_kind: MssqlR1StageArtifactKindV1,
    core_scan_procedure_ref: MssqlR1PhysicalResourceRefV1,
    core_scan_template_digest: bytes,
    ordered_columns: tuple[MssqlR1StageScanColumnV1, ...],
    result_cardinality: Literal["zero_or_many"],
)
```

Domain: `dpone-mssql-r1-instantiated-stage-scan-result-v1\0`.

```text
business(batch_payload)      = all mapped business columns
business(xmin_delta)         = all mapped business columns
business(xmin_complete_keys) = mapped key columns only

instantiated result = business(kind) + fixed suffix
```

The core scan procedure/template resolve inside the exact descriptor; no extra,
missing, reordered or retyped column is legal.

## Module-local stage buffers

```python
MssqlR1BindingScalarParameterBindingV1(
    ordinal: int,
    scan_parameter_ref: MssqlR1PhysicalParameterRefV1,
    value_source: (
        MssqlR1BindingModuleParameterRefV1
        | MssqlR1BindingSessionFieldRefV1
    ),
    scalar_shape: MssqlR1CanonicalSqlScalarShapeV1,
    value_cardinality: MssqlR1ValueCardinalityV1,
    codec_ref: MssqlR1RequestCodecRefV1,
)
```

Domain: `dpone-mssql-r1-binding-scalar-parameter-binding-v1\0`. Ordinals are
contiguous one-based. Each required core scan input parameter occurs exactly
once and no other parameter occurs. Source and destination scalar kind,
nullability, cardinality and codec are byte-identical; a module-parameter or
session-field source must resolve in the owning module execution contract.

```python
MssqlR1ModuleStageInputV1(
    ordinal: int,
    artifact_kind: MssqlR1StageArtifactKindV1,
    buffer_symbol: MssqlR1StageBufferSymbolV1,
    instantiated_result_digest: bytes,
    scan_procedure_parameter_bindings: tuple[
        MssqlR1BindingScalarParameterBindingV1, ...
    ],
)
MssqlR1ModuleStageBufferPlanV1(
    module_kind: MssqlR1BindingModuleKindV1,
    ordered_inputs: tuple[MssqlR1ModuleStageInputV1, ...],
)
```

Domains: `dpone-mssql-r1-module-stage-{input|buffer-plan}-v1\0`.
Parameter bindings reference exact core scan input parameters and exact binding
module parameters; no example parameter name becomes implicit authority.

Fixed symbols render only as module-local table variables:

```text
batch_payload      → @dpone_batch_payload
xmin_delta         → @dpone_xmin_delta
xmin_complete_keys → @dpone_xmin_complete_keys
```

The renderer must produce the semantic equivalent of:

```sql
DECLARE @dpone_batch_payload TABLE (... exact instantiated columns ...);
INSERT INTO @dpone_batch_payload (... exact ordered names ...)
EXEC dpone_authority.dpone_scan_stage_v3 ... exact bound parameters ...;
```

The scan procedure cannot itself use nested `INSERT EXEC`. A buffer is declared
and populated once. Module parameters contain no schema/table/stage identifier;
dynamic SQL is forbidden. Matrix:

| Module family | Required buffers |
|---|---|
| Batch mutate/hash/quality | batch payload |
| XMin mutate/hash/quality | XMin delta, XMin complete keys |

Table-variable suitability for realistic payload sizes is certification-bound;
no GA performance claim exists before its benchmark.

## Binding modules, signer and permissions

```python
MssqlR1InstantiatedBindingModuleV1(
    module_kind: MssqlR1BindingModuleKindV1,
    schema_name: str,
    object_name: str,
    template_digest: bytes,
    ordered_parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
    result_contract: MssqlR1ProcedureResultContractV3,
    buffer_plan: MssqlR1ModuleStageBufferPlanV1,
    definition_bytes: bytes,
    definition_digest: bytes,
    ordered_read_set: tuple[MssqlR1PhysicalResourceAccessV1, ...],
    ordered_write_set: tuple[MssqlR1PhysicalResourceAccessV1, ...],
)
```

Domain: `dpone-mssql-r1-instantiated-binding-module-v1\0`. Definition digest is
the existing validated module-definition content digest. The six kinds occur
once in exact order and reproduce their core templates, target mapping, buffer
plans and caller-UoW execution semantics.

```python
MssqlR1BindingSignerIdentityV1(
    target_binding_uuid: UUID,
    lifecycle_policy_digest: bytes,
    certificate_name: str,
    certificate_user_name: str,
    certificate_subject: str,
    certificate_owner: MssqlR1EnvironmentPrincipalRefV1,
    start_date_yyyymmdd: str,
    expiry_date_yyyymmdd: str,
    certificate_creation_profile: MssqlR1CertificateCreationProfileV1,
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1,
    secret_policy_digest: bytes,
    ordered_module_definition_digests: tuple[bytes, ...],
    create_certificate_template: MssqlR1SecretSqlTemplateV1,
    ordered_add_signature_templates: tuple[MssqlR1SecretSqlTemplateV1, ...],
)
```

Domain: `dpone-mssql-r1-binding-signer-identity-v1\0`. It is the sole
instantiated binding-signer model and resolves one exact shared-security
`MssqlR1BindingSignerLifecyclePolicyV1`. The only naming placeholder is
`{binding_uuid_hex}` and expands to 32 lowercase hex digits:

```text
dpone_b_{binding_uuid_hex}_cert_v3
dpone_b_{binding_uuid_hex}_cert_user_v3
dpone R1 V3 binding {binding_uuid_hex}
```

Instantiation resolves the UUID-dependent certificate/user/module identifiers
to literal SQL identifier bytes before constructing these seven templates. The
create template and one add-signature template per module contain only the one
ephemeral-secret placeholder allowed by `MssqlR1SecretSqlTemplateV1`; there is
no remaining identifier placeholder. Template order equals module-definition
order and all template bytes are embedded in the portable signer identity, so
the renderer never resolves a bare digest.

```python
MssqlR1BindingPermissionEdgeV1(
    ordinal: int,
    subject: (
        MssqlR1BindingSignerSubjectRefV1
        | MssqlR1ResolvedRuntimePrincipalRefV1
    ),
    access_kind: MssqlR1AccessKindV1,
    target_resource: MssqlR1PhysicalResourceRefV1,
    permission_scope: MssqlR1BindingPermissionScopeV1,
    grant_option: Literal[False],
)
MssqlR1BindingSignatureEdgeV1(
    ordinal: int,
    module_kind: MssqlR1BindingModuleKindV1,
    module_definition_digest: bytes,
    signer_identity_digest: bytes,
)
MssqlR1BindingPermissionContractV1(
    contract_version: Literal["dpone-mssql-r1-binding-permission-1"],
    permission_profile: MssqlR1BindingPermissionProfileV1,
    ordered_permission_edges: tuple[MssqlR1BindingPermissionEdgeV1, ...],
    ordered_signature_edges: tuple[MssqlR1BindingSignatureEdgeV1, ...],
)
```

Domains use `dpone-mssql-r1-binding-{permission-edge|signature-edge|permission-contract}-v1\0`.

```text
required binding-certificate permissions =
minimal SQL permission projection of the union of target and row-hash
resource accesses in the six modules
```

There are exactly six signature edges. Runtime executes six modules and the
shared scan procedure. The binding certificate receives no stage access,
authority-table DML except exact derived row-hash access, schema-wide DML,
grant option, role membership or redundant permission edge. Runtime EXECUTE is
represented by explicit runtime-subject edges, not inferred from a parallel
module-kind tuple. The closed projection maps each declared resource access to
one exact SQL permission/scope pair; no weaker or wider scope is admitted.

## Portable pack

The companion's top-level static authority is:

```python
MssqlR1BindingInstantiationContractV1(
    contract_version: Literal["dpone-mssql-r1-binding-instantiation-1"],
    physical_descriptor_digest: bytes,
    shared_security_profile_digest: bytes,
    type_policy_authority_digest: bytes,
    registered_target_contract_digest: bytes,
    ordered_binding_template_refs: tuple[MssqlR1BindingTemplateRefV1, ...],
    fixed_stage_suffix: tuple[MssqlR1StageScanColumnV1, ...],
    ordered_buffer_matrix: tuple[MssqlR1ModuleStageBufferPlanV1, ...],
    compiler_policy: MssqlR1BindingCompilerPolicyV1,
    signer_lifecycle_policy: MssqlR1BindingSignerLifecyclePolicyV1,
    permission_projection_policy: MssqlR1BindingPermissionProjectionPolicyV1,
    attestation_policy: MssqlR1BindingAttestationPolicyV1,
)
```

Domain: `dpone-mssql-r1-binding-instantiation-contract-v1\0`. The six template
refs occur once in canonical module-kind order; suffix/buffer matrix is exact;
every referenced policy is embedded or resolves by exact 32-byte digest. This
aggregate, rather than the renderer registry, owns all reusable binding
instantiation semantics.

```python
MssqlR1BindingInstantiationInputV1(
    registration_payload: bytes,
    registered_target_authority_payload: bytes,
    target_mapping: MssqlR1BindingTargetMappingV1,
    ordered_stage_scan_results: tuple[
        MssqlR1InstantiatedStageScanResultV1, ...
    ],
    target_contract_revision: int,
)
MssqlR1BindingPortableIdentityV1(
    target_mapping: MssqlR1BindingTargetMappingV1,
    signer_identity: MssqlR1BindingSignerIdentityV1,
    ordered_module_definitions: tuple[MssqlR1InstantiatedBindingModuleV1, ...],
    permission_contract: MssqlR1BindingPermissionContractV1,
)
MssqlR1BindingModulePackV1(
    contract_version: Literal["dpone-mssql-r1-binding-pack-1"],
    physical_descriptor_payload: bytes,
    shared_security_profile_payload: bytes,
    binding_contract_payload: bytes,
    instantiation_input: MssqlR1BindingInstantiationInputV1,
    portable_identity: MssqlR1BindingPortableIdentityV1,
)
```

Domains use `dpone-mssql-r1-binding-{instantiation-input|portable-identity|pack}-v1\0`.
All payloads decode as their exact contracts. Every core/type/target/security
reference resolves within those payloads; all binding UUIDs equal; all six
module kinds occur once; permission/signature edges reference only this pack.
The pack contains no catalog IDs, SID, thumbprint, deployment time or own digest.
`binding_contract_payload` decodes exactly as
`MssqlR1BindingInstantiationContractV1`; a free-form or renderer-owned substitute
is rejected.

## Live attestation

Binding-specific observations do not reuse the schema-2 shared-signer enum:

```python
MssqlR1ObservedBindingModuleV1(
    module_kind: MssqlR1BindingModuleKindV1,
    schema_name: str,
    object_name: str,
    object_id: int,
    normalized_definition_bytes: bytes,
    persisted_options: MssqlR1ObservedModuleOptionsV1,
)
MssqlR1ObservedBindingSignerV1(
    target_binding_uuid: UUID,
    certificate_name: str,
    certificate_id: int,
    certificate_thumbprint: bytes,
    certificate_subject: str,
    start_date_yyyymmdd: str,
    expiry_date_yyyymmdd: str,
    certificate_owner_principal_id: int,
    certificate_user_name: str,
    principal_id: int,
    sid_bytes: bytes,
    private_key_present: bool,
)
MssqlR1ObservedBindingPermissionV1(
    semantic_edge: MssqlR1BindingPermissionEdgeV1,
    catalog_coordinate: MssqlR1ObservedPermissionCoordinateV1,
    grantor_principal_id: int,
)
MssqlR1ObservedBindingSignatureV1(
    semantic_edge: MssqlR1BindingSignatureEdgeV1,
    module_object_id: int,
    certificate_id: int,
    crypt_property_bytes: bytes,
)
```

Domains use `dpone-mssql-r1-observed-binding-{module|signer|permission|signature}-v1\0`.

```python
MssqlR1BindingLiveCatalogIdentityV1(
    database_identity: MssqlR1ObservedDatabaseIdentityV1,
    ordered_modules: tuple[MssqlR1ObservedBindingModuleV1, ...],
    signer: MssqlR1ObservedBindingSignerV1,
    ordered_permissions: tuple[MssqlR1ObservedBindingPermissionV1, ...],
    ordered_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...],
    ordered_binding_prefix_inventory: tuple[
        MssqlR1ObservedBindingPrefixObjectV1, ...
    ],
)
MssqlR1BindingAttestationV1(
    contract_version: Literal["dpone-mssql-r1-binding-attestation-1"],
    binding_pack_payload: bytes,
    live_catalog_identity: MssqlR1BindingLiveCatalogIdentityV1,
    observed_at: str,
)
```

Domains: `dpone-mssql-r1-binding-{live-catalog-identity|attestation}-v1\0`.
Every nested observation above is a complete typed payload with its own literal
domain and digest equation; opaque `*_digest` fields are forbidden when the
provider is the authority that observed the bytes. Database identity includes
database UUID/name/collation and server product/build coordinates. Module
options include ANSI_NULLS, QUOTED_IDENTIFIER, execute-as, schema owner and
object type. Permission coordinates include class, major/minor IDs, permission,
state, grant option and grantee/grantor IDs.

The attestor deterministically projects a portable identity from the typed live
observations and requires that projection to equal the pack portable identity;
the caller cannot supply the projected value. Prefix inventory is the complete
catalog result for the exact binding certificate/user/module name prefixes and
is sorted by `(schema_name.casefold(), object_name.casefold(), object_type,
object_id)`. It proves the expected closed set and rejects every unexpected
object. Attestation thereby proves exactly six modules/signatures, exact
permissions, private key absent, no stage grants and no unexpected
binding-prefixed objects.

## Module DAG and implementation split

```text
binding_enums / binding_primitives
    -> binding_target_mapping
    -> binding_stage_contract
    -> binding_module_contract
shared_security lifecycle policy
    -> binding_signer_contract
binding modules + signer
    -> binding_permission_contract
all portable leaves
    -> binding_pack
typed catalog observations
    -> binding_attestation
```

Writer tasks own these paths as disjoint groups: mapping/stage; module/pack;
signer/permission; attestation. Only the integrator may change shared registries,
composition roots, package exports or provider umbrella specifications.
The security package is the sole owner of
`MssqlR1BindingSignerLifecyclePolicyV1`; binding imports it and is the sole owner
of instantiated signer identity/templates/edges. No reverse security→binding
import exists.

## Validation and approval

Hermetic tests cover every field/enum/union, mapping/type-policy splices,
casefold/key ordinal errors, all three stage shapes and fixed suffix, buffer
matrix, dynamic identifier injection, module/template mismatch, permission
projection widening, missing/extra signature, shared/binding signer splice,
portable/live identity separation and canonical round-trip. Mutation registry
classifies `must_reject` versus `valid_distinct`; a security/correctness weakening
is always reject.

Live SQL definition, permission, signature, catalog and table-variable
performance checks remain `UNVERIFIED`.

Implementation requires pinned exact dependency commits, fresh architecture/
security/test review, maintainer `APPROVED` status and disjoint task contracts
for mapping/stage, module/pack/security and attestation. Persisted V1 bytes are
immutable; successors use new domains.

## Approval checklist

- [x] Dynamic stage identifiers cannot enter binding modules.
- [x] Stage result and local buffer shapes are exact.
- [x] Binding signer has no direct stage permission.
- [x] Portable and live identities are separate.
- [x] Own digest recursion is absent.
- [ ] Canonical type-policy and registered-target dependencies are pinned.
- [ ] Core/security dependency commits are pinned.
- [ ] Fresh reviews approve the exact specification.
- [ ] Maintainer changes status to `APPROVED`.
