<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider security authority amendment V2

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Approved by: dpone maintainer implementation authorization, 2026-09-05
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Amends:
  `feature-design-postgres-mssql-r1-v3-physical-descriptor-contract-v1.md`
  and `feature-design-postgres-mssql-r1-v3-provider-security-contract-v1.md`

## Outcome and boundary

Fresh implementation review found three missing authorities in the approved R1
contracts: no physical resource was assigned to the provider-install receipt,
the forbidden-permission tuple could never prove the complement of allowed SQL
Server permissions, and certificate replay could not compare a receipt-pinned
lifecycle digest with the active lifecycle authority.

This amendment closes only those gaps. It adds one immutable table to the pure
physical descriptor and versions the internal, activation-blocked security
contracts. It does not add SQL rendering, database I/O, a public import,
manifest/CLI behavior or route activation. Live SQL Server behavior remains
`UNVERIFIED`.

## Exact provider-install receipt

`dpone_control_receipt_v3` is not reusable for provider installation. It is
binding/registration-scoped and requires registration, writer-head and control
operation fields that do not exist when a database-level provider is first
installed. Reusing it would join provider-install and registration/control-
effect namespaces and make provider installation depend on registration state.

The physical descriptor therefore contains exactly 20 tables. The new table is
ordered immediately after `dpone_generation_authority_consumption_v3` and
before `dpone_control_receipt_v3`:

```text
dpone_authority.dpone_provider_install_receipt_v3

installation_effect_key binary(32)    NOT NULL PRIMARY KEY
request_digest          binary(32)    NOT NULL
payload_bytes           varbinary(max) NOT NULL
payload_digest          binary(32)    NOT NULL UNIQUE
committed_at            datetime2(7)  NOT NULL

CHECK (DATALENGTH(payload_bytes) > 0)
CHECK (HASHBYTES('SHA2_256', payload_bytes) = payload_digest)
```

Its descriptor contract is:

```yaml
resource_kind: static_object
lifecycle: immutable
mutation_policy: installer_only
allowed_access: [ddl, insert, read]
forbidden_access: [update, delete]
comparison_fields:
  - installation_effect_key
  - request_digest
  - payload_bytes
  - payload_digest
```

The receipt has no registration foreign key and no TTL/GC path. It is inserted
after stable post-install attestation in the same target database, physical
session and installer transaction as the provider installation. Replay only
reads it. `payload_digest` is exactly `SHA256(payload_bytes)`; the canonical
payload already contains its own domain separator and is not hash-wrapped a
second time.

The physical table is database-global, but every row is the immutable receipt
for one combined provider-and-binding-pack install request. The migration child
must version its receipt identity before implementation:

```text
installation_effect_key = SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-receipt-effect-key-v2\0",
  (
    target_database_identity_digest,
    provider_contract_digest,
    binding_pack_digest,
  ),
))
```

The binding-pack digest prevents two target bindings in one database from
colliding while retaining deterministic replay for the same install request.
No new shared procedure is introduced; the future approved renderer owns the
typed probe and append statements.

The amended descriptor literal is exactly
`dpone-mssql-r1-v3-physical-schema-2-r2`. The canonical
`MssqlR1PhysicalSchemaDescriptorV1` domain and the embedded portable schema-2
contract remain unchanged; the new literal prevents old 19-table bytes from
being decoded as the 20-table descriptor revision.

## Permission closure instead of a finite deny-list

SQL Server permission names and scope combinations are not a safe finite
deny-list. The active shared profile V2 replaces
`ordered_forbidden_permission_edges` with:

```python
MssqlR1PermissionClosurePolicyV1(
    contract_version: Literal["dpone-mssql-r1-permission-closure-1"],
    physical_schema_descriptor_digest: bytes,
    observation_policy: MssqlR1PermissionObservationPolicyV2,
    expected_shared_source: Literal["schema2_exact_permission_rules"],
    expected_binding_source: Literal[
        "resolved_binding_pack_exact_permission_rules"
    ],
    observation_scope: Literal[
        "complete_managed_principal_database_permissions"
    ],
    unexpected_permission_disposition: Literal["block"],
    ordering: Literal["canonical_observation_bytes"],
)
```

Domain:
`dpone-r1-security-permission-closure-policy-v1\0`.

The embedded policy replaces the previously unbound projection digest:

```python
MssqlR1PermissionObservationPolicyV2(
    contract_version: Literal["dpone-mssql-r1-permission-observation-2"],
    managed_schema_source: Literal["exact_schema_contract_names"],
    managed_object_source: Literal["exact_schema_and_binding_objects"],
    managed_principal_source: Literal["exact_resolved_principal_authority"],
    securable_scope: Literal["database_and_all_descendants"],
    origin_coverage: Literal["direct_role_and_public"],
    effect_coverage: Literal["grant_grant_option_and_deny"],
    unknown_row_disposition: Literal["block"],
)
```

Domain: `dpone-r1-security-permission-observation-policy-v2\0`. Its canonical
digest must equal the exact
`expected_schema_contract.permission_projection_policy_digest` embedded in the
physical descriptor. The policy payload, descriptor and digest are all present;
a caller-supplied bare 32-byte digest is insufficient.

Both expected rules and catalog observations are normalized into the same
closed type before set operations:

```python
MssqlR1ResolvedPermissionPathV2(
    beneficiary: MssqlR1SecurityPrincipalRefV1,
    grantor: MssqlR1SecurityPrincipalRefV1,
    origin: MssqlR1PermissionOriginV1,
    target: MssqlR1PermissionTargetV1,
    permission: str,
    effect: MssqlR1PermissionEffectV3,
    grant_option: bool,
)
```

Domain: `dpone-r1-security-resolved-permission-path-v2\0`. Expected shared and
binding rules always normalize to the direct-origin arm. Catalog role/PUBLIC
paths retain their distinct origin arm; they can therefore never compare equal
to an allowed direct path. All fields use exact runtime types and canonical
bytes; no structural or string-enum equality is accepted.

Resolved-path union admission is closed. `beneficiary` is exactly an
environment, shared-signer or binding-signer-instance principal; PUBLIC,
named/any-role and binding-signer-class refs are forbidden. `grantor` is exactly
the provisioner environment principal. Expected paths require direct origin;
observed paths may use direct, one exact named-role or PUBLIC origin, but never
the any-role selector. `effect` is exact `GRANT|DENY`; `DENY` requires
`grant_option=false`. Raw enum strings, structural substitutes and every other
principal/origin/effect combination reject.

The shared projection is exact: provisioner/runtime/loader/observer subject
roles map to their environment-principal refs; attestation/stage-owner module
roles map to their corresponding shared-signer refs; and the grantor role maps
to the provisioner principal. Schema-2 `DIRECT` maps to the direct-origin arm.
Database/schema/object/column scopes map to identically named target scopes,
preserve exact coordinates and fix `include_descendants` respectively to
`false/true/true/false`. Permission, effect and grant option are copied without
coercion.

The binding pack contains its own complete tuple of already resolved
`MssqlR1ResolvedPermissionPathV2` values. The researched binding child may not
implement its current prose-only `access_kind/permission_scope` projection;
before approval it must freeze that one-to-one mapping and prove the resolved
tuple is the minimal target/row-hash access projection. This security contract
does not guess or duplicate the mapping.

Catalog projection resolves grantee and grantor IDs through exact principal
authority, maps SQL class/major/minor IDs to one target and coordinate,
preserves the permission name, maps
`GRANT/GRANT_WITH_GRANT_OPTION/DENY` to exact effect and grant-option values,
and retains direct, named-role or PUBLIC origin as distinct tagged arms. An
unknown principal, role, class, object or column blocks instead of disappearing
from `O`.

For one resolved installation:

```text
A = set[MssqlR1ResolvedPermissionPathV2](
      exact schema-2 shared permission rules
      union exact resolved binding-pack permission rules
    )

O = set[MssqlR1ResolvedPermissionPathV2](
      every typed catalog permission path whose effective beneficiary is a
      managed environment, shared-signer or binding-signer principal
    )

matched = direct observations whose complete canonical projection equals
          exactly one A member, including beneficiary, grantor, origin, target,
          permission, effect and grant_option

forbidden = canonical_sort(O - matched)
missing   = canonical_sort(A - matched)

admit iff forbidden is empty and missing is empty
```

Consequently every unexpected direct permission, role/PUBLIC-derived path,
DENY, grant-option widening, legacy/unknown object permission and scope
widening blocks. There is no transient-stage exclusion in provider-install
attestation: installation requires the route/stage lifecycle to be quiescent,
and any loader grant present in this stable observation is an unexpected row.
The separately approved runtime stage lifecycle may observe its own bounded
transient grant but cannot weaken this install-time complement.

`MssqlR1ForbiddenPermissionEdgeV1` is not an active aggregate authority in V2.
Forbidden permission observations remain evidence generated by the exact
complement calculation and must be empty in stable post-install attestation.
The exact seven forbidden database-role memberships remain an independent,
closed tuple because that finite identity set is fully specified.

## Replay authority V2

V1 canonical domains and layouts are not reinterpreted. The active contracts
are:

```python
MssqlR1CertificateInstallIdentityRefV2(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    installation_effect_key: bytes,
    receipt_payload_digest: bytes,
    stable_catalog_attestation_digest: bytes,
    signer_ref: (
        MssqlR1SharedSignerPrincipalRefV1
        | MssqlR1BindingSignerInstancePrincipalRefV1
    ),
    lifecycle_policy_digest: bytes,
    pinned_certificate_observation_digest: bytes,
)

MssqlR1CertificateReplayEvidenceV2(
    installed_identity_ref: MssqlR1CertificateInstallIdentityRefV2,
    pinned_certificate_observation_payload: bytes,
    active_lifecycle_authority_payload: bytes,
    current_catalog_observation: MssqlR1CertificateCatalogObservationV1,
)
```

Domains:

```text
dpone-r1-security-certificate-install-identity-ref-v2\0
dpone-r1-security-certificate-replay-evidence-v2\0
```

The receipt resource is exactly the static object
`dpone_authority.dpone_provider_install_receipt_v3`; catalog, stage, target,
control-receipt and other static-object substitutions are rejected.

The active lifecycle payload decodes as exactly one closed arm selected by the
signer ref:

```text
shared signer  -> MssqlR1SharedInstallSecurityProfileV2
binding signer -> MssqlR1BindingSignerLifecyclePolicyV2
```

The pure security evidence requires all of the following:

```text
SHA256(active_lifecycle_authority_payload)
  = installed_identity_ref.lifecycle_policy_digest

SHA256(pinned_certificate_observation_payload)
  = installed_identity_ref.pinned_certificate_observation_digest

pinned observation canonical bytes = current observation canonical bytes
current observation is installed-public-key-only
```

This evidence is not replay admission by itself. Full admission belongs to the
migration/application composition layer, which can decode the receipt without
creating a security-to-migration dependency:

```python
MssqlR1ProviderInstallReplayAdmissionV2(
    fresh_receipt_probe: MssqlR1MigrationReceiptProbeResultV2,
    decoded_receipt_payload: MssqlR1MigrationInstallReceiptPayloadV2,
    current_install_request_authority: MssqlR1MigrationInstallRequestAuthorityV1,
    ordered_certificate_replay_evidence: tuple[
        MssqlR1CertificateReplayEvidenceV2, ...
    ],
    active_physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
    active_binding_pack: MssqlR1BindingModulePackV2,
)
```

Domain: `dpone-r1-provider-install-replay-admission-v2\0`. It requires this
complete chain:

```text
fresh probe outcome = exact and row_count = 1
fresh stored effect key = installed identity effect key
fresh stored payload digest = installed identity receipt payload digest
SHA256(fresh stored payload bytes) = fresh stored payload digest
decode(fresh stored payload bytes) = decoded V2 receipt payload
decoded installation effect key = installed identity effect key
fresh stored request digest
  = SHA256(decoded install request canonical bytes)
  = SHA256(current install request canonical bytes)
decoded install request canonical bytes = current install request canonical bytes
recomputed V2 effect key from current/decoded target database, provider contract
  and binding-pack digests = fresh stored/decoded/installed-identity effect key
decoded stable attestation digest
  = installed identity stable attestation digest
  = SHA256(decoded stable attestation payload)
active physical descriptor is exact revision R2 and its canonical digest
  = decoded/current install-request physical_schema_descriptor_digest
decoded stable attestation validate_against_authorities(
  active_physical_descriptor, active_binding_pack
) succeeds before replay classification
ordered certificate evidence signer refs are exactly:
  shared attestor, shared stage owner, binding signer instance
each pinned certificate observation is selected exactly once from that decoded
  stable attestation under the corresponding signer ref
each pinned selected bytes/digest = corresponding security evidence bytes/digest
all three installed identity refs share exactly one receipt resource, effect
  key, receipt payload digest and stable-attestation digest
```

The combined install always carries one exact `MssqlR1BindingModulePackV2`; its
canonical digest equals both decoded/current install-request
`binding_pack_digest`. For evidence entries one and two, the lifecycle payload
is the active shared profile V2 and the respective shared signer occurs exactly
once there. Those two shared lifecycle payload bytes are identical; their
bytes equal the exact shared-security-profile V2 canonical bytes embedded in
the active binding pack. SHA-256 of those bytes equals both decoded/current
install-request `shared_security_profile_digest`; the binding pack's own
canonical digest already binds the embedded profile bytes. For entry three,
the lifecycle payload is the
binding policy V2,
the pack's embedded binding-instantiation contract contains that exact policy,
and the binding signer instance occurs exactly once in the pack's portable
identity. Wrong lifecycle arm/order, a V1 pack, digest-only reference or
structural substitute blocks.

Before binding implementation, the researched binding child must version its
instantiation contract, portable identity and module pack to V2. The V2 pack
keeps the V1 ownership/layout but binds the V2 shared-security profile,
binding-lifecycle policy and resolved-permission-path tuple under new V2 domains
and version literals. These exact binding layouts are approved by that child,
not guessed by the security implementation task.

Only this composed V2 object may classify an existing installation as exact.
The pure security evidence reports observation equality or conflict but cannot
authorize reinstall/replay, and an activation boundary never accepts a
caller-constructed bare digest.

The evidence tuple has exact cardinality three and the canonical signer order
above. Omission, extra, duplicate, reorder, cross-receipt splice or a second
binding signer blocks the complete admission; no single-certificate admission
can authorize replay of a combined installation.

Migration/binding query and stable-attestation contracts are versioned so every
signer uses the same certificate-observation bytes and V1 permission result
types cannot leak into V2:

```python
MssqlR1ObservedBindingSignerV2(
    target_binding_uuid: UUID,
    certificate_observation: MssqlR1CertificateCatalogObservationV1,
    certificate_id: int,
    certificate_owner_principal_id: int,
    certificate_user_principal_id: int,
)

MssqlR1CertificateQueryResultV2(
    ordered_shared_certificates: tuple[
        MssqlR1CertificateCatalogObservationV1, ...
    ],
    binding_signer: MssqlR1ObservedBindingSignerV2,
)

MssqlR1PermissionQueryResultV2(
    ordered_observed_permission_paths: tuple[
        MssqlR1ResolvedPermissionPathV2, ...
    ],
)

MssqlR1PermissionClosureResultV2(
    observation_policy_digest: bytes,
    ordered_expected_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_observed_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_missing_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ordered_forbidden_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
)

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

MssqlR1BindingLiveCatalogIdentityV2(
    database_identity: MssqlR1ObservedDatabaseIdentityV1,
    ordered_modules: tuple[MssqlR1ObservedBindingModuleV1, ...],
    signer: MssqlR1ObservedBindingSignerV2,
    ordered_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...],
    ordered_binding_prefix_inventory: tuple[
        MssqlR1ObservedBindingPrefixObjectV1, ...
    ],
)

MssqlR1StableSchemaAttestationV2(
    schema_contract_version: str,
    target_binding_uuid: UUID,
    registration_payload_digest: bytes,
    registration_verification_receipt_digest: bytes,
    registered_resolved_profile_digest: bytes,
    registered_physical_authority_digest: bytes,
    server_instance_identity_sha256: bytes,
    database_id: int,
    database_guid: UUID,
    database_family_guid: UUID,
    recovery_fork_guid: UUID,
    expected_contract_bytes: bytes,
    principal_authority_set_bytes: bytes,
    ordered_observed_schemas: tuple[MssqlR1ObservedSchemaV3, ...],
    ordered_observed_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
    ordered_observed_principals: tuple[MssqlR1ObservedPrincipalV3, ...],
    ordered_observed_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...],
    expected_schema_contract_digest: bytes,
    principal_authority_set_digest: bytes,
    observed_schema_inventory_digest: bytes,
    observed_principal_membership_inventory_digest: bytes,
    live_identity_digest: bytes,
    projection_revision: Literal[1],
)

MssqlR1StableSharedSecurityAttestationV2(
    ordered_certificate_observations: tuple[
        MssqlR1CertificateCatalogObservationV1, ...
    ],
    ordered_forbidden_membership_observations: tuple[
        MssqlR1ObservedRoleMembershipV3, ...
    ],
)

MssqlR1StableBindingAttestationV2(
    binding_pack_digest: bytes,
    live_catalog_identity: MssqlR1BindingLiveCatalogIdentityV2,
)

MssqlR1StableCatalogAttestationV2(
    attestation_version: Literal["dpone-r1-stable-catalog-attestation-2"],
    target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    renderer_registry_digest: bytes,
    ordered_statement_results: tuple[MssqlR1PostInstallStatementResultV2, ...],
    stable_schema_attestation: MssqlR1StableSchemaAttestationV2,
    stable_shared_security_attestation: MssqlR1StableSharedSecurityAttestationV2,
    stable_binding_attestation: MssqlR1StableBindingAttestationV2,
    permission_closure_result: MssqlR1PermissionClosureResultV2,
)
```

The exact domains are:

| Class | Canonical domain |
|---|---|
| `MssqlR1ObservedBindingSignerV2` | `dpone-mssql-r1-observed-binding-signer-v2\0` |
| `MssqlR1CertificateQueryResultV2` | `dpone-r1-certificate-query-result-v2\0` |
| `MssqlR1PermissionQueryResultV2` | `dpone-r1-permission-query-result-v2\0` |
| `MssqlR1PermissionClosureResultV2` | `dpone-r1-security-permission-closure-result-v2\0` |
| `MssqlR1PostInstallStatementResultV2` | `dpone-r1-post-install-statement-result-v2\0` |
| `MssqlR1BindingLiveCatalogIdentityV2` | `dpone-mssql-r1-binding-live-catalog-identity-v2\0` |
| `MssqlR1StableSchemaAttestationV2` | `dpone-r1-stable-schema-attestation-v2\0` |
| `MssqlR1StableSharedSecurityAttestationV2` | `dpone-r1-stable-shared-security-attestation-v2\0` |
| `MssqlR1StableBindingAttestationV2` | `dpone-r1-stable-binding-attestation-v2\0` |
| `MssqlR1StableCatalogAttestationV2` | `dpone-r1-stable-catalog-attestation-v2\0` |

V1 classes listed inside a V2 union are reused only where their complete
semantics are unchanged; certificate, permission, statement, schema,
binding-live and stable aggregate classes are always V2. No duplicate V1
certificate/permission tuple is retained in the stable V2 authority.

Let the seven typed results in exact statement-registry order be
`QS, QT, QM, QC, QP, QG, QB`. They are respectively the sole schema, table,
module, V2 certificate, V2 permission, signature and binding-prefix results.
The V2 stable projection consumes them through these exact byte equalities:

```text
stable_schema database coordinates       = projection(QS.database_identity)
stable_schema.ordered_observed_schemas    = QS.ordered_schemas
stable_schema.ordered_observed_principals = QS.ordered_principals
stable_schema.ordered_observed_role_memberships
                                           = QS.ordered_role_memberships
stable_schema.ordered_observed_objects = stable_merge(
  QT.ordered_table_objects,
  QM.ordered_core_module_objects,
  schema_object_canonical_order
)
flatten(stable_schema object signatures)  = QG.ordered_core_signatures

stable_shared.ordered_certificate_observations
                                           = QC.ordered_shared_certificates

stable_binding.live_catalog_identity.database_identity
                                           = QS.database_identity
stable_binding.live_catalog_identity.ordered_modules
                                           = QM.ordered_binding_modules
stable_binding.live_catalog_identity.signer
                                           = QC.binding_signer
stable_binding.live_catalog_identity.ordered_signatures
                                           = QG.ordered_binding_signatures
stable_binding.live_catalog_identity.ordered_binding_prefix_inventory
                                           = QB.ordered_binding_prefix_inventory

permission_closure_result.ordered_observed_paths
                                           = QP.ordered_observed_permission_paths
```

`stable_merge` is the V1 stable two-way merge: both inputs are independently
canonical, disjoint, kind-checked and together cover the expected core-object
set. Every query definition and result kind occurs exactly once; every result
field is consumed exactly once except for the deliberate membership reuse in a
predicate below. Extra, missing, reordered, duplicated or wrong-arm results
reject. These equalities are self-contained and are revalidated by ordinary
V2 canonical decode.

The V2 stable-schema derived fields are fixed as follows:

```text
expected_schema_contract_digest = SHA256(expected_contract_bytes)
principal_authority_set_digest = SHA256(principal_authority_set_bytes)

observed_schema_inventory_digest = SHA256(canonical_bytes(
  b"dpone-r1-stable-schema-observed-inventory-v2\0",
  (
    tuple(item.canonical_bytes for item in ordered_observed_schemas),
    tuple(item.canonical_bytes for item in ordered_observed_objects),
  ),
))

observed_principal_membership_inventory_digest = SHA256(canonical_bytes(
  b"dpone-r1-stable-schema-principal-membership-inventory-v2\0",
  (
    tuple(item.canonical_bytes for item in ordered_observed_principals),
    tuple(item.canonical_bytes for item in ordered_observed_role_memberships),
  ),
))

live_identity_digest = SHA256(canonical_bytes(
  b"dpone-r1-stable-schema-live-identity-v2\0",
  (
    server_instance_identity_sha256,
    database_id,
    database_guid,
    database_family_guid,
    recovery_fork_guid,
    observed_schema_inventory_digest,
    observed_principal_membership_inventory_digest,
  ),
))
```

All five digests are recomputed during construction and canonical decode.
`projection_revision` is copied from the exact source
`MssqlR1SchemaAttestationV3` and this V2 capability admits only revision `1`;
another source revision requires a new approved stable-projection capability.
The stable constructor also requires all retained non-time fields to equal the
source schema attestation byte-for-byte and excludes its `observed_at`,
permission tuple, V1 security digest and time-bearing attestation digest.

The closure-result constructor requires canonical unique ordering and proves
`missing=expected-observed` and `forbidden=observed-expected` under the exact
observation-policy digest. Stable attestation construction additionally
requires both derived tuples empty. Query result and statement-result coverage
contain exactly one member per query definition; a V1 certificate/permission
result or wrong union arm rejects.

`MssqlR1StableCatalogAttestationV2` removes the remaining duplicate-observation
authority through two validation layers. Canonical decode proves the complete
seven-result projection above, observed-path equality, derived set
algebra/digests and all internal structure. Authority-dependent equations are
proved only by `create(..., active_physical_descriptor, active_binding_pack)`
and by the same typed `validate_against_authorities(...)` call at every install
and replay admission boundary; ordinary decode never pretends to recover
external authority from a digest.

The authority-aware validation requires:

```text
permission_closure_result.ordered_observed_paths
  == the ordered_observed_permission_paths tuple from the sole
     MssqlR1PermissionQueryResultV2 statement result

permission_closure_result.ordered_expected_paths
  == canonical_unique_order(
       schema-2 projected shared-security permission rules
       union
       active binding-pack resolved permission rules
     )

permission_closure_result.observation_policy_digest
  == SHA256(
       active permission_closure_policy.observation_policy.canonical_bytes
     )
  == expected_schema_contract.permission_projection_policy_digest

stable_schema_attestation.ordered_observed_role_memberships
  == the ordered_role_memberships tuple from the sole
     MssqlR1SchemaQueryResultV1 statement result

stable_shared_security_attestation.ordered_forbidden_membership_observations
  == filter_forbidden_shared(
       stable_schema_attestation.ordered_observed_role_memberships
     )
  == filter_forbidden_shared(
       the ordered_role_memberships tuple from the sole
       MssqlR1SchemaQueryResultV1 statement result
     )
  == ()
```

`active_physical_descriptor` is an exact decoded
`MssqlR1PhysicalSchemaDescriptorV1` revision R2 and
`active_binding_pack` is an exact decoded `MssqlR1BindingModulePackV2` carrying
the active shared-security profile V2 and binding resolved-permission tuple.
Validation requires the descriptor and pack canonical digests to equal the
stable/install-request references, requires
`stable_schema.expected_contract_bytes` to equal the descriptor's embedded
expected schema-contract bytes, and then evaluates every authority-dependent
equation above. No registry lookup, bare digest or structural substitute is an
input. A caller cannot provide one catalog observation to the statement result
and calculate closure over another. Any missing, extra, reordered, duplicated
or cross-authority path blocks construction or admission; there is no caller-
supplied success flag.
`filter_forbidden_shared` is the exact predicate of the active shared-security
profile, not caller code. A forbidden membership present in the schema query
cannot be hidden by supplying an independently empty stable-shared tuple.

The binding signer observation's catalog IDs resolve, in the same query result,
to the embedded certificate observation's certificate owner, user SID,
principal/authentication types and bound thumbprint. Its certificate/user names
equal the V2 binding portable identity. The stable shared certificate tuple is
exactly attestor then stage owner; the binding observation is the third source.
Those embedded `MssqlR1CertificateCatalogObservationV1.canonical_bytes` are the
only bytes selectable by replay evidence. Forbidden membership observations
must be empty before the stable V2 attestation or receipt can be constructed.

`MssqlR1SharedInstallSecurityProfileV2` has the V1 field order with
`contract_version="dpone-mssql-r1-shared-security-2"` and replaces only
`ordered_forbidden_permission_edges` with
`permission_closure_policy: MssqlR1PermissionClosurePolicyV1`. Its domain is
`dpone-r1-shared-install-security-profile-v2\0`.

The six individual lifecycle transitions reuse the exact
`MssqlR1CertificateLifecycleTransitionV1` type/domain unchanged; their state,
action and ordering did not change. V2 introduces one explicit set authority:

```text
lifecycle_transition_set_digest = SHA256(canonical_bytes(
  b"dpone-r1-security-certificate-lifecycle-transition-set-v2\0",
  tuple(transition.canonical_bytes for transition in exact_six_transitions),
))
```

The shared profile V2 embeds exactly those six transitions in ordinal order and
recomputes the set digest. Omission, extra, gap, branch, reorder, duplicate or
V1/V2 aggregate splice rejects. Reusing the unchanged individual V1 transition
bytes does not reinterpret their canonical domain.

`MssqlR1BindingSignerLifecyclePolicyV2` has the V1 field order and types with
`contract_version="dpone-mssql-r1-binding-signer-policy-2"`; its domain is
`dpone-r1-binding-signer-lifecycle-policy-v2\0`. Its
`lifecycle_transition_set_digest` must equal the digest of the exact six V2
shared-profile transitions. For a shared signer,
`lifecycle_policy_digest` equals the complete active shared-profile V2 digest;
for a binding signer it equals the active binding-lifecycle V2 digest. The
branch-specific composed replay rules above prove signer occurrence; the
generic binding lifecycle policy never pretends to contain an instance signer.

The migration receipt contracts are versioned before implementation:

```python
MssqlR1MigrationTargetReceiptBindingV2(
    binding_version: Literal["dpone-r1-migration-target-receipt-binding-2"],
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    payload_codec_version: Literal["dpone-r1-migration-receipt-payload-2"],
    ordered_field_bindings: tuple[MssqlR1MigrationReceiptFieldBindingV1, ...],
    same_database_required: Literal[True],
    same_transaction_required: Literal[True],
)

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
    original_disposition: Literal["install"] | Literal["coexist_then_install"],
)

MssqlR1MigrationReceiptRefV2(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    installation_effect_key: bytes,
    request_digest: bytes,
    receipt_payload_digest: bytes,
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

Their domains are respectively
`dpone-r1-provider-migration-{receipt-binding|install-receipt-payload|receipt-ref|receipt-probe-result}-v2\0`.
The ordered field bindings are exactly effect key, request digest, payload bytes
and payload digest mapped once to the dedicated table fields in that order.
`payload_bytes` is the exact V2 payload canonical bytes and
`receipt_payload_digest = SHA256(payload_bytes)`. The request digest remains
`SHA256(install_request_authority.canonical_bytes)`. Exact probe equality
requires all four stored values; zero, duplicate, partial or mismatching rows
block. The V2 receipt payload constructor decodes
`stable_catalog_attestation_payload` as exact
`MssqlR1StableCatalogAttestationV2`, requires byte-identical re-encoding and
requires `stable_catalog_attestation_digest =
SHA256(stable_catalog_attestation_payload)`. V1, unknown-domain and trailing-byte
payloads reject. New-write construction is available only through a typed
`create_for_install(..., active_physical_descriptor, active_binding_pack)`
factory. It invokes the stable attestation's
`validate_against_authorities(...)` before deriving or retaining payload bytes;
a decoded historical payload alone cannot authorize a new receipt. Every replay
path invokes the same validation through
`MssqlR1ProviderInstallReplayAdmissionV2` before returning exact admission.

V1 is read-only/inactive and cannot enter a provider aggregate, renderer,
installer or activation path.

To preserve an acyclic graph, certificate values and identity remain in the
certificate module; permission closure and binding lifecycle remain in the
permissions module; and the shared aggregate plus certificate replay evidence
remain in the profile module, which already depends on both. Full provider-
install replay admission lives downstream in the future migration/application
composition and imports the security evidence, never the reverse. No replay
facade or reverse import is introduced.

## Recovery and compatibility

No V1 security contract or 19-table descriptor was publicly exported,
production-certified or admitted as durable provider authority. Nevertheless,
their bytes are never decoded as V2.

There is no production concrete 19-table descriptor payload/digest to trust;
the old test fixture is synthetic and is never runtime authority. Compatibility
classification instead uses the exact pre-amendment ordered 19 table names,
29 shared procedure names, six binding template kinds and schema-2 version:

```text
exact old 19/29/6 inventory
  -> disposition=block
  -> blocker_code=pre_amendment_v3_manual_disposition

partial 20-table, arbitrary 19-object or unknown V3 inventory
  -> disposition=block
  -> blocker_code=partial_or_unknown_v3_manual_disposition
```

The system performs no automatic table addition, synthesized receipt, drop or
upgrade. Operators must explicitly rebaseline/reinstall after approved runtime
support exists. An ambiguous or missing provider receipt blocks replay.

## Architecture and quality budget

Implementation is split into three ordered tasks:

1. physical descriptor inventory/resource amendment;
2. security V2 rework pinned to the amended descriptor commit.
3. migration V2 receipt/probe/full replay admission, only after the migration
   and binding children are independently `APPROVED`.

The descriptor task introduces no new module or import edge. The security task
may not hide real type dependencies through re-exports, untyped payloads or
metric-only facades. Raw architecture-fitness output remains visible. Because
the repository already exceeds the global `0.182` limit, the final exact
security commit requires either real separately approved remediation or a new
time-bounded ADR with an exact ceiling, expiry, activation block and removal
plan. ADR 0057 does not cover security modules.

ADR 0056 records the separate provider-install effect/table. ADR 0057 must be
remeasured against the amended descriptor commit; its global budget is never
silently raised.

## Test and certification plan

Required hermetic tests include:

- exact 20-table order/count and descriptor round trip;
- exact R2 descriptor literal rejects pre-amendment R1 bytes;
- exact receipt resource and four comparison fields;
- control receipt, unrelated static object and non-static substitutions reject;
- UPDATE/DELETE authority for the provider receipt rejects;
- two binding-pack digests produce distinct V2 install effect keys;
- permission complement accepts exactly complete expected direct edges;
- missing, extra, role/PUBLIC, DENY, scope and grant-option mutations block;
- observation-policy payload/digest mismatch and every illegal principal/origin
  branch block; no transient-stage row is excluded;
- exact seven membership rules reject omission/extra/reorder/duplicate;
- all seven query-result projections reject aggregate/result splices, including
  certificate, table, module, signature and binding-prefix disagreements;
- stable-schema derived digest domains, inputs and projection revision reject
  every field mutation;
- self-contained decode succeeds only for internal equalities, while install
  and replay both reject a missing or mismatching descriptor/binding pack;
- raw enum strings, bool/float-as-int and tagged-union splices reject;
- active lifecycle, signer, receipt, pinned and current observation splices
  reject;
- lifecycle transition-set omission/gap/branch/reorder/duplicate and digest
  mutation reject;
- the exact three-certificate tuple rejects omission/extra/reorder/duplicate,
  cross-receipt and shared/binding observation splices;
- every V2 payload round-trips byte-identically and valid-distinct mutations
  change the owning digest;
- V1 bytes cannot enter a V2 aggregate;
- exact old 19/29/6 and every partial/unknown V3 inventory block with their
  respective exact blocker codes and zero mutation;
- descriptor/schema regression suites remain green.
- a future approved live SQL Server 2022 test compiles the receipt CHECK and
  proves `HASHBYTES('SHA2_256', payload_bytes)` for payloads above 8 KiB.

Live provider-install SQL, permission observation and replay remain
`UNVERIFIED` in these pure-contract tasks.

## Documentation and CJM impact

There is no public manifest, CLI or Python API change. Provider architecture,
migration and recovery documentation must describe the dedicated receipt and
manual disposition before executable installation is approved. First-success
CJM remains blocked until the provider aggregate and live certification exist.

## Definition of Done

1. The descriptor contains exactly the 20-table inventory and exact receipt
   resource semantics under revision R2.
2. Security V2 uses total-complement permission closure, not a deny-list.
3. Replay proves active lifecycle bytes and exact pinned/current observation.
4. Receipt identity resolves only the dedicated same-database receipt.
5. V1 remains inactive and cannot be reinterpreted as V2.
6. Stable V2 projection consumes all seven query results and re-derives every
   digest without duplicate observation authority.
7. Install and replay validate stable evidence against the active physical
   descriptor and binding pack; decode alone never grants admission.
8. Focused mutation/round-trip and descriptor/schema regressions pass.
9. Raw architecture evidence and any bounded exception remain truthful.
10. Live SQL Server behavior remains `UNVERIFIED` until a separately approved
   installer/live-certification task.
