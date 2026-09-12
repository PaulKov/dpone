
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider security contract

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Approved by: dpone maintainer implementation authorization, 2026-09-05
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: exact implemented `MssqlR1PhysicalSchemaDescriptorV1` commit
  `source record 139`
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.
  The amendment owns the 20th receipt resource, permission-closure policy and
  active V2 replay contracts; conflicting V1 prose below is read-only history.

## Outcome and boundary

This child specification defines the immutable security authority used by the
R1 SQL Server provider. It separates shared schema signers from per-binding
signers so the dependency graph is acyclic:

```text
physical descriptor
  → shared install-security profile
  → binding signer lifecycle policy
  → instantiated modules
  → binding signer instance
  → binding pack
```

The shared profile never contains a binding UUID, binding module definition or
binding-pack digest. Binding security depends on the shared profile; the shared
profile never depends on binding security.

In scope are pure canonical contracts, lifecycle closure, permission-path
policy, secret-template policy and hermetic tests. SQL rendering, secret
generation, installation, catalog I/O and live evidence are separate scopes.
Public CLI, manifest and Python APIs do not change.

## Canonical rules

Every type is an exact frozen slotted dataclass. Raw enum strings, bool/int
substitution, unknown/trailing canonical fields and structural duck typing fail.

```text
canonical_bytes = canonical_bytes(EXACT_DOMAIN, ordered_fields)
digest = SHA256(canonical_bytes)
```

An object's own digest is a derived property, never a constructor field.
External digest references are exact 32-byte values and are validated against
the resolved nested object in aggregate context. Ordered tuples use contiguous
one-based ordinals; semantic sets sort by canonical bytes and reject duplicates.
Identifiers reuse schema-2 validation and casefold-collision rules.

## Closed enums

```text
EnvironmentPrincipalProfile: contained_sql_users_v1
CertificateCreationProfile: sqlserver_self_signed_sha2_256_v1
CertificateSignatureAlgorithm: sha2_256
SecretGeneratorProfile: cryptographic_random_ascii_256bit_v1
SecretPersistencePolicy: memory_only
SecretLoggingPolicy: dpone_logging_forbidden
SecretLifetimePolicy: installer_transaction_only
PrivateKeyDisposition: remove_after_all_signatures
ReinstallPolicy: observe_exact_or_block
BindingSignerNamingProfile: lowercase_binding_uuid_v1
BindingPermissionProfile: exact_target_and_row_hash_v1

CertificateLifecycleState:
  absent | private_key_present | certificate_user_ready |
  permissions_ready | signatures_complete | installed_public_key_only |
  conflict

CertificateLifecycleAction:
  create_certificate | create_certificate_user | grant_exact_permissions |
  sign_exact_modules | remove_private_key | observe_replay

PermissionTargetScope: database | schema | object | column
ForbiddenPermissionEffect: grant | deny | grant_with_grant_option
```

All values are exact `StrEnum` members.

The closed-enum registry is ordered and complete. Its final member is
`SecretTemplateKind: create_certificate | add_signature`; omission, reorder or
an additional enum/member fails aggregate validation. Canonical enum order is
the display order in this section. Character classes are exactly
`uppercase, lowercase, digit, symbol`; permission effects are exactly
`grant, deny, grant_with_grant_option`; binding module kinds use the canonical
six-kind order from the physical descriptor. These tuples are duplicate-free
and retain this order rather than being sorted at runtime.

## Principal and permission algebra

The closed principal tagged union is:

```python
MssqlR1EnvironmentPrincipalRefV1(
    subject_role: MssqlR1SubjectRoleV3,
)
MssqlR1SharedSignerPrincipalRefV1(
    signer_profile: MssqlR1SignerProfileKindV3,
)
MssqlR1BindingSignerClassPrincipalRefV1()
MssqlR1BindingSignerInstancePrincipalRefV1(
    target_binding_uuid: UUID,
)
MssqlR1PublicPrincipalRefV1()
MssqlR1NamedDatabaseRoleRefV1(
    role_name: str,
)
MssqlR1AnyDatabaseRoleRefV1()
```

Domains are respectively:

```text
dpone-r1-security-environment-principal-ref-v1\0
dpone-r1-security-shared-signer-principal-ref-v1\0
dpone-r1-security-binding-signer-class-ref-v1\0
dpone-r1-security-binding-signer-instance-ref-v1\0
dpone-r1-security-public-principal-ref-v1\0
dpone-r1-security-named-database-role-ref-v1\0
dpone-r1-security-any-database-role-ref-v1\0
```

Environment roles are exactly provisioner, runtime, loader and observer. Shared
signers are exactly attestor and stage owner. The any-role selector is legal
only in forbidden-membership policy and as the role arm of a forbidden
role-permission origin. It is forbidden in every positive expected-authority
edge.

`MssqlR1SecurityPrincipalRefV1` is the closed tagged union of the seven arms
above. A shared aggregate rejects
`MssqlR1BindingSignerInstancePrincipalRefV1`; only the binding-signer class ref
may appear in shared global negative policy. An instance ref is legal only in
the resolved binding companion.

Permission origin is the tagged union:

```python
MssqlR1DirectPermissionOriginV1()
MssqlR1RolePermissionOriginV1(
    role: MssqlR1NamedDatabaseRoleRefV1 | MssqlR1AnyDatabaseRoleRefV1,
)
MssqlR1PublicPermissionOriginV1()
```

with these exact domains:

```text
dpone-r1-security-direct-permission-origin-v1\0
dpone-r1-security-role-permission-origin-v1\0
dpone-r1-security-public-permission-origin-v1\0
```

`MssqlR1PermissionOriginV1` is exactly the three-arm union above. A role origin
with `MssqlR1AnyDatabaseRoleRefV1` is legal only inside a forbidden edge.

```python
MssqlR1PermissionTargetV1(
    scope: MssqlR1PermissionTargetScopeV1,
    schema_name: str | None,
    object_name: str | None,
    column_name: str | None,
    include_descendants: bool,
)
```

Domain: `dpone-r1-security-permission-target-v1\0`.

| Scope | Schema | Object | Column | Descendants |
|---|---|---|---|---|
| database | NULL | NULL | NULL | false |
| schema | required | NULL | NULL | true or false |
| object | required | required | NULL | true or false |
| column | required | required | required | false |

`include_descendants=true` has closed expansion: schema scope covers every
object and column in that schema; object scope covers every column of that
object. The database and column scopes never admit descendants. Expansion uses
typed catalog parent coordinates and cannot cross a schema/object boundary.

```python
MssqlR1ForbiddenPermissionEdgeV1(
    ordinal: int,
    grantee: MssqlR1SecurityPrincipalRefV1,
    origin: MssqlR1PermissionOriginV1,
    target: MssqlR1PermissionTargetV1,
    permission: str,
    ordered_forbidden_effects: tuple[
        MssqlR1ForbiddenPermissionEffectV1, ...
    ],
)
```

Domain: `dpone-r1-security-forbidden-permission-edge-v1\0`. Permission names
reuse the existing schema-2 permission validator. PUBLIC and role-derived paths
never normalize to direct grants. A binding-class edge applies to every binding
signer instance.

Schema-2 observations map to forbidden effects without inference:

```text
grant                   -> effect=GRANT, grant_option=false
grant_with_grant_option -> effect=GRANT, grant_option=true
deny                    -> effect=DENY,  grant_option=false
```

```python
MssqlR1ForbiddenRoleMembershipV1(
    ordinal: int,
    member: MssqlR1SecurityPrincipalRefV1,
    forbidden_role: (
        MssqlR1NamedDatabaseRoleRefV1 | MssqlR1AnyDatabaseRoleRefV1
    ),
)
```

Domain: `dpone-r1-security-forbidden-role-membership-v1\0`. The exact R1 set
forbids any database-role membership for all four environment principals, both
shared certificate users and the binding certificate-user class. PUBLIC is
checked through permission-origin observation rather than membership.

## Shared signer authority

```python
MssqlR1SharedSignerProfileRefV1(
    signer_profile: MssqlR1SignerProfileKindV3,
    schema2_signer_profile_digest: bytes,
)
```

Domain: `dpone-r1-security-shared-signer-profile-ref-v1\0`. It resolves exactly
one `MssqlR1SignerProfileV3` and validates its canonical SHA-256 digest. Shared
certificate and user names come only from that profile.

```python
MssqlR1SharedCertificateMetadataV1(
    creation_profile: MssqlR1CertificateCreationProfileV1,
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1,
    subject: str,
    start_date_yyyymmdd: str,
    expiry_date_yyyymmdd: str,
    owner: MssqlR1EnvironmentPrincipalRefV1,
)
```

Domain: `dpone-r1-security-shared-certificate-metadata-v1\0`. R1 fixes SHA2-256,
owner=provisioner, start `20000101` and expiry `99991231`. Dates are valid exact
eight-digit calendar dates. Subject is bounded NFC text without CR/LF/NUL and at
most 64 UTF-16 code units. Subjects are exactly `dpone R1 V3 attestor` and
`dpone R1 V3 stage owner` for their respective profiles.

```python
MssqlR1SharedSignerInstallAuthorityV1(
    ordinal: int,
    signer_ref: MssqlR1SharedSignerProfileRefV1,
    certificate_metadata: MssqlR1SharedCertificateMetadataV1,
    create_certificate_template: MssqlR1SecretSqlTemplateV1,
    ordered_add_signature_templates: tuple[
        MssqlR1SecretSqlTemplateV1, ...
    ],
    ordered_module_refs: tuple[MssqlR1PhysicalResourceRefV1, ...],
    ordered_permission_rule_digests: tuple[bytes, ...],
)
```

Domain: `dpone-r1-security-shared-signer-install-authority-v1\0`. The tuple is
exactly attestor then stage owner. Module refs and permission digests resolve
byte-identically to the core/schema-2 signer mappings and contain no binding
permissions. There is exactly one fully identifier-resolved add-signature
template per ordered module ref, in the same order. Each template contains only
the ephemeral-secret placeholder; module identifiers are sealed literal bytes,
never interpolation slots.

## Ephemeral secret contract

```python
MssqlR1EphemeralSecretPolicyV1(
    generator_profile: MssqlR1SecretGeneratorProfileV1,
    minimum_entropy_bits: int,
    output_length: int,
    alphabet_ascii: str,
    required_character_classes: tuple[str, ...],
    placeholder_ascii: str,
    placeholder_occurrences_per_template: int,
    persistence_policy: MssqlR1SecretPersistencePolicyV1,
    logging_policy: MssqlR1SecretLoggingPolicyV1,
    lifetime_policy: MssqlR1SecretLifetimePolicyV1,
)
```

Domain: `dpone-r1-security-ephemeral-secret-policy-v1\0`. Exact values are:

```text
minimum_entropy_bits: 256
output_length: 64
alphabet_ascii:
  ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&*+-=?@^_
required_character_classes: uppercase, lowercase, digit, symbol
placeholder_ascii: {{DPONE_EPHEMERAL_CERT_PASSWORD_V1}}
placeholder_occurrences_per_template: 1
```

The alphabet excludes quote, backslash, CR, LF and NUL. Runtime generation uses
an OS CSPRNG with uniform rejection sampling and rejects candidates missing any
required class. One transaction-local secret is reused only for certificate
creation/signing of that certificate, then application references and mutable
temporary buffers are dropped best-effort after private-key removal.

```python
MssqlR1SecretSqlTemplateV1(
    template_kind: MssqlR1SecretTemplateKindV1,
    sql_template_utf8_bytes: bytes,
    secret_policy_digest: bytes,
)
```

`MssqlR1SecretTemplateKindV1` is `create_certificate|add_signature`. Domain:
`dpone-r1-security-secret-sql-template-v1\0`. The exact placeholder occurs once
in the certificate-password literal position; no other interpolation token is
legal. Bytes are NFC UTF-8, LF-only, NUL-free and have one terminal LF. Only
template bytes are retained or hashed. Transient executable bytes never become
a rendered-statement artifact.

dpone guarantees secret absence from its contracts, rendered artifacts,
JSON/Markdown evidence, application logs, exceptions and telemetry. It does
not claim control over SQL Server Audit, Extended Events, process dumps or host
instrumentation. Runtime enforcement remains `UNVERIFIED` until installer and
live security tests exist.

## Certificate lifecycle

```python
MssqlR1CertificateLifecycleTransitionV1(
    ordinal: int,
    from_state: MssqlR1CertificateLifecycleStateV1,
    action: MssqlR1CertificateLifecycleActionV1,
    to_state: MssqlR1CertificateLifecycleStateV1,
)
```

Domain: `dpone-r1-security-certificate-lifecycle-transition-v1\0`.

| # | From | Action | To |
|---:|---|---|---|
| 1 | `absent` | `create_certificate` | `private_key_present` |
| 2 | `private_key_present` | `create_certificate_user` | `certificate_user_ready` |
| 3 | `certificate_user_ready` | `grant_exact_permissions` | `permissions_ready` |
| 4 | `permissions_ready` | `sign_exact_modules` | `signatures_complete` |
| 5 | `signatures_complete` | `remove_private_key` | `installed_public_key_only` |
| 6 | `installed_public_key_only` | `observe_replay` | `installed_public_key_only` |

Only absent, installed-public-key-only and conflict are legal transaction-start
observations. Intermediate states may exist only inside the active installer
transaction; a durable partial state is conflict. Exact reinstall performs only
observation. Conflict has no outgoing action and causes zero mutation.

Transaction-start classification uses a complete typed observation:

```python
MssqlR1CertificateInstallIdentityRefV1(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    installation_effect_key: bytes,
    lifecycle_policy_digest: bytes,
    certificate_name: str,
    certificate_user_name: str,
    certificate_thumbprint: bytes,
    certificate_user_sid_bytes: bytes,
    certificate_user_type: MssqlR1PrincipalTypeV3,
    authentication_type: MssqlR1AuthenticationTypeV3,
    ordered_module_signature_digests: tuple[bytes, ...],
    ordered_permission_edge_digests: tuple[bytes, ...],
    stable_catalog_attestation_digest: bytes,
)
MssqlR1CertificateCatalogObservationV1(
    certificate_name: str,
    certificate_exists: bool,
    certificate_subject: str | None,
    start_date_yyyymmdd: str | None,
    expiry_date_yyyymmdd: str | None,
    certificate_owner: MssqlR1SecurityPrincipalRefV1 | None,
    certificate_thumbprint: bytes | None,
    private_key_present: bool | None,
    certificate_user_name: str | None,
    certificate_user_exists: bool,
    certificate_user_type: MssqlR1PrincipalTypeV3 | None,
    authentication_type: MssqlR1AuthenticationTypeV3 | None,
    certificate_user_sid_bytes: bytes | None,
    certificate_user_bound_thumbprint: bytes | None,
    ordered_module_signature_digests: tuple[bytes, ...],
    ordered_permission_edge_digests: tuple[bytes, ...],
)
```

Domains are `dpone-r1-security-certificate-{install-identity-ref|catalog-observation}-v1\0`.
The identity ref resolves an immutable same-database receipt committed with the
original installation; a bare caller-supplied digest is invalid. It refers
specifically to the receipt-embedded stable catalog attestation payload/digest,
never to the run-specific post-install envelope.

`MssqlR1CertificateCatalogObservationV1` is stable catalog content and contains
no receipt/back-reference. `absent` means certificate and user are both absent
and every optional certificate field/vector is empty. Existing state is
classified through a separate non-recursive wrapper:

```python
MssqlR1CertificateReplayAdmissionV1(
    installed_identity_ref: MssqlR1CertificateInstallIdentityRefV1,
    pinned_stable_catalog_attestation_payload: bytes,
    current_catalog_observation: MssqlR1CertificateCatalogObservationV1,
)
```

Domain: `dpone-r1-security-certificate-replay-admission-v1\0`. The pinned
payload decodes, reproduces the ref's `stable_catalog_attestation_digest` and
contains the pinned certificate observation. `installed_exact` requires the
new independent observation to equal that pinned observation and all static
lifecycle authority byte-for-byte. The ref pins certificate/user names,
SQL-generated thumbprint, SID/type/authentication, signatures and permissions.
Both typed discriminator fields are exactly `CERTIFICATE` and `NONE`; SID bytes
are nonempty and the typed catalog join's bound thumbprint equals the observed
certificate thumbprint. Existing state without an exact replay admission is
`conflict`. These predicates are exhaustive and pairwise disjoint.

## Shared aggregate and separate binding policy

```python
MssqlR1SharedInstallSecurityProfileV1(
    contract_version: Literal["dpone-mssql-r1-shared-security-1"],
    physical_schema_descriptor_digest: bytes,
    environment_principal_profile: MssqlR1EnvironmentPrincipalProfileV1,
    ordered_required_environment_principals: tuple[
        MssqlR1EnvironmentPrincipalRefV1, ...
    ],
    ordered_shared_signers: tuple[
        MssqlR1SharedSignerInstallAuthorityV1, ...
    ],
    ephemeral_secret_policy: MssqlR1EphemeralSecretPolicyV1,
    ordered_lifecycle_transitions: tuple[
        MssqlR1CertificateLifecycleTransitionV1, ...
    ],
    ordered_forbidden_permission_edges: tuple[
        MssqlR1ForbiddenPermissionEdgeV1, ...
    ],
    ordered_forbidden_role_memberships: tuple[
        MssqlR1ForbiddenRoleMembershipV1, ...
    ],
    private_key_disposition: MssqlR1PrivateKeyDispositionV1,
    reinstall_policy: MssqlR1ReinstallPolicyV1,
)
```

Domain: `dpone-r1-shared-install-security-profile-v1\0`. Required environment
principal order is provisioner/runtime/loader/observer; shared signer order is
attestor/stage owner.

Canonical tuple order is explicit: module refs use the core descriptor's
procedure/module order; module-definition digests use those resolved module
refs. Shared schema-2 permission rules sort directly by their resolved
`MssqlR1PermissionRuleV3.canonical_bytes`; no independently invented enum-rank
table exists. Provider forbidden-permission edges and lifecycle transitions use
their explicit ordinal. Every tuple rejects duplicates before digesting.

Shared security owns the reusable binding lifecycle policy contract:

```python
MssqlR1BindingSignerLifecyclePolicyV1(
    contract_version: Literal["dpone-mssql-r1-binding-signer-policy-1"],
    physical_schema_descriptor_digest: bytes,
    shared_install_security_profile_digest: bytes,
    naming_profile: MssqlR1BindingSignerNamingProfileV1,
    certificate_name_template: str,
    certificate_user_name_template: str,
    certificate_subject_template: str,
    certificate_owner: MssqlR1EnvironmentPrincipalRefV1,
    start_date_yyyymmdd: str,
    expiry_date_yyyymmdd: str,
    certificate_creation_profile: MssqlR1CertificateCreationProfileV1,
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1,
    secret_policy_digest: bytes,
    template_instantiation_profile: Literal[
        "uuid_resolved_literal_identifiers_v1"
    ],
    ordered_module_kinds: tuple[MssqlR1BindingModuleKindV1, ...],
    permission_profile: MssqlR1BindingPermissionProfileV1,
    lifecycle_transition_set_digest: bytes,
)
```

Domain: `dpone-r1-binding-signer-lifecycle-policy-v1\0`. Templates are exactly:

```text
dpone_b_{binding_uuid_hex}_cert_v3
dpone_b_{binding_uuid_hex}_cert_user_v3
dpone R1 V3 binding {binding_uuid_hex}
```

The sole placeholder expands to 32 lowercase hex digits. Module order is batch
mutate/hash/quality followed by XMin mutate/hash/quality. Binding permissions
are target/row-hash only. Direct stage access and authority-table DML are
forbidden except for exact derived row-hash reads/writes required by the six
certified binding templates. Owner is provisioner; dates are `20000101` and
`99991231`. The instantiation profile requires the binding companion to embed
one exact create template and six add-signature templates in canonical
module-kind order after resolving UUID-dependent identifiers to literal bytes;
the lifecycle policy contains no orphan template digest reference.

Security owns this reusable lifecycle policy only. The binding companion is
the sole owner of the instantiated signer identity, permission edges and
signature edges; no security-owned instance model or reverse dependency on a
binding type exists.

## Validation and certification

Hermetic red-green coverage includes every enum/field/optional/tagged-union
branch, canonical truncation/trailing bytes, principal-union substitution,
shared/binding splice, permission target optionality, direct/role/PUBLIC path,
grant-option widening, role membership, wrong signer/module/permission mapping,
secret injection and placeholder count/position, lifecycle gap/branch/reorder,
durable partial state, private-key retention and reinstall mutation.

The mutation registry classifies each case as `must_reject` or
`valid_distinct`; security weakening is always `must_reject`. Valid-distinct
objects must change the owning aggregate digest. Architecture/module-size,
imports and schema-contract regression gates are mandatory. Live SQL Server
secret, signature, permission and rollback checks remain `UNVERIFIED`, never
`PASS`, in this pure-contract task.

## Rollout and approval

Implementation is activation-blocked. Approval records:

1. exact implemented core-descriptor commit above;
2. fresh exact security/binding authority review at
   `source record 140`;
3. maintainer status `APPROVED`;
4. implementation still requires one path-scoped task contract for shared
   security contracts;
5. a later separate binding-task contract imports this lifecycle policy and owns
   only instantiated signer identity, templates, permission/signature edges and
   attestation.

Rollback removes unused internal models. Once persisted by a provider bundle,
V1 canonical bytes are immutable and successors use new version/domain names.

## Approval checklist

- [x] Shared security and binding security are acyclic.
- [x] Principal, permission, certificate and secret models are exact.
- [x] Self-digest recursion is forbidden.
- [x] Lifecycle and reinstall behavior are closed.
- [x] Public compatibility impact is none.
- [x] Core implementation commit is pinned.
- [x] Fresh reviews approve the exact specification.
- [x] Maintainer changed status to `APPROVED`.
