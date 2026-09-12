<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider renderer contract

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: exact core descriptor, security, migration and binding commits
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.
  Receipt rendering must resolve the dedicated provider-install receipt and its
  V2 parameter/result authority before this child can become implementation-ready.

## Outcome and authority boundary

This child specification defines a closed statement registry and deterministic
SQL evidence for one R1 provider authority:

```text
canonical provider inputs
→ exact source-leaf registry
→ deterministic render
→ source/definition/coverage validation
→ in-session execution
→ retained non-executable evidence
```

The rendered bundle is evidence, not executable authority. The installer never
executes a deserialized bundle; it re-renders from canonical provider inputs in
the owned transaction, validates the bytes, obtains ephemeral secrets and
executes only that fresh result.

In scope are pure renderer source/rule/registry/evidence contracts and hermetic
tests. SQL renderer implementation, installer, secret port, catalog I/O and
live SQL Server evidence are separate. Public APIs do not change.

This algebra specification is approved before the exact SQL-template catalog.
An executable renderer implementation task starts only after both this contract
and `feature-design-postgres-mssql-r1-v3-provider-sql-template-catalog-v1.md`
are separately maintainer-`APPROVED`; renderer and catalog may then be
implemented in one path-scoped task. This ordering avoids a circular gate.

## Canonical rules

All types are exact frozen slotted dataclasses. Own digests are derived:

```text
canonical_bytes = canonical_bytes(EXACT_DOMAIN, ordered_fields)
digest = SHA256(canonical_bytes)
```

External digest references resolve byte-identically in aggregate context.
Ordered statement and leaf sequences use contiguous one-based ordinals.
Semantic identifiers reject exact and casefold collisions. Raw enum strings,
opaque SQL source refs, unknown fields and silent render fallback are forbidden.

## Closed enums

```text
RendererPhase:
  01_observe_migration
  02_receipt_precheck
  03_shared_schema
  04_shared_security_prepare
  05_shared_modules
  06_shared_security_sign
  07_shared_security_remove_key
  08_binding_security_prepare
  09_binding_modules
  10_binding_permissions
  11_binding_security_sign
  12_binding_security_remove_key
  13_attest
  14_receipt_append

RendererStatementKind:
  migration_query | create_schema | create_table | create_index |
  create_trigger | create_procedure | add_extended_property |
  create_certificate | create_certificate_user | grant_permission |
  sign_module | remove_private_key | binding_module_create |
  binding_permission_grant | attestation_query |
  receipt_probe | receipt_append

RendererExecutionMode:
  parameterized | static_definition | ephemeral_secret_template

RendererTemplateCompositionKind:
  fixed_render | complete_intrinsic_passthrough |
  fixed_render_matches_intrinsic

RendererIntrinsicUseKind:
  complete_statement | validation_only | equality_witness

RendererResultAbiKind:
  none | source_owned | receipt_storage_observation

RendererResultColumnSourceKind:
  count_big | stored_coordinate

RendererTransactionOwnership:
  installer_serializable

RendererEffectKind:
  rowset | ddl | grant | signature | private_key_removal |
  receipt_read | receipt_append | none

TableLeafKind:
  base_table | index | trigger | extended_property

IdentifierBindingKind:
  schema | object | column | environment_principal |
  certificate | certificate_user

ScalarBindingKind:
  typed_literal | procedure_parameter | receipt_runtime_value

TokenBindingKind:
  permission_keyword | grant_option_clause

ParameterCoordinateSelectorKind:
  extended_property_value | receipt_effect_key | receipt_request_digest |
  receipt_payload_bytes | receipt_payload_digest

ReceiptAuthorityKind: receipt_probe | receipt_append  # imported from migration

ClosedRenderRuleKind:
  migration_query_v1 | create_schema_v1 | create_table_v1 |
  create_index_v1 | create_trigger_v1 | create_procedure_v1 |
  add_extended_property_v1 | shared_create_certificate_v1 |
  shared_create_certificate_user_v1 | shared_grant_permission_v1 |
  shared_sign_module_v1 | shared_remove_private_key_v1 |
  binding_create_certificate_v1 | binding_create_certificate_user_v1 |
  binding_module_create_v1 | binding_permission_grant_v1 |
  binding_sign_module_v1 | binding_remove_private_key_v1 |
  attestation_query_v1 | receipt_probe_v1 | receipt_append_v1

SharedSecurityLeafKind:
  create_certificate | create_certificate_user | grant_permission |
  add_signature | remove_private_key

BindingSecurityLeafKind:
  create_certificate | create_certificate_user | grant_permission |
  add_signature | remove_private_key

AttestationQueryKind:  # imported from migration
  schema | table | module | certificate | permission | signature |
  binding_prefix_inventory

RendererSourceLeafKind:
  identifier | scalar | definition | secret_template | observation |
  permission | receipt_identity | receipt_payload |
  attestation_query_definition

RendererSourceArmKind:
  core_schema | core_table_leaf | core_procedure | shared_security_leaf |
  migration_observation | binding_module | binding_security_leaf |
  attestation_query | receipt_authority

RendererEligibilityKind:
  pre_decision_observation | post_decision | post_decision_authority
```

Runtime binding-module caller-UoW is an execution semantic, not an installer
renderer transaction scope, and therefore is absent from this enum.

## Exact source tagged union

```python
MssqlR1CoreSchemaStatementSourceV1(
    schema_name: str,
)
MssqlR1CoreTableLeafStatementSourceV1(
    table_ref: MssqlR1PhysicalResourceRefV1,
    leaf_kind: MssqlR1TableLeafKindV1,
    leaf_ordinal: int,
    source_leaf_digest: bytes,
)
MssqlR1CoreProcedureStatementSourceV1(
    procedure_ref: MssqlR1PhysicalResourceRefV1,
    definition_digest: bytes,
)
MssqlR1SharedSecurityLeafStatementSourceV1(
    signer_profile: MssqlR1SignerProfileKindV3,
    security_leaf_kind: MssqlR1SharedSecurityLeafKindV1,
    leaf_ordinal: int,
    source_leaf_digest: bytes,
)
MssqlR1MigrationObservationStatementSourceV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    observation_digest: bytes,
)
MssqlR1BindingModuleStatementSourceV1(
    module_kind: MssqlR1BindingModuleKindV1,
    module_definition_digest: bytes,
)
MssqlR1BindingSecurityLeafStatementSourceV1(
    binding_security_leaf_kind: MssqlR1BindingSecurityLeafKindV1,
    leaf_ordinal: int,
    source_leaf_digest: bytes,
)
MssqlR1AttestationQueryStatementSourceV1(
    attestation_kind: MssqlR1AttestationQueryKindV1,
    query_definition_digest: bytes,
)
MssqlR1ReceiptAuthorityStatementSourceV1(
    authority_kind: MssqlR1ReceiptAuthorityKindV1,
    source_contract_digest: bytes,
)
```

The nine exact arm domains, in union order, are:

```text
dpone-mssql-r1-renderer-core-schema-source-v1\0
dpone-mssql-r1-renderer-core-table-leaf-source-v1\0
dpone-mssql-r1-renderer-core-procedure-source-v1\0
dpone-mssql-r1-renderer-shared-security-leaf-source-v1\0
dpone-mssql-r1-renderer-migration-observation-source-v1\0
dpone-mssql-r1-renderer-binding-module-source-v1\0
dpone-mssql-r1-renderer-binding-security-leaf-source-v1\0
dpone-mssql-r1-renderer-attestation-query-source-v1\0
dpone-mssql-r1-renderer-receipt-authority-source-v1\0
```

Decoder selects by exact domain. No arm accepts arbitrary SQL bytes or a
generic semantic path.
Binding module and binding security are different arms.
`MssqlR1RendererStatementSourceV1` is exactly these nine arms. Every leaf kind
above has a dedicated payload/ref codec; `source_leaf_digest` always resolves
to that payload. Attestation query definition bytes are owned by the migration
post-install-attestation contract, not authored in the renderer.

## Source leaves and table expansion

The provider aggregate constructs the expected ordered source tuple:

```text
all managed schemas once
+ for every table:
    base table once
    every schema-2 index once in ordinal order
    every schema-2 trigger once in ordinal order
    every schema-2 extended property once in ordinal order
+ every shared procedure once
+ every shared-security lifecycle leaf once
+ every migration observation once
+ every one of six static binding module template selectors once
+ every binding-security lifecycle leaf once
+ every attestation query once
+ one receipt probe and one receipt append authority source
```

The base-table leaf includes columns, keys, checks, uniqueness and foreign-key
constraints. Those constraints do not receive separate statement leaves in V1.
Every expected source is flattened as `(owner_rank, leaf_kind_rank,
local_ordinal, source_digest)`. Local ordinals are one-based within their exact
owner/kind tuple. Registry sources must equal this ordered tuple, not merely a
multiset; missing, extra, duplicate, foreign or reordered leaves fail.

Owner ranks equal source-union order: core schema=1, core table=2, core
procedure=3, shared security=4, migration observation=5, binding module=6,
binding security=7, attestation query=8, receipt authority=9. Table leaf ranks
are base table=1, index=2, trigger=3, extended property=4. Shared/binding
security ranks are create certificate=1, create certificate user=2, grant=3,
signature=4, remove private key=5. Attestation ranks equal the declared
`AttestationQueryKind` order; receipt probe=1 and append=2. Final statement
order is `(phase rank, owner_rank, owner canonical coordinate,
leaf_kind_rank, local_ordinal)`.

For each table:

```text
whole_table_ddl_utf8 = CONCAT(render(leaf) by leaf_ordinal)

expected_digest = SHA256(canonical_bytes(
  b"dpone-r1-physical-table-ddl-v1\0",
  (whole_table_ddl_utf8,)
))

expected_digest == core_table.definition_digest
whole_table_ddl_utf8 == core_table.definition.utf8_bytes
```

No separator or blank line may be inserted unless present in the leaf bytes.
Shared procedure and binding module definitions validate with the existing
`module_definition_digest()` and direct byte equality.

## Typed bindings and render rules

```python
MssqlR1RendererSourceLeafRefV1(
    source_digest: bytes,
    leaf_kind: MssqlR1RendererSourceLeafKindV1,
    field_ordinal: int,
)
MssqlR1IdentifierBindingV1(
    ordinal: int,
    binding_kind: MssqlR1IdentifierBindingKindV1,
    placeholder: str,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
)
MssqlR1ScalarBindingV1(
    ordinal: int,
    binding_kind: MssqlR1ScalarBindingKindV1,
    placeholder: str,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    scalar_kind: MssqlR1ProjectionScalarKindV1,
)
MssqlR1TokenBindingV1(
    ordinal: int,
    binding_kind: MssqlR1TokenBindingKindV1,
    placeholder: str,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
)
MssqlR1IntrinsicSourcePolicyV1(
    source_arm_kind: MssqlR1RendererSourceArmKindV1,
    leaf_kind: MssqlR1RendererSourceLeafKindV1,
    field_ordinal: int,
    use_kind: MssqlR1RendererIntrinsicUseKindV1,
)
MssqlR1ApprovedSqlTemplateKeyV1(
    rule_kind: MssqlR1ClosedRenderRuleKindV1,
    permission_scope: MssqlR1PermissionScopeV3 | None,
)
MssqlR1ApprovedParameterAbiV1(
    position: int,
    placeholder: str,
    binding_kind: MssqlR1ScalarBindingKindV1,
    scalar_kind: MssqlR1ProjectionScalarKindV1,
    coordinate_selector: MssqlR1ParameterCoordinateSelectorKindV1,
    sql_type_descriptor_payload: bytes,
    direction: Literal["input"],
)
MssqlR1ApprovedSqlTemplateV1(
    template_key: MssqlR1ApprovedSqlTemplateKeyV1,
    composition_kind: MssqlR1RendererTemplateCompositionKindV1,
    template_utf8_bytes: bytes | None,
    template_payload_digest: bytes,
    ordered_identifier_placeholders: tuple[str, ...],
    ordered_scalar_placeholders: tuple[str, ...],
    ordered_parameter_abi: tuple[MssqlR1ApprovedParameterAbiV1, ...],
    ordered_token_placeholders: tuple[str, ...],
    ordered_intrinsic_source_policies: tuple[
        MssqlR1IntrinsicSourcePolicyV1, ...
    ],
    result_abi_kind: MssqlR1RendererResultAbiKindV1,
    fixed_result_abi_payload: bytes | None,
    secret_placeholder_count: int,
)
MssqlR1ApprovedTemplateCatalogV1(
    catalog_version: Literal["dpone-mssql-r1-approved-template-catalog-1"],
    ordered_templates: tuple[MssqlR1ApprovedSqlTemplateV1, ...],
)
MssqlR1FixedIdentifierSchemaV1(
    ordered_identifier_kinds: tuple[MssqlR1IdentifierBindingKindV1, ...],
)
MssqlR1PermissionScopeBindingRowV1(
    scope: MssqlR1PermissionScopeV3,
    ordered_identifier_kinds: tuple[MssqlR1IdentifierBindingKindV1, ...],
)
MssqlR1PermissionIdentifierSchemaV1(
    ordered_grantee_kinds: tuple[MssqlR1IdentifierBindingKindV1, ...],
    ordered_scope_rows: tuple[MssqlR1PermissionScopeBindingRowV1, ...],
)
MssqlR1RenderCompatibilityRowV1(
    rule_kind: MssqlR1ClosedRenderRuleKindV1,
    source_arm_kind: MssqlR1RendererSourceArmKindV1,
    statement_kind: MssqlR1RendererStatementKindV1,
    phase: MssqlR1RendererPhaseV1,
    execution_mode: MssqlR1RendererExecutionModeV1,
    effect_kind: MssqlR1RendererEffectKindV1,
    eligibility_kind: MssqlR1RendererEligibilityKindV1,
    ordered_dispositions: tuple[MssqlR1ProviderMigrationDispositionV1, ...],
    composition_kind: MssqlR1RendererTemplateCompositionKindV1,
    result_abi_kind: MssqlR1RendererResultAbiKindV1,
    identifier_schema: (
        MssqlR1FixedIdentifierSchemaV1 |
        MssqlR1PermissionIdentifierSchemaV1
    ),
    ordered_scalar_specs: tuple[
        tuple[MssqlR1ScalarBindingKindV1, MssqlR1ProjectionScalarKindV1], ...
    ],
    ordered_token_binding_kinds: tuple[MssqlR1TokenBindingKindV1, ...],
    ordered_intrinsic_leaf_kinds: tuple[MssqlR1RendererSourceLeafKindV1, ...],
    secret_placeholder_count: int,
)
MssqlR1RenderRuleV1(
    rule_kind: MssqlR1ClosedRenderRuleKindV1,
    rule_version: Literal[1],
    compatibility: MssqlR1RenderCompatibilityRowV1,
    ordered_template_keys: tuple[MssqlR1ApprovedSqlTemplateKeyV1, ...],
    ordered_consumed_source_leaves: tuple[
        MssqlR1RendererSourceLeafRefV1, ...
    ],
)
```

Domains use `dpone-mssql-r1-renderer-{source-leaf-ref|identifier-binding|scalar-binding|token-binding|intrinsic-source-policy|approved-template-key|approved-parameter-abi|approved-template-catalog|fixed-identifier-schema|permission-scope-binding-row|permission-identifier-schema|render-compatibility-row|render-rule}-v1\0`.
`MssqlR1ClosedRenderRuleKindV1` has exactly one member for every row of the
compatibility table below. Each non-permission member resolves exactly one
NULL-scope template; each of the two permission members resolves exactly four
templates in permission-scope enum order. Every resolved template has one exact
composition/identifier/scalar/token/intrinsic schema; arbitrary rule IDs or template bytes
are not admitted.
`MssqlR1ApprovedSqlTemplateV1` has domain
`dpone-mssql-r1-approved-sql-template-v1\0`; its payload digest is
`SHA256(canonical_bytes(b"dpone-mssql-r1-approved-sql-template-payload-v1\0",
(composition_kind, template_utf8_bytes, ordered_identifier_placeholders,
ordered_scalar_placeholders, ordered_parameter_abi, ordered_token_placeholders,
ordered_intrinsic_source_policies, result_abi_kind, fixed_result_abi_payload,
secret_placeholder_count)))`. The only admissible catalog is the code-owned
`MSSQL_R1_APPROVED_TEMPLATE_CATALOG_V1` canonical payload. The exact catalog
payload/digest, all required fixed skeleton bytes and every passthrough source
authority must first be frozen or pinned in a separate
maintainer-`APPROVED` SQL-template-catalog child specification. Until that child
exists, this algebra may be reviewed and approved, but executable renderer
implementation and registry construction remain activation-blocked. The
implementation constant and golden vectors must equal
that approved payload byte-for-byte; a recomputed digest for different SQL is
invalid. Non-permission rules have one key with NULL scope. Each permission
rule has four keys in permission-scope enum order and therefore four fixed
placeholder schemas; no optional identifier slot exists. Placeholder tuples
are exact, duplicate-free and equal the statement
bindings in ordinal order. Identifiers are resolved only from typed schema/object/column/principal/
certificate leaves and quoted by one canonical SQL Server identifier renderer.
String concatenation of identifiers inside rules is forbidden. Scalars are
typed non-secret literals/parameters.

Composition is closed and mechanical:

```text
fixed_render:
  template bytes required; render typed identifier, scalar and token bindings
  into their exact positions; any intrinsic leaves are validation-only and
  are never inserted into SQL

complete_intrinsic_passthrough:
  template bytes absent; identifier/scalar/token tuples empty; exactly one
  intrinsic leaf contains the complete already-authoritative SQL bytes

fixed_render_matches_intrinsic:
  template bytes required; exactly one complete intrinsic SQL-template leaf;
  render typed non-secret bindings into the approved skeleton, require the
  result to equal the intrinsic bytes byte-for-byte, then substitute only the
  transaction-local secret for execution
```

The passthrough arm is not concatenation: the intrinsic value is the complete
statement, is validated against its owning descriptor/query/module digest and
is consumed exactly once. The compare arm reconciles the generic approved
skeleton with the fully identifier-resolved `MssqlR1SecretSqlTemplateV1`
authority; identifier leaves are consumed by rendering and the intrinsic leaf
is consumed only as the required equality witness. No intrinsic bytes are
inserted into a fixed template.

Each intrinsic policy names the exact source tagged-union arm, one-based field
ordinal and use. The catalog does not retain an instance digest. At registry
construction the statement's source arm must match the policy, the field
ordinal must resolve to that arm's exact declared field, the resulting
`MssqlR1RendererSourceLeafRefV1` must occur in the rule, and its leaf kind/use
must match. `complete_intrinsic_passthrough` requires one
`complete_statement`; `fixed_render_matches_intrinsic` requires one
`equality_witness`; `fixed_render` admits only zero or more
`validation_only` policies. This is the exact key-to-source policy equation;
per-instance refs and digests remain statement-registry authority.

The complete consumed source-leaf multiset declared by the render rule must
equal the union of identifier/scalar/token binding source leaves and intrinsic
source
leaves required by the statement kind. Every leaf is consumed exactly once;
unused or multiply consumed authority fails.

Secret templates reuse `MssqlR1SecretSqlTemplateV1`. They contain one typed
secret placeholder but no secret bytes or secret scalar binding. Their catalog
entries use `fixed_render_matches_intrinsic`; the fully resolved source template
contains no identifier placeholder. The secret placeholder is derived from
execution mode/template kind, not an independent boolean. Private-key removal
is not a secret template and uses `fixed_render` with zero secret placeholders.
Create-certificate comparison resolves, in exact order, certificate identifier,
certificate owner identifier, subject text, start-date text and expiry-date
text from the shared certificate metadata or instantiated binding lifecycle
authority. Those are all non-secret values that vary across signer instances;
omitting any one is invalid. Add-signature comparison resolves certificate,
schema and module identifiers. The generic skeleton with all non-secret values
rendered must equal the source-owned secret template byte-for-byte before the
ephemeral secret can be substituted.

## Statement admission

Eligibility is a tagged union:

```python
MssqlR1PreDecisionObservationEligibilityV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
)
MssqlR1PostDecisionEligibilityV1(
    ordered_dispositions: tuple[
        MssqlR1ProviderMigrationDispositionV1, ...
    ],
)
MssqlR1PostDecisionAuthorityEligibilityV1(
    ordered_dispositions: tuple[
        MssqlR1ProviderMigrationDispositionV1, ...
    ],
    authority_operation: MssqlR1ReceiptAuthorityKindV1,
)
```

Migration queries use pre-decision only. Mutations admit install and/or
coexist-then-install. Attestation may additionally admit read-only replay.
BLOCK is never admissible. Every install observation, mutation, attestation and
authority statement executes in the same installer-serializable UoW; effect
kind distinguishes read-only query from mutation.

```python
MssqlR1RendererStatementSpecV1(
    ordinal: int,
    statement_id: str,
    phase: MssqlR1RendererPhaseV1,
    statement_kind: MssqlR1RendererStatementKindV1,
    source: MssqlR1RendererStatementSourceV1,
    render_rule: MssqlR1RenderRuleV1,
    ordered_identifier_bindings: tuple[MssqlR1IdentifierBindingV1, ...],
    ordered_scalar_bindings: tuple[MssqlR1ScalarBindingV1, ...],
    ordered_token_bindings: tuple[MssqlR1TokenBindingV1, ...],
    result_abi_kind: MssqlR1RendererResultAbiKindV1,
    result_abi_payload: bytes | None,
    execution_mode: MssqlR1RendererExecutionModeV1,
    transaction_ownership: Literal["installer_serializable"],
    eligibility: (
        MssqlR1PreDecisionObservationEligibilityV1 |
        MssqlR1PostDecisionEligibilityV1 |
        MssqlR1PostDecisionAuthorityEligibilityV1
    ),
    expected_effect_kind: MssqlR1RendererEffectKindV1,
)
```

Domain: `dpone-mssql-r1-renderer-statement-spec-v1\0`.

The canonical `MssqlR1RenderCompatibilityRowV1` tuple is normative. The table
below is its complete human projection as `(rule_kind, source_arm,
statement_kind, phase, mode, effect, dispositions, composition, result ABI,
identifier schema, scalar schema, token schema, intrinsic schema, secret count)`.
Shorthands below are exact ordered
tuples: `S=(schema)`,
`SO=(schema,object)`, `SOC=(schema,object,column)`, `C=(certificate)`,
`CU=(certificate_user)`, `CE=(certificate,environment_principal)`,
`T=((typed_literal,text))`,
`CM=((typed_literal,text),(typed_literal,text),(typed_literal,text))`,
`R=((receipt_runtime_value,digest),(receipt_runtime_value,digest),
(receipt_runtime_value,binary),(receipt_runtime_value,digest))`,
`PT=(permission_keyword,grant_option_clause)`; `FX/IP/MI` mean
`fixed_render/complete_intrinsic_passthrough/fixed_render_matches_intrinsic`;
`source/storage/-` mean
`source_owned/receipt_storage_observation/none` result ABI;
`-` is the
empty tuple. `I/C/R` dispositions mean
install/coexist/replay respectively; `PRE` means
`eligibility_kind=pre_decision_observation` with an empty disposition tuple.

| Rule | Source | Kind | Phase | Mode | Effect | Disp. | Comp. | Result | IDs | Scalars | Tokens | Intrinsic | Secret |
|---|---|---|---:|---|---|---|---|---|---|---|---|---|---:|
| `migration_query_v1` | migration observation | migration query | 01 | static | rowset | PRE | IP | source | - | - | - | observation | 0 |
| `receipt_probe_v1` | receipt authority/probe | receipt probe | 02 | parameterized | receipt read | I,C,R | FX | storage | SO | `((receipt_runtime_value,digest))` | - | receipt identity | 0 |
| `create_schema_v1` | core schema | create schema | 03 | static | ddl | I,C | FX | - | S | - | - | - | 0 |
| `create_table_v1` | core table/base | create table | 03 | static | ddl | I,C | IP | - | - | - | - | definition | 0 |
| `create_index_v1` | core table/index | create index | 03 | static | ddl | I,C | IP | - | - | - | - | definition | 0 |
| `create_trigger_v1` | core table/trigger | create trigger | 03 | static | ddl | I,C | IP | - | - | - | - | definition | 0 |
| `add_extended_property_v1` | core table/property | add extended property | 03 | parameterized | ddl | I,C | FX | - | SO | T | - | - | 0 |
| `shared_create_certificate_v1` | shared security/create cert | create certificate | 04 | secret | ddl | I,C | MI | - | CE | CM | - | secret template | 1 |
| `shared_create_certificate_user_v1` | shared security/create user | create certificate user | 04 | static | ddl | I,C | FX | - | C,CU | - | - | - | 0 |
| `shared_grant_permission_v1` | shared security/grant | grant permission | 04 | static | grant | I,C | FX | - | `PERM_SHARED` | - | PT | - | 0 |
| `create_procedure_v1` | core procedure | create procedure | 05 | static | ddl | I,C | IP | - | - | - | - | definition | 0 |
| `shared_sign_module_v1` | shared security/sign | sign module | 06 | secret | signature | I,C | MI | - | C,SO | - | - | secret template | 1 |
| `shared_remove_private_key_v1` | shared security/remove key | remove private key | 07 | static | private-key removal | I,C | FX | - | C | - | - | - | 0 |
| `binding_create_certificate_v1` | binding security/create cert | create certificate | 08 | secret | ddl | I,C | MI | - | CE | CM | - | secret template | 1 |
| `binding_create_certificate_user_v1` | binding security/create user | create certificate user | 08 | static | ddl | I,C | FX | - | C,CU | - | - | - | 0 |
| `binding_module_create_v1` | binding module | binding module create | 09 | static | ddl | I,C | IP | - | - | - | - | definition | 0 |
| `binding_permission_grant_v1` | binding security/grant | binding permission grant | 10 | static | grant | I,C | FX | - | `PERM_BINDING` | - | PT | - | 0 |
| `binding_sign_module_v1` | binding security/sign | sign module | 11 | secret | signature | I,C | MI | - | C,SO | - | - | secret template | 1 |
| `binding_remove_private_key_v1` | binding security/remove key | remove private key | 12 | static | private-key removal | I,C | FX | - | C | - | - | - | 0 |
| `attestation_query_v1` | attestation query | attestation query | 13 | static | rowset | I,C,R | IP | source | - | - | - | attestation query definition | 0 |
| `receipt_append_v1` | receipt authority/append | receipt append | 14 | parameterized | receipt append | I,C | FX | - | SO | R | - | receipt payload | 0 |

`static`, `parameterized` and `secret` above are the exact enum values
`static_definition`, `parameterized` and `ephemeral_secret_template`.
`PERM_SHARED` permits grantee kinds `(environment_principal,
certificate_user)`; `PERM_BINDING` permits `(certificate_user,
environment_principal)`, both in this exact order. Each is one
`MssqlR1PermissionIdentifierSchemaV1` whose exact four scope rows are:

```text
database -> (G)
schema   -> (G, schema)
object   -> (G, schema, object)
column   -> (G, schema, object, column)
```

Scope rows occur once in enum order. The statement's typed permission edge
selects exactly one row; the resolved identifier tuple must equal it, while the
other three rows are not statement placeholders. Shared rows use the exact
environment/shared-signer grantee kind selected by their edge; binding rows use
the exact binding-certificate-user or resolved-runtime-principal grantee kind
selected by their edge. Grantee kind changes only the typed identifier source,
not SQL grammar, so the scope-keyed template remains byte-identical across its
allowed grantee kinds. Unused positions are absent, never NULL. Effect
display aliases in the table map exactly as `private-key removal ->
private_key_removal`, `receipt read -> receipt_read`, `receipt append ->
receipt_append`; intrinsic display aliases map `secret template ->
secret_template`, `receipt identity -> receipt_identity`, `receipt payload ->
receipt_payload`, and `attestation query definition ->
attestation_query_definition`; all other effect/intrinsic tokens are literal
enum values. Each
identifier/scalar/token/intrinsic entry denotes exactly one source-field ref per
listed element in tuple order. No other combination or default is legal.
Construction expands every shorthand before model creation, then requires the
21 canonical compatibility rows to occur exactly once in the phase/execution
order shown by the normative table. This is a separate closed tuple because a
receipt precheck must execute before mutation even though `receipt_probe_v1`
occurs later in `ClosedRenderRuleKind`. The template key catalog instead uses
closed-rule enum order and contains exactly 27 entries: one NULL-scope key
for each of 19 non-permission rules plus four scope-keyed entries for each of
the two permission rules. Keys are unique and globally ordered by closed-rule
enum order, then permission-scope enum order; NULL scope sorts as the sole key
for a non-permission rule. The row's source arm matches the exact tagged-union class; eligibility
kind and dispositions match the statement eligibility arm; template and
binding placeholder schemas equal the row byte-for-byte. Markdown tokens are
never parsed at runtime. Receipt ordering and ABSENT/EXACT branches are further
constrained by the migration contract.

Permission statements render only positive expected edges. Their typed source
must have `effect=GRANT`; `DENY` and forbidden-edge observations are attestation
inputs and are never executable renderer statements. `permission_keyword`
accepts only an exact uppercase token present in the resolved approved
security/binding authority; it is not free text. `grant_option_clause` maps
`False` to the empty token and `True` to the exact catalog-owned clause
` WITH GRANT OPTION`. Binding permissions require `False`; only a shared edge
whose approved authority explicitly has `grant_option=True` may select the
nonempty token. `include_descendants` affects catalog-attestation expansion,
not SQL grammar. The four scope templates use the exact SQL Server
`DATABASE`, `SCHEMA`, `OBJECT` and column coordinate forms; no wider-scope
fallback is allowed.

Renderer placeholders form a parsed token stream, not repeated string
replacement. Identifier, scalar and token placeholders have separate exact
ASCII namespaces and contiguous three-digit ordinals; each declared token
occurs exactly once, undeclared brace tokens are invalid, and overlapping or
recursive replacement is impossible. The SQL-template-catalog child freezes
the literal token spellings and all resulting golden bytes.

## Parameter and result ABI

```python
MssqlR1ResultColumnAbiV1(
    ordinal: int,
    name: str,
    projection_scalar_kind: MssqlR1ProjectionScalarKindV1,
    source_kind: MssqlR1RendererResultColumnSourceKindV1,
    source_coordinate: MssqlR1ComparisonCoordinateV1 | None,
    sql_type_descriptor_payload: bytes,
    nullable: bool,
)
MssqlR1ResultSetAbiV1(
    cardinality: Literal["exactly_one"],
    ordered_columns: tuple[MssqlR1ResultColumnAbiV1, ...],
)
MssqlR1ReceiptProbeStorageObservationV1(
    row_count: int,
    stored_request_digest: bytes | None,
    stored_payload_bytes: bytes | None,
    stored_payload_digest: bytes | None,
)
```

Domains are `dpone-mssql-r1-renderer-{result-column-abi|result-set-abi|receipt-probe-storage-observation}-v1\0`.
Result ordinals are contiguous and names are exact/casefold unique. `count_big`
requires NULL source coordinate, exact SQL `bigint` and non-nullability;
`stored_coordinate` requires a coordinate that resolves into the receipt
resource. SQL type payloads decode through the physical descriptor type model
and must equal the stored coordinate's resolved column. `nullable` is exact,
not inferred from a Python value.

Parameterized templates retain DB-API positional markers in rendered SQL and
execute with one ordered parameter tuple. In parameterized mode a scalar is
never embedded as a SQL literal. Stable non-secret `typed_literal` bindings in
static/secret skeletons use one strict kind-specific literal renderer; their
escaped bytes are compared to source authority where the composition arm
requires it. Each DB-API marker occurs once and is wrapped by the exact target
type. The catalog-owned `MssqlR1ApprovedParameterAbiV1` is executable authority,
not evidence: positions are contiguous, placeholders equal the parameterized
scalar subset in binding order, and its closed coordinate selector resolves to
the exact physical receipt field or approved extended-property system ABI. Its
SQL type payload must equal that resolved authority. Input-only is the sole R1
direction. Rendered evidence contains a byte-identical resolved projection of
the approved ABI and never runtime values.

The receipt probe accepts only `installation_effect_key` and returns exactly one
storage-observation row:

```text
row_count,
stored_request_digest,
stored_payload_bytes,
stored_payload_digest
```

`row_count=0` and `row_count>=2` return all stored fields NULL; only
`row_count=1` returns the three stored fields. The SQL never chooses an
arbitrary duplicate and never receives current expected request/payload values.
The application constructs `absent|exact|mismatch|ambiguous` against the current
admitted authority. Probe column order, cardinality and SQL types, the append
parameter order `effect_key, request_digest, payload_bytes, payload_digest`,
and the extended-property text parameter type are derived from and pinned to
the exact concrete physical descriptor in the catalog child.

Result ABI selection is closed:

```text
migration_query_v1   -> source_owned observation result contract
attestation_query_v1 -> source_owned attestation result contract
receipt_probe_v1     -> receipt_storage_observation result set above
all other rules      -> none and NULL result_abi_payload
```

`source_owned` payloads reproduce the exact query-definition result authority.
`receipt_storage_observation` reproduces a five-model chain: exact result-set
ABI, one decoded raw row, the storage observation above, current admitted
request authority and the derived migration probe result. No query row directly
claims `exact` or `mismatch`.

The catalog template carries `fixed_result_abi_payload` only for
`receipt_storage_observation`; it must decode as the exact result set above.
`source_owned` and `none` require NULL at catalog level. Each statement carries
the resolved `result_abi_payload`: for source-owned queries it is selected from
the exact observation/attestation definition; for receipt probe it must equal
the catalog fixed payload; for all other statements it is NULL. Thus evidence
never creates result ABI authority.

## Registry and render validation

```python
MssqlR1RendererStatementRegistryV1(
    contract_version: Literal["dpone-mssql-r1-renderer-registry-1"],
    physical_descriptor_payload: bytes,
    shared_security_profile_payload: bytes,
    migration_contract_payload: bytes,
    binding_contract_payload: bytes,
    approved_template_catalog_payload: bytes,
    ordered_compatibility_rows: tuple[MssqlR1RenderCompatibilityRowV1, ...],
    ordered_statements: tuple[MssqlR1RendererStatementSpecV1, ...],
)
```

Domain: `dpone-mssql-r1-renderer-statement-registry-v1\0`. Nested payloads
decode exactly; the approved catalog payload must byte-equal the code-owned
catalog constant; the registry proves source multiset equality, compatibility,
phase order, typed bindings and disposition closure against those objects.
The registry is static provider authority: binding entries select only the six
template kinds and lifecycle leaf kinds from the binding-instantiation
contract. Per-binding render resolves those selectors against the embedded
`MssqlR1BindingModulePackV1`, records the resolved typed payloads below and
requires all binding UUID/module-definition/security-leaf digests to match.
Per-instance digests never appear as static registry source leaves.
The migration payload contains the exact seven ordered attestation query
definitions and typed result-authority mapping; the registry's seven
attestation sources resolve them one-to-one. Receipt probe renders only one
`installation_effect_key` scalar and returns the closed storage-observation row; the
application constructs `MssqlR1MigrationReceiptProbeResultV1`. Receipt append
alone renders the four receipt payload scalars.

```python
MssqlR1ResolvedIdentifierV1(
    binding_ordinal: int,
    binding_kind: MssqlR1IdentifierBindingKindV1,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    canonical_identifier: str,
    quoted_sql_utf8_bytes: bytes,
)
MssqlR1ResolvedScalarV1(
    binding_ordinal: int,
    binding_kind: MssqlR1ScalarBindingKindV1,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    scalar_kind: MssqlR1ProjectionScalarKindV1,
    canonical_value_payload: bytes,
)
MssqlR1ResolvedTokenV1(
    binding_ordinal: int,
    binding_kind: MssqlR1TokenBindingKindV1,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    canonical_token_ascii: str,
)
MssqlR1ResolvedParameterAbiV1(
    approved_parameter_abi_payload: bytes,
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    resolved_target_coordinate: MssqlR1ComparisonCoordinateV1 | None,
)
MssqlR1ResolvedRendererSourceLeafV1(
    source_leaf: MssqlR1RendererSourceLeafRefV1,
    source_payload: bytes,
    source_payload_digest: bytes,
)
MssqlR1RenderedStatementEvidenceV1(
    statement_spec_payload: bytes,
    ordered_resolved_identifiers: tuple[MssqlR1ResolvedIdentifierV1, ...],
    ordered_resolved_scalars: tuple[MssqlR1ResolvedScalarV1, ...],
    ordered_resolved_tokens: tuple[MssqlR1ResolvedTokenV1, ...],
    ordered_parameter_abi: tuple[MssqlR1ResolvedParameterAbiV1, ...],
    ordered_resolved_source_leaves: tuple[
        MssqlR1ResolvedRendererSourceLeafV1, ...
    ],
    sql_template_utf8_bytes: bytes,
)
```

Domain: `dpone-mssql-r1-rendered-statement-evidence-v1\0`. The SQL bytes are
static executable bytes for non-secret statements or the non-executable
secret-template bytes. Executed secret-substituted SQL never enters the model.
The resolved tuples are complete plan evidence for this statement, in declared
binding/leaf order; they are not an executed-subset log. Eligibility and
execution outcome are recorded by installer evidence, not inferred here.
The resolved models use domains
`dpone-mssql-r1-rendered-{identifier|scalar|token|resolved-parameter-abi|source-leaf}-v1\0`. Identifier bytes
must reproduce the canonical quote function; scalar/source payloads decode by
their exact kind and reproduce adjacent digests. Ordinals and tuple order equal
the statement spec.

```python
MssqlR1RenderedProviderBundleEvidenceV1(
    contract_version: Literal["dpone-mssql-r1-rendered-evidence-1"],
    provider_contract_payload: bytes,
    principal_authority_payload: bytes,
    binding_pack_payload: bytes,
    ordered_statement_evidence: tuple[
        MssqlR1RenderedStatementEvidenceV1, ...
    ],
)
```

Domain: `dpone-mssql-r1-rendered-provider-evidence-v1\0`. Full canonical inputs,
not bare digest strings, are retained to prevent splice. Validation requires:

1. decode and validate provider/principal/binding authorities;
2. deterministically re-render every statement;
3. reproduce each source/input/template byte sequence;
4. prove complete registry coverage;
5. prove every whole-table and module definition equality;
6. reproduce the bundle digest property.

The installer still cannot execute this deserialized evidence. Offline
execution requires a future signed-envelope ADR and capability.

## Module architecture and tests

Recommended cohesive modules, each targeting 200–300 SLOC:

```text
mssql_r1_renderer_sources.py
mssql_r1_renderer_bindings.py
mssql_r1_renderer_rules.py
mssql_r1_renderer_registry.py
mssql_r1_rendered_evidence.py
```

Dependencies flow only downward in that order; evidence may depend on the
registry. No generic SQL AST, plugin registry or umbrella god module is added.

Hermetic red-green tests cover every enum/field/union, source-arm substitution,
missing/extra/reordered leaf, identifier/scalar/token/parameter binding splice,
composition-arm mismatch, leaf consumption gap/duplicate, full compatibility
matrix, pre/post admission, secret template
leak, table concatenation/digest, module definition equality, nested-payload
splice and canonical decode. Mutation cases are explicitly `must_reject` or
`valid_distinct`; authority/security weakening always rejects.

Live SQL syntax, catalog equivalence, transaction ownership, secret handling
and execution remain `UNVERIFIED` until separate approved provider tasks.

## Approval checklist

- [x] Source algebra is a closed tagged union.
- [x] Statement phase/kind/source/admission compatibility is closed.
- [x] Whole-table and module byte equations are exact.
- [x] Rendered bundle is evidence, not executable authority.
- [x] Own digest recursion is absent.
- [x] Intrinsic SQL has a closed complete-statement passthrough arm.
- [x] Source-owned secret templates are reconciled by exact skeleton equality.
- [x] Private-key removal is non-secret and has zero secret placeholders.
- [x] Permission tokens, grant-option selection and receipt observation ABI are closed.
- [ ] Exact dependency commits are pinned.
- [ ] Concrete physical descriptor and exact receipt/type coordinates are pinned.
- [ ] Before executable implementation, the exact SQL-template-catalog child is
  maintainer-APPROVED and pinned.
- [ ] Fresh architecture/security/test reviews approve the specification.
- [ ] Maintainer changes status to `APPROVED`.
