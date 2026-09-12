<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider migration contract

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: exact implemented `MssqlR1PhysicalSchemaDescriptorV1` commit
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`.
  Before implementation this researched child must adopt the dedicated receipt,
  V2 effect key/payload digest and V2 security replay authority.

## Outcome and scope

This child specification closes the migration authority chain:

```text
core probe authority
→ finite read-only observation
→ deterministic decision
→ admitted statement sequence
→ post-install attestation
→ target-local receipt
```

All steps execute under one SQL Server `SERIALIZABLE` installer transaction and
one exact schema lock. A decision never commits independently of the mutation
it admits. This scope defines pure immutable contracts and algorithms only; SQL
rendering, ODBC execution, catalog I/O and live certification are separate.
Public APIs, CLI and manifests do not change.

## Canonical rules and limits

Every model is an exact frozen slotted dataclass. Canonical encoding uses the
existing V3 codec; decode reruns all validation and rejects unknown/trailing
fields. Own digests are derived properties:

```text
canonical_bytes = canonical_bytes(EXACT_DOMAIN, ordered_fields)
digest = SHA256(canonical_bytes)
```

A constructor digest is legal only as a reference to a different authority and
must be reproduced in aggregate context.

Exact R1 limits are:

```text
observations                       <= 16
result columns per observation     <= 12
token values per column            <= 32
result vectors per observation     <= 4096
complete decision vectors          <= 65536
decision rules                     <= 4096
```

Exceeding a limit is a stable contract error, never fallback behavior.

All ordinals in observations, result columns, classifiers and rules are
contiguous one-based. Result-column and outcome/rule identifiers are NFC ASCII,
1..64 bytes, match `[a-z][a-z0-9_]*`, and are exact- and casefold-unique in
their owner. Every digest is exactly 32 bytes. `observed_at` and `generated_at`
use canonical UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Token vectors are sorted by
canonical bytes and duplicate-free; classifier order is exactly MATCH then
NO_MATCH. For a result contract:

```text
domain_cardinality = product(2 for boolean else token_count)
2 <= domain_cardinality <= 4096
```

Every finite vector occurs exactly once across the two classifier sets.

## Closed enums

```text
ProviderMigrationDisposition:
  install | coexist_then_install | read_only_replay_attest | block

MigrationMatchKind:
  match | no_match

MigrationObservationFailureKind:
  zero_rows | multiple_rows | null_value |
  type_mismatch | out_of_domain

MigrationObservationAbortKind: query_error

MigrationExecutionFailureKind:
  sql_error | admitted_plan_error | result_contract_error |
  attestation_mismatch | receipt_error

MigrationExecutionFailureSourceKind:
  statement_sql | aggregate_validation | receipt_validation

MigrationBlockerSourceKind:
  visibility_failure | core_probe | decision_rule | default

MigrationReceiptSourceField:
  effect_key | request_digest | payload_bytes | payload_digest

ReceiptAuthorityKind: receipt_probe | receipt_append

MigrationReceiptProbeOutcome:
  absent | exact | mismatch | ambiguous

MigrationReceiptStorageOutcome:
  absent | single | ambiguous

AttestationQueryKind:
  schema | table | module | certificate | permission | signature |
  binding_prefix_inventory

AttestationResultAuthorityKind:
  schema_inventory | table_inventory | module_inventory |
  certificate_inventory | permission_inventory | signature_inventory |
  binding_prefix_inventory

MigrationStatementPhase:
  pre_decision_observe | post_decision_receipt_precheck |
  post_decision_mutate | post_decision_attest |
  post_decision_receipt_append

MigrationExecutionStage:
  admitted_plan_validation | receipt_precheck | mutations |
  attestation | receipt_append

MigrationUowFailureOperation:
  session_configuration | begin_transaction | schema_lock | commit | rollback

MigrationUowOutcomeCertainty:
  known_not_committed | commit_outcome_unknown

MigrationRollbackTriggerKind:
  prior_uow_failure | observation_abort | decision_block | execution_abort

MigrationOperationOutcome:
  committed | blocked_rolled_back | execution_failed_rolled_back |
  cleanup_failed_not_committed | commit_outcome_unknown

MigrationReceiptDisposition:
  created_in_uow | existing_probed
```

The immutable core-to-provider mapping is:

```text
core install  → install
core coexist  → coexist_then_install
core replay   → read_only_replay_attest
core block    → block
```

## Exact core references

```python
MssqlR1CoreMigrationProbeRefV1(
    probe_id: str,
    probe_leaf_digest: bytes,
)
```

Domain: `dpone-r1-provider-migration-core-probe-ref-v1\0`. Resolution requires
one core probe with the exact ID and SHA-256 of its canonical bytes. The provider
derives, rather than copies, core semantics from the resolved probe. Its
read-resource tuple must contain the exact core observation resource; the core
probe must require zero mutation; MATCH derives the immutable mapped
disposition and exact blocker source/code from that probe. These values are not
duplicated as provider constructor fields and therefore cannot diverge.

Error ownership is a tagged union:

```python
MssqlR1SharedProcedureErrorOwnerRefV1(
    procedure_name: str,
)
MssqlR1BindingTemplateErrorOwnerRefV1(
    module_kind: MssqlR1BindingModuleKindV1,
)
MssqlR1CorePhysicalErrorRefV1(
    owner: (
        MssqlR1SharedProcedureErrorOwnerRefV1 |
        MssqlR1BindingTemplateErrorOwnerRefV1
    ),
    condition_id: str,
    error_leaf_digest: bytes,
)
```

Domains use `dpone-r1-provider-migration-{shared-error-owner|binding-error-owner|core-error-ref}-v1\0`.
The visibility error resolves to one core condition with error 51011, blocked
retry, no fresh probe, public redaction and a non-null public blocker code.

## Finite observation algebra

Arbitrary SQL comparison formulas are forbidden. Each query normalizes catalog
facts into a finite boolean/token vector.

```python
MssqlR1MigrationBooleanResultColumnV1(
    ordinal: int,
    name: str,
)
MssqlR1MigrationTokenResultColumnV1(
    ordinal: int,
    name: str,
    ordered_allowed_tokens: tuple[str, ...],
)
```

Domains: `dpone-r1-provider-migration-{boolean|token}-result-column-v1\0`.
Tokens are a nonempty canonical set of ASCII values matching
`[a-z][a-z0-9_]{0,63}`. Allowed tokens sort by unsigned UTF-8 bytes and reject
duplicates. Result vectors sort lexicographically by the canonical bytes of
their ordered typed values and reject duplicates. Outcome IDs use the same
grammar. Blocker/default codes match `[a-z][a-z0-9_.]{0,127}` and are
casefold-unique in their owner.

```python
MssqlR1MigrationResultContractV1(
    contract_version: Literal["dpone-r1-migration-result-1"],
    ordered_columns: tuple[
        MssqlR1MigrationBooleanResultColumnV1 |
        MssqlR1MigrationTokenResultColumnV1,
        ...
    ],
)
```

Domain: `dpone-r1-provider-migration-result-contract-v1\0`. It describes exactly
one non-null row. Zero/multiple rows, NULL, wrong type and unknown values are
visibility failures rather than classifier inputs.

Values are a tagged union:

```python
MssqlR1MigrationBooleanValueV1(value: bool)
MssqlR1MigrationTokenValueV1(value: str)
MssqlR1MigrationResultVectorV1(
    ordered_values: tuple[
        MssqlR1MigrationBooleanValueV1 |
        MssqlR1MigrationTokenValueV1,
        ...
    ],
)
```

Domains: `dpone-r1-provider-migration-{boolean-value|token-value|result-vector}-v1\0`.
A vector exactly matches column count, order, tag and allowed domain.

```python
MssqlR1MigrationObservationClassifierV1(
    ordinal: int,
    outcome_id: str,
    match_kind: MssqlR1MigrationMatchKindV1,
    ordered_result_vectors: tuple[MssqlR1MigrationResultVectorV1, ...],
)
```

Domain: `dpone-r1-provider-migration-classifier-v1\0`. Every observation has
exactly one MATCH and one NO_MATCH classifier. Both vector sets are nonempty,
disjoint and together equal the finite Cartesian result domain.

```python
MssqlR1ReadOnlySqlDefinitionV1(
    utf8_bytes: bytes,
    definition_digest: bytes,
)
```

Domain: `dpone-r1-provider-migration-read-only-sql-definition-v1\0`.
`definition_digest` is a validated content digest, not a self-digest:

```text
SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-read-only-sql-payload-v1\0",
  (utf8_bytes,)
))
```

Bytes are NFC UTF-8, LF-only, NUL-free and end in exactly one LF.

```python
MssqlR1MigrationObservationContractV1(
    ordinal: int,
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    query_definition: MssqlR1ReadOnlySqlDefinitionV1,
    result_contract: MssqlR1MigrationResultContractV1,
    ordered_read_resources: tuple[MssqlR1PhysicalResourceRefV1, ...],
    ordered_classifiers: tuple[MssqlR1MigrationObservationClassifierV1, ...],
    visibility_failure_error_ref: MssqlR1CorePhysicalErrorRefV1,
)
```

Domain: `dpone-r1-provider-migration-observation-contract-v1\0`. There is one
observation per core probe in core order. Read resources are nonempty, canonical,
resolve to READ-capable declarations and include the exact core observation
resource. SQL read-only semantics remain a renderer/live-test responsibility.

```python
MssqlR1MigrationOutcomeRefV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    outcome_id: str,
)
```

Domain: `dpone-r1-provider-migration-outcome-ref-v1\0`. It resolves to exactly
one classifier; no independent observation ID namespace exists.

## Decision plan

```python
MssqlR1CertifiedServerBuildProfileV1(
    profile_id: Literal["sqlserver-2022-standalone-r1"],
    product_major_version: Literal[16],
    product_version_prefix: Literal["16."],
    database_compatibility_level: Literal[160],
    ordered_allowed_product_levels: tuple[str, ...],
    ordered_allowed_engine_editions: tuple[int, ...],
)
MssqlR1MigrationDecisionRuleV1(
    ordinal: int,
    rule_id: str,
    ordered_required_outcome_refs: tuple[MssqlR1MigrationOutcomeRefV1, ...],
    disposition: MssqlR1ProviderMigrationDispositionV1,
    blocker_code: str | None,
)
```

Domain: `dpone-r1-provider-migration-decision-rule-v1\0`. Required refs form a
nonempty canonical conjunctive partial vector and cannot name two outcomes for
one probe. BLOCK requires a blocker; all other dispositions require NULL.

```python
MssqlR1MigrationUowProfileV1(
    isolation_level: Literal["SERIALIZABLE"],
    xact_abort: Literal[True],
    autocommit: Literal[False],
    schema_lock_step: MssqlR1PhysicalLockStepV1,
)
```

Domain: `dpone-r1-provider-migration-uow-profile-v1\0`. The lock is the exact
declared transaction-owned exclusive singleton schema application lock using
ACQUIRE.

```python
MssqlR1MigrationDecisionPlanV1(
    plan_version: Literal["dpone-r1-migration-decision-plan-1"],
    physical_schema_descriptor_digest: bytes,
    certified_server_build_profile: MssqlR1CertifiedServerBuildProfileV1,
    ordered_observations: tuple[MssqlR1MigrationObservationContractV1, ...],
    ordered_attestation_query_definitions: tuple[
        MssqlR1AttestationQueryDefinitionV1, ...
    ],
    ordered_decision_rules: tuple[MssqlR1MigrationDecisionRuleV1, ...],
    default_blocker_code: str,
    uow_profile: MssqlR1MigrationUowProfileV1,
    target_receipt_binding: MssqlR1MigrationTargetReceiptBindingV1,
    zero_mutation_before_decision: Literal[True],
)
```

Domains are `dpone-r1-certified-server-build-profile-v1\0` and
`dpone-r1-provider-migration-decision-plan-v1\0`. The build profile's allowed
product levels are nonempty, case-sensitive canonical ASCII tokens in sorted
byte order; engine editions are a nonempty strictly increasing tuple of
positive SQL integers. GA1 freezes `ordered_allowed_product_levels=("RTM",)`
and `ordered_allowed_engine_editions=(2,3,4)`; a later edition/profile requires
a new capability tuple. The R1 profile is embedded in the provider aggregate
and is not caller-selected at runtime. Construction enumerates
the bounded complete observation space and proves every rule reachable, no
vector matches two rules, matched core BLOCK dominates, mapped matched-core
nonblocking dispositions equal the selected rule, and no nonblocking rule
admits a vector without matched nonblocking authority. Unmatched vectors always
BLOCK with the default blocker.

## Runtime artifacts

```python
MssqlR1MigrationTargetRefV1(
    identity_contract_version: Literal[
        "dpone-mssql-target-physical-identity-1"
    ],
    target_database_identity_digest: bytes,
)
MssqlR1MigrationUowBindingV1(
    installer_attempt_uuid: UUID,
    target_ref: MssqlR1MigrationTargetRefV1,
    uow_profile_digest: bytes,
    session_nonce_digest: bytes,
)
```

Domains: `dpone-r1-provider-migration-{target-ref|uow-binding}-v1\0`. Session
nonce is correlation only and never restart authority.

Observation entries are:

```python
MssqlR1MigrationObservedResultV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    result_vector: MssqlR1MigrationResultVectorV1,
    outcome_ref: MssqlR1MigrationOutcomeRefV1,
)
MssqlR1MigrationVisibilityFailureV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    failure_kind: MssqlR1MigrationObservationFailureKindV1,
    error_ref: MssqlR1CorePhysicalErrorRefV1,
)
MssqlR1MigrationObservationSetV1(
    contract_version: Literal["dpone-r1-migration-observation-set-1"],
    migration_plan_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    uow_binding: MssqlR1MigrationUowBindingV1,
    ordered_entries: tuple[
        MssqlR1MigrationObservedResultV1 |
        MssqlR1MigrationVisibilityFailureV1,
        ...
    ],
    observed_at: str,
)
```

Domains: `dpone-r1-provider-migration-{observed-result|visibility-failure|observation-set}-v1\0`.
The set contains one entry per observation in plan order; time is canonical UTC.

A SQL execution error may doom the transaction under `XACT_ABORT ON`, so it
never fabricates a complete set:

```python
MssqlR1MigrationObservationAbortV1(
    migration_plan_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    uow_binding: MssqlR1MigrationUowBindingV1,
    ordered_completed_entries: tuple[
        MssqlR1MigrationObservedResultV1 |
        MssqlR1MigrationVisibilityFailureV1,
        ...
    ],
    failed_core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
    failure_kind: MssqlR1MigrationObservationAbortKindV1,
    error_ref: MssqlR1CorePhysicalErrorRefV1,
    observed_at: str,
)
```

Domain: `dpone-r1-provider-migration-observation-abort-v1\0`. Completed entries
are the exact contiguous prefix and may contain normalized non-query visibility
failures. The installer rolls back immediately and emits
`execution_failed_rolled_back`; it constructs no observation set, decision,
admitted plan, attestation or receipt. Non-execution visibility failures are
typed non-query-error visibility entries and remain complete entries.

```python
MssqlR1MigrationBlockerRefV1(
    source_kind: MssqlR1MigrationBlockerSourceKindV1,
    blocker_code: str,
    source_digest: bytes,
)
MssqlR1MigrationDecisionRuleRefV1(
    rule_id: str,
    rule_digest: bytes,
)
MssqlR1MigrationDecisionV1(
    contract_version: Literal["dpone-r1-migration-decision-1"],
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    installer_attempt_uuid: UUID,
    selected_rule_ref: MssqlR1MigrationDecisionRuleRefV1 | None,
    disposition: MssqlR1ProviderMigrationDispositionV1,
    ordered_blockers: tuple[MssqlR1MigrationBlockerRefV1, ...],
)
```

Domains: `dpone-r1-provider-migration-{blocker-ref|decision-rule-ref|decision}-v1\0`.
Blocker sources are visibility_failure, core_probe, decision_rule or default.
Visibility failure dominates matched core blocker, then explicit rule, then
default. BLOCK requires nonempty blockers; nonblocking requires none and an
exact selected-rule ref.

The total algorithm retains only the first dominant blocker class, then all
members of that class in plan-observation order; explicit-rule blockers use
rule order and default produces exactly one blocker. `source_digest` is the
canonical digest of the complete failure entry, resolved core probe, selected
decision rule or decision plan respectively. `selected_rule_ref` is NULL for
visibility/core/default blocks, exact for an explicit-rule BLOCK, and exact for
every nonblocking decision. Complete outcomes in plan order and matched rule
refs in decision-rule order are deterministically derived from the bound
observation set and plan; they are not duplicated as decision constructor
fields.

## Statement admission

Renderer statement eligibility is a tagged union:

```python
MssqlR1PreDecisionObservationAdmissionV1(
    core_probe_ref: MssqlR1CoreMigrationProbeRefV1,
)
MssqlR1PostDecisionAdmissionV1(
    ordered_dispositions: tuple[MssqlR1ProviderMigrationDispositionV1, ...],
)
MssqlR1PostDecisionAuthorityAdmissionV1(
    authority_operation: MssqlR1ReceiptAuthorityKindV1,
    ordered_dispositions: tuple[MssqlR1ProviderMigrationDispositionV1, ...],
)
```

Migration queries use only pre-decision admission. Mutating entries admit only
install/coexist. Attestation may additionally admit read-only replay. BLOCK is
never admitted.
Receipt probe is complete for install/coexist/replay; receipt append is complete
only for install/coexist. Probe and append have distinct migration phases and
must resolve to the renderer registry's typed receipt authority statements.

Migration phases project to renderer phases exactly:

```text
renderer 01       -> pre_decision_observe
renderer 02       -> post_decision_receipt_precheck
renderer 03..12   -> post_decision_mutate
renderer 13       -> post_decision_attest
renderer 14       -> post_decision_receipt_append
```

`MssqlR1PreDecisionObservationAdmissionV1` projects field-for-field to renderer
pre-decision eligibility. Post-decision disposition tuples project
field-for-field. Authority admission uses the shared
`MssqlR1ReceiptAuthorityKindV1` directly; no second authority enum or reordered
tuple is allowed.

```python
MssqlR1MigrationStatementRefV1(
    statement_id: str,
    statement_spec_digest: bytes,
    phase: MssqlR1MigrationStatementPhaseV1,
)
MssqlR1AdmittedInstallPlanV1(
    contract_version: Literal["dpone-r1-admitted-install-plan-1"],
    provider_contract_digest: bytes,
    renderer_registry_digest: bytes,
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    decision_digest: bytes,
    installer_attempt_uuid: UUID,
    target_ref: MssqlR1MigrationTargetRefV1,
    disposition: MssqlR1ProviderMigrationDispositionV1,
    ordered_pre_decision_observation_refs: tuple[MssqlR1MigrationStatementRefV1, ...],
    ordered_post_decision_receipt_precheck_refs: tuple[
        MssqlR1MigrationStatementRefV1, ...
    ],
    ordered_post_decision_mutation_refs: tuple[MssqlR1MigrationStatementRefV1, ...],
    ordered_post_decision_attestation_refs: tuple[MssqlR1MigrationStatementRefV1, ...],
    ordered_post_decision_receipt_append_refs: tuple[
        MssqlR1MigrationStatementRefV1, ...
    ],
    target_receipt_binding_digest: bytes,
)
```

Domains: `dpone-r1-provider-migration-{statement-ref|admitted-install-plan}-v1\0`.
BLOCK cannot construct a plan. Pre-decision refs exactly cover observations.
Mutation is empty only for read-only replay; install/coexist requires the exact
nonempty mutation set. Attestation, receipt precheck and receipt append are
complete. Execution concatenates the five tuples in field order above; every
tuple independently preserves renderer registry order. No mixed authority tuple
or runtime merge/sort is permitted.

Exact authority sequencing is disposition-dependent:

```text
install/coexist:
  one receipt_probe -> ABSENT required -> admitted mutations -> attestation
  -> one receipt_append -> EXACTLY_ONE -> verify

read_only_replay_attest:
  one receipt_probe -> EXACT required -> no mutation/no append
  -> attestation -> compare stable catalog authority

lost-commit recovery:
  fresh session + same lock -> one receipt_probe
  EXACT -> committed; ABSENT -> restart observation; MISMATCH/AMBIGUOUS -> block
```

No disposition admits both probe and append at the same authority point, and
`append/probe` is never selected by runtime convenience.

## Receipt authority and recovery

Post-install authority is a typed payload, not a bare digest:

```python
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
MssqlR1AttestationQueryDefinitionV1(
    attestation_kind: MssqlR1AttestationQueryKindV1,
    query_definition: MssqlR1ReadOnlySqlDefinitionV1,
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1,
    result_cardinality: Literal["zero_or_many"],
)
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
MssqlR1CertificateQueryResultV1(
    ordered_shared_certificates: tuple[
        MssqlR1CertificateCatalogObservationV1, ...
    ],
    binding_signer: MssqlR1ObservedBindingSignerV1,
)
MssqlR1PermissionQueryResultV1(
    ordered_schema_permissions: tuple[MssqlR1ObservedPermissionV3, ...],
    ordered_forbidden_shared_permissions: tuple[MssqlR1ObservedPermissionV3, ...],
    ordered_binding_permissions: tuple[MssqlR1ObservedBindingPermissionV1, ...],
)
MssqlR1SignatureQueryResultV1(
    ordered_core_signatures: tuple[MssqlR1ModuleSignatureObservationV3, ...],
    ordered_binding_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...],
)
MssqlR1BindingPrefixInventoryQueryResultV1(
    ordered_binding_prefix_inventory: tuple[
        MssqlR1ObservedBindingPrefixObjectV1, ...
    ],
)
MssqlR1PostInstallStatementResultV1(
    statement_ref: MssqlR1MigrationStatementRefV1,
    attestation_kind: MssqlR1AttestationQueryKindV1,
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1,
    query_definition_digest: bytes,
    typed_result: (
        MssqlR1SchemaQueryResultV1 | MssqlR1TableQueryResultV1 |
        MssqlR1ModuleQueryResultV1 | MssqlR1CertificateQueryResultV1 |
        MssqlR1PermissionQueryResultV1 | MssqlR1SignatureQueryResultV1 |
        MssqlR1BindingPrefixInventoryQueryResultV1
    ),
)
MssqlR1StableSchemaAttestationV1(
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
    ordered_observed_permissions: tuple[MssqlR1ObservedPermissionV3, ...],
    expected_schema_contract_digest: bytes,
    principal_authority_set_digest: bytes,
    observed_schema_inventory_digest: bytes,
    observed_security_inventory_digest: bytes,
    live_identity_digest: bytes,
    projection_revision: int,
)
MssqlR1StableSharedSecurityAttestationV1(
    ordered_certificate_observations: tuple[
        MssqlR1CertificateCatalogObservationV1, ...
    ],
    ordered_forbidden_permission_observations: tuple[
        MssqlR1ObservedPermissionV3, ...
    ],
    ordered_forbidden_membership_observations: tuple[
        MssqlR1ObservedRoleMembershipV3, ...
    ],
)
MssqlR1StableBindingAttestationV1(
    binding_pack_digest: bytes,
    live_catalog_identity: MssqlR1BindingLiveCatalogIdentityV1,
)
MssqlR1StableCatalogAttestationV1(
    attestation_version: Literal["dpone-r1-stable-catalog-attestation-1"],
    target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    renderer_registry_digest: bytes,
    ordered_statement_results: tuple[MssqlR1PostInstallStatementResultV1, ...],
    stable_schema_attestation: MssqlR1StableSchemaAttestationV1,
    stable_shared_security_attestation: MssqlR1StableSharedSecurityAttestationV1,
    stable_binding_attestation: MssqlR1StableBindingAttestationV1,
)
MssqlR1PostInstallAttestationEnvelopeV1(
    envelope_version: Literal["dpone-r1-post-install-attestation-envelope-1"],
    migration_plan_digest: bytes,
    observation_set_digest: bytes,
    decision_digest: bytes,
    admitted_install_plan_digest: bytes,
    installer_attempt_uuid: UUID,
    stable_catalog_attestation_payload: bytes,
    stable_catalog_attestation_digest: bytes,
)
```

Every declared model has a literal matching domain:

```text
dpone-r1-observed-database-identity-v1\0
dpone-r1-attestation-query-definition-v1\0
dpone-r1-{schema|table|module|certificate|permission|signature|binding-prefix}-query-result-v1\0
dpone-r1-post-install-statement-result-v1\0
dpone-r1-stable-{schema|shared-security|binding|catalog}-attestation-v1\0
dpone-r1-post-install-attestation-envelope-v1\0
```

Observed database identity is sourced from the same physical session as all
catalog queries. `database_id` is a fresh positive live SQL integer and is not
a registered/restart identity. Server digest is exactly 32 bytes; database,
family and recovery-fork values are exact non-nil UUIDs. Database name uses the
existing schema-2 SQL Server identifier validator (maximum 128 Unicode code
points) and its digest equals the canonical identifier digest. Collation is
the exact ASCII token `Latin1_General_100_BIN2` and therefore equals the
physical descriptor engine profile. Product version and level are nonempty
ASCII with maximum 64 and 32 bytes respectively; version matches
`16\.[0-9]+\.[0-9]+\.[0-9]+`, begins with the embedded profile prefix and has
major 16. Compatibility level equals 160; product level and engine edition are
members of the embedded exact profile tuples. Server digest, GUID/family/fork
and database name/digest equal `MssqlTargetPhysicalIdentityV1`. Stable schema copies
`server_instance_identity_sha256`, `database_id`, `database_guid`,
`database_family_guid` and `recovery_fork_guid` exactly. Stable binding retains
the complete identity including name, collation and build coordinates. No
session/SPID/transaction timestamp belongs to this identity.

Query definitions are
embedded in the migration aggregate and are the sole authority for renderer
attestation-query sources. Coverage/mapping is exact:

```text
schema                   -> schema_inventory         -> SchemaQueryResult
table                    -> table_inventory          -> TableQueryResult
module                   -> module_inventory         -> ModuleQueryResult
certificate              -> certificate_inventory    -> CertificateQueryResult
permission               -> permission_inventory     -> PermissionQueryResult
signature                -> signature_inventory      -> SignatureQueryResult
binding_prefix_inventory -> binding_prefix_inventory -> BindingPrefixInventoryQueryResult
```

Each query kind occurs once in this order. Result-authority kind selects the
listed union arm and exact row type; raw/self-digested result bytes are forbidden.
Results cover every admitted attestation statement once in registry order.
Target/provider/descriptor/renderer bindings are stable; run-specific
plan/observation/decision/attempt bindings exist only in the envelope.

`MssqlR1StableSchemaAttestationV1.from_schema_attestation()` copies the exact
implemented `MssqlR1SchemaAttestationV3` fields in declared order except
`observed_at` and its time-bearing `attestation_digest`; it revalidates every
remaining derived digest. Stable shared security is built only from typed
catalog observations. Stable binding embeds the timestamp-free live catalog
identity from `MssqlR1BindingAttestationV1`, never that attestation's
`observed_at`. Let the seven typed query results in declared order be
`QS, QT, QM, QC, QP, QG, QB`. Stable projection uses only these exact equations:

```text
stable_schema.database coordinates          = projection(QS.database_identity)
stable_schema.ordered_observed_schemas       = QS.ordered_schemas
stable_schema.ordered_observed_principals    = QS.ordered_principals
stable_schema.ordered_observed_role_memberships = QS.ordered_role_memberships
stable_schema.ordered_observed_objects       = stable_merge(
  QT.ordered_table_objects,
  QM.ordered_core_module_objects,
  schema_object_canonical_order
)
stable_schema.ordered_observed_permissions   = QP.ordered_schema_permissions
flatten(stable_schema object signatures)     = QG.ordered_core_signatures

stable_shared.ordered_certificate_observations = QC.ordered_shared_certificates
stable_shared.ordered_forbidden_permission_observations = QP.ordered_forbidden_shared_permissions = ()
stable_shared.ordered_forbidden_membership_observations = filter_forbidden_shared(
  QS.ordered_role_memberships
) = ()

stable_binding.live_catalog_identity.database_identity = QS.database_identity
stable_binding.live_catalog_identity.ordered_modules = QM.ordered_binding_modules
stable_binding.live_catalog_identity.signer = QC.binding_signer
stable_binding.live_catalog_identity.ordered_permissions = QP.ordered_binding_permissions
stable_binding.live_catalog_identity.ordered_signatures = QG.ordered_binding_signatures
stable_binding.live_catalog_identity.ordered_binding_prefix_inventory = QB.ordered_binding_prefix_inventory
```

`stable_merge` is a stable two-way merge of two individually canonical,
disjoint tuples; table/module object-kind predicates are mutually exclusive and
their union equals the expected core object set. `filter_forbidden_shared` is
the exact shared-security policy predicate, not caller code. Any nonempty
forbidden tuple blocks stable-catalog and receipt construction. Every source
field above is consumed exactly where stated; the intentional membership reuse
is a predicate check, not a duplicated authority value. All equalities compare
canonical bytes and ordering. There is no post-decode normalization.

Replay compares canonical `MssqlR1StableCatalogAttestationV1` bytes and never
compares run envelopes. All envelope bindings equal the current admitted plan
byte-for-byte, and its embedded stable payload decodes and reproduces the
adjacent digest. The receipt retains that stable payload so certificate
thumbprint/user/SID pinning is source-free.

```python
MssqlR1MigrationReceiptFieldBindingV1(
    source_field: MssqlR1MigrationReceiptSourceFieldV1,
    target_field: MssqlR1ComparisonCoordinateV1,
)
MssqlR1MigrationTargetReceiptBindingV1(
    binding_version: Literal[
        "dpone-r1-migration-target-receipt-binding-1"
    ],
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    payload_codec_version: Literal["dpone-r1-migration-receipt-payload-1"],
    ordered_field_bindings: tuple[MssqlR1MigrationReceiptFieldBindingV1, ...],
    same_database_required: Literal[True],
    same_transaction_required: Literal[True],
)
MssqlR1MigrationReceiptProbeResultV1(
    installation_effect_key: bytes,
    storage_outcome: MssqlR1MigrationReceiptStorageOutcomeV1,
    outcome: MssqlR1MigrationReceiptProbeOutcomeV1,
    row_count: int,
    stored_request_digest: bytes | None,
    stored_payload_bytes: bytes | None,
    stored_payload_digest: bytes | None,
)
```

Domains:
`dpone-r1-provider-migration-{receipt-field-binding|receipt-binding|receipt-probe-result}-v1\0`.
The four source fields effect_key, request_digest, payload_bytes and
payload_digest occur once and map to one immutable target-local READ+INSERT
receipt resource. If the core cannot express the mapping, implementation stops
for a core amendment. The approved security-authority V2 amendment now supplies
the dedicated twentieth table; implementation may not invent a twenty-first
table or fall back to a control/effect receipt.

The probe accepts exactly one runtime scalar, `installation_effect_key`, and
returns the typed result above. `row_count` is a non-negative SQL integer.
Storage/result shape is closed:

```text
row_count = 0  -> storage=absent,    outcome=absent,    all stored fields NULL
row_count = 1  -> storage=single,    outcome=exact|mismatch
row_count >= 2 -> storage=ambiguous, outcome=ambiguous, all stored fields NULL
```

For `single`, `exact` requires all three stored fields present, valid payload
decode and every equation below. `mismatch` permits either all three present or
a partial NULL/non-NULL tuple so corrupt legacy/storage rows remain
representable but never admissible. The result constructor receives the current
`MssqlR1MigrationInstallRequestAuthorityV1` from the admitted aggregate and
performs the current-request comparison; expected request/payload values are
never SQL probe inputs. Ambiguous results retain no arbitrarily selected row.

```python
MssqlR1MigrationInstallRequestAuthorityV1(
    target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    physical_schema_descriptor_digest: bytes,
    renderer_registry_digest: bytes,
    migration_plan_digest: bytes,
    shared_security_profile_digest: bytes,
    binding_pack_digest: bytes,
)
MssqlR1MigrationInstallReceiptPayloadV1(
    receipt_version: Literal["dpone-r1-migration-install-receipt-1"],
    installation_effect_key: bytes,
    install_request_authority: MssqlR1MigrationInstallRequestAuthorityV1,
    observation_set_digest: bytes,
    decision_digest: bytes,
    admitted_install_plan_digest: bytes,
    rendered_bundle_digest: bytes,
    stable_catalog_attestation_payload: bytes,
    stable_catalog_attestation_digest: bytes,
    original_disposition: (
        Literal["install"] | Literal["coexist_then_install"]
    ),
)
```

Domains are
`dpone-r1-provider-migration-install-request-authority-v1\0` and
`dpone-r1-provider-migration-receipt-payload-v1\0`.
The embedded stable catalog attestation decodes exactly and its canonical
digest equals `stable_catalog_attestation_digest`; a digest-only receipt is
invalid. Run-specific attestation envelopes are evidence, not replay identity.

```text
installation_effect_key = SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-receipt-effect-key-v1\0",
  (target_database_identity_digest, provider_contract_digest)
))

request_digest = SHA256(
  install_request_authority.canonical_bytes
)

payload_bytes =
  MssqlR1MigrationInstallReceiptPayloadV1.canonical_bytes

payload_digest = SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-install-receipt-bytes-v1\0",
  (payload_bytes,)
))
```

The four `MssqlR1MigrationReceiptSourceFieldV1` members are exactly
`effect_key, request_digest, payload_bytes, payload_digest` in that order and
map one-to-one to the target receipt coordinates. INSERT compares no prior row
and must create exactly one immutable effect key. Probe reads exactly that key
and requires all four stored values to equal the equations above; partial or
coherent-but-different rows block.

Append validates the current stable request authority, constructs the original
audit payload and stores its four derived fields. Replay decodes the stored
payload, validates every internal digest/equation, and compares the current run
only to `install_request_authority` plus the newly observed stable catalog
attestation. Original decision/admitted-plan/attempt fields remain audit facts
and are never recomputed from a replay attempt. Lost-commit recovery uses the
same stored-payload validation. Thus a new replay envelope is not expected to
equal the original install envelope.

```python
MssqlR1MigrationInstallReceiptRefV1(
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1,
    installation_effect_key: bytes,
    receipt_payload_digest: bytes,
    disposition: MssqlR1MigrationReceiptDispositionV1,
)
```

Domain: `dpone-r1-provider-migration-receipt-ref-v1\0`. Install/coexist appends
the receipt in the same UoW after attestation. Read-only replay probes the exact
existing receipt without rewriting it.

Lost commit response discards the session and probes under the same schema lock
on a fresh session. Exact receipt plus exact attestation means committed; row
absence restarts observation; mismatch/corruption/ambiguity blocks.

## One-UoW algorithm

```text
fresh non-pooled connection
→ autocommit OFF, XACT_ABORT ON, SERIALIZABLE
→ BEGIN
→ acquire exact transaction-owned schema lock
→ execute every pre-decision observation
→ construct observation set and deterministic decision
→ BLOCK: ROLLBACK and emit blocked evidence
→ construct admitted install plan
→ execute disposition-specific receipt precheck
→ execute admitted mutations
→ execute exact post-install attestation
→ append target-local authority receipt for install/coexist only
→ verify receipt and attestation
→ COMMIT
```

No observation, decision or admitted plan is committed before final COMMIT.

## Evidence and testing

```python
MssqlR1StatementSqlFailureRefV1(
    failed_statement_ref: MssqlR1MigrationStatementRefV1,
    error_number: int | None,
)
MssqlR1AggregateValidationFailureRefV1(
    validation_kind: Literal[
        "result_contract" | "stable_attestation" | "admitted_plan"
    ],
    failed_aggregate_digest: bytes | None,
)
MssqlR1ReceiptValidationFailureRefV1(
    authority_operation: MssqlR1ReceiptAuthorityKindV1,
    probe_outcome: MssqlR1MigrationReceiptProbeOutcomeV1 | None,
    receipt_probe_result_digest: bytes | None,
)
MssqlR1RollbackTriggerRefV1(
    trigger_kind: MssqlR1MigrationRollbackTriggerKindV1,
    trigger_payload: bytes,
    trigger_digest: bytes,
)
MssqlR1PriorUowFailureTriggerV1(
    operation: Literal["schema_lock"],
    native_error_number: int | None,
    transaction_state_before_rollback: Literal["active", "doomed"],
)
MssqlR1MigrationUowFailureV1(
    operation: MssqlR1MigrationUowFailureOperationV1,
    certainty: MssqlR1MigrationUowOutcomeCertaintyV1,
    native_error_number: int | None,
    transaction_state: Literal["not_started", "rolled_back", "unknown"],
    ordered_completed_statement_refs: tuple[MssqlR1MigrationStatementRefV1, ...],
    completed_statement_prefix_digest: bytes,
    rollback_trigger_ref: MssqlR1RollbackTriggerRefV1 | None,
)
MssqlR1MigrationExecutionAbortV1(
    failure_kind: MssqlR1MigrationExecutionFailureKindV1,
    failure_source_kind: MssqlR1MigrationExecutionFailureSourceKindV1,
    failure_source: (
        MssqlR1StatementSqlFailureRefV1 |
        MssqlR1AggregateValidationFailureRefV1 |
        MssqlR1ReceiptValidationFailureRefV1
    ),
    execution_stage: MssqlR1MigrationExecutionStageV1,
    statement_phase: MssqlR1MigrationStatementPhaseV1 | None,
    ordered_completed_statement_refs: tuple[MssqlR1MigrationStatementRefV1, ...],
    core_error_ref: MssqlR1CorePhysicalErrorRefV1 | None,
    attempted_attestation_envelope_payload: bytes | None,
)
MssqlR1MigrationEvidenceV1(
    evidence_version: Literal["dpone-r1-migration-evidence-1"],
    target_ref: MssqlR1MigrationTargetRefV1,
    provider_contract_digest: bytes,
    migration_plan_digest: bytes,
    observation_set: MssqlR1MigrationObservationSetV1 | None,
    observation_abort: MssqlR1MigrationObservationAbortV1 | None,
    uow_failure: MssqlR1MigrationUowFailureV1 | None,
    execution_abort: MssqlR1MigrationExecutionAbortV1 | None,
    decision: MssqlR1MigrationDecisionV1 | None,
    admitted_install_plan: MssqlR1AdmittedInstallPlanV1 | None,
    receipt_probe_result: MssqlR1MigrationReceiptProbeResultV1 | None,
    target_receipt_ref: MssqlR1MigrationInstallReceiptRefV1 | None,
    post_install_attestation_envelope_payload: bytes | None,
    post_install_attestation_envelope_digest: bytes | None,
    operation_outcome: MssqlR1MigrationOperationOutcomeV1,
    generated_at: str,
)
```

Domains use
`dpone-r1-provider-migration-{statement-sql-failure-ref|aggregate-validation-failure-ref|receipt-validation-failure-ref|rollback-trigger-ref|prior-uow-failure-trigger|uow-failure|execution-abort|evidence}-v1\0`.
Blocked has no admitted
plan/receipt/attestation; committed has all required authority; unknown outcome
is never success and requires fresh recovery.

Presence and equality are closed. `Probe` is the typed receipt-probe result;
`Receipt` is the committed receipt ref; `Uow` and `Exec` are the two failure
fields:

| Outcome/case | Observation | Decision/plan | Uow | Exec | Probe | Receipt | Attestation |
|---|---|---|---|---|---|---|---|
| failed UoW setup/begin/lock | NULL | NULL/NULL | required | NULL | NULL | NULL | NULL |
| `cleanup_failed_not_committed` after schema-lock failure | NULL | NULL/NULL | rollback failure with prior-UoW trigger | NULL | NULL | NULL | NULL |
| blocked, rollback succeeds | complete set | BLOCK/NULL | NULL | NULL | NULL | NULL | NULL |
| `cleanup_failed_not_committed` after BLOCK | complete set | BLOCK/NULL | rollback failure | NULL | NULL | NULL | NULL |
| observation abort, rollback succeeds | abort only | NULL/NULL | NULL | NULL | NULL | NULL | NULL |
| `cleanup_failed_not_committed` after observation abort | abort only | NULL/NULL | rollback failure | NULL | NULL | NULL | NULL |
| post-decision abort, rollback succeeds | complete set | nonblock/conditional | NULL | required | completed-only | NULL | optional untrusted |
| `cleanup_failed_not_committed` after post-decision abort | complete set | nonblock/conditional | rollback failure | required | completed-only | NULL | optional untrusted |
| known COMMIT rollback | complete set | nonblock/required | commit failure | NULL | exact disposition result | NULL | attempted exact |
| committed install/coexist | complete set | matching/matching | NULL | NULL | `absent` | `created_in_uow` | exact |
| committed replay | complete set | replay/matching | NULL | NULL | `exact` | `existing_probed` | exact |
| unknown COMMIT response | complete set | nonblock/required | commit unknown | NULL | exact disposition result | NULL | attempted exact |

Exactly one of observation set/abort is present after observation begins. A
pre-observation UoW start/lock failure has neither and requires only a
`known_not_committed` UoW failure. Execution abort is legal only with a complete
set and post-decision failure. Every repeated target,
provider, descriptor, plan, observation, decision and attempt identity is
byte-identical across artifacts. Unknown is never PASS and must be resolved by
fresh-session receipt plus attestation probe. Any deterministic execution,
attestation or receipt failure before COMMIT rolls back and emits
`execution_failed_rolled_back`; its execution stage, optional statement phase,
typed failure source and completed registry-order prefix are retained.
`statement_sql` requires the statement arm and matching non-NULL statement
phase, and permits a core error only when that statement resolves a declared
core condition. Aggregate/receipt validation has no fabricated failed
statement; `statement_phase` is NULL for admitted-plan validation and otherwise
equals the most recently completed statement phase. Exact compatibility is:

```text
sql_error            -> statement_sql        -> failing statement's stage/phase
admitted_plan_error  -> aggregate_validation -> admitted_plan_validation / NULL
result_contract_error-> aggregate_validation -> stage owning typed result / last completed phase
attestation_mismatch -> aggregate_validation -> attestation / post_decision_attest
receipt_error        -> receipt_validation   -> receipt_precheck|receipt_append / matching phase
```

The aggregate arm's `validation_kind` must respectively be `admitted_plan`,
`result_contract` or `stable_attestation`; receipt validation names probe or
append consistently with its stage. A phase-02 SQL failure has no completed
probe and therefore requires `receipt_probe_result=NULL`. Once the probe
statement completed, its exact result is retained for every later validation
or statement failure. A partially
constructed attestation may be
retained only inside the abort as untrusted attempted evidence. Only
loss/ambiguity of the COMMIT response emits
`commit_outcome_unknown`.

`MssqlR1MigrationUowFailureV1` is the only authority for transaction-control
failure. Its complete compatibility matrix is:

| Operation | Certainty | Tx state | Outcome | Observation/decision/plan | Completed prefix | Trigger |
|---|---|---|---|---|---|---|
| session configuration | known not committed | not started | execution failed rolled back | NULL/NULL/NULL | empty | NULL |
| begin transaction | known not committed | not started | execution failed rolled back | NULL/NULL/NULL | empty | NULL |
| schema lock | known not committed | rolled back | execution failed rolled back | NULL/NULL/NULL | empty | NULL |
| commit | known not committed | rolled back | execution failed rolled back | complete/nonblock/required | full admitted sequence | NULL |
| commit | commit outcome unknown | unknown | commit outcome unknown | complete/nonblock/required | full admitted sequence | NULL |
| rollback | known not committed | unknown | cleanup failed not committed | exact triggering artifacts | trigger's completed prefix | required |

No other cross-product is valid. Enum display labels above map underscore for
space. Prefix identity is exact:

```text
completed_statement_prefix_digest = SHA256(canonical_bytes(
  b"dpone-r1-provider-migration-completed-statement-prefix-v1\0",
  tuple(ref.canonical_bytes for ref in ordered_completed_statement_refs)
))
```

The tuple is an exact registry-order prefix with no gap. Setup/begin/lock use
the empty tuple. COMMIT uses the complete concatenated admitted sequence.
Rollback uses the exact prefix retained by its trigger. A rollback trigger ref
contains canonical payload plus digest; the discriminator decodes it exactly as
a prior UoW failure trigger, present observation abort, present BLOCK decision
or present execution abort. The prior-UoW arm is mandatory for
`schema_lock failure -> rollback failure` and retains the original operation,
native error and pre-rollback state. The other three arms must byte-equal their
corresponding evidence artifact. This is the only case where UoW failure may coexist with an
execution abort, and it preserves rather than replaces the original cause. A
trigger digest must equal
`SHA256(canonical_bytes(b"dpone-r1-provider-migration-rollback-trigger-payload-v1\0", (trigger_payload,)))`.
A foreign domain, trailing bytes or a mismatched discriminator fails closed. A
definite COMMIT error may claim `known_not_committed` only with SQL
Server/session rollback evidence; otherwise response loss is the exact unknown
row. Because rollback failure occurs before any COMMIT dispatch, certainty is
still known-not-committed even though transaction cleanup state is unknown. Its
truthful outcome is `cleanup_failed_not_committed`, never a rolled-back claim.
It raises an operator alert and cannot create a receipt ref.

Hermetic tests cover every field/enum/union, finite vector gap/overlap, every
synthetic `2^N` decision vector, unreachable/overlapping rules, blocker
dominance, core-policy weakening, missing/reordered observations/statements,
pre/post eligibility, UoW/target splice, receipt presence/effect key, canonical
round-trip and `must_reject` versus `valid_distinct` mutations. Security or
correctness weakenings are always `must_reject`.

SQL read-only behavior, one-session transactionality, rollback, lock ownership,
receipt durability and crash injection remain `UNVERIFIED` until an approved
SQL Server provider task and live environment exist.

## Rollout and approval

Implementation requires the exact core descriptor commit, renderer statement
contract, target receipt mapping proof, fresh architecture/test review,
maintainer `APPROVED` status and a path-scoped task contract. Rollback removes
unused internal models; persisted V1 bytes require a new version/domain for any
successor.

## Approval checklist

- [x] Core policy cannot be weakened by the provider.
- [x] Observation classification is finite and mechanically decidable.
- [x] Decision overlap, reachability and default block are exact.
- [x] Observation, decision, mutation, attestation and receipt share one UoW.
- [x] Lost commit response recovery is source-free and fail closed.
- [ ] Exact core and renderer commits are pinned.
- [ ] Target receipt mapping resolves against the concrete descriptor.
- [ ] Fresh reviews approve the exact specification.
- [ ] Maintainer changes status to `APPROVED`.
