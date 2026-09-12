<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 physical descriptor contract

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Parent: `docs/feature-design-postgres-mssql-r1-v3-physical-schema-v1.md`
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`

## Executive summary

The researched physical-schema design cannot safely proceed directly to SQL:
20 tables, 29 shared procedures and six per-binding modules need one closed,
machine-readable authority that rejects omissions, unresolved coordinates and
hand-written renderer drift. This approved sub-scope introduces only the pure,
SQL-free descriptor algebra and its adversarial tests. It does not instantiate
the final schema, render DDL, change a port or enable R1.

The measurable outcome is that a complete in-memory fixture can represent the
schema-2 physical authority, round-trip byte-identically and reject any count,
ordering, type, lock, permission, signer, result, error or digest mutation.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Physical-schema designer | Freeze every SQL contract before rendering | Prose permits omissions | One closed descriptor rejects incomplete entries |
| SQL implementer | Consume data instead of inventing semantics | Existing stage SQL is hand-written schema-1 | Renderer accepts only a validated descriptor |
| Security reviewer | Audit roles and module signers | Permission intent is distributed across prose | Exact execute/signer sets are typed and hashed |
| Test certifier | Generate mutation matrices | No common procedure/table metadata model | Stable field-level descriptors and canonical bytes |

The end-user manifest and CLI journey do not change. This is an internal
precondition for later provisioning, status and execution work.

## Scope

### In scope

- Pure immutable physical-only contracts for engine/session, definition bytes,
  projection fields, lock steps, read/write sets, transitions, replay
  comparators, binding-scoped signer authority and stable errors.
- Reuse `MssqlR1PortableSchemaObjectV3`, `MssqlR1SchemaContractV3` and their
  existing column/constraint/index/trigger/property/parameter/result/shared-
  signer/permission models as the single portable schema authority.
- Closed aggregate identities for exactly two schemas, 20 tables, 29 shared
  procedures and a six-module binding template.
- Exact cardinality domains: non-scan procedure `exactly_one`; stage scan
  `zero_or_many`; no output parameters or extra result sets.
- Canonical codecs, derived SHA-256 digests, strict construction/decoding and
  schema-2 portable projection hooks.
- Hermetic positive, negative, round-trip, ordering and mutation tests.

### Non-goals

- Concrete table/procedure/module entries for the production descriptor, apart
  from the explicitly approved 20/29/6 identity constants used for completeness
  checks.
- SQL rendering, pyodbc, catalog reads, installation or migration.
- Changes to schema-authority/pre-source/effect ports.
- Public manifest, CLI, evidence claim or activation changes.
- Treating the contract fixture as vendor-live evidence.

### Assumptions and constraints

- The parent physical spec remains `RESEARCHED` after this task.
- Schema-2 pure contracts are already integrated.
- Unknown enum values, raw strings for enums, bool/int substitution, duplicate
  semantic coordinates and dataclass reflection as implicit field mapping fail
  closed with `MssqlR1V3ContractError`.
- Canonical definition text is NFC UTF-8, contains no CR/NUL, ends in exactly
  one LF and is paired with the exact digest algorithm defined below.
- SQL type/facet validity remains owned by schema-2 portable objects in this
  sub-scope. Exact production type allowlists and semantic reproduction of SQL
  definitions belong to the concrete descriptor/renderer task.

## Public contract

There is no public CLI, Python API or manifest change. New classes live under
`dpone.contracts`, remain internal and are not re-exported from the package
root. Their canonical bytes are an internal future physical-schema contract;
changing them after a concrete descriptor is approved requires a new version.

No evidence artifact is emitted by this sub-scope. Tests are hermetic and must
be reported as such. Rollback is code-only because no SQL object can be
installed from these contracts.

## Detailed contract and algorithm

### Closed aggregate

```python
MssqlR1PhysicalSchemaDescriptorV1(
    descriptor_version="dpone-mssql-r1-v3-physical-schema-2-r2",
    engine_profile=...,
    session_profile=...,
    expected_schema_contract=...,   # existing schema-2 portable authority
    resource_declarations=(...),    # closed references/access/lock oracle
    tables=(...),                   # exactly 20 physical wrappers
    procedures=(...),               # exactly 29 physical wrappers
    binding_module_templates=(...), # exactly six ordered kinds
    migration_probes=(...),
)
```

### Normative algebra and codec

The codec is positional and uses the existing V3 length-framed
`canonical_bytes()` implementation. Each class below encodes fields in the
declared order. Decode requires the exact field count, reconstructs every enum
with `expect_enum()`, reconstructs every nested member from its own canonical
bytes and reruns `__post_init__`. Extra/truncated/trailing fields are contract
errors; unknown Python constructor keywords are ordinary Python call errors and
are not a canonical decode case.

Closed enums and literal order:

```text
DefinitionKind: table_ddl, module_text, module_template
TableLifecycle: immutable, append_only, cas_head, state_machine, guarded_mutable
MutationPolicy: installer_only, guarded_procedure_only, binding_module_only
ResourceKind: static_object, dynamic_stage, registered_target, catalog, session,
              permission, signature, extended_property
AccessKind: read, insert, update, delete, ddl, execute, grant, revoke, sign,
            add_property, drop_property
LockKind: schema, physical, binding, operation, artifact, registration, authority
LockMechanism: application, guarded_row, table
LockAction: acquire, assert_held
LockMode: shared, update, exclusive
LockOwner: transaction
LockCardinality: one, exact_request_set
LockTimeoutPolicy: bounded_environment
ResourceInstanceSelectorKind: declared_singleton, single_value, ordered_values
TransitionKind: create, cas, append
TransitionAuthority: self_contained, caller_uow, read_only
TransitionCardinality: one, zero_or_one, one_or_more, exact_request_set
RevisionRuleKind: create_one, equal_request, adjacent, unchanged,
                  nullable_initial, set_candidate
ReplayComparator: exact_request, exact_projection, fresh_coherent_proof
ReplayBooleanOperator: all, any
ComparisonOperator: equal, not_equal, adjacent, is_null, is_not_null,
                    row_exists, row_absent
ComparisonSource: request_field, result_column, resource_field,
                  predecessor_resource_field, candidate_resource_field,
                  descendant_receipt, procedure_parameter, session_binding
PrincipalKind: provisioner, runtime, loader, observer
ProjectionScalarKind: text, binary, digest, uuid, integer, boolean, utc
ValueCardinality: scalar, ordered_set
ProjectionRole: request_json
RequestBindingKind: payload_projection, sealed_request_digest
ExecutionPath: fresh_mutation, idempotent_replay, read_only_probe
OutcomeClass: known_not_committed, committed, unknown
RetryClass: immediate, bounded_backoff, sealed_takeover, blocked
FreshProbeKind: none, control, stage_open, stage_chunk, stage_seal,
                open_recovery, effect
RedactionClass: public, internal_redacted
MigrationObservationKind: inventory_absent, exact_schema2, empty_legacy,
                          legacy_writer_grant, durable_legacy_state,
                          incompatible_v3, partial_inventory,
                          unreadable_inventory, unknown_codec,
                          nonterminal_state, retained_stage, sealed_intent,
                          ambiguous_state
MigrationDisposition: install, replay, coexist, block
BindingModuleKind: batch_mutate, batch_row_hash, batch_quality,
                   xmin_mutate, xmin_row_hash, xmin_quality
BindingSignerKind: binding_scoped
```

Exact dataclass fields and domain separators:

```text
MssqlR1PhysicalEngineProfileV1
  (engine_major, compatibility_level, contract_collation,
   containment, delayed_durability, mars_enabled, pooling_enabled)
  domain: dpone-r1-physical-engine-profile-v1\0
  literals: 16, 160, Latin1_General_100_BIN2, PARTIAL, DISABLED, false, false

MssqlR1PhysicalSessionProfileV1
  (isolation_level, ansi_nulls, ansi_padding, ansi_warnings, arithabort,
   concat_null_yields_null, quoted_identifier, numeric_roundabort,
   xact_abort, nocount, autocommit_enabled)
  domain: dpone-r1-physical-session-profile-v1\0
  literals: SERIALIZABLE, true, true, true, true, true, true, false, true, true,
            true

MssqlR1DefinitionPayloadV1
  (definition_kind, utf8_bytes, definition_digest)
  domain: dpone-r1-physical-definition-v1\0

MssqlR1PhysicalResourceRefV1
  (resource_kind, schema_name, object_name, coordinate_name)
  domain: dpone-r1-physical-resource-ref-v1\0

MssqlR1PhysicalResourceAccessV1
  (resource, access_kind)
  domain: dpone-r1-physical-resource-access-v1\0

MssqlR1PhysicalResourceDeclarationV1
  (resource, associated_static_object, ordered_fields,
   ordered_allowed_access_kinds,
   allowed_lock_kind, lock_subrank, lock_cardinality)
  domain: dpone-r1-physical-resource-declaration-v1\0

MssqlR1PhysicalLockStepV1
  (ordinal, lock_kind, resource, mechanism, action, mode, owner,
   cardinality, instance_selector, timeout_policy, requires_updlock,
   requires_holdlock, requires_tablockx)
  domain: dpone-r1-physical-lock-step-v1\0

MssqlR1ResourceInstanceSelectorV1
  (selector_kind, ordered_coordinates, allow_empty)
  domain: dpone-r1-physical-resource-instance-selector-v1\0

MssqlR1ProjectionFieldV1
  (ordinal, name, scalar_kind, value_cardinality, nullable)
  domain: dpone-r1-physical-projection-field-v1\0

MssqlR1ResourceFieldV1
  (ordinal, name, scalar_kind, value_cardinality, nullable,
   instance_key_ordinal)
  domain: dpone-r1-physical-resource-field-v1\0

MssqlR1ProjectionGrammarV1
  (grammar_version, projection_role, ordered_fields)
  domain: dpone-r1-physical-projection-grammar-v1\0

MssqlR1RequestAuthorityV1
  (binding_kind, codec, request_payload_parameter, request_digest_parameter,
   projection_parameter, projection_grammar,
   ordered_scalar_parameter_bindings)
  domain: dpone-r1-physical-request-authority-v1\0

MssqlR1ScalarParameterBindingV1
  (parameter_name, value_source)
  domain: dpone-r1-physical-scalar-parameter-binding-v1\0

MssqlR1OutcomeVariantV1
  (outcome_literal, ordered_null_columns, ordered_present_columns)
  domain: dpone-r1-physical-outcome-variant-v1\0

MssqlR1RevisionRuleV1
  (rule_kind, current_value, expected_value, requested_candidate,
   candidate_value, delta)
  domain: dpone-r1-physical-revision-rule-v1\0

MssqlR1StateChangeV1
  (state_field, ordered_predecessor_states, candidate_state)
  domain: dpone-r1-physical-state-change-v1\0

MssqlR1StateTransitionV1
  (ordinal, transition_kind, owner_resource, instance_selector, cardinality,
   state_change, ordered_revision_rules, ordered_applicabilities)
  domain: dpone-r1-physical-state-transition-v1\0

MssqlR1TransitionApplicabilityV1
  (outcome_literal, execution_path)
  domain: dpone-r1-physical-transition-applicability-v1\0

MssqlR1ComparisonCoordinateV1
  (source, resource, field_name, scalar_kind, value_cardinality, nullable)
  domain: dpone-r1-physical-comparison-coordinate-v1\0

MssqlR1ComparisonLiteralV1
  (scalar_kind, value)
  domain: dpone-r1-physical-comparison-literal-v1\0

MssqlR1ResourceExistenceOperandV1
  (resource, instance_selector)
  domain: dpone-r1-physical-resource-existence-operand-v1\0

MssqlR1ComparisonPairV1
  (left, operator, right)
  domain: dpone-r1-physical-comparison-pair-v1\0

MssqlR1ReplayClauseV1
  (ordinal, comparator, ordered_comparisons,
   fresh_proof_resource, requires_descendant_proof)
  domain: dpone-r1-physical-replay-clause-v1\0

MssqlR1ReplayClauseGroupV1
  (ordinal, boolean_operator, ordered_clauses)
  domain: dpone-r1-physical-replay-clause-group-v1\0

MssqlR1ReplayOutcomePolicyV1
  (outcome_literal, execution_path, group_operator, ordered_groups)
  domain: dpone-r1-physical-replay-outcome-policy-v1\0

MssqlR1PhysicalErrorConditionV1
  (condition_id, error_number, error_state, outcome_class, retry_class,
   fresh_probe_kind, public_blocker_code, redaction_class)
  domain: dpone-r1-physical-error-condition-v1\0

MssqlR1ExecutionSemanticsV1
  (request_authority, ordered_outcome_variants, ordered_lock_steps,
   ordered_read_set, ordered_write_set, transition_authority,
   ordered_state_transitions, ordered_replay_outcome_policies,
   ordered_error_conditions)
  domain: dpone-r1-physical-execution-semantics-v1\0

MssqlR1PhysicalTableDescriptorV1
  (portable_object, definition, lifecycle, mutation_policy,
   lock_resource)
  domain: dpone-r1-physical-table-descriptor-v1\0

MssqlR1PhysicalProcedureDescriptorV1
  (portable_object, definition, execution_semantics,
   ordered_execute_principals)
  domain: dpone-r1-physical-procedure-descriptor-v1\0

MssqlR1BindingModuleTemplateV1
  (module_kind, name_template, ordered_parameters, result_contract,
   definition_template, execution_semantics, ordered_execute_principals,
   signer_kind)
  domain: dpone-r1-physical-binding-module-template-v1\0

MssqlR1MigrationProbeV1
  (probe_id, observation_kind, observation_resource, disposition,
   blocker_code, zero_mutation_before_decision)
  domain: dpone-r1-physical-migration-probe-v1\0

MssqlR1PhysicalSchemaDescriptorV1
  (descriptor_version, engine_profile, session_profile,
   expected_schema_contract, ordered_resource_declarations, ordered_tables,
   ordered_procedures, ordered_binding_module_templates,
   ordered_migration_probes)
  domain: dpone-r1-physical-schema-descriptor-v1\0
```

The corresponding field types are closed and positional:

```text
engine profile:
  int, int, str, str, str, bool, bool
session profile:
  str, bool, bool, bool, bool, bool, bool, bool, bool, bool, bool
definition payload:
  MssqlR1DefinitionKindV1, bytes, bytes
resource ref:
  MssqlR1ResourceKindV1, str|None, str|None, str|None
resource access:
  MssqlR1PhysicalResourceRefV1, MssqlR1AccessKindV1
resource declaration:
  MssqlR1PhysicalResourceRefV1, MssqlR1PhysicalResourceRefV1|None,
  tuple[MssqlR1ResourceFieldV1, ...],
  tuple[MssqlR1AccessKindV1, ...], MssqlR1LockKindV1|None, int,
  MssqlR1LockCardinalityV1
lock step:
  int, MssqlR1LockKindV1, MssqlR1PhysicalResourceRefV1,
  MssqlR1LockMechanismV1, MssqlR1LockActionV1, MssqlR1LockModeV1,
  MssqlR1LockOwnerV1, MssqlR1LockCardinalityV1,
  MssqlR1ResourceInstanceSelectorV1, MssqlR1LockTimeoutPolicyV1,
  bool, bool, bool
resource instance selector:
  MssqlR1ResourceInstanceSelectorKindV1,
  tuple[MssqlR1ComparisonCoordinateV1, ...], bool
projection field:
  int, str, MssqlR1ProjectionScalarKindV1, MssqlR1ValueCardinalityV1,
  bool
resource field:
  int, str, MssqlR1ProjectionScalarKindV1, MssqlR1ValueCardinalityV1,
  bool, int|None
projection grammar:
  str, MssqlR1ProjectionRoleV1, tuple[MssqlR1ProjectionFieldV1, ...]
request authority:
  MssqlR1RequestBindingKindV1, MssqlR1SupportedCodecEntryV3,
  str|None, str, str|None, MssqlR1ProjectionGrammarV1|None,
  tuple[MssqlR1ScalarParameterBindingV1, ...]
scalar parameter binding:
  str, MssqlR1ComparisonCoordinateV1
outcome variant:
  str, tuple[str, ...], tuple[str, ...]
revision rule:
  MssqlR1RevisionRuleKindV1, MssqlR1ComparisonCoordinateV1|None,
  MssqlR1ComparisonCoordinateV1|None,
  MssqlR1ComparisonCoordinateV1|MssqlR1ComparisonLiteralV1|None,
  MssqlR1ComparisonCoordinateV1, int
state change:
  MssqlR1ComparisonCoordinateV1, tuple[str, ...], str
state transition:
  int, MssqlR1TransitionKindV1, MssqlR1PhysicalResourceRefV1,
  MssqlR1ResourceInstanceSelectorV1, MssqlR1TransitionCardinalityV1,
  MssqlR1StateChangeV1|None,
  tuple[MssqlR1RevisionRuleV1, ...],
  tuple[MssqlR1TransitionApplicabilityV1, ...]
transition applicability:
  str, MssqlR1ExecutionPathV1
comparison coordinate:
  MssqlR1ComparisonSourceV1, MssqlR1PhysicalResourceRefV1|None, str,
  MssqlR1ProjectionScalarKindV1, MssqlR1ValueCardinalityV1, bool
comparison literal:
  MssqlR1ProjectionScalarKindV1, str|bytes|int|bool
resource existence operand:
  MssqlR1PhysicalResourceRefV1, MssqlR1ResourceInstanceSelectorV1
comparison pair:
  MssqlR1ComparisonCoordinateV1|MssqlR1ResourceExistenceOperandV1,
  MssqlR1ComparisonOperatorV1,
  MssqlR1ComparisonCoordinateV1|MssqlR1ComparisonLiteralV1|None
replay clause:
  int, MssqlR1ReplayComparatorV1,
  tuple[MssqlR1ComparisonPairV1, ...],
  MssqlR1PhysicalResourceRefV1|None, bool
replay clause group:
  int, MssqlR1ReplayBooleanOperatorV1, tuple[MssqlR1ReplayClauseV1, ...]
replay outcome policy:
  str, MssqlR1ExecutionPathV1, MssqlR1ReplayBooleanOperatorV1,
  tuple[MssqlR1ReplayClauseGroupV1, ...]
error condition:
  str, int, int, MssqlR1OutcomeClassV1, MssqlR1RetryClassV1,
  MssqlR1FreshProbeKindV1, str|None, MssqlR1RedactionClassV1
execution semantics:
  MssqlR1RequestAuthorityV1,
  tuple[MssqlR1OutcomeVariantV1, ...],
  tuple[MssqlR1PhysicalLockStepV1, ...],
  tuple[MssqlR1PhysicalResourceAccessV1, ...],
  tuple[MssqlR1PhysicalResourceAccessV1, ...],
  MssqlR1TransitionAuthorityV1,
  tuple[MssqlR1StateTransitionV1, ...],
  tuple[MssqlR1ReplayOutcomePolicyV1, ...],
  tuple[MssqlR1PhysicalErrorConditionV1, ...]
table descriptor:
  MssqlR1PortableSchemaObjectV3, MssqlR1DefinitionPayloadV1,
  MssqlR1TableLifecycleV1, MssqlR1MutationPolicyV1,
  MssqlR1PhysicalResourceRefV1
procedure descriptor:
  MssqlR1PortableSchemaObjectV3, MssqlR1DefinitionPayloadV1,
  MssqlR1ExecutionSemanticsV1, tuple[MssqlR1PrincipalKindV1, ...]
binding module template:
  MssqlR1BindingModuleKindV1, str,
  tuple[MssqlR1SchemaProcedureParameterV3, ...],
  MssqlR1ProcedureResultContractV3, MssqlR1DefinitionPayloadV1,
  MssqlR1ExecutionSemanticsV1, tuple[MssqlR1PrincipalKindV1, ...],
  MssqlR1BindingSignerKindV1
migration probe:
  str, MssqlR1MigrationObservationKindV1,
  MssqlR1PhysicalResourceRefV1, MssqlR1MigrationDispositionV1,
  str|None, bool
schema descriptor:
  str, MssqlR1PhysicalEngineProfileV1,
  MssqlR1PhysicalSessionProfileV1, MssqlR1SchemaContractV3,
  tuple[MssqlR1PhysicalResourceDeclarationV1, ...],
  tuple[MssqlR1PhysicalTableDescriptorV1, ...],
  tuple[MssqlR1PhysicalProcedureDescriptorV1, ...],
  tuple[MssqlR1BindingModuleTemplateV1, ...],
  tuple[MssqlR1MigrationProbeV1, ...]
```

Only fields explicitly typed with `|None` are nullable. No model accepts
dictionaries, lists, raw enum strings or structurally similar subclasses.
Definition payloads for tables require `table_ddl`; shared procedures require
`module_text`; binding templates require `module_template`. Portable
table/procedure kinds must agree with their wrapper. Shared-procedure portable
objects remain the sole parameter/result/shared-signer authority; a physical
procedure cannot override them.

All identifiers use schema-2 `require_schema_identifier`; other text uses
bounded NFC canonical text (256 bytes, except definition bytes). Ordinals start
at one and are contiguous. Integer deltas are exact integers in `{0,1}` and
reject booleans. Tuples are exact tuples. Canonical sets are sorted by member
canonical bytes with no duplicates; ordered inventories use the explicit
identity order below, not lexical sorting.

Semantic coordinates are compared twice: first as exact NFC UTF-8 bytes and
then as `NFC(value).casefold()` UTF-8 bytes. An exact duplicate or case-fold
collision is rejected. The portable schema-2 projection is independently
ordered by `(schema_name.encode("utf-8"), object_name.encode("utf-8"))`, as
required by `MssqlR1SchemaContractV3`; the physical aggregate retains the
explicit parent-spec table/procedure order. Construction must therefore build
the portable projection explicitly rather than relying on either input order.

Definition bytes must decode as UTF-8, be NFC, contain neither CR nor NUL and
end in exactly one LF. For `module_text` and `module_template`, digest equals
existing `module_definition_digest(text)`. For `table_ddl`, digest equals
SHA-256 of `canonical_bytes(b"dpone-r1-physical-table-ddl-v1\0",
(utf8_bytes,))`. This sub-scope proves byte/digest binding only; semantic
metadata reproduction is a concrete renderer golden-test obligation.

For every shared procedure, the wrapper and portable object bind the same
definition authority:

```text
wrapper.definition.definition_digest
== wrapper.portable_object.module_definition_digest
```

The aggregate rejects a mismatch even when each digest is individually valid
for its own bytes. Binding templates have no second portable definition digest;
their template payload is the sole definition authority until instantiation.

#### Resource, access and lock closure

A `static_object` resource requires schema/object and no coordinate name and
must resolve to the schema-2 inventory. Every other resource kind requires a
coordinate name and NULL schema/object. The aggregate carries one canonical,
case-fold-unique declaration for every resource. Every access, lock, transition,
replay proof and migration observation must byte-equal one declaration; an
undeclared coordinate is an `unbound resource` contract error. Declaration
fields are contiguous by ordinal, case-fold unique and freeze scalar kind,
scalar-vs-ordered-set cardinality, nullability and optional instance-key
ordinal. Non-NULL instance-key ordinals are contiguous from one, refer only to
non-null scalar fields and define the exact lock/transition instance key.
Static-table resource fields must correspond to portable columns through the
closed SQL-type/facet-to-scalar compatibility table below; other resource-
template fields are frozen by the later concrete descriptor. A non-NULL
`associated_static_object` must be a declared schema-2 static object; a
static-object declaration must associate to itself.

Comparison coordinates repeat scalar kind/cardinality/nullability and must
exactly reproduce the referenced request/resource/result/parameter/session
field metadata. SQL compatibility is closed to:

```text
char/varchar/nchar/nvarchar                 -> text
binary/varbinary                            -> binary
binary(32)                                  -> binary or digest
uniqueidentifier                            -> uuid
tinyint/smallint/int/bigint                 -> integer
bit                                         -> boolean
datetime2/datetimeoffset                    -> utc
```

No implicit conversion, width relaxation or nullable-to-nonnullable binding is
accepted. A `digest` field is exact 32 bytes. Unsupported SQL types cannot be
used by a physical comparison/scalar binding even if they remain legal in a
different portable schema capability.

Result-column coordinates reproduce schema-2 result nullability. Request-field
coordinates reproduce the request grammar. Physical procedure parameters are
required inputs in this profile, so `procedure_parameter` coordinates are
scalar and non-null; session bindings are also scalar and non-null. A
`state_field` is scalar, non-null `text` on the exact transition owner.

The hard-coded `ResourceKind × AccessKind` upper bound is:

```text
static_object:      read, insert, update, delete, ddl, execute,
                    add_property, drop_property
dynamic_stage:      read, insert, update, delete, ddl, grant, revoke,
                    add_property, drop_property
registered_target: read, insert, update, delete
catalog:            read
session:            read, update
permission:         read, grant, revoke
signature:          read, sign
extended_property: read, add_property, drop_property
```

A declaration may narrow but never widen that matrix. Read/write/access tuples
are canonical sets, and every write access must use a non-`read` action. GRANT,
REVOKE, signature and extended-property effects therefore cannot collapse into
generic DDL. A declaration with `allowed_lock_kind=NULL` has the sole canonical
shape `lock_subrank=0`, `lock_cardinality=one` and is forbidden from every lock
plan. No ignored lock field is allowed to vary only to change the digest.

Lock rank is the enum order
`schema→physical→binding→operation→artifact→registration→authority`. Artifact
subranks are Batch payload=1, XMin delta=2, XMin complete keys=3; authority
subranks are initial cutover=1, rebaseline=2, empty refresh=3; every other lock
has subrank 0. The resource declaration binds allowed lock kind, subrank and
cardinality. Lock steps are contiguous and ordered by
`(global_rank, subrank, resource.canonical_bytes, lock_phase)`, where
`lock_phase` is application=1, guarded-row=2 and table=3. A step cannot
contradict its declaration. This makes an application lock followed by a row
assertion/acquisition deterministic even when both protect the same resource.

Lock mechanisms have exact shapes:

```text
application:
  mode=exclusive, owner=transaction, timeout=bounded_environment,
  updlock=false, holdlock=false, tablockx=false
guarded_row:
  mode=update, owner=transaction, timeout=bounded_environment,
  updlock=true, holdlock=true, tablockx=false
table:
  mode=exclusive, owner=transaction, timeout=bounded_environment,
  updlock=false, holdlock=true, tablockx=true
```

`acquire` obtains the declared lock. `assert_held` is permitted only for a
procedure/module whose caller contract already acquired that same canonical
resource in the active UoW; it is a typed precondition and never silently
acquires. The pure descriptor validates its shape; the later concrete
composition contract must prove the caller/callee edge and pre-held resource.
Every table wrapper's `lock_resource` must byte-equal exactly one declared lock
resource. That declaration's `associated_static_object` must byte-equal the
wrapped portable table resource. A static-object declaration associates to
itself; dynamic/physical templates either name one exact static association or
NULL when genuinely cross-object. This gives table locks an exact resolution
oracle without turning an applock name into a fake SQL object.

`exact_request_set` requires the corresponding declaration cardinality and
canonical resource-set order. The reusable instance selector is valid as
follows:

```text
declared_singleton: zero coordinates, allow_empty=false
single_value:       one coordinate per instance-key part, explicit allow_empty
ordered_values:     one or more comparison coordinates, explicit allow_empty
```

The complete cardinality matrix is:

```text
one:              declared_singleton, or single_value allow_empty=false
zero_or_one:      single_value allow_empty=true
one_or_more:      ordered_values allow_empty=false
exact_request_set: ordered_values allow_empty=false|true as declared
```

Selector coordinates are ordered by and type-compatible with the declaration's
instance-key ordinals. `single_value` coordinates are scalar;
`ordered_values` coordinates are `ordered_set`, have equal runtime lengths and
are zipped into complete scalar instance keys. Coordinates resolve to request,
parameter, session or resource fields. Concrete instances are the canonical
tuple of selected keys plus declaration rank/subrank; missing, duplicate or
reordered instances fail. The same selector type is used by lock steps and
state transitions, so both act on the same explicit dynamic singleton or
ordered request set. This captures zero/one/two authorities, Batch/XMin
artifact sets, full fence acquisition and downstream binding modules that
assert it.

#### Request, outcome, transition and replay closure

Each shared procedure and binding module carries one request authority whose
codec byte-equals an entry in
`expected_schema_contract.ordered_supported_codecs`. A shared procedure uses
`payload_projection`: its named `request_payload`, `request_digest` and
`projection_json` parameters must exist as exact INPUT parameters in the
portable signature, and its non-NULL `request_json` projection grammar is an
ordered, closed field set. The descriptor freezes the future runtime equation:

```text
decode(request_payload) under exact codec
→ reproduce every projection_json field and scalar type
→ canonical re-encode byte-equals request_payload
→ SHA-256 authority equals request_digest
```

This SQL-free task proves exact codec membership and complete parameter/field
mapping; it does not execute the codec against runtime payload values. The
later provider/runtime must enforce the equation before authority admission.
No projection field is accepted merely because an untrusted JSON value has the
right spelling. A binding template instead uses `sealed_request_digest`: the
named request-digest INPUT is its only request-envelope member;
payload/projection names and grammar are NULL, while additional scalar/digest
INPUT parameters are legal only through the typed bindings below. A replay
clause must bind the request digest to the declared sealed-effect resource.
This preserves the parent rule that generated target
modules receive digests and closed scalar coordinates, never executable SQL or
untrusted JSON/payload bytes. In both modes,
`ordered_scalar_parameter_bindings` maps every remaining INPUT parameter
exactly once. The binding names the incoming `procedure_parameter`; its typed
value source may be a request field, declared resource field or declared
`session_binding` such as transaction identity. No unbound extra input
parameter is permitted.

For a fixed result, outcome variants are a nonempty canonical set. Outcome
literals are bounded lowercase ASCII and unique. For each variant,
`ordered_null_columns` and `ordered_present_columns` are disjoint and their
union exactly equals the nullable result-column names. Each tuple is ordered by
the corresponding portable result-column ordinal, so a NULL shape has one
canonical encoding; non-nullable columns are always present. The result must
contain the non-null `outcome` prefix column.
The stage-scan template alone has no outcome variants. Thus opened/replayed,
committed/absent/blocked/unknown and future concrete literals carry an exact,
testable NULL-shape rather than prose.

State transitions are an ordered contiguous tuple. `self_contained` requires a
nonempty tuple; `read_only` requires empty transitions and an empty write set;
`caller_uow` requires empty transitions, at least one read or write access and
only `assert_held` lock steps, and is legal only for the six binding templates
whose caller owns the surrounding transition. The converse is mandatory: all
six binding templates use exactly `caller_uow`, while every shared procedure
uses `self_contained` or `read_only` and can never use `caller_uow`. The four `batch_mutate`,
`batch_row_hash`, `xmin_mutate` and `xmin_row_hash` templates require a
nonempty write set. The `batch_quality` and `xmin_quality` templates require an
empty write set and a nonempty read set; no synthetic quality mutation is
invented. Each represented transition owner
resolves to a declared write resource. Cardinality freezes whether the effect
targets one, zero-or-one, one-or-more or the exact request set, and its reusable
instance selector must match that cardinality and declaration. `state_field`
is carried only inside an optional `MssqlR1StateChangeV1`. When present it must
be a scalar, non-null `resource_field` coordinate for the exact owner resource,
must exist in its declaration and must be type-compatible with the predecessor/
candidate text literals. State-machine CAS requires it; create/append/CAS that
changes only typed fields or revisions may set `state_change=NULL`. State
literals can never float above an unnamed SQL field.

The transition-kind/state-change matrix is exact:

```text
create: state_change=NULL, or state_change with empty predecessor tuple
cas:    state_change=NULL, or state_change with nonempty predecessor tuple
append: state_change=NULL
```

For state-changing `cas`, `candidate_state` is not one of the predecessor
states. The predecessor tuple is canonical, nonempty and duplicate-free. A
state-changing `create` establishes its candidate without pretending that an
earlier row state existed. Append-only effects never encode an in-place state
transition.

Revision rules are canonical and unique by candidate comparison coordinate.
Every rule names the exact candidate
comparison coordinate, which must be a non-null integer
`candidate_resource_field` on the exact transition owner. Expected coordinates
are closed to request/procedure/session authority; `current_value`, when present, must be a
`predecessor_resource_field` on that same owner. Cross-resource, wrong-source
and wrong-type substitution fail in this algebra. A same-owner, same-type field
substitution is a different valid descriptor with a different digest; the later
concrete descriptor freezes and tests the exact revision-field mapping. Exact
equations are:

```text
create_one:
  current=NULL; expected=NULL; requested_candidate=NULL;
  candidate=1; delta=1
equal_request:
  current=non-null predecessor_resource_field;
  expected=request|procedure_parameter|session_binding;
  requested_candidate=NULL;
  require current=expected; candidate=current; delta=0
adjacent:
  current=non-null integer predecessor_resource_field;
  expected=NULL or non-null integer request/parameter/session equal to current;
  requested_candidate=NULL;
  candidate=current+1; delta=1
unchanged:
  current=non-null predecessor_resource_field; expected=NULL;
  requested_candidate=NULL;
  candidate=current; delta=0
nullable_initial:
  current=nullable predecessor_resource_field and IS NULL; expected=NULL;
  requested_candidate=NULL;
  candidate=1; delta=1
set_candidate:
  current=non-null predecessor_resource_field;
  expected=request|procedure_parameter|session_binding as expected-current;
  requested_candidate=request|procedure_parameter|session_binding|typed literal;
  require current=expected;
  candidate=requested_candidate; delta=0
```

Current/expected/requested-candidate/committed-candidate scalar kind and
nullability must satisfy the selected row;
all candidate fields are non-null integer revision fields.
This permits one procedure to atomically describe registration/head,
operation/writer, artifact-set, receipt/checkpoint/consumption and generation
revision changes without pretending that every revision is an operation epoch.

Every transition has a nonempty canonical applicability set of
`(outcome_literal, execution_path)`. Literals must exist in the procedure's
outcome variants. Every applicability has exactly one byte-identical replay
outcome policy coordinate. `fresh_mutation` is the only path admitted by a
transition; `idempotent_replay` and `read_only_probe` are forbidden in every
transition applicability. For `self_contained`, every `fresh_mutation` policy
has at least one matching transition applicability and at least one such policy
exists; its complete allowed policy-path set is
`{fresh_mutation, idempotent_replay}`. For `read_only`, the only allowed path is
`read_only_probe` and both the transition tuple and write set are empty. The
stage-scan result-template exception has no replay policies. For binding-only
`caller_uow`, the complete allowed policy-path set is
`{fresh_mutation, idempotent_replay}` and at least one fresh policy is required;
local transitions remain empty because the caller owns the surrounding
transition. The same committed candidate outcome can therefore be
associated with a transition only on self-contained fresh mutation while its
replay path returns the same projection without mutation. No local transition
or fresh self-contained policy floats outside the shared outcome/path truth
table.

Replay clauses are ordered and contiguous. Comparison pairs are canonical;
all `ordered_comparisons` inside one clause are evaluated conjunctively in
tuple order. There is no OR or short-circuit semantic inside a clause; OR is
represented only by the explicit group/policy boolean operators.
Their left/right coordinates resolve as follows: request fields must exist in the request
grammar; result columns must exist in the portable result; resource fields must
exist in the referenced declaration; procedure parameters must exist in the
exact signature; session bindings must resolve to a declared session resource.
`descendant_receipt` requires a `static_object` whose exact portable identity
is one of `dpone_control_receipt_v3`,
`dpone_open_stage_recovery_receipt_v3`, `dpone_effect_receipt_v3`,
`dpone_batch_effect_receipt_v3` or `dpone_xmin_effect_receipt_v3`.

The source/resource presence matrix is exact:

```text
request_field:        resource=NULL
result_column:        resource=NULL
procedure_parameter: resource=NULL
resource_field:       declared resource required
predecessor_resource_field: exact transition-owner resource required
candidate_resource_field:   exact transition-owner resource required
descendant_receipt:   exact receipt static resource required
session_binding:      declared session resource required
```

Every comparison or existence operand with a non-NULL resource requires a
byte-identical `MssqlR1PhysicalResourceAccessV1(resource, read)` member in the
procedure's `ordered_read_set`, and that declaration must include `read` in
`ordered_allowed_access_kinds`. This applies equally to resource fields,
predecessor/candidate fields, descendant receipts, session bindings and
existence operands. Removing the read edge or substituting another resource is
an invalid descriptor, not merely a different digest.

Typed literals use the same scalar grammar as coordinates. `text` is bounded
NFC canonical text under the common 256-byte rule. `uuid` is a lowercase,
hyphenated ASCII string whose parse through `UUID(value)` and re-render through
`str(parsed)` is byte-identical to the input. `utc` is a string accepted by the
existing `parse_canonical_utc_text()` helper, hence exact UTC ISO-8601 with six
fractional digits and terminal `Z`. `binary` is exact bytes; `digest` is exactly
32 bytes; `integer` rejects bool and fits a signed SQL bigint; `boolean` is
exact bool. Literals are scalar and non-null.

`equal`, `not_equal` and `adjacent` require a coordinate left operand and a
type-compatible coordinate or literal right operand. `is_null` and
`is_not_null` require a nullable coordinate left and `right=NULL`.
`row_exists` and `row_absent` require a
`MssqlR1ResourceExistenceOperandV1` left, whose exact instance selector defines
the row predicate, and `right=NULL`. Field NULL and row absence are therefore
different contracts. Every other left/operator shape fails. A clause has a
nonempty canonical comparison tuple, so no predicate is supplied by an
implicit renderer convention.

Equality requires identical scalar kind and value cardinality. Every nullable
coordinate used as either operand of `equal|not_equal` requires an accompanying
`is_not_null` comparison for that byte-identical coordinate in the same replay
clause; SQL `UNKNOWN` is never an implicit proof result. `adjacent`
requires non-null scalar integers on both sides and means exactly
`left = right + 1`; ordered sets and literals are forbidden for it. Ordered-set
`equal|not_equal` is legal only between the same typed ordered-set cardinality
and compares canonical element count, order and value without set coercion.

`fresh_coherent_proof` alone requires a declared fresh-proof resource.
The clause must contain at least one `row_exists|row_absent` comparison whose
left `MssqlR1ResourceExistenceOperandV1` references that byte-identical resource
and binds its complete declared instance-key selector. Field comparisons are
supplemental projection checks and never establish row identity. The
procedure's lock plan must acquire/assert the corresponding resource scope with
a byte-identical selector before the proof read; unrelated literal/request
comparisons cannot establish coherence.
`requires_descendant_proof=true` additionally requires at least one
`row_exists` comparison whose left existence operand names one of the exact
receipt resources and binds its complete declared instance key. A
`descendant_receipt` field comparison is supplemental and, when present,
requires that receipt's qualifying `row_exists` comparison in the same clause.
`row_absent` never establishes descendant proof. Descendant proof is legal only
on a fresh coherent clause; the inverse is also required.

Each replay outcome policy is unique by `(outcome_literal, execution_path)` and
references an existing outcome. Every outcome has at least one policy; a
committed outcome shared by fresh and replay paths has two distinct policies.
A policy is an explicit `all|any` formula over one or more ordered clause
groups; each group is an explicit `all|any` formula over one or more clauses.
This two-level tree can encode conjunctions/disjunctions of exact request,
relational projection, typed literal and descendant proofs and binds the
formula to the path/outcome it establishes. No outcome can be claimed by an
empty formula. Stage scan alone has no replay outcome policies. A bare
classification enum is never replay authority.

#### Error and migration closure

Every error number is an exact integer `51001..51012`, state is `1..255`, and
condition IDs are unique within one shared procedure or binding template. The
aggregate coordinate is `(module_identity, condition_id)`; the same condition
or `(number,state)` may intentionally recur in another module. The union of
error numbers across all 29 shared procedures and six binding templates must
equal all 12 numbers; there is no second error registry. A public blocker code
is either NULL or bounded lowercase ASCII. `redaction_class=public` requires a
non-NULL code; `internal_redacted` requires NULL. The code, not message text, is
the stable public family.

Migration probes are a nonempty canonical set and cover typed observations,
not free-form blocker text. `install`, `replay` and `coexist` require NULL
blocker code; `block` requires a bounded stable blocker code. Every probe has
`zero_mutation_before_decision=true`. This algebra can therefore represent
inventory absent→install, exact schema-2→replay, recognized empty
V1/V2→coexist and every partial/unreadable/incompatible/nonterminal/ambiguous
observation→block. Exact coverage of the parent matrix is frozen by the later
concrete descriptor.

Execute principals are canonical enum tuples. Derived execute sets are checked
against the existing schema-2 permission rules. Loader has no procedure or
module execute entry. Binding modules require `(runtime,)` and
`binding_scoped`; shared module signer truth remains exclusively the existing
schema-2 `none|stage_owner|attestor` authority.

For each of the 28 non-scan procedures, the reused schema-2 result contract is
`MssqlR1FixedResultV3(EXACTLY_ONE, ...)` and its first five columns are exactly:

```text
1 result_contract_version varchar(64) max_length=64 precision=0 scale=0 NN Latin1_General_100_BIN2
2 outcome                 varchar(32) max_length=32 precision=0 scale=0 NN Latin1_General_100_BIN2
3 request_digest          binary(32)  max_length=32 precision=0 scale=0 NN no collation
4 projection_revision     bigint      max_length=8  precision=19 scale=0 NN no collation
5 server_observed_at      datetime2   max_length=8  precision=27 scale=7 NN no collation
```

`dpone_scan_stage_v3` alone uses `MssqlR1StageScanTemplateV3(ZERO_OR_MANY,
...)`; `MssqlR1NoResultV3`, `ZERO_OR_ONE`, output/input-output parameters and a
fixed `ZERO_OR_MANY` result are rejected by aggregate closure. Procedure-
specific columns follow the prefix and are frozen only by the later concrete
descriptor task.

All six binding modules also use `MssqlR1FixedResultV3(EXACTLY_ONE, ...)`, the
same exact five-column prefix and nonempty outcome variants. Their name is not
free-form: `name_template` must equal
`dpone_b_{binding_uuid_hex}_<module_kind>_v3`, where the placeholder is the
literal ASCII text `{binding_uuid_hex}` and `<module_kind>` is the exact enum
value. A renderer substitutes exactly 32 lowercase hexadecimal UUID digits;
the pure contract accepts no other placeholder, brace, prefix or suffix.

Shared signer mapping is exact:

```text
attestor:
  dpone_attest_schema_v3

stage_owner:
  dpone_open_stage_v3
  dpone_begin_stage_chunk_v3
  dpone_complete_stage_chunk_v3
  dpone_observe_stage_v3
  dpone_scan_stage_v3
  dpone_seal_stage_v3
  dpone_recover_expired_open_v3
  dpone_consume_stage_set_v3

none:
  every other shared procedure
```

The four principal identities are environment principals, not signer profiles.
The aggregate derives their exact execute sets from procedure entries and
requires the parent-spec matrix: provisioner 6 procedures, runtime 25,
observer 8, loader 0. It rejects PUBLIC, roles and any binding signer encoded as
a shared `MssqlR1SignerProfileKindV3`.

The exact environment-principal execute sets are:

```text
provisioner:
  provision_registration, rotate_registration,
  import_generation_authority_set, probe_control_effect,
  attest_schema, probe_registration

runtime:
  probe_control_effect, admit_operation, seal_effect, take_over_sealed,
  probe_pre_source, open_stage, renew_stage, begin_stage_chunk,
  complete_stage_chunk, observe_stage, observe_stage_chunk, scan_stage,
  seal_stage, recover_expired_open, probe_open_recovery, admit_writer,
  resolve_admit_authority_set, append_effect_receipt,
  write_xmin_checkpoint, consume_stage_set, consume_authority_set,
  advance_operation_and_head, prove_candidate_effect, probe_effect,
  probe_registration

observer:
  probe_control_effect, attest_schema, probe_pre_source, observe_stage,
  observe_stage_chunk, probe_open_recovery, probe_effect, probe_registration

loader:
  none
```

The aggregate expands these short names to the exact `dpone_..._v3`
identities before comparison. Runtime alone executes all six binding-module
templates. Neither a certificate user nor a shared signer profile is an
environment principal.

For a shared procedure, one derived execute edge must correspond to exactly one
schema-2 permission rule with:

```text
subject_role = matching environment role
grantor_role = provisioner
source = direct
scope = object
schema_name = dpone_authority
object_name = exact procedure identity
column_name = NULL
permission = EXECUTE
effect = grant
grant_option = false
```

Every such schema-2 environment-principal EXECUTE rule must have one derived
edge; missing and extra rules both fail. Certificate-user permissions and
signatures are validated through schema-2 signer profiles and the exact signer
mapping above, not through this environment-principal projection. Binding-
template runtime EXECUTE is checked inside the physical aggregate because
binding objects are generated and are intentionally absent from the shared
portable inventory.

Construction performs, in order:

1. exact type/domain validation for every leaf;
2. local uniqueness and deterministic semantic ordering;
3. exact inventory count/name validation;
4. resource declaration, access-matrix, lock-rank and reference closure;
5. request-codec/parameter/projection and outcome/result closure;
6. ordered multi-resource transition and replay-clause closure;
7. exact procedure cardinality/result-template/definition validation;
8. exact six-kind binding-template/name/signer validation;
9. error-union and migration-disposition closure;
10. derive principal execute sets from procedure/module entries and compare them
   with schema-2 permission rules; never accept a second independent set;
11. require `expected_schema_contract.ordered_objects` to equal the canonical
   projection of all table and shared-procedure wrappers;
12. canonical encoding and aggregate digest derivation.

Decode reconstructs typed leaves, reruns all invariants, rejects trailing or
unknown fields and requires byte-identical re-encoding.

### Required inventory identities

The aggregate hard-codes only the ordered identities below from the parent
spec. Table identities are all in `dpone_authority`:

```text
dpone_authority_contract_v3
dpone_target_registration_v3
dpone_active_registration_head_v3
dpone_target_generation_head_v3
dpone_writer_operation_v3
dpone_sealed_effect_v3
dpone_staging_artifact_v3
dpone_staging_chunk_v3
dpone_generation_authority_issuance_v3
dpone_generation_authority_ref_v3
dpone_generation_authority_consumption_v3
dpone_provider_install_receipt_v3
dpone_control_receipt_v3
dpone_open_stage_recovery_receipt_v3
dpone_open_stage_recovery_artifact_v3
dpone_effect_receipt_v3
dpone_batch_effect_receipt_v3
dpone_xmin_effect_receipt_v3
dpone_target_row_hash_v3
dpone_xmin_checkpoint_v3
```

Shared procedures, also all in `dpone_authority`, are ordered exactly:

```text
dpone_provision_registration_v3
dpone_rotate_registration_v3
dpone_import_generation_authority_set_v3
dpone_probe_control_effect_v3
dpone_attest_schema_v3
dpone_admit_operation_v3
dpone_seal_effect_v3
dpone_take_over_sealed_v3
dpone_probe_pre_source_v3
dpone_open_stage_v3
dpone_renew_stage_v3
dpone_begin_stage_chunk_v3
dpone_complete_stage_chunk_v3
dpone_observe_stage_v3
dpone_observe_stage_chunk_v3
dpone_scan_stage_v3
dpone_seal_stage_v3
dpone_recover_expired_open_v3
dpone_probe_open_recovery_v3
dpone_admit_writer_v3
dpone_resolve_admit_authority_set_v3
dpone_append_effect_receipt_v3
dpone_write_xmin_checkpoint_v3
dpone_consume_stage_set_v3
dpone_consume_authority_set_v3
dpone_advance_operation_and_head_v3
dpone_prove_candidate_effect_v3
dpone_probe_effect_v3
dpone_probe_registration_v3
```

The remaining identities are:

- schemas: `dpone_authority`, `dpone_stage`;
- binding kinds: `batch_mutate`, `batch_row_hash`, `batch_quality`,
  `xmin_mutate`, `xmin_row_hash`, `xmin_quality`;
- principals: `provisioner`, `runtime`, `loader`, `observer`;
- result cardinalities: `exactly_one`, `zero_or_many`;
- errors: numbers `51001..51012`, with positive state and closed outcome/retry/
  probe/redaction classifications.
- migration probes are a nonempty canonical set in this algebra. The concrete
  descriptor task freezes the exact recognized/blocked observation set from
  the parent migration matrix; this pure contract does not pretend that a
  hard-coded probe count proves that future SQL catalog coverage is complete.

The existing schema-2 contract, rather than a duplicate physical model, owns
column, constraint, index, trigger, property, parameter, result, permission,
codec and shared signer truth. Names are identities, not enough to make an
entry valid: every physical wrapper must point to the exact typed schema-2
object and carry its own physical-only execution metadata.

### Reference and failure semantics

References use semantic coordinates, never database-assigned IDs. An aggregate
with a missing/extra entry, case-fold collision, wrong order, a schema-2 object
that fails its own portable type/facet/default/constraint validation, ambiguous
result cardinality, output parameter, unbound lock, read/write escape,
role/signer mismatch, missing error mapping or incorrect digest fails during
construction and decode. There is no repair or normalization path.

## Architecture

| Component | Responsibility |
|---|---|
| `physical_descriptor_enums` | Closed discriminators only; has no dependency on descriptor validators, codec or identity modules |
| `physical_descriptor_primitives` | Shared scalar validators only; has no dependency on physical-descriptor enums and exports no symbols owned by codec/identity modules |
| `physical_descriptor_profiles` | Exact engine/session profiles; depends only on canonical scalar validators and codec/identity leaves |
| `physical_descriptor_definitions` | Definition bytes, normalization and digest binding |
| `physical_descriptor_resources` | Physical resource references/declarations, fields, access edges and SQL-scalar compatibility |
| `physical_descriptor_coordinates` | Comparison coordinates/literals/existence operands and reusable instance selectors; source-sensitive owner checks remain aggregate-aware |
| `physical_descriptor_locks` | Lock mechanism/action/mode and total order |
| `physical_descriptor_requests` | Request codecs, scalar bindings, projections and outcomes |
| `physical_descriptor_transitions` | Compound transitions and exact revision/CAS rules |
| `physical_descriptor_replay` | Comparison pairs and outcome-scoped proof formulas |
| `physical_descriptor_errors` | Stable SQL condition and migration-probe classifications |
| `physical_descriptor_relations` | Physical table wrappers around schema-2 portable objects |
| `physical_descriptor_execution` | Immutable execution-semantics model, canonical codec and local shape validation |
| `physical_descriptor_execution_validation` | Cross-resource read/write, request, selector, transition, replay and authority-path closure through a structural execution view |
| `physical_descriptor_procedures` | Thin shared-procedure and binding-module wrappers |
| `physical_schema_descriptor` | Counts, identities, delegated closure, portable projection and aggregate codec |

All modules depend only on existing pure contracts and the V3 canonical codec.
Adapters, ports, runtime and composition roots remain untouched. Each module
must remain below repository SLOC/graph budgets; split by stable responsibility,
not by numbered fragments.

Dependency direction is acyclic. Shared enums, scalar validators and
engine/session profiles remain independent leaves. Resource/access kinds, lock
kinds/cardinalities, projection scalar kinds and value cardinalities belong to
the resource inventory owner. Lock action, mechanism, mode, owner and timeout
policy belong to the lock-step owner. Locks import resource discriminators;
resources never import locks. Declaration order is preserved before access and
lock-rank constants, including enum iteration order. The remaining shared enum
owner imports neither validators nor codec; profiles import validators but no
physical enum. Consumers import only the defining owners they use. This prevents enum↔validator↔consumer and
validator↔codec↔consumer dependency triangles while keeping every leaf below
the new-module warning threshold. Selectors and SQL-scalar compatibility live
below locks/requests/replay; request and transition leaves do not import replay;
the execution-semantics model invokes a lower-level aggregate validator that
does not import or re-export the model owner; thin procedure wrappers depend on
the model owner; the aggregate delegates validation to its composites instead
of importing leaf validators solely to repeat policy. Imports always name the
canonical defining module. Pass-through imports, wildcard-derived `__all__`,
and `# noqa: F401` re-export tunnels are forbidden. Every module has an explicit
owner-only `__all__` (or no `__all__`) and must stay at or below the repository
warning threshold for new modules, not merely below the hard SLOC limit.

`physical_descriptor_execution_validation` validates coordinate sources in
aggregate context. `session_binding` must resolve to a declared `session`
resource and a non-null scalar field. `predecessor_resource_field` and
`candidate_resource_field` must resolve to the exact transition owner and are
invalid when no owning transition exists. Resource declaration lookup happens
before lock ordering or selector comparison, and malformed revision candidates
are type-checked before member access; every invalid public construction path
therefore raises `MssqlR1V3ContractError`, never raw `KeyError` or
`AttributeError`.

ADR 0056 and the parent physical spec already own the architecture decision; no
new ADR is required for the representation itself. ADR 0057 separately records
the time-bounded integration exception for the measured repository clustering
delta. It leaves the global 0.182 budget and visible FAIL unchanged, permits
only this internal activation-blocked implementation to integrate, and blocks
production promotion until a separately approved remediation removes the
exception.

### Alternatives

| Alternative | Decision |
|---|---|
| Hand-written SQL as authority | Rejected: renderer/catalog drift cannot be reproduced |
| JSON/YAML dict without typed algebra | Rejected: weak unions and references fail late |
| Dataclass reflection for field mapping | Rejected: source refactors silently change SQL |
| Typed immutable descriptor | Adopted: explicit, testable and renderer-neutral |

## Market comparison

External ETL products are `N/A` for this internal representation sub-scope.
The parent feature specification owns current market research and measurable
route comparisons. No product claim is introduced here.

## Security and operations

The contracts contain identifiers, definitions and digests, never credentials,
certificate private keys, SIDs or connection data. Canonical diagnostics name
only a bounded semantic coordinate. No filesystem, network or import-time I/O
is allowed.

## Test and certification plan

| Layer | Required scenarios | Status meaning |
|---|---|---|
| Unit | Every leaf/union boundary and exact bool/int/string rejection | Hermetic PASS/FAIL |
| Contract | Full synthetic 20/29/6 fixture round-trip and digest | Hermetic PASS/FAIL |
| Property | Remove/add/reorder/tamper every coordinate class | All rejected |
| Architecture | Import rules, layer metrics, module size | No regression |
| Live SQL | N/A; no adapter exists | UNVERIFIED, never PASS |

Tests freeze the inventory identities and use an explicit mutation registry.
The registry maps every descriptor dataclass to its ordered field-name tuple;
the suite first asserts this tuple equals `dataclasses.fields()` and then
mutates every field, enum discriminator, optional/union branch and nested
reference. Reflection is allowed only for this completeness assertion, never
for production serialization or validation. Every mutation must either raise a
stable `MssqlR1V3ContractError` or change the aggregate digest. Removing,
adding, reordering, exact-duplicating and case-fold-colliding every semantic
coordinate class are separate mandatory cases.

The risk matrix also includes exact/truncated/extra canonical fields, trailing
bytes, bool-for-int, raw enum strings, wrong domain separators, invalid UTF-8,
non-NFC/CR/NUL/multiple-newline definitions, definition digest mismatch,
portable-projection order drift, shared-signer drift, principal execute-set
drift, scan/fixed-result substitution, output parameters, read/write escape,
lock mechanism/action/mode/owner/rank/subrank drift, undeclared resources,
lock-phase ties, singleton/set-selector and empty-set drift, security-access
widening, outcome NULL-shape/transition/replay-policy drift, request-codec or
scalar-parameter-source mismatch, wrong state/current/expected/requested-
candidate/committed-candidate revision source or type, state-change predecessor
matrix drift, candidate-on-foreign-resource, UUID/UTC literal grammar mismatch,
adjacent direction/cardinality, nullable equality without explicit guards,
ordered-set order drift, row-absence-vs-NULL substitution, `row_absent` used as
descendant proof, proof-resource/key/lock mismatch, missing transition-policy
coordinate, missing proof-resource read edge, non-conjunctive comparison
interpretation, wrong transition owner/execution path/authority path,
zero-or-one/one-or-more selector boundaries, missing compound transition/replay clause, incomplete error-number union and
missing/duplicate/reordered migration probes. The same valid input must always
produce identical bytes/digest; repeated decode/encode must remain
byte-identical; a wrong descriptor version and every foreign/legacy canonical
domain must fail closed.

## Rollout, rollback and ownership

The change lands before any concrete descriptor. A later approved task creates
the complete production instance and generated SQL. Rollback removes only these
unused internal models. Once production descriptor bytes exist, this version is
immutable and successor changes require V2.

One path-scoped contract implementer owns the new modules and focused test.
The integrator alone owns the parent spec, ADR, package exports and shared
registries. SQL/adapters/runtime/ports are forbidden.

## Approval checklist

- [x] Scope is SQL-free and public-contract neutral.
- [x] Exact identities, counts, ordering and failure semantics are explicit.
- [x] Dependency direction and module responsibilities are bounded.
- [x] Compatibility and rollback are code-only.
- [x] Hermetic tests cannot claim live certification.
- [x] Parent physical spec remains `RESEARCHED`.
- [x] User explicitly authorized implementation of V7 and its scoped gates.
- [x] Maintainer status is `APPROVED`.
