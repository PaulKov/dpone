
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 physical schema and backend

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Depends on: `source record 134`
- Amended by:
  `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`
  for the dedicated provider-install receipt and 20-table inventory.

## Executive summary

The approved R1 contracts deliberately stop before choosing SQL Server table,
procedure and permission details. This specification freezes the smallest
physical authority domain able to execute Batch full refresh and XMin
current-state effects atomically in one standalone SQL Server 2022 database.

The public manifest does not change. The measurable outcome is:

```text
exact schema install and attestation
→ signed target registration
→ receipt-backed OPEN/SEALED staging
→ one physical-session target effect
→ exact candidate proof
→ fully durable commit
→ source-free replay after an ambiguous response
```

This document does not approve the concrete backend by itself. Production code
starts only after fresh architecture, security and test/certification review
changes this status to `APPROVED`.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Platform operator | Provision one certifiable target authority domain | Current V3 providers depend on abstract backends and experimental staging DDL | Exact install/status output and fresh attestation |
| Data-product author | Run Batch/XMin without physical SQL knowledge | Physical tables, locks and receipts leak into low-level setup | Existing semantic manifest works unchanged |
| Incident responder | Resolve lost commit or retained SEALED work safely | Partial catalog state can be mistaken for success | Closed committed/known-not-committed/unknown proof |
| Security reviewer | Prove least privilege | Experimental stage renderer grants schema-wide EXECUTE | Exact named-procedure and object permissions only |

Journey:

1. Author and validate the existing semantic Batch/XMin manifest.
2. Operator resolves the signed environment profile and four existing database
   principals.
3. Provisioner opens a fresh non-pooled session, installs or exactly attests the
   shared schema, registers the physical target and commits a control receipt.
4. Runtime opens and seals private stage objects, then executes one fenced V3
   effect transaction.
5. Status reports only redacted binding/registration/generation/receipt state.
6. Ambiguous commit uses a different physical connection and exact fresh proof.
7. Drift, extra permission, legacy state or partial inventory blocks with a
   stable reason; no source reread or target guess occurs.
8. Upgrade is additive. After the first V3 effect an older writer never regains
   permissions.

## Scope

### In scope

- SQL Server 2022 engine major 16, compatibility level 160.
- `standalone_same_database`, one ordinary disk rowstore target binding.
- Batch full refresh and XMin current-state only.
- Exact V3 schema renderer, attestor, fresh non-pooled session, locks, control,
  stage, effect and fresh-replay backends.
- Nineteen target-local tables, 29 shared stored procedures, six signed modules
  per target binding, descriptor-assigned mutation triggers, protected
  target/stage properties and exact database-scoped permission attestation.
- Existing external database principals for provisioner, runtime, loader and
  observer; the schema installer does not create server logins.

### Non-goals

- WAL, journals, R2 wire/Batch engine, AG, DTC or cross-database state.
- Registration retirement/revocation or revoked-registration override.
- Automatic stage GC, receipt GC, table drop after consumption or TTL for an
  ambiguous outcome.
- Views, arbitrary-SQL procedures, schema-wide EXECUTE, `EXECUTE AS OWNER`,
  connection pooling or MARS. Certificate-signed modules are required only for
  the closed stage-owner and attestation procedure sets below.
- Runtime schema learning, in-place interpretation of V1/V2 rows, or upgrade of
  any nonempty pre-amendment V3 inventory.
- A SQL Server CU/edition certification claim. Vendor-live evidence binds the
  exact edition/build/CU independently of this portable schema revision.

### Assumptions and constraints

```yaml
schema_contract_version: dpone-mssql-r1-v3-schema-2
authority_schema: dpone_authority
stage_schema: dpone_stage
database_compatibility_level: 160
delayed_durability: DISABLED
transaction_isolation: SERIALIZABLE
mars: disabled
connection_pooling: disabled
automatic_stage_gc: disabled
contract_text_collation: Latin1_General_100_BIN2
module_execution_context: caller
module_signing: certificate_profiles_v1
database_containment: PARTIAL
environment_principal_profile: contained_sql_users_v1
binding_module_pack: six_signed_modules_v1
stage_loader_profile: transactional_odbc_reference_v1
bcp_activation: blocked_until_r2a_wire_certification
```

The database recovery model may be SIMPLE, BULK_LOGGED or FULL, but the exact
value belongs to environment evidence and admission limits. `READ_COMMITTED_SNAPSHOT`
does not change guarded reads because they use `UPDLOCK,HOLDLOCK`.

The GA1 principal profile contains four distinct contained database SQL users
(`authentication_type=database`) with no server SID and no database-role
membership. Hostile `sysadmin`, server-level impersonation/control and a
malicious provisioner are outside the R1 threat model. Instance, Windows and
external identities remain representable by schema-2 but are activation-blocked
until a separate server-authority capability proves their complete privilege
paths.

## Public contract

### CLI and Python API

No new author-facing command or import is introduced. Existing R1 plan/status
surfaces gain only redacted `schema_contract_version`, contract/permission
digests and projection revision. Physical object names, database GUIDs,
procedure names and grants remain internal.

Those plan/status fields, stable blocker behavior and new permission
prerequisites are additive public contracts even though the manifest is
unchanged. Their output schemas, compatibility tests, migration guidance,
operator documentation and changelog entry are required before activation.

Provision/status failures use existing R1 exit codes. Backend reasons are
mapped to the stable public R1 blocker family; raw SQL error text is diagnostic
stderr only after identifier/credential redaction.

### Manifest/schema

No manifest field changes. The resolved environment tuple must select this
exact schema version and four principal references. Weaker or unknown schema
revisions fail before PostgreSQL business I/O.

### Artifacts and evidence

Hermetic schema evidence records renderer version, ordered inventory digest,
permission digest, supported-codec digest and DDL SHA-256. Live evidence also
binds SQL Server edition/build/CU, database options, ODBC/pyodbc versions,
principal identity digests and the exact Git/dependency/image digest.

Fresh attestation derives target binding and registered physical-authority
digest from the exact verified registration and requires the observed server
identity, database GUID, database-family GUID and recovery-fork GUID to match
that registration; `database_id` remains live-only. Decoded retained
attestations must pass the same registration-binding oracle on every install,
status, activation and replay path. A complete registration-A/physical-B splice
fails with `schema_attestation_registration_mismatch` even after all internal
attestation digests are recomputed.

### Compatibility and migration

- V1/V2 tables and bytes are retained and never decoded as V3.
- Automatic migration is allowed only when V3 inventory is wholly absent.
- Exact empty V1/V2 objects may coexist only when no legacy writer grant exists.
- Any schema-1/unversioned/pre-amendment V3 object, any V1/V2 receipt, sealed
  intent, nonterminal operation, stage row or ambiguous state, any
  partial/extra/unreadable inventory or unknown codec blocks with
  `migration_blocked` and requires manual disposition outside this backend.
- Any schema-wide or legacy writer grant blocks before mutation; external
  disposition must revoke the direct grant or remove every membership/grant
  path before a fresh install/status probe. A compensating `DENY` does not
  remove the forbidden observed grant path.
- After a V3 receipt, rollback means a V3-reader-compatible binary or roll
  forward. An older writer is never reauthorized.

The physical descriptor enumerates the complete inspection namespace and exact
digests for recognized empty V1/V2 inventories, schema-1/unversioned V3 names,
writer grants and terminal/nonterminal row predicates. Missing catalog
visibility, encrypted/unreadable definitions, unknown names/codecs or any
partial state are distinct `migration_blocked` observations. Every blocked
migration path is proven to execute zero mutation SQL before returning.

### Internal schema-authority boundary

Schema-2 cannot be attested from a portable contract or registration ID alone.
The separately approved physical implementation therefore replaces the
unimplemented internal schema-authority port with explicit dependency inputs:

```python
class MssqlR1PrincipalAuthorityResolverV3Port(Protocol):
    def resolve(
        self,
        verification: MssqlTargetRegistrationVerificationV1,
    ) -> MssqlR1PrincipalAuthoritySetV3: ...


class MssqlR1SchemaAuthorityV3Port(Protocol):
    def install_and_attest(
        self,
        transaction: MssqlR1TransactionV3,
        *,
        expected: MssqlR1SchemaContractV3,
        registration_verification: MssqlTargetRegistrationVerificationV1,
        principal_authority: MssqlR1PrincipalAuthoritySetV3,
        binding_modules: MssqlR1BindingModulePackV1,
    ) -> MssqlR1SchemaAttestationV3: ...

    def attest_fresh(
        self,
        *,
        expected: MssqlR1SchemaContractV3,
        registration_verification: MssqlTargetRegistrationVerificationV1,
        principal_authority: MssqlR1PrincipalAuthoritySetV3,
        binding_modules: MssqlR1BindingModulePackV1,
    ) -> MssqlR1SchemaAttestationV3: ...
```

`attest_fresh` owns a new non-pooled connection. Both methods byte-reproduce
the verified registration, prove the principal set belongs to that exact
registration/profile, construct the typed observation, call
`assert_for_registration`, attest the binding pack and compare the resulting
live identities with retained baselines. Mutable request state, closures,
service locators and target/caller-provided authority projections are forbidden.
The broad legacy port module may only re-export the narrow canonical boundary.

Pre-source admission is likewise split into an explicit create-or-observe
command and a read-only retry probe. An effect key alone can identify an
existing operation, but cannot safely create one. The physical implementation
therefore replaces the unimplemented one-argument boundary with:

```python
@dataclass(frozen=True, slots=True)
class MssqlR1OperationAdmissionRequestV3:
    identity: MssqlR1EffectIdentityV3
    registration_id: UUID
    registration_payload_digest: bytes
    registered_physical_authority_digest: bytes
    recovery_identity_digest: bytes
    owner_id_digest: bytes
    server_lease_seconds: int


class MssqlTargetAuthorityPreparationV3Port(Protocol):
    def admit_or_observe(
        self,
        request: MssqlR1OperationAdmissionRequestV3,
    ) -> MssqlR1PreSourceOutcomeV3: ...

    def probe_pre_source(self, effect_key: bytes) -> MssqlR1PreSourceOutcomeV3: ...
```

The request has its own versioned canonical codec/digest. SQL decodes the
identity, recomputes operation/effect keys, proves the exact active registration
and physical/recovery coordinates, and captures the current writer-head
predecessor under the binding lock. `admit_or_observe` may create only the
canonical `admitted, epoch=1, projection=1` row. `probe_pre_source` never
creates or repairs state. Both return the same closed pre-source discriminator;
only a successful admission returns `SOURCE_REQUIRED`. The compatibility
`pre_source(effect_key)` surface becomes a probe-only shim and is never used to
admit a new operation.

## Normative SQL domains

The aliases below are specification notation; generated DDL uses the exact SQL
types on the right and does not create alias types.

| Alias | SQL Server type | Rule |
|---|---|---|
| `uuid` | `uniqueidentifier` | nonzero where the pure contract requires UUID |
| `digest` | `binary(32)` | SHA-256 only |
| `payload` | `varbinary(max)` | nonempty canonical bytes; digest check |
| `state16` | `varchar(32) COLLATE Latin1_General_100_BIN2` | closed lowercase ASCII enum |
| `state_upper16` | `varchar(32) COLLATE Latin1_General_100_BIN2` | closed uppercase legacy-codec literal; only where explicitly named |
| `identifier` | `nvarchar(128) COLLATE Latin1_General_100_BIN2` | NFC, SQL identifier validation |
| `bounded_text` | `nvarchar(512) COLLATE Latin1_General_100_BIN2` | NFC, no control characters |
| `revision` | `bigint` | `>0` |
| `count64` | `bigint` | `>=0` |
| `utc` | `datetime2(7)` | canonical UTC supplied/observed by server |
| `row_token` | `rowversion` | CAS observation only; never canonical payload |
| `nonce16` | `binary(16)` | canonical arbitrary 128-bit nonce; not a UUID |

Every stored canonical payload column `x_payload` has an accompanying digest
(`x_digest` unless the inventory explicitly names the semantic digest) and a
CHECK `HASHBYTES('SHA2_256', x_payload)=digest`. Payload/receipt/checkpoint
tables declare one exact lifecycle in the normative descriptor: `immutable`,
`append_only`, `cas_head`, `state_machine` or `guarded_mutable`. Only immutable
and append-only rows receive an `INSTEAD OF UPDATE, DELETE` fail-closed trigger;
all other writes are possible only through their guarded procedures. Defaults
are forbidden in schema revision 2, so any `sys.default_constraints` row on a
managed table is attestation drift. Cross-row invariants are procedures plus
locks, never pretend-deferred triggers.

## Exact table inventory

All tables are in `dpone_authority`. `NN` means `NOT NULL`; `N` means nullable.
Constraint and index names are renderer-owned deterministic identifiers and are
part of the portable inventory digest.

### Schema and registration

`dpone_authority_contract_v3`

```text
schema_contract_version varchar(64) NN PK
schema_contract_payload payload NN
schema_contract_digest digest NN UNIQUE
object_inventory_digest digest NN
permission_contract_digest digest NN
supported_codec_set_digest digest NN
projection_revision revision NN CHECK = 1
schema_identity_generation revision NN CHECK = 1
baseline_registration_verification_payload payload NN
baseline_registration_verification_receipt_digest digest NN
baseline_principal_authority_set_payload payload NN
baseline_principal_authority_set_digest digest NN
baseline_schema_attestation_payload payload NN
baseline_schema_attestation_digest digest NN
baseline_live_identity_digest digest NN
baseline_observed_at utc NN
installed_at utc NN
```

Exactly one row exists for this schema revision; the table is immutable. The
baseline is inserted only after the exact verified registration and independent
principal authority set have produced a valid fresh schema attestation. It is
never updated by registration rotation. Replacing a schema/module certificate,
changing catalog/security identity or adopting a new baseline requires a future
approved schema-generation operation; status and runtime never self-adopt it.

`dpone_target_registration_v3`

```text
registration_id uuid NN PK
registration_action state16 NN CHECK IN ('initial','rotate')
predecessor_registration_id uuid N FK self
expected_active_registration_revision revision N
issued_at utc NN
expires_at utc NN CHECK expires_at > issued_at
nonce nonce16 NN UNIQUE
profile_id bounded_text NN
capability_tuple_digest digest NN
resolved_profile_digest digest NN
route_source_authority_sha256 digest NN
target_object_profile state16 NN CHECK = 'ordinary_disk_rowstore_v1'
catalog_projection_version varchar(64) NN COLLATE Latin1_General_100_BIN2
revocation_revision count64 NN CHECK = 0
target_binding_uuid uuid NN
target_object_uuid uuid NN
recovery_domain_uuid uuid NN
recovery_domain_epoch revision NN
server_instance_identity_sha256 digest NN
database_guid uuid NN
database_family_guid uuid NN
recovery_fork_guid uuid NN
database_name identifier NN
database_name_digest digest NN
schema_name identifier NN
schema_name_digest digest NN
object_name identifier NN
object_name_digest digest NN
object_id int NN CHECK object_id > 0
physical_generation_uuid uuid NN
catalog_contract_digest digest NN
target_contract_revision revision NN
registration_payload payload NN
registration_payload_digest digest NN UNIQUE
verification_payload payload NN
verification_receipt_digest digest NN UNIQUE
verification_policy_digest digest NN
schema_attestation_payload payload NN
schema_attestation_digest digest NN
schema_live_identity_digest digest NN
binding_module_pack_payload payload NN
binding_module_pack_digest digest NN
binding_module_attestation_payload payload NN
binding_module_attestation_digest digest NN
binding_module_live_identity_digest digest NN
created_at utc NN
UNIQUE(target_binding_uuid, registration_id)
UNIQUE(target_object_uuid, registration_id)
```

Initial registration requires both predecessor fields NULL. Rotation requires
both, exact predecessor, and adjacent active revision; procedures enforce these
cross-row rules. A rotation must reproduce the immutable baseline
`live_identity_digest` and byte-identical six-module binding pack. The successor
schema attestation is checked against the successor verification, while the
baseline attestation is always rechecked against the verification retained with
that baseline. Rows are immutable.

`dpone_active_registration_head_v3`

```text
target_binding_uuid uuid NN PK
physical_coordinate_digest digest NN UNIQUE
target_object_uuid uuid NN UNIQUE
active_registration_id uuid NN UNIQUE FK registration
active_registration_payload_digest digest NN
active_registration_revision revision NN
revocation_revision count64 NN CHECK = 0
registered_physical_authority_digest digest NN
schema_contract_digest digest NN
permission_contract_digest digest NN
last_control_receipt_id uuid NN
last_control_receipt_digest digest NN
projection_revision revision NN
updated_at utc NN
row_token row_token NN
```

Only provision/rotation may CAS this guarded mutable head.

### Writer, operation and sealed effect

`dpone_target_generation_head_v3`

```text
target_binding_uuid uuid NN PK
target_object_uuid uuid NN UNIQUE
physical_coordinate_digest digest NN UNIQUE
registered_physical_authority_digest digest NN
recovery_identity_digest digest NN
writer_state state16 NN CHECK IN ('uninitialized','active','paused')
writer_mode state16 N CHECK IN ('batch_full_refresh','xmin_current_state')
target_generation revision N
row_hash_generation revision N
business_head_revision revision N
last_receipt_id uuid N
last_receipt_digest digest N
active_effect_key digest N
active_operation_epoch revision N
active_operation_projection_revision revision N
active_lease_expires_at utc N
source_authority_sha256 digest N
writer_head_payload payload NN
writer_head_digest digest NN
projection_revision revision NN
updated_at utc NN
row_token row_token NN
```

Checks enforce target/hash generation equality; generation/head/last-receipt
all-or-none; active effect/epoch/projection/lease all-or-none; `uninitialized`
has no committed tuple; `active|paused` has a complete tuple.

`dpone_writer_operation_v3`

```text
operation_key digest NN PK
effect_key digest NN UNIQUE
target_binding_uuid uuid NN FK active registration head
source_mode state16 NN CHECK IN ('batch_full_refresh','xmin_current_state')
state state16 NN CHECK IN ('admitted','staging','sealed','committed')
operation_epoch revision NN
operation_projection_revision revision NN
owner_id_digest digest NN
lease_expires_at utc NN
sealed_request_digest digest N
committed_receipt_id uuid N
committed_receipt_digest digest N
created_at utc NN
updated_at utc NN
row_token row_token NN
```

The receipt pair is all-or-none and only `committed` has it. A filtered unique
index on `target_binding_uuid WHERE state <> 'committed'` allows one open
operation per binding. Exact transitions are:

```text
create                     -> admitted epoch=1 projection=1
admitted                   -> staging epoch same projection+1
staging                    -> sealed epoch same projection+1
sealed                     -> committed epoch same projection+1
expired staging recovery   -> admitted epoch+1 projection+1
sealed takeover            -> sealed epoch+1 projection+1
```

`dpone_sealed_effect_v3`

```text
effect_key digest NN PK FK operation
operation_key digest NN UNIQUE FK operation
seal_intent_payload payload NN
seal_intent_digest digest NN UNIQUE
sealed_request_payload payload NN
sealed_request_digest digest NN UNIQUE
registration_id uuid NN FK registration
registration_payload_digest digest NN
verification_policy_digest digest NN
source_snapshot_digest digest NN
source_schema_digest digest NN
artifact_set_digest digest NN
mutation_plan_digest digest NN
rendered_bundle_digest digest NN
generation_transition_digest digest NN
authority_set_digest digest NN
contract_digest digest NN
sealed_at utc NN
```

Immutable; `sealed_request_digest` must equal the operation projection.

### Staging

`dpone_staging_artifact_v3`

```text
artifact_id uuid NN PK
operation_key digest NN FK operation
effect_key digest NN
artifact_kind state16 NN CHECK IN ('batch_payload','xmin_delta','xmin_complete_keys')
target_binding_uuid uuid NN
owner_epoch revision NN
open_plan_payload payload NN
open_plan_digest digest NN
exact_stage_ddl_digest digest NN
object_uuid uuid N UNIQUE
object_id int N CHECK object_id > 0
physical_token uuid N UNIQUE
stage_schema identifier N CHECK stage_schema = 'dpone_stage'
stage_object identifier N UNIQUE
state state16 NN CHECK IN ('planned','open','sealed','consumed','abandoned')
lease_seconds int NN CHECK BETWEEN 1 AND 86400
opened_at utc N
hard_expires_at utc N
lease_revision revision N
last_lease_request_digest digest N
lease_expires_at utc N
manifest_payload payload N
manifest_digest digest N
completed_chunk_count count64 N
completed_chunk_set_digest digest N
sealed_at utc N
retention_until utc N
consumed_receipt_id uuid N
consumed_receipt_digest digest N
consumed_at utc N
abandoned_recovery_receipt_id uuid N
abandoned_at utc N
created_recovery_receipt_id uuid N
projection_revision revision NN
row_token row_token NN
```

State-shape checks use five all-or-none groups: physical identity `P`, active
lease `L`, manifest `M`, consumption `C` and abandonment `A`.

| State | P | L | M | C | A |
|---|---|---|---|---|---|
| `planned` | all NULL | NULL | NULL | NULL | NULL |
| `open` | all present | all present | NULL | NULL | NULL |
| `sealed` | all present | NULL | all present | NULL | NULL |
| `consumed` | all present | NULL | all present | all present | NULL |
| `abandoned` | all present | NULL | NULL | NULL | all present |

`P` is object UUID/ID, physical token and schema/object. `L` is opened time,
hard expiry, lease revision, last renewal request and lease expiry. `M` is
manifest payload/digest, completed-chunk count/set digest, sealed time and
retention time. `C` is receipt ID/digest/time. `A` is recovery-receipt ID/time.
`lease_expires_at <= hard_expires_at` and `retention_until > sealed_at`.
`created_recovery_receipt_id` is NULL for an initial artifact and present only
for a replacement proved as `set_kind=new` by the guarded procedure. A filtered unique index on
`(effect_key, artifact_kind) WHERE state IN ('planned','open','sealed')` and
indexes `(operation_key,artifact_kind,artifact_id)`, OPEN lease,
`consumed_receipt_id`, and `abandoned_recovery_receipt_id` are mandatory.

`PLANNED→OPEN`, physical table creation/properties and the initial lease commit
in one transaction. A durable observed `planned` row is corruption/UNKNOWN,
not a resumable state. Operation, writer head and every OPEN artifact for an
operation carry the same server-time lease boundary and adjacent lease
revision. A lost renewal response authorizes continued loading only after a
fresh exact request-digest/revision proof.

`dpone_staging_chunk_v3`

```text
artifact_id uuid NN FK artifact
chunk_sequence int NN CHECK chunk_sequence >= 0
chunk_key digest NN UNIQUE
operation_key digest NN
effect_key digest NN
owner_epoch revision NN
chunk_request_payload payload NN
chunk_request_digest digest NN UNIQUE
logical_chunk_digest digest NN
declared_row_count count64 NN CHECK declared_row_count > 0
declared_payload_bytes count64 NN
wire_contract_digest digest NN
wire_artifact_digest digest NN
wire_bytes count64 NN
load_token uuid NN UNIQUE
state state16 NN CHECK IN ('loading','complete')
observed_row_count count64 N
completed_at utc N
projection_revision revision NN
row_token row_token NN
PRIMARY KEY(artifact_id, chunk_sequence)
UNIQUE(artifact_id, chunk_sequence, chunk_key, load_token)
```

Only COMPLETE has `observed_row_count` and `completed_at`; those rows are then
immutable. A filtered unique index permits one LOADING chunk per artifact and
sequence must be adjacent to the completed prefix. `chunk_key` is SHA-256 over
the exact `dpone-r1-stage-chunk-load-v1` request containing operation/effect,
binding/artifact/kind, epoch, sequence, logical/payload/wire facts, load token
and expected artifact revision. LOADING is never resumed by a new process;
expiry recovery abandons the entire artifact set.

Every dynamic stage row has the fixed suffix:

```text
__dpone_artifact_id uniqueidentifier
__dpone_chunk_sequence int
__dpone_chunk_key binary(32)
__dpone_load_token uniqueidentifier
__dpone_chunk_row_ordinal int CHECK >= 0
```

The suffix has a composite FK to the exact chunk identity and a unique
`(chunk_sequence,chunk_row_ordinal)` key. Complete/seal prove the ordinal range
is exactly `0..declared_row_count-1`; BCP exit status and stdout counts are
telemetry only.

### Generation authority

`dpone_generation_authority_issuance_v3`

```text
issuance_id uuid NN PK
target_binding_uuid uuid NN
effect_key digest NN
registration_id uuid NN FK registration
registration_payload_digest digest NN
registration_revocation_revision count64 NN CHECK = 0
issued_at utc NN
expires_at utc NN CHECK expires_at > issued_at
nonce nonce16 NN UNIQUE
authority_set_payload payload NN
authority_set_digest digest NN UNIQUE
issuance_payload payload NN
issuance_payload_digest digest NN UNIQUE
verification_payload payload NN
verification_receipt_digest digest NN UNIQUE
verification_policy_digest digest NN
created_at utc NN
UNIQUE(target_binding_uuid, effect_key)
```

`dpone_generation_authority_ref_v3`

```text
authority_id uuid NN PK
issuance_id uuid NN FK issuance
ordinal tinyint NN CHECK IN (0,1)
purpose state16 NN CHECK IN ('initial_cutover','rebaseline','empty_refresh')
effect_key digest NN
source_snapshot_digest digest NN
artifact_set_digest digest NN
mutation_plan_digest digest NN
expected_writer_generation revision N
expected_head_revision revision N
candidate_writer_generation revision NN
candidate_head_revision revision NN
recovery_identity_digest digest NN
authority_payload payload NN
authority_digest digest NN UNIQUE
UNIQUE(issuance_id, ordinal)
UNIQUE(effect_key, purpose)
```

Expected generation/head are both NULL or both present. Import proves exact
canonical purpose/UUID order and one or two refs; zero refs has no issuance row.
Both tables are immutable.

`dpone_generation_authority_consumption_v3`

```text
issuance_id uuid NN PK FK issuance
consuming_receipt_id uuid NN UNIQUE FK effect receipt
consuming_receipt_digest digest NN
consumed_at utc NN
projection_revision revision NN CHECK = 1
row_token row_token NN
```

Absence means ISSUED; one row consumes the entire issuance/ref set atomically.

### Receipts and recovery

`dpone_provider_install_receipt_v3`

```text
installation_effect_key binary(32) NN PK
request_digest binary(32) NN
payload_bytes varbinary(max) NN CHECK DATALENGTH(payload_bytes) > 0
payload_digest binary(32) NN UNIQUE
committed_at datetime2(7) NN
CHECK HASHBYTES('SHA2_256', payload_bytes) = payload_digest
```

This immutable, installer-only row is the same-database authority for one
combined provider-and-binding-pack installation request. It has no registration
foreign key, update/delete path or TTL. The exact four comparison fields are
installation effect key, request digest, payload bytes and payload digest. The
V2 receipt payload/effect identity is owned by the approved provider-security
authority amendment; observed pre-amendment 19-table V3 state requires manual
disposition and never receives a synthesized row.

`dpone_control_receipt_v3`

```text
control_receipt_id uuid NN PK
control_effect_key digest NN UNIQUE
operation state16 NN CHECK IN ('provision','registration_rotate','authority_import')
physical_object_coordinate_digest digest NN
registered_physical_authority_digest digest NN
target_binding_uuid uuid NN
registration_id uuid NN
input_payload_digest digest NN
verification_receipt_digest digest NN
expected_active_registration_revision revision N
committed_active_registration_revision revision NN
expected_writer_head_revision revision N
observed_writer_head_revision revision NN
expected_writer_head_digest digest N
observed_writer_head_digest digest NN
schema_contract_digest digest NN
permission_contract_digest digest NN
affected_authority_set_digest digest N
affected_override_payload_digest digest N CHECK IS NULL
committed_at utc NN
canonical_receipt_payload payload NN
receipt_digest digest NN UNIQUE
UNIQUE(control_receipt_id,receipt_digest)
FK(target_binding_uuid,registration_id) -> registration
```

The operation discriminator freezes the approved predecessor/revision and
authority-nullability shapes. `verification_receipt_digest` is polymorphic
between registration and authority-import receipts and therefore has no
pretend FK; the guarded procedure and canonical decode prove it. The table is
immutable.

`dpone_open_stage_recovery_receipt_v3`

```text
recovery_receipt_id uuid NN PK
effect_key digest NN
recovery_effect_key digest NN UNIQUE
operation_key digest NN
old_artifact_set_digest digest NN
expected_operation_epoch revision NN
committed_operation_epoch revision NN CHECK expected + 1
expected_operation_projection_revision revision NN
committed_operation_projection_revision revision NN CHECK expected + 1
expected_writer_head_digest digest N
observed_writer_head_digest digest N
new_open_plan_set_digest digest NN
committed_at utc NN
committed_operation_state state_upper16 NN CHECK = 'ADMITTED'
canonical_receipt_payload payload NN
receipt_digest digest NN UNIQUE
UNIQUE(recovery_receipt_id,receipt_digest)
FK(operation_key,effect_key) -> writer_operation
```

The two writer-head digests are both NULL or both present and equal. Artifact
tuples are normalized only in the child table and are not duplicated as blob
columns. The uppercase state is an explicit compatibility literal of the
existing canonical receipt codec, not the lowercase lifecycle domain. The
table is immutable.

`dpone_open_stage_recovery_artifact_v3`

```text
recovery_receipt_id uuid NN FK recovery receipt
set_kind state16 NN CHECK IN ('abandoned','new')
ordinal int NN CHECK ordinal >= 0
artifact_id uuid NN FK artifact
PRIMARY KEY(recovery_receipt_id,set_kind,ordinal)
UNIQUE(recovery_receipt_id,artifact_id)
```

The guarded procedure proves both tuples nonempty, their ordinals contiguous,
their order byte-equal to the canonical receipt, and old/new sets disjoint and
complete. Cardinality equality is not assumed by SQL; the exact signed recovery
plan determines the successor set.

`dpone_effect_receipt_v3`

```text
receipt_id uuid NN PK
receipt_kind state16 NN
contract_version varchar(64) BIN2 NN CHECK = 'mssql_effect_receipt_v3'
contract_digest digest NN
operation_key digest NN
effect_key digest NN UNIQUE
physical_coordinate_digest digest NN
registered_physical_authority_digest digest NN
target_binding_uuid uuid NN
target_object_uuid uuid NN
recovery_identity_digest digest NN
writer_mode state16 NN
expected_writer_generation revision N
candidate_writer_generation revision NN
expected_head_revision revision N
candidate_head_revision revision NN
operation_epoch revision NN
operation_projection_revision revision NN
predecessor_receipt_id uuid N
predecessor_receipt_digest digest N
route_identity_sha256 digest NN
source_authority_sha256 digest NN
registration_id uuid NN
registration_payload_digest digest NN
verification_policy_digest digest NN
registration_admitted_at utc NN
sealed_request_digest digest NN
artifact_set_digest digest NN
mutation_plan_digest digest NN
generation_transition_digest digest NN
generation_authority_issuance_id uuid N
generation_authority_issuance_payload_digest digest N
generation_authority_verification_receipt_digest digest N
authority_set_digest digest NN
body_digest digest NN
revoked_override_id uuid N CHECK IS NULL
revoked_override_digest digest N CHECK IS NULL
committed_at utc NN
canonical_header_payload payload NN
canonical_receipt_payload payload NN
header_digest digest NN
receipt_digest digest NN UNIQUE
UNIQUE(receipt_id,receipt_digest)
UNIQUE(receipt_id,body_digest)
UNIQUE(target_binding_uuid,candidate_writer_generation,candidate_head_revision)
INDEX(predecessor_receipt_id,predecessor_receipt_digest)
INDEX(operation_key,operation_epoch,operation_projection_revision)
```

Composite FKs bind operation/effect, registration/binding/payload,
predecessor ID/digest and the optional issuance triple. Their referenced tables
expose matching composite UNIQUE keys. Expected generation/head and predecessor
ID/digest are all-or-none; a predecessor exists exactly for a successor.
Receipt kind equals writer mode. Candidate generation/head checks implement the
closed initial, Batch replacement and XMin same/new-generation transitions.
The issuance triple is all NULL exactly for the canonical empty authority set.

`dpone_batch_effect_receipt_v3`

```text
receipt_id uuid NN PK
source_snapshot_digest digest NN
source_schema_digest digest NN
manifest_bytes payload NN
manifest_digest digest NN
payload_row_count count64 NN
target_row_count_before count64 NN
target_row_count_after count64 NN
quality_bytes payload NN
quality_digest digest NN
canonical_body_payload payload NN
body_digest digest NN
FK(receipt_id,body_digest) -> effect_receipt(receipt_id,body_digest)
```

`dpone_xmin_effect_receipt_v3`

```text
receipt_id uuid NN PK
source_snapshot_digest digest NN
source_schema_digest digest NN
delta_manifest_bytes payload NN
delta_manifest_digest digest NN
complete_keys_manifest_bytes payload NN
complete_keys_manifest_digest digest NN
affected_count count64 NN
inserted_count count64 NN
updated_count count64 NN
deleted_count count64 NN
no_effect_count count64 NN
previous_checkpoint_payload payload N
previous_checkpoint_payload_digest digest N
previous_checkpoint_value count64 N
candidate_checkpoint_payload payload NN
candidate_checkpoint_payload_digest digest NN
candidate_checkpoint_value count64 NN
quality_bytes payload NN
quality_digest digest NN
canonical_body_payload payload NN
body_digest digest NN
FK(receipt_id,body_digest) -> effect_receipt(receipt_id,body_digest)
```

The checkpoint payload digests are derived SQL projections, not additional
canonical body fields. Every payload/digest pair is checked, prior checkpoint
payload/digest/value is all-or-none, and the candidate does not regress. Exactly
one body whose discriminator matches the header is required by candidate proof.

Receipt tables are immutable. To avoid cyclic FK insertion, the receipt first
references existing operation/sealed effect/registration/predecessor. Operation,
head, artifact, authority consumption and checkpoint then reference it inside
the same transaction; candidate proof is the final cross-row guard.

### Relational coordinate closure

Simple existence FKs never substitute for same-effect ownership. The
descriptor adds the matching composite UNIQUE/FK coordinates for:

```text
active registration head -> registration(binding,registration,payload)
active registration head -> control receipt(id,digest)
generation head -> active registration(binding,object,physical authority)
generation head -> effect receipt(id,digest)
writer operation -> UNIQUE(operation,effect)
writer operation -> committed effect receipt(id,digest)
sealed effect -> writer operation(operation,effect)
staging artifact -> writer operation(operation,effect)
staging chunk -> artifact/operation/effect/owner epoch
authority issuance -> registration(binding,registration,payload)
authority ref -> issuance(issuance,effect)
authority consumption -> effect receipt(id,digest)
checkpoint -> effect receipt(id,digest)
```

Where insertion order makes an immediate FK impossible, the final candidate
proof is the declared cross-row guard. In particular,
`dpone_target_row_hash_v3.last_receipt_id` has no immediate receipt FK because
row hashes are mutated before the receipt is inserted in the approved UoW;
candidate proof must bind every changed hash row to the just-built effect.
Changing that order requires a separate contract amendment.

### Row hash and XMin checkpoint

`dpone_target_row_hash_v3`

```text
target_binding_uuid uuid NN
target_generation revision NN
normalized_key_digest digest NN
canonical_key_payload payload NN
canonical_row_hash digest NN
hash_policy_digest digest NN
last_effect_key digest NN
last_receipt_id uuid NN
updated_at utc NN
PRIMARY KEY(target_binding_uuid,target_generation,normalized_key_digest)
```

Exact key bytes are compared on every digest hit. Only the fenced effect may
insert/update/delete rows. The table CHECK also requires
`normalized_key_digest = HASHBYTES('SHA2_256', canonical_key_payload)`.

`dpone_xmin_checkpoint_v3`

```text
state_key_digest digest NN
writer_generation revision NN
checkpoint_revision revision NN
target_binding_uuid uuid NN
checkpoint_payload payload NN
checkpoint_payload_digest digest NN
checkpoint_value bigint NN CHECK checkpoint_value >= 0
previous_writer_generation revision N
previous_checkpoint_revision revision N
previous_checkpoint_payload payload N
previous_checkpoint_payload_digest digest N
source_snapshot_digest digest NN
source_authority_sha256 digest NN
consuming_receipt_id uuid NN UNIQUE FK effect receipt
committed_at utc NN
PRIMARY KEY(state_key_digest,writer_generation,checkpoint_revision)
```

Predecessor fields, including `previous_checkpoint_payload_digest`, are all NULL
or all present. Both checkpoint payload digests are checked with `HASHBYTES`.
The table is append-only and immutable.

## Protected properties and dynamic stages

Authority schema properties:

```text
dpone.r1.v3.schema_contract_version
dpone.r1.v3.schema_contract_digest
dpone.r1.v3.object_inventory_digest
dpone.r1.v3.permission_contract_digest
dpone.r1.v3.supported_codec_set_digest
dpone.r1.v3.projection_revision
```

Registered target properties:

```text
dpone.r1.v3.target_binding_uuid
dpone.r1.v3.target_object_uuid
dpone.r1.v3.physical_generation_uuid
dpone.r1.v3.registration_id
dpone.r1.v3.registration_payload_digest
dpone.r1.v3.registered_physical_authority_digest
dpone.r1.v3.recovery_domain_uuid
dpone.r1.v3.recovery_domain_epoch
dpone.r1.v3.catalog_contract_digest
dpone.r1.v3.target_contract_revision
```

Dynamic stage properties add staging object UUID/token, operation/effect,
artifact kind, open-plan/stage-DDL/schema/catalog/permission/type-policy digests,
and manifest digest after seal. Dynamic stages are not static inventory entries;
their exact table/index/property/grant projection is proved by the sealed
manifest. Any unregistered `dpone_stage.a_*` object blocks admission.

## Stored procedure boundary

There are no views or universal execution procedures. Each procedure is
renderer-owned, uses `SET NOCOUNT ON`, validates the active session identity,
and returns the exact result contract from the normative descriptor. All 28
non-scan procedures return exactly one row in exactly one result set, including
an explicit `absent|known_not_committed|blocked|unknown` discriminator for
fresh probes. A zero-row result is never used to encode absence. Only
`dpone_scan_stage_v3` returns `0..N` rows in exactly one result set.
`dpone_scan_stage_v3` uses `stage_scan_template`: business columns are
instantiated from retained `R1OpenStagePlanV1`, followed by a fixed system
suffix. The adapter normalizes `cursor.description` into the schema-2 typed
result-column model and byte-compares that canonical model with the instantiated
template before reading a row; collation and other catalog-only facts are
proved separately from the retained stage inventory. Raw driver tuples are
never canonical authority. Extra result sets, row-count messages and output
parameters are forbidden.

The exact shared inventory is 29 procedures:

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

Each active target binding additionally has exactly six generated, closed
modules whose names use the lowercase, hyphen-free binding UUID:

```text
dpone_b_<binding_uuid_hex>_batch_mutate_v3
dpone_b_<binding_uuid_hex>_batch_row_hash_v3
dpone_b_<binding_uuid_hex>_batch_quality_v3
dpone_b_<binding_uuid_hex>_xmin_mutate_v3
dpone_b_<binding_uuid_hex>_xmin_row_hash_v3
dpone_b_<binding_uuid_hex>_xmin_quality_v3
```

They are one `MssqlR1BindingModulePackV1`, generated from the sealed target
mapping and bound into registration/catalog authority by pack and live-identity
digests. Registered target identifiers are compiled into the definitions;
dynamic stage identifiers are never compiler inputs and callers cannot submit
identifiers or arbitrary SQL. Each module materializes the instantiated
`dpone_scan_stage_v3` result into a module-local table variable whose shape is
the exact business projection plus fixed system suffix. A binding-specific
no-login certificate user grants the six signed modules only the exact target
DML and row-hash DML required by that binding; it has no direct stage `SELECT`.
Stage access remains exclusively inside the shared signed scan procedure.
Runtime receives only object-level EXECUTE. Partial, extra, unknown or
coherently replaced packs block activation and replay. Direct runtime target
DML and a universal dynamic-SQL module are forbidden.

`MssqlR1BindingModulePackV1` is an explicit closed contract, not a list of
procedure names. For each of the six ordered modules it contains binding UUID,
module kind, exact schema/name, ordered parameter descriptor, exactly-one-row
result descriptor, read/write object set, signer profile, definition bytes and
definition/live-identity digests. All six share one target mapping digest,
type-policy digest, permission digest and pack digest. Unknown fields, duplicate
kinds, a missing kind or a module whose definition does not reproduce its
descriptor reject the pack.

The two mutate modules return exact affected-key/row/log-budget counters; the
two row-hash modules return exact inserted/updated/deleted sidecar counters and
the candidate hash-generation digest. Batch quality returns, in order:

```text
staged_rows
typed_scan_rows
candidate_target_rows
candidate_sidecar_rows
non_null_keys
unique_keys
canonical_reencode_matches
sidecar_hash_matches
```

XMin quality returns, in order:

```text
affected_key_count
expected_present_keys
observed_target_present_keys
observed_sidecar_present_keys
expected_absent_keys
observed_target_absent_keys
observed_sidecar_absent_keys
survivor_hash_matches
complete_key_count
target_row_count_after
target_row_count_before
inserted_count
deleted_count
delta_no_effect_count
unchanged_row_count
delta_keys_not_in_complete
target_keys_missing_from_complete
complete_keys_missing_from_target
checkpoint_predecessor_matches
```

All counters are nonnegative SQL bigint; the final XMin field is `bit`. The
descriptor freezes exact types/nullability and the Python result models. The
binding modules receive only transaction/request digests and closed scalar
coordinates; identifiers and statement bytes are never caller parameters.

Schema-2 also closes the old retained-statement ambiguity. Target DML, row-hash
and quality steps are implemented only by these signed binding modules, and
XMin checkpoint append only by the shared
`dpone_write_xmin_checkpoint_v3` procedure. Existing
`MssqlR1RenderedMutationBundleV1` statement execution is a schema-1
compatibility prototype and is activation-blocked for schema-2. A schema-2
sealed request instead binds the exact binding-pack digest and shared-procedure
descriptor digest; it cannot carry executable SQL bytes. This contract change
must be implemented and migrated in the scoped physical-provider release before
activation, with no dual execution path or silent fallback.

### Parameter convention

Every mutating procedure starts with:

```text
@transaction_id uniqueidentifier,
@request_payload varbinary(max),
@request_digest binary(32),
@projection_json nvarchar(max)
```

Fresh probes omit transaction ID and use exact effect/control/registration key
plus expected physical/binding digests. Stage chunk procedures additionally
take `@artifact_id uniqueidentifier`, `@owner_epoch bigint`,
`@chunk_sequence int`, `@chunk_key binary(32)`, exact logical/payload/wire
digests and counts, `@load_token uniqueidentifier` and expected artifact
revision. The descriptor—not this abbreviated convention—is the only signature
authority.

The 28 non-scan shared procedures use a fixed-result contract whose first five
columns are this exact prefix:

```text
result_contract_version varchar(64) BIN2
outcome state16
request_digest digest
projection_revision revision
server_observed_at utc
```

Each fixed result then appends its procedure-specific closed projection.
`dpone_scan_stage_v3` is the sole exception: it uses the separately versioned
`stage_scan_template`, returns `0..N` rows, and does not pretend to carry this
fixed-result prefix. Mutating procedures return the committed candidate
projection even when the mutation is an idempotent replay. The Python decoder
rejects missing, extra or reordered columns, NULL-shape drift, raw enums, extra
rows and extra result sets before the result can become authority.

`projection_json` is an untrusted, versioned ASCII-key JSON projection used for
typed SQL checks and indexes. It has duplicate-key rejection, closed field set,
exact JSON scalar types and canonical lowercase hex/UUID/time grammar. Canonical
payload bytes remain authority. The candidate/fresh Python decoder re-reads and
compares every relational/JSON projection with canonical bytes before commit or
success. A projection mismatch always rolls back/returns UNKNOWN; it never
repairs itself.

Before approval this specification must include a reviewed machine-readable
`MssqlR1PhysicalSchemaDescriptorV1` with version
`dpone-mssql-r1-v3-physical-schema-2-r2`. It is a closed bundle containing engine
and session profiles, exactly two schemas, 20 tables, 29 shared procedures, the
six-module binding template, permission/signer/codec/property rules, migration
probes, lock profiles and the error registry. Every table entry freezes exact
ordered catalog column metadata, explicit absence of defaults, deterministic
constraints/FK actions/indexes/triggers/properties, lifecycle, mutation policy,
lock coordinate and canonical DDL digest. Every procedure/module entry freezes
ordered parameter metadata, result cardinality plus fixed/template columns,
closed projection JSON grammar, read/write set, lock plan, guarded reads,
state/CAS transition, idempotency and replay comparator, execute roles, signer,
canonical definition bytes/digest and every stable error condition.

The descriptor rejects unknown keys, wrong counts, case-folded duplicates,
unresolved references, missing definition bytes/digests and drift from schema-2
typed models. SQL text is UTF-8 with LF endings, one renderer-owned statement
per ordered entry and no locale/hash-seed dependence. Renderer DDL, portable
inventory, report schemas and golden fixtures are generated from the descriptor;
hand-written divergence and dataclass reflection as an implicit field mapping
are forbidden.

The stage descriptor freezes these minimum result authorities:

| Procedure | Exact semantic result |
|---|---|
| `dpone_open_stage_v3` | `opened|replayed`, exact artifact/object/operation/binding/epoch, OPEN state, DDL/open-plan digests, lease revision/deadlines and row token |
| `dpone_begin_stage_chunk_v3` | `created|existing_loading|existing_complete`, exact chunk request/key/digests/counts/load token, stage physical identity, epoch/lease/revision and row token |
| `dpone_renew_stage_v3` | `renewed|replayed`, active chunk, previous/committed adjacent lease revisions, exact request digest and deadlines |
| `dpone_complete_stage_chunk_v3` | `completed|replayed`, exact chunk identity, declared/observed count, ordinal minimum/maximum, completion time/revision/token |
| `dpone_observe_stage_chunk_v3` | Complete relational chunk projection plus exact canonical request payload |
| `dpone_scan_stage_v3` | Typed business projection followed by canonical key/row/hash and exact artifact/chunk/load-token/row-ordinal suffix |
| `dpone_seal_stage_v3` | `sealed|replayed`, exact manifest and completed chunk-set count/digest, seal/retention/revision/token |

`existing_loading` is observation, not restart authorization. Only the same
uninterrupted call stack that proves BCP/reference loading has not started may
continue; a new process waits for expiry recovery. For a future BCP adapter,
loader renewal runs on a fresh control connection before one-third of the lease
remains. An unproved renewal aborts/reaps BCP and forbids chunk completion.

Seal uses physical→binding→operation→ordered-artifact locks, revokes loader
INSERT, acquires `TABLOCKX,HOLDLOCK`, proves zero effective loader DML paths and
zero LOADING chunks, proves sequences `0..N-1`, exact row ordinals and chunk
digests, then persists the manifest and transitions OPEN→SEALED atomically.
An empty artifact has zero chunks and the canonical empty chunk-set digest.

### Stable SQL errors

Renderer-owned `THROW` numbers are fixed by category:

```text
51001 schema_or_codec_mismatch
51002 session_identity_mismatch
51003 lock_timeout_or_deadlock_retryable
51004 physical_or_registration_mismatch
51005 operation_or_epoch_conflict
51006 stage_or_chunk_conflict
51007 authority_resolution_conflict
51008 receipt_or_candidate_conflict
51009 checkpoint_conflict
51010 permission_contract_mismatch
51011 migration_blocked
51012 resource_limit_exceeded
```

The stage error states under `51006` are fixed as:

```text
1 stage_identity_conflict                 blocked
2 durable_planned_state_observed         unknown
3 stage_lease_lost                       known_not_committed
4 chunk_identity_conflict                blocked
5 chunk_sequence_gap                     blocked
6 chunk_incomplete                       known_not_committed/open_recovery
7 chunk_row_coverage_mismatch             blocked/open_recovery
8 loading_chunk_exists                   blocked
9 stage_chunk_set_mismatch                blocked
10 dynamic_stage_drift                   unknown
```

Message text is non-authoritative and redacted. Deadlock 1205 and lock timeout
1222 are normalized to 51003 only when transaction outcome is positively known
not committed; otherwise recovery is UNKNOWN.

The normative descriptor assigns each procedure condition an exact
`(error_number,error_state)` and records:

```yaml
reason_code:
outcome_class: known_not_committed | committed | unknown
retry_class: immediate | bounded_backoff | sealed_takeover | blocked
fresh_probe: none | control | stage_open | stage_chunk | stage_seal |
             open_recovery | effect
public_blocker:
redaction_class:
```

Native duplicate-key, truncation, conversion/overflow, permission, JSON,
deadlock and timeout errors are not allowed to leak as an alternate contract;
the per-procedure matrix either normalizes them inside a known transaction
outcome or classifies the result UNKNOWN and requires its named fresh probe.

## Lock, session and transaction rules

Applock resources and order:

```text
dpone:r1:v3:schema:<lowercase-database-guid>
dpone:r1:v3:physical:<lowercase-64hex>
dpone:r1:v3:binding:<lowercase-uuid>
dpone:r1:v3:operation:<lowercase-64hex-effect-key>
dpone:r1:v3:artifact:<01|02|03>:<lowercase-uuid>
dpone:r1:v3:registration:<lowercase-uuid>
dpone:r1:v3:authority:<01|02|03>:<lowercase-uuid>
```

Artifact ranks are Batch payload, XMin delta, XMin complete keys. Authority
ranks are initial cutover, rebaseline, empty refresh. Each uses Exclusive,
Transaction owner and the environment's bounded positive lock timeout, followed
by `UPDLOCK,HOLDLOCK` row reads.

Every fresh probe owns a short SERIALIZABLE transaction, acquires the same
ordered subset before any guarded read, and has descriptor-bounded statement,
lock and descendant-receipt traversal limits. Receipt absence outside this
coherent locked snapshot is never positive known-not-committed evidence.

Every UoW receives a fresh non-pooled physical connection with MARS disabled:

```text
disable pyodbc pooling before the first R1 connection
connect with autocommit=true and MARS_Connection=No
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET QUOTED_IDENTIFIER ON
SET NUMERIC_ROUNDABORT OFF
SET XACT_ABORT ON
SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
BEGIN TRANSACTION
set read-only SESSION_CONTEXT transaction UUID
require @@TRANCOUNT=1 and XACT_STATE()=1 before every effect statement
require stable @@SPID, DB_ID(), database/family/recovery GUIDs
COMMIT TRANSACTION WITH (DELAYED_DURABILITY = OFF)
```

Because `pyodbc.pooling=False` is process-global and only effective before the
first connection, the certified profile runs in a dedicated process whose
composition root disables pooling before importing/constructing any ODBC
client. Failure to prove that process precondition blocks activation.

The Python factory also binds handle object identity and a random 128-bit token.
After commit dispatch the wrapper permits no SQL/rollback. It is discarded even
if close fails; a different connection performs fresh classification.

## Permissions

Environment authority supplies exactly four distinct contained database SQL
users. The schema contract binds symbolic roles; fresh observation binds their
IDs, names and database SID digests. Their server SID is NULL and their allowed
database-role set is empty. Any role/public-derived permission path in the
closed managed projection blocks rather than being normalized into a valid
attestation.

- Provisioner: install/alter exact V3 objects; execute provision/rotate/import/
  attest; attach protected properties; manage exact target/stage grants.
- Runtime: execute only named shared procedures and the six exact signed
  binding modules;
  it has no direct authority-table or stage DDL/grant right.
- Loader: temporary object-level INSERT on exactly one admitted LOADING chunk,
  granted by begin and revoked before complete/recovery/seal.
- Observer: execute only status/fresh-proof/attestation procedures.
- Stage-owner certificate user without login: `CREATE TABLE`, `ALTER` on
  `dpone_stage`, and stage-schema DML with grant option, available only through
  the exact signed stage procedure set.
- Attestation certificate user without login: metadata visibility required by
  the exact attestation procedure, available only through that signed module.
- Binding certificate user without login: exact target-object and row-hash DML
  required by one binding, available only through its six signed modules; no
  dynamic-stage permission.

Forbidden for these principals, including through roles/public:

```text
EXECUTE ON SCHEMA
direct INSERT/UPDATE/DELETE on dpone_authority tables
ALTER, CONTROL, TAKE OWNERSHIP or grant option for runtime/loader/observer
db_owner or db_ddladmin membership
V1/V2 writer procedure execution
```

The schema-2 execute matrix is closed. No procedure omitted from a principal's
set is executable by that principal, directly or through membership:

| Principal | Exact shared procedures |
|---|---|
| Provisioner | `provision_registration`, `rotate_registration`, `import_generation_authority_set`, `probe_control_effect`, `attest_schema`, `probe_registration` |
| Runtime | `probe_control_effect`, `admit_operation`, `seal_effect`, `take_over_sealed`, `probe_pre_source`, `open_stage`, `renew_stage`, `begin_stage_chunk`, `complete_stage_chunk`, `observe_stage`, `observe_stage_chunk`, `scan_stage`, `seal_stage`, `recover_expired_open`, `probe_open_recovery`, `admit_writer`, `resolve_admit_authority_set`, `append_effect_receipt`, `write_xmin_checkpoint`, `consume_stage_set`, `consume_authority_set`, `advance_operation_and_head`, `prove_candidate_effect`, `probe_effect`, `probe_registration` |
| Loader | none; only the temporary exact stage-table `INSERT` grant for one LOADING chunk |
| Observer | `probe_control_effect`, `attest_schema`, `probe_pre_source`, `observe_stage`, `observe_stage_chunk`, `probe_open_recovery`, `probe_effect`, `probe_registration` |

Names in this table have the fixed `dpone_` prefix and `_v3` suffix shown in
the inventory above. Runtime alone executes the six exact binding modules.
Provisioner alone owns installation/signing operations; certificate users are
never login principals and receive no EXECUTE grant.

The provisioner alone has the exact DDL/certificate/signing/grant authority
needed by installation. Procedures execute as caller. The installer creates
versioned certificates/users, signs the closed procedure sets, removes every
certificate private key, and attests signatures before commit. Runtime never
inherits the certificate-user permissions outside a signed module.

The descriptor enumerates every provisioner/runtime/loader/observer EXECUTE
grant, certificate-user permission and forbidden database-level
privilege-expansion permission. Exact certificate/user names, algorithm,
ephemeral key-password handling, thumbprint binding, private-key removal and
reinstall behavior are descriptor fields. Exact reinstall never ALTERs a signed
module. The provisioner verifies the attestor module definition and signature
through direct catalog reads before using that module's result.

This boundary follows SQL Server's documented module-signing behavior:
altering a module drops its signature and signature metadata is exposed through
`sys.crypt_properties` ([Microsoft `ADD SIGNATURE`](https://learn.microsoft.com/en-us/sql/t-sql/statements/add-signature-transact-sql?view=sql-server-ver17),
[`sys.crypt_properties`](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-crypt-properties-transact-sql?view=sql-server-ver17),
verified 2026-09-05).

Static schema permissions and dynamic stage grants have separate digests so
opening an artifact does not change the global contract.

Certificate profile `stage_owner` grants its no-login certificate user:

```text
CREATE TABLE
ALTER ON SCHEMA::dpone_stage
SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dpone_stage WITH GRANT OPTION
```

It signs only `dpone_open_stage_v3`, `dpone_begin_stage_chunk_v3`,
`dpone_complete_stage_chunk_v3`, `dpone_observe_stage_v3`,
`dpone_scan_stage_v3`, `dpone_seal_stage_v3`,
`dpone_recover_expired_open_v3` and `dpone_consume_stage_set_v3`.
Certificate profile `attestor` receives only the closed metadata-visibility
permissions and signs only `dpone_attest_schema_v3`. Altering a signed module
invalidates the signature and therefore fails fresh admission.

## Attestation

`MssqlR1SchemaContractV3` is portable and excludes environment object IDs.
`MssqlR1SchemaAttestationV3` adds live IDs and physical identity. Catalog sources
are closed to:

```text
sys.schemas
sys.tables, sys.columns, sys.types, sys.computed_columns, sys.default_constraints
sys.key_constraints, sys.foreign_keys, sys.foreign_key_columns, sys.check_constraints
sys.indexes, sys.index_columns
sys.triggers, sys.sql_modules
sys.procedures, sys.parameters, sys.dm_exec_describe_first_result_set_for_object
sys.extended_properties
sys.database_principals, sys.database_role_members, sys.database_permissions
sys.certificates, sys.crypt_properties, sys.symmetric_keys
```

`sys.server_*` is intentionally outside the contained-user GA1 oracle. The
signed registration's server identity is still compared with a versioned,
descriptor-owned `SERVERPROPERTY` observation codec, but this is identity
binding, not a claim that a database module can prove absence of hostile
server-level administrators.

Renderer-owned canonicalization orders semantic coordinates, not object IDs.
Encrypted/unreadable modules, extra/missing objects, altered type/collation,
untrusted/disabled constraints/triggers, owner changes, index/filter/order
changes, parameter/result/SET-option/execution-context/signature changes,
certificate private-key retention, property drift, inherited broad/legacy
grants and unknown codecs all fail attestation.

## Detailed algorithms

### Install/provision

```text
verify detached registration outside SQL
→ resolve MssqlR1PrincipalAuthoritySetV3 from the environment registry
→ build exact portable schema and six-module binding pack
→ reject every cross-input mismatch
→ fresh provisioner session
→ acquire schema, physical, binding locks
→ prove exact V3 inventory is absent or already exact schema-2
→ block every pre-amendment/partial/unknown inventory for manual disposition
→ install exact shared objects, mutation policies and module signatures
→ build typed fresh schema attestation from verification + principal authority
→ persist the first immutable live-identity baseline
→ install and attest the exact six-module binding pack
→ insert registration
→ insert uninitialized writer head
→ append control receipt
→ insert active registration head referencing that receipt
→ attach target properties
→ remove certificate private keys
→ repeat full shared-schema and binding-pack attestation
→ re-prove control receipt and complete candidate
→ delayed-durability-off commit
```

Exact reinstall returns the existing proof. Same control key with different
payload or any partial state blocks.

### Stage lifecycle

```text
admit operation
→ register PLANNED row before DDL
→ in the same transaction create exact private stage/properties
→ PLANNED→OPEN without a loader DML grant
→ begin exact chunk: persist LOADING authority + grant object-local INSERT
→ load one declared chunk on the separate loader connection
→ revoke INSERT + TABLOCKX + prove row coverage + LOADING→COMPLETE
→ repeat adjacent chunks; at most one LOADING chunk exists
→ revoke/re-prove loader denial + TABLOCKX
→ prove chunk prefix, per-row coordinates and whole artifact digest
→ scan exact typed/canonical rows
→ OPEN→SEALED with manifest/chunk-set digest and retention
→ seal exact effect request
```

Expired OPEN recovery atomically abandons the complete old set, records one
recovery receipt, increments operation epoch/revision and creates disjoint
replacement IDs. Each replacement plan is decoded and validated first, then its
artifact row, physical table, properties and initial lease are created and
transitioned through transaction-local PLANNED to durable OPEN inside that same
recovery transaction. The recovery receipt child rows reference only those
durable OPEN replacements; no FK ever targets a separately committed PLANNED
row. If any replacement DDL or attestation fails, old artifacts, operation,
receipt and all replacements roll back together. Incomplete/ambiguous state
blocks. No physical table is automatically dropped in schema revision 2.

R1 correctness uses a bounded transactional ODBC reference loader. The BCP
subprocess adapter remains activation-blocked until R2A certifies the exact
wire contract. For that future adapter, begin/control, BCP and complete are
three independently committed boundaries: BCP exit/count is never authority,
failed or unknown renewal aborts and reaps the loader, and a process restart
never resumes a LOADING chunk in place.

### Effect UoW

```text
fresh runtime session and exact transaction binding
→ physical/binding/operation/artifact/registration/authority locks
→ exact preexisting receipt probe
→ re-prove registration, schema and sealed stages
→ target-local resolve/admit exact issuance set using server time
→ execute the exact binding mutate module
→ execute the exact binding row-hash module
→ execute the exact binding bounded-quality module
→ append one typed effect receipt/body
→ append XMin checkpoint when applicable
→ atomically consume stage set and authority issuance
→ advance operation and writer head
→ full candidate read-back
→ proof.assert_for(binding, attempt, receipt)
→ reassert ACTIVE session
→ fully durable commit
```

Any failure before commit rolls back all effects. Post-dispatch exception closes
the wrapper and uses `dpone_probe_effect_v3` on a fresh connection.

### Replay truth table

| Fresh observation | Result |
|---|---|
| Exact receipt/body + committed operation + descendant head + exact consumed resources/checkpoint | COMMITTED; suppress source and DML |
| Receipt absent + exact unchanged SEALED operation/predecessor head + resources ISSUED/SEALED + no checkpoint | KNOWN_NOT_COMMITTED; retry same sealed attempt |
| Any partial, conflicting, unreadable, changed recovery domain or unproved absence | UNKNOWN; operator recovery |

## Alternatives and tradeoffs

| Alternative | Advantage | Risk | Decision |
|---|---|---|---|
| Central control database | One catalog | Reintroduces cross-database atomicity/DTC | Rejected |
| Direct authority-table DML from runtime | Less SQL | Permission and invariant surface too broad | Rejected |
| Schema-wide EXECUTE | Simple grants | Makes future/unreviewed procedures callable | Rejected |
| One row per consumed authority ref | Simple row update | Partial two-ref consumption possible | Rejected |
| One consumption row per issuance | Atomic set lifecycle | Extra table/join | Adopted |
| Automatic stage drop/TTL | Saves space | Destroys ambiguous/replay evidence | Deferred |
| Pooled connection | Throughput | SESSION_CONTEXT/handle reuse ambiguity | Rejected for R1 |
| Projection JSON plus canonical bytes | Small stable procedure signatures | Requires closed JSON and final Python reproof | Adopted |

### ADR requirement

ADR 0056 already owns same-database target authority. Its amendment must record
the closed descriptor, 20-table inventory, one-row issuance consumption,
explicit verification/principal schema-authority inputs, immutable identity
baseline, 29 shared plus six per-binding signed modules, contained-user threat
model, crash-safe chunk protocol, non-pooled session and fresh-probe lock rules
before implementation.

### Quality-budget impact

No production module may exceed the repository's current 400-SLOC hard limit.
Schema, permissions, attestation, session/locks, control, stage and effect
backends are separate cohesive modules. Only the integrator changes shared
exports/composition. The experimental 393-line staging renderer is replaced,
not grown or edited concurrently.

## Market comparison

This internal physical-authority slice adds no new competitive claim. The
approved parent design contains the current product comparison. dlt,
Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and Beam
are `N/A` here because their public contracts do not define dpone's internal
target-local receipt schema. No vendor pattern is inferred from absence.

## Measurable differentiation

```yaml
axis: target-local atomic recovery authority
scenario: failure after every R1 target UoW boundary
baseline: current experimental V3 providers with abstract/fake backends
metric:
  partial_committed_effects: count
  repeated_source_reads_after_seal: count
  accepted_catalog_or_permission_tamper: count
target:
  partial_committed_effects: 0
  repeated_source_reads_after_seal: 0
  accepted_catalog_or_permission_tamper: 0
procedure: hermetic model suite plus approved live SQL Server fault campaign
artifact: test_artifacts/route_live_wide/postgres-mssql/dpone-route-live-certification/postgres_mssql_r1_certification.json
limitations: one PG16 Batch/XMin relation into standalone SQL Server 2022 same database
```

## Security, privacy, and operations

No credentials, payload bodies, physical GUIDs or object names enter user
output. SQL identifiers are closed, quoted by the renderer and rejected before
SQL on malformed/reserved/oversize input. Parameterized calls are mandatory.

Capacity admission covers authority/stage/receipt/hash/checkpoint bytes, target
data/log headroom and retained ambiguous artifacts. Exhaustion pauses admission;
it never deletes recovery evidence. Alerts cover inventory/permission drift,
lock timeout, retained OPEN/SEALED age, unknown outcome and authority expiry.

## Test and certification plan

| Layer | Scenario | Environment | Expected artifact |
|---|---|---|---|
| Unit | Byte-identical DDL, identifiers, catalog canonicalization, every field mutation | Hermetic | Focused pytest |
| Contract/model | State adjacency, idempotency, locks, replay truth table, migration matrix | SQL recording fakes | Contract report |
| Integration | Install/reinstall/rollback, permissions, stages, complete UoW | Approved local SQL Server | Integration result, not certification |
| Route live | Real PG16 snapshot through production V3 composition | Approved PG/MSSQL | Route result |
| Certification | Closed L01–L18/C01–C04 matrix, exact build/dependencies | Approved vendor environment | Create-only JSON/JUnit |
| Compatibility | Fixed V1/V2/pre-amendment V3 fixtures | Hermetic + live migration | Compatibility evidence |

Mandatory hermetic cases:

```text
H01 repeated render is byte-identical
H02 every catalog dimension mutation changes digest or is rejected
H03 identifier injection/reserved/oversize is rejected
H04 SQL integer/time/digest/payload bounds are exact
H05 V1/V2/pre-amendment V3 goldens never decode as this revision
H06 extra/missing/reordered/duplicate semantic inventory is rejected
H07 lock resources and total order match golden vectors
H08 every valid and invalid state transition is model-tested
H09 fault after every UoW step leaves no partial state
H10 exact duplicate calls replay; conflicting same-key calls block
H11 replay union never guesses ambiguous state
H12 every schema-1/unversioned/pre-amendment V3 inventory blocks automatic upgrade
H13 descriptor closure is exactly 20 tables, 29 shared procedures and six binding modules
H14 every descriptor leaf and procedure condition belongs to the mutation/case registry
H15 valid portable permissions/triggers survive arbitrary live catalog-ID ordering
H16 PLANNED, partial chunk rows, LOADING restart and loader-grant races never seal
H17 coherent schema/binding certificate replacement fails retained-baseline comparison
```

Mandatory live SQL Server cases:

```text
L01 fresh install and exact reinstall
L02 fault after every DDL/grant/property statement
L03 concurrent installers and installer/runtime serialization
L04 catalog/constraint/index/trigger/procedure/property tamper
L05 handle/SPID/DB/session-context/MARS/TRANCOUNT/XACT_STATE negatives
L06 lock timeout/deadlock and independent-target concurrency
L07 direct/inherited permission negatives for all principals
L08 OPEN/load/renew/seal/consume/recovery including two-artifact XMin
L09 authority empty/one/two, zero/multiple/expired/wrong-set resolution
L10 atomic Batch and XMin I/U/D/no-op
L11 fault at every effect-UoW boundary
L12 candidate partial/wrong read-back rejection
L13 lost commit before/after durable commit
L14 descendant replay
L15 partial/corrupt/copied receipt/resource state
L16 full V1/V2/pre-amendment V3 diagnosis and no-auto-migration matrix
L17 transactional grant/schema fresh-install failure
L18 restore/clone/recovery-fork/object-ABA rejection
C01 real route composition with no connector doubles
C02 exact reviewed scenario registry, zero missing/SKIP/N/A
C03 stale/foreign dependency or SQL build remains UNVERIFIED
C04 committed replay and sealed resume perform zero source rereads
```

Mocks never populate live evidence. Unavailable live infrastructure remains
`SKIP/UNVERIFIED`, not PASS.

## Documentation plan

On implementation update the R1 first-success guide, target permissions guide,
status/error reference, migration and recovery runbooks, architecture diagram,
capability matrix, CJM and generated schema/procedure reference. Before
implementation this file and ADR are the only documentation changes.

## Rollout and rollback

1. Keep activation blocked.
2. Complete schema-2 contracts/report and freeze the reviewed physical
   descriptor plus ADR amendment.
3. Mark this specification `APPROVED`; only then implement renderer/attestor,
   session/locks, control, stage and effect providers in separate tasks.
4. Compose those providers with the already integrated abstract runtime.
5. Run local integration without certifying.
6. Run exact vendor-live campaign; missing cases remain UNVERIFIED.
7. Enable explicit opt-in only for the exact passing tuple.
8. Default activation is a later release decision.

Rollback may remove only objects created by the current failed fresh-install
transaction. No existing V3 inventory is automatically dropped or altered.
After a V3 receipt, preserve the schema/receipts and use a compatible reader or
roll forward. Durable effects are never undone by deleting evidence.

## Agent execution plan

| Role | Owned paths | Read-only paths | Forbidden paths | Dependency |
|---|---|---|---|---|
| Descriptor/integrator | Spec/ADR, closed descriptor, shared ports/exports/composition/docs/changelog | All R1 | Unrelated connectors | Schema-2 contracts + reviewed descriptor |
| Schema renderer/attestor | New schema/permission/attestation modules and focused tests | Pure V3 contracts | Session/backends/composition | Approved spec |
| Session/locks implementer | New ODBC session and lock modules | Schema + transaction ports | Control/effect/composition | Schema contracts |
| Control implementer | Registration/authority/control replay backends | Schema/session/pure contracts | Stage/effect/composition | Schema + session |
| Stage implementer | Stage control/recovery and reference-loader runtime | Schema/session/contracts | Control/effect/composition | Schema + session |
| R2A bulk-loader implementer | BCP subprocess and exact wire integration | Approved stage protocol | Shared descriptor/control/effect | R2A wire certification |
| Effect implementer | Fence/receipt/resource/fresh replay backends | All provider ports | App/shared docs | Schema + session |
| Test certifier | Model/live harness and evidence producer | Integrated code | Production policy | Integrated commit |
| Docs/UX reviewer | Guides/runbooks/CJM draft | Exact behavior | Generated evidence | Integrated behavior |

Every writer gets a separate validated task contract and worktree. Only the
integrator changes shared semantic files.

## Approval checklist

- [x] User problem and CJM are clear.
- [ ] Exact procedure parameter/result contract is generated and reviewed.
- [x] Pre-amendment V3 migration is fail-closed and never automatic.
- [x] Portable/observed schema-2 amendment is approved and implemented.
- [ ] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market scope is recorded without an unsupported claim.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are defined.
- [x] Path ownership and integration plan are conflict-safe.
- [ ] ADR 0056 physical-schema amendment is accepted.
- [ ] Maintainer changed status to `APPROVED`.
