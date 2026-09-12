# Feature design: PostgreSQL → MSSQL R1 V3 canonical type and registered-target authority

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

- Status: APPROVED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-05
- Parent: [Provider implementation map](developer-postgres-mssql-r1-v3-provider-implementation.md)
- Upstream physical descriptor R2 implementation:
  `source record 052`
- Upstream provider security V2 implementation:
  `source record 001`
- Upstream registration contract implementation:
  `source record 053`
- Upstream registration provisioner/transaction-port implementation:
  `source record 141`
- Required SQL Server registration persistence/probe implementation:
  `UNIMPLEMENTED`
- Approval review basis: exact commit
  `5beba5f39` approved by fresh architecture and test/certification reviews;
  live behavior remains `UNVERIFIED`.
- Compatibility reference only:
  `PostgresMssqlTypeMapper` at
  `source record 031`

## Outcome and boundary

This internal child closes two missing authorities required before binding V2:

```text
canonical PostgreSQL scalar shape
  -> exact lossless PostgreSQL-to-MSSQL decision
  -> exact target scalar shape

verified target registration
  -> rotation-stable physical target identity
  -> exact target catalog and columns
  -> registered-target column references
```

The current `PostgresMssqlTypeMapper` remains the compatibility planner for
existing routes. It is not renamed or reinterpreted as canonical authority.
The current runtime target-catalog snapshot remains an adapter/readiness model;
it is not imported into `dpone.contracts`.

This child changes no manifest, CLI, public Python API or activation status. It
contains no SQL, I/O, credential, renderer, installer or target mutation.
Its intended audience is the provider-contract implementer, architecture
reviewer and test certifier; data-product authors use the public R1 guide.

## Scope

### Closed R1 enums

Enum declaration order below is canonical:

```text
PostgresMssqlSourceScalarFamilyV1:
  bool | int2 | int4 | int8 | numeric | float4 | float8 | uuid |
  date | time | timestamp | timestamptz | text | varchar | bytea

MssqlR1TargetScalarFamilyV1:
  bit | smallint | int | bigint | decimal | real | float_53 |
  uniqueidentifier | date | time | datetime2 | datetimeoffset |
  nvarchar | varbinary

PostgresMssqlLengthKindV1:
  not_applicable | bounded | maximum

PostgresMssqlCodecV1:
  bool_ascii_v1 | signed_integer_ascii_v1 | decimal_fixed_ascii_v1 |
  ryu_binary32_shortest_ascii_v1 | ryu_binary64_shortest_ascii_v1 |
  uuid_lower_ascii_v1 |
  iso_date_ascii_v1 | iso_time_ascii_v1 | iso_timestamp_ascii_v1 |
  iso_utc_timestamp_ascii_v1 | utf8_to_utf16le_v1 | raw_binary_v1

PostgresMssqlNormalizationV1:
  identity | canonical_positive_zero | utc_instant |
  unicode_scalar_identity

PostgresMssqlEqualityPolicyV1:
  boolean_exact | integer_exact | decimal_exact |
  ieee_value_after_positive_zero | uuid_octets | date_exact |
  time_exact | timestamp_exact | utc_instant_exact |
  unicode_codepoint_exact | binary_octets

PostgresMssqlValueGuardV1:
  boolean_domain | int2_range | int4_range | int8_range |
  decimal_shape_and_value | finite_float4 | finite_float8 |
  uuid_domain | sqlserver_date_range | sqlserver_time_precision |
  sqlserver_datetime2_range_precision |
  sqlserver_datetimeoffset_range_precision |
  utf8_and_utf16_capacity | binary_capacity
```

Enum domains use
`dpone-postgres-mssql-{source-scalar-family|length-kind|codec|normalization|equality-policy|value-guard}-v1\0`
and `dpone-mssql-r1-target-scalar-family-v1\0`.

### Exact facet and value admission

Unused facets are always `None`; `None` never means both maximum and not
applicable because `length_kind` is explicit.

| Source family | Required facets | Forbidden facets |
|---|---|---|
| bool/int2/int4/int8/float4/float8/uuid/date/bytea | `length_kind=not_applicable` | precision, scale, maximum characters |
| numeric | integer `precision>=1`, integer scale; `length_kind=not_applicable` | maximum characters |
| time/timestamp/timestamptz | precision integer `0..6`; `length_kind=not_applicable` | scale, maximum characters |
| text | `length_kind=maximum` | precision, scale, maximum characters |
| varchar | `length_kind=bounded`, maximum characters integer `1..10485760` | precision, scale |

Bool and float values are constraints of the value guard, not type facets.
All integer validators reject bool, float and integer subclasses and accept only
the exact closed interval for their family. For target scale zero, numeric uses
`0|-?[1-9][0-9]*`. For target scale `S>0`, it uses
an optional minus followed by integer part `0|[1-9][0-9]*`, a dot and exactly
`S` digits. Minus is legal only when the complete value is nonzero: `0.1` and
`-0.1` are canonical, while `-0.0` rejects. Exponent, plus sign and leading
zeros reject. Negative source scale admits only values divisible by
`10**(-scale)`. Precision is counted after canonical positive-zero
normalization and must fit the derived target `decimal(P,S)` without rounding.

Float codecs reject NaN and both infinities. The binary32 codec first rounds the
source value to exact IEEE-754 binary32 and applies width-specific Ryu shortest
round-trip formatting; the binary64 codec applies the binary64 form. Both
normalize negative zero to ASCII `0`; output is lowercase ASCII with no plus
sign and parses back to the identical target-width bit pattern. Algorithm and
width are part of the codec enum, and Python/compiled providers must match the
approved golden vectors byte-for-byte. For one shortest significand, fixed and
scientific spellings are both considered: the fewer-ASCII-byte spelling wins,
then fixed notation wins an equal-length tie, then lexical order pins the last
tie. Python `repr` notation thresholds are not authority.

Required canonical float vectors include:

| Width/bits | Bytes |
|---|---|
| binary32 `00000000` / `80000000` | `0` |
| binary32 `3dcccccd` | `0.1` |
| binary32 `00800000` | `1.1754944e-38` |
| binary32 `00000001` | `1e-45` |
| binary32 `7f7fffff` | `3.4028235e38` |
| binary64 `0000000000000000` / `8000000000000000` | `0` |
| binary64 `3fb999999999999a` | `0.1` |
| binary64 `0010000000000000` | `2.2250738585072014e-308` |
| binary64 `0000000000000001` | `5e-324` |
| binary64 `7fefffffffffffff` | `1.7976931348623157e308` |
| binary64 `416312d000000000` | `1e7` |
| binary64 `430c6bf526340000` | `1e15` |
| binary64 `3f50624dd2f1a9fc` | `1e-3` |
| binary64 `3f1a36e2eb1c432d` | `1e-4` |

Date/time guards require SQL Server's `0001-01-01` through `9999-12-31` range.
They reject nonzero fractional digits beyond declared precision rather than
rounding. `timestamptz` normalizes the instant to UTC before the same range and
precision check. Text rejects invalid UTF-8 and lone surrogates and checks the
actual UTF-16 code-unit count before every bounded target write. Binary and
LOB values are bounded by the environment-owned per-value byte limit recorded
in the sealed execution plan; exceeding it blocks before target mutation.

The PostgreSQL 16 character-type contract sets the maximum declared
`varchar(n)` length to 10,485,760 characters; this bound is pinned to the
[official PostgreSQL 16 documentation](https://www.postgresql.org/docs/16/datatype-character.html).

Unconstrained numeric, non-finite numeric/float values, `bpchar`, `timetz`, JSON/XML,
arrays, composites, ranges, domains, enums, PostGIS and custom types do not
enter this authority. A future explicit string-landing profile uses a distinct
version and capability tuple.

### Exact mapping rules

| PostgreSQL shape | Stage/transport grammar | SQL Server shape |
|---|---|---|
| `bool` | ASCII `0` or `1` | `bit` |
| `int2` | canonical base-10 | `smallint` |
| `int4` | canonical base-10 | `int` |
| `int8` | canonical base-10 | `bigint` |
| `numeric(p,s)` | exact canonical decimal, no rounding | lossless `decimal(P,S)` |
| `float4` | finite shortest round-trip, `-0` normalized to `0` | `real` |
| `float8` | finite shortest round-trip, `-0` normalized to `0` | `float(53)` |
| `uuid` | lowercase canonical UUID | `uniqueidentifier` |
| `date` | ISO date within target range | `date` |
| `time(p)` | ISO time at exact declared precision | `time(p)` |
| `timestamp(p)` | ISO `T`, no zone | `datetime2(p)` |
| `timestamptz(p)` | UTC ISO with `Z` | `datetimeoffset(p)` |
| `text` | valid UTF-8; target UTF-16 bound checked | `nvarchar(max)` |
| `varchar(n)` | valid UTF-8; at most `2*n` UTF-16 units | `nvarchar(2*n)` when `2*n<=4000`, otherwise `nvarchar(max)` |
| `bytea` | raw bytes | `varbinary(max)` |

Numeric mapping is exact:

```text
1 <= p <= 38 and 0 <= s <= p -> decimal(p,s)
s < 0 and p-s <= 38     -> decimal(p-s,0)
s > p and s <= 38       -> decimal(s,s)
otherwise               -> not admitted
```

Rounding, truncation and implicit collation changes are forbidden. R1 text
columns use exact `Latin1_General_100_BIN2` target collation. Text columns cannot
be business keys in this profile; canonical row-hash equality remains Unicode
code-point equality and does not delegate equality to SQL Server collation.

## Canonical contracts

All models are frozen slotted dataclasses. Decode uses the existing V3
length-framed codec, reruns validation and rejects unknown domains, trailing
bytes, raw enum strings, bool-as-int and structural substitutes. Digests are
derived SHA-256 values, never caller-provided self-digests.

Shared primitive bounds are exact: digests are 32 bytes; row tokens are 8
bytes; UUIDs are exact `UUID` values; object IDs are exact ints in
`1..2147483647`; revisions/counts are exact ints in `1..9223372036854775807`
except the explicitly zero-valued revocation baseline; identifiers are NFC and
1..128 UTF-16 code units; semantic IDs are 1..64 ASCII bytes; canonical payloads
are at most 16 MiB; columns number 1..1024 and secondary indexes 0..999.
Timestamps are timezone-aware UTC with exact microsecond precision. Bool,
float and integer subclasses reject in every integer field.

The physical descriptor intentionally retains its shared `datetime2(7)` result
ABI. The future exact `dpone_probe_registration_v3` SQL template must produce
`server_observed_at` as:

```sql
CAST(CAST(SYSUTCDATETIME() AS datetime2(6)) AS datetime2(7))
```

The inner SQL Server cast is the sole 100-nanosecond-to-microsecond transform:
it rounds to the nearest microsecond, with a five-or-greater discarded
100-nanosecond digit rounding upward and a carry into the next second when
required. The outer cast preserves the descriptor ABI and guarantees a zero
seventh digit before ODBC/Python conversion. Python, the driver and callers may
not truncate, round or source this value. Expiry comparisons use the resulting
microsecond instant, including when rounding makes it exactly equal to
`expires_at`; equality is expired because admission requires
`observed_at < expires_at`. SQL Server supports seven fractional digits (100 ns)
for `datetime2`; this profile's deliberate reduction is pinned against the
[official datetime2 contract](https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-ver16).

```python
PostgresMssqlSourceScalarShapeV1(
    family: PostgresMssqlSourceScalarFamilyV1,
    source_type_oid: int,
    source_typmod: int,
    length_kind: PostgresMssqlLengthKindV1,
    precision: int | None,
    scale: int | None,
    maximum_characters: int | None,
)

MssqlR1CanonicalTargetScalarShapeV1(
    family: MssqlR1TargetScalarFamilyV1,
    length_kind: PostgresMssqlLengthKindV1,
    precision: int | None,
    scale: int | None,
    maximum_utf16_units: int | None,
    maximum_bytes: int | None,
    collation: str | None,
)

PostgresMssqlValueAdmissionAuthorityV1(
    guard: PostgresMssqlValueGuardV1,
    maximum_input_bytes: int,
    maximum_utf16_units: int | None,
)

PostgresMssqlTypeDecisionAuthorityV1(
    decision_id: str,
    source_shape: PostgresMssqlSourceScalarShapeV1,
    stage_shape: MssqlR1CanonicalTargetScalarShapeV1,
    target_shape: MssqlR1CanonicalTargetScalarShapeV1,
    codec: PostgresMssqlCodecV1,
    normalization: PostgresMssqlNormalizationV1,
    value_admission: PostgresMssqlValueAdmissionAuthorityV1,
    loss_policy: Literal["exact_or_block"],
    equality_policy: PostgresMssqlEqualityPolicyV1,
    hash_policy: Literal["dpone-canonical-logical-row-sha256-v1"],
)

PostgresMssqlTypePolicyAuthorityV1(
    policy_version: Literal["dpone-postgres-mssql-type-policy-1"],
    ordered_decisions: tuple[PostgresMssqlTypeDecisionAuthorityV1, ...],
)
```

Domains are respectively:

```text
dpone-postgres-mssql-source-scalar-shape-v1\0
dpone-mssql-r1-target-scalar-shape-v1\0
dpone-postgres-mssql-value-admission-authority-v1\0
dpone-postgres-mssql-type-decision-authority-v1\0
dpone-postgres-mssql-type-policy-authority-v1\0
```

Decision IDs are derived, not caller-selected:

```text
"pgmssql_" + first_24_lower_hex(SHA256(source_shape.canonical_bytes))
```

Policy members are nonempty, unique by exact source-shape canonical bytes and
ordered by source-family enum order, then length kind, precision, scale and
maximum characters with `None` sorting before integers, then by the complete
`source_shape.canonical_bytes` in unsigned byte order as the final tie-breaker.
There is exactly one decision for every distinct source shape used by the
sealed binding. Every column resolves one decision and every decision is
referenced one or more times. Duplicate semantics under another ID, duplicate
shapes, alternate order and unused/missing members reject. Thus temporal
precision six with default typmod `-1` and explicit typmod `6` are distinct,
stably ordered decisions, while two columns using the same exact shape reuse
one decision.

The exact target facet matrix is:

| Target family | Required facets | Forbidden facets |
|---|---|---|
| bit/smallint/int/bigint/real/float_53/uniqueidentifier/date | `length_kind=not_applicable`; exact vendor `maximum_bytes`; collation `None` | precision, scale, UTF-16 units |
| decimal | precision `1..38`, scale `0..precision`; `length_kind=not_applicable`; collation `None` | UTF-16 units |
| time/datetime2/datetimeoffset | precision `0..6`; `length_kind=not_applicable`; collation `None` | scale, UTF-16 units |
| nvarchar bounded | `length_kind=bounded`, UTF-16 units `1..4000`, maximum bytes exactly twice units, `Latin1_General_100_BIN2` | precision, scale |
| nvarchar maximum | `length_kind=maximum`, maximum bytes `-1`, UTF-16 units `None`, `Latin1_General_100_BIN2` | precision, scale |
| varbinary maximum | `length_kind=maximum`, maximum bytes `-1`, collation `None` | precision, scale, UTF-16 units |

Stage and target shapes are byte-identical in this first provider profile.
`maximum_input_bytes` is exact-positive and at most SQL-int maximum. It is the
sealed per-value admission ceiling for every family. `maximum_utf16_units` is
required only for bounded nvarchar and otherwise `None`; a smaller ceiling
creates a distinct decision and policy digest.

Built-in source OIDs are exact: bytea 17, bool 16, int8 20, int2 21, int4 23,
text 25, float4 700, float8 701, varchar 1043, date 1082, time 1083,
timestamp 1114, timestamptz 1184, numeric 1700 and uuid 2950. Domain and alias
OIDs are not admitted. `source_typmod=-1` is legal only for no-typmod shapes and
the canonical default temporal precision six. Varchar typmod is `n+4`;
temporal typmod is its precision. Numeric typmod is exactly
`((precision << 16) | (scale & 0x7ff)) + 4`; decode derives precision as
`((typmod-4) >> 16) & 0xffff` and sign-extends scale as
`(((typmod-4) & 0x7ff) ^ 1024) - 1024`. These equations follow the
[PostgreSQL numeric implementation](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/utils/adt/numeric.c).
The implementation verifies the exact catalog shape and never reconstructs
signed typmods from `information_schema`.

Fixed vendor storage bytes are literal authority:

```text
bit=1, smallint=2, int=4, bigint=8, real=4, float_53=8,
uniqueidentifier=16, date=3

decimal: P 1..9 -> 5; 10..19 -> 9; 20..28 -> 13; 29..38 -> 17
time:     p 0..2 -> 3; 3..4 -> 4; 5..6 -> 5
datetime2:       -> time bytes + 3
datetimeoffset:  -> time bytes + 5
```

Any catalog byte width outside this table is a target-shape mismatch.

## Registered-target catalog authority

```python
MssqlR1RegisteredTargetColumnV1(
    ordinal: int,
    name: str,
    system_type_schema: Literal["sys"],
    system_type_name: str,
    user_type_schema: Literal["sys"],
    user_type_name: str,
    scalar_shape: MssqlR1CanonicalTargetScalarShapeV1,
    max_length: int,
    precision: int,
    scale: int,
    nullable: bool,
    identity: bool,
    computed: bool,
    sparse: bool,
    rowguidcol: bool,
    generated_always_type: int,
    default_definition_digest: bytes | None,
    computed_definition_digest: bytes | None,
)

MssqlR1RegisteredTargetIndexV1(
    ordinal: int,
    name: str,
    index_kind: Literal["clustered", "nonclustered"],
    unique: bool,
    primary_key: bool,
    unique_constraint: bool,
    disabled: bool,
    hypothetical: bool,
    ignore_dup_key: bool,
    filter_definition_digest: bytes | None,
    ordered_key_column_ordinals: tuple[int, ...],
    ordered_descending: tuple[bool, ...],
    ordered_included_column_ordinals: tuple[int, ...],
)

MssqlR1ClosedTargetFeatureObservationV1(
    temporal_type: int,
    ledger_type: int,
    memory_optimized: bool,
    durability_desc: Literal["SCHEMA_AND_DATA"],
    filetable: bool,
    graph_node: bool,
    graph_edge: bool,
    ordered_trigger_digests: tuple[bytes, ...],
    ordered_inbound_foreign_key_digests: tuple[bytes, ...],
    ordered_outbound_foreign_key_digests: tuple[bytes, ...],
    ordered_check_constraint_digests: tuple[bytes, ...],
    ordered_indexed_view_dependency_digests: tuple[bytes, ...],
    ordered_encrypted_column_ordinals: tuple[int, ...],
)

MssqlR1RegisteredTargetCatalogV1(
    catalog_version: Literal["dpone-mssql-r1-registered-target-catalog-1"],
    target_binding_uuid: UUID,
    target_object_uuid: UUID,
    physical_generation_uuid: UUID,
    target_object_profile: Literal["ordinary_disk_rowstore_v1"],
    database_name: str,
    schema_name: str,
    object_name: str,
    object_id: int,
    target_contract_revision: int,
    database_collation: str,
    ordered_columns: tuple[MssqlR1RegisteredTargetColumnV1, ...],
    primary_key: MssqlR1RegisteredTargetIndexV1,
    ordered_secondary_indexes: tuple[MssqlR1RegisteredTargetIndexV1, ...],
    feature_observation: MssqlR1ClosedTargetFeatureObservationV1,
)

MssqlR1RotationStableTargetAuthorityV1(
    profile_id: str,
    capability_tuple_digest: bytes,
    resolved_profile_digest: bytes,
    route_source_authority_sha256: bytes,
    target_object_profile: Literal["ordinary_disk_rowstore_v1"],
    catalog_projection_version: Literal["dpone-mssql-target-catalog-v1"],
    revocation_revision: int,
    target_binding_uuid: UUID,
    target_object_uuid: UUID,
    recovery_domain_uuid: UUID,
    recovery_domain_epoch: int,
    server_instance_identity_sha256: bytes,
    database_guid: UUID,
    database_family_guid: UUID,
    recovery_fork_guid: UUID,
    database_name_digest: bytes,
    schema_name_digest: bytes,
    object_name_digest: bytes,
    database_name: str,
    schema_name: str,
    object_name: str,
    object_id: int,
    physical_generation_uuid: UUID,
    catalog_contract_digest: bytes,
    target_contract_revision: int,
)

MssqlR1ActiveRegistrationHeadObservationV1(
    target_binding_uuid: UUID,
    physical_coordinate_digest: bytes,
    target_object_uuid: UUID,
    active_registration_id: UUID,
    active_registration_revision: int,
    active_registration_payload_digest: bytes,
    registration_verification_policy_digest: bytes,
    registration_verification_receipt_digest: bytes,
    revocation_revision: int,
    registered_physical_authority_digest: bytes,
    schema_contract_digest: bytes,
    permission_contract_digest: bytes,
    last_control_receipt_id: UUID,
    last_control_receipt_digest: bytes,
    projection_revision: int,
    updated_at: datetime,
    row_token: bytes,
    observation_contract_digest: bytes,
    schema_lock_binding_digest: bytes,
    observed_at: datetime,
)

MssqlR1RegistrationAdmissionEvidenceV1(
    registration_payload: MssqlTargetRegistrationPayloadV1,
    registration_verification: MssqlTargetRegistrationVerificationV1,
    active_head: MssqlR1ActiveRegistrationHeadObservationV1,
    observed_at: datetime,
    stable_target: MssqlR1RotationStableTargetAuthorityV1,
    target_catalog: MssqlR1RegisteredTargetCatalogV1,
)

MssqlR1RegistrationRotationTransitionEvidenceV1(
    predecessor_head: MssqlR1ActiveRegistrationHeadObservationV1,
    successor_head: MssqlR1ActiveRegistrationHeadObservationV1,
    rotation_control_receipt_payload: bytes,
    rotation_control_receipt_digest: bytes,
)
```

Domains are:

```text
dpone-mssql-r1-registered-target-column-v1\0
dpone-mssql-r1-registered-target-index-v1\0
dpone-mssql-r1-closed-target-feature-observation-v1\0
dpone-mssql-r1-registered-target-catalog-v1\0
dpone-mssql-r1-rotation-stable-target-authority-v1\0
dpone-mssql-r1-active-registration-head-observation-v1\0
dpone-mssql-r1-registration-admission-evidence-v1\0
dpone-mssql-r1-registration-rotation-transition-evidence-v1\0
```

`dpone-mssql-r1-registered-target-catalog-1` is the sole canonical realization
admitted for registration projection version `dpone-mssql-target-catalog-v1`.
`create(...)` accepts exact catalog payload bytes, decodes and re-encodes them
byte-identically and rejects runtime snapshot objects, alternate serialization
and digest-only substitution.

The catalog has at least one business column. Column and index ordinals are
contiguous one-based; all references resolve to exact column ordinals. The
primary key is enabled, non-hypothetical, unfiltered, unique, exact and has one
nonnullable source/target key column of type smallint/int/bigint or UUID. Its
exact flags are `index_kind=clustered|nonclustered`, `unique=True`,
`primary_key=True`, `unique_constraint=False`, `disabled=False`,
`hypothetical=False`, `ignore_dup_key=False`, `filter_definition_digest=None`,
one ascending key (`ordered_descending=(False,)`) and no included columns.
Secondary indexes are ordered by canonical bytes and may not claim primary-key
status. They must be enabled, non-hypothetical, unfiltered and have
`ignore_dup_key=False`; `unique_constraint=True` requires `unique=True`.
Their key tuple is nonempty, duplicate-free and disjoint from included columns;
included columns are duplicate-free. `ordered_descending` has the same length
as key columns. Both clustered and nonclustered indexes are admitted, but at
most one index including the primary key is clustered. Every other combination
rejects rather than becoming a valid-distinct policy.

For R1 every business column has `identity=computed=sparse=rowguidcol=False`,
`generated_always_type=0`, and both definition digests `None`; defaults and
target-managed values are forbidden. `max_length=-1` means SQL Server MAX and
is legal only for nvarchar/varbinary with `length_kind=maximum`. Fixed-width
families require their exact vendor catalog length/precision/scale; text uses
`Latin1_General_100_BIN2`, non-text collation is `None`. System/user types are
built-in `sys` types and must reproduce the canonical target scalar shape.

The closed feature observation requires temporal/ledger type zero, every flag
false, `SCHEMA_AND_DATA`, and all six digest/ordinal tuples empty. Thus triggers,
inbound/outbound foreign keys, CHECK constraints, indexed-view dependencies and
encrypted business columns are forbidden and are bound into catalog bytes.

The descriptor remains the sole owner of the registration probe ABI, and
Security V2 remains the sole owner of permission authority. This child resolves
and compares those upstream authorities; it does not copy or redefine them.
These pure contracts must not depend on or import the provisioner adapter.
Before provider aggregation, installation or replay, an exact pinned SQL Server
backend must atomically persist and freshly probe the canonical registration
payload bytes, canonical verification-receipt bytes, verification-policy digest
and verification-receipt digest under the governing session and locks. Until
that implementation and its round-trip/anti-splice evidence exist,
registration persistence remains `UNVERIFIED` and cannot authorize activation.

The authority chain is exact:

1. decode the exact persisted registration payload and verification receipt;
2. require verification payload and `registration_payload.canonical_bytes` to
   be byte-identical;
3. require `SHA256(verification.payload_bytes)` to equal both the verification
   registration-payload digest and the active-head payload digest; require the
   head verification-policy and verification-receipt digests to equal
   `verification.verification_policy_digest` and `verification.receipt_digest`;
4. under the governing transaction-owned lock and on the same physical SQL
   Server session, execute the exact `dpone_probe_registration_v3`; derive
   `observed_at` only from the SQL Server UTC instant returned by that result,
   then validate the active-registration head against every modeled head
   field, row token and projection revision; caller, environment and local
   wall-clock time are forbidden;
5. require `active_head.observed_at == observed_at`,
   `active_head.updated_at <= observed_at`, and
   `issued_at <= verification.verified_at <= observed_at < expires_at`;
6. derive the physical identity and rotation-stable target authority;
7. require head physical coordinate, target object and registered-physical
   digests to equal the re-derived values, and head schema/permission digests
   to equal the exact active descriptor/security authorities supplied to
   validation;
8. select exactly one `MssqlR1PhysicalProcedureDescriptorV1` named
   `dpone_probe_registration_v3` from the active physical descriptor and
   require `active_head.observation_contract_digest` to equal
   `SHA256(selected_procedure.canonical_bytes)`; this exact preimage includes
   the procedure's portable request/result contract, definition, execution
   semantics and execute-principal closure; also require
   `active_head.schema_lock_binding_digest` to equal the independent
   transaction/lock binding supplied to validation;
9. decode and byte-reencode the exact target-catalog payload named by the
   registration;
10. require `SHA256(target_catalog.canonical_bytes)` to equal the registration
   `catalog_contract_digest`;
11. require every object coordinate/profile/revision to agree;
12. validate key, column and closed feature authority;
13. only then expose registered-target and column references.

`from_canonical_bytes()` validates self-contained structure only.
`MssqlR1RegistrationAdmissionEvidenceV1.create(...)` derives all projections
from decoded authorities plus the fresh locked head observation and trusted
instant. The expected observation-contract and schema-lock-binding digests are
separate validation arguments; the evidence cannot derive either value from
itself. `validate_against_authorities(...)` repeats the complete chain. A bare
digest, caller-created projection, signature verification alone, stale/expired/
revoked registration, payload splice or runtime snapshot is not authority.

This pure child does not claim a fresh live SQL Server catalog read. It admits
only the stable canonical catalog payload signed by registration. The later
binding/provider attestation adapter owns the exact live catalog query,
same-lock observation and byte-for-byte comparison against this payload before
installation or replay.

Rotation transition proof is separate from current-registration admission.
`MssqlR1RegistrationRotationTransitionEvidenceV1` is constructed only while
performing rotation and requires predecessor/successor adjacency, the payload's
predecessor ID and expected revision, and an immutable exact control receipt.
Later replay uses only the fresh current active head and never synthesizes or
requires a historical predecessor head.

The original `SignedTargetRegistrationCommandV1` and detached Sigstore bundle
exist only at the initial verifier boundary that produces the persisted
`MssqlTargetRegistrationVerificationV1`. Current admission never requires the
original command or bundle. Rotation verifies a new signed command before
constructing the separate rotation-transition evidence.

Registration attempt fields (`registration_id`, action, predecessor, revision
CAS, issuance/expiry, nonce and signature evidence) remain outside binding-pack
identity. Rotation succeeds only when a fresh verified registration re-derives
the exact same `MssqlR1RotationStableTargetAuthorityV1` and catalog digest.
The stable authority contains every other signed registration field, including
profile/capability/source authority, catalog version, revocation revision,
recovery-domain epoch and canonical coordinate digests. Binding V2 embeds only
the stable authority and catalog payload, never admission evidence.

## Binding-facing references

```python
PostgresMssqlSourceColumnRefV1(
    ordinal: int,
    name: str,
    nullable: bool,
    source_shape: PostgresMssqlSourceScalarShapeV1,
    type_policy_digest: bytes,
)

MssqlR1RegisteredTargetColumnRefV1(
    ordinal: int,
    name: str,
    nullable: bool,
    scalar_shape: MssqlR1CanonicalTargetScalarShapeV1,
    target_catalog_digest: bytes,
    primary_key_ordinal: int | None,
)
```

Domains are `dpone-postgres-mssql-source-column-ref-v1\0` and
`dpone-mssql-r1-registered-target-column-ref-v1\0`.

Refs are constructed only by the active policy/catalog aggregates. Column
ordinals are contiguous one-based; identifiers are NFC, 1..128 UTF-16 code
units, exact- and casefold-unique. Empty column sets reject. A target ref cannot
outlive or silently switch its catalog digest.

Source/target column pairs resolve one exact policy decision. Every policy
decision is used by one or more columns and none is unused. The single target key ref has
`primary_key_ordinal=1`, is nonnullable and resolves to the catalog primary key;
all other refs have `None`. A text ref cannot be a key.

## Determinism, failures and compatibility

Construction is pure and all-or-nothing. Identical exact inputs produce
byte-identical authorities. Cancellation or failure produces no partial
authority or evidence. Decode alone is never replay or activation admission.

One stable `MssqlR1TypeTargetAuthorityError` family exposes a reason ID and
recovery class. The closed mapping is:

| Reason IDs | Recovery class |
|---|---|
| `wrong_domain`, `wrong_version`, `malformed_canonical_bytes`, `enum_unsupported`, `invalid_facet`, `value_grammar_invalid`, `identifier_invalid`, `ordinal_invalid`, `decision_id_invalid`, `decision_duplicate`, `decision_order_invalid`, `policy_coverage_invalid`, `duplicate_column`, `target_key_invalid`, `catalog_order_invalid` | `permanent_input_error` |
| `registration_expired`, `registration_not_active`, `active_head_stale` | `refresh_authority_and_retry` |
| `registration_verification_mismatch`, `registration_revoked`, `rotation_transition_mismatch`, `physical_identity_mismatch`, `catalog_digest_mismatch`, `catalog_coordinate_mismatch`, `catalog_behavior_mismatch`, `target_profile_unsupported`, `target_feature_unsupported`, `authority_splice`, `lossy_mapping` | `operator_intervention` |

Every check maps to exactly one row. Unknown internal exceptions are wrapped as
`malformed_canonical_bytes` only at the decode boundary; domain validation does
not collapse a known reason into a generic one.

Raw `KeyError`, `AttributeError`, `TypeError` and `ValueError` do not escape
internal contract constructors/decoders.

Existing manifests and `PostgresMssqlTypeMapper` behavior remain unchanged.
Differential tests cover every overlapping admitted mapping after physical type
normalization. A difference blocks this new provider authority; it does not
silently change the legacy mapper.

## Architecture and implementation plan

Dependency direction is:

```text
physical descriptor R2 + provider security V2 + registration contracts
  -> registration admission evidence
type_target_enums/primitives
  -> type_policy
registration + type_policy
  -> registered_target_catalog
all above
  -> verified_target_authority

later SQL persistence/probe adapter + verified_target_authority
  -> provider aggregate
```

New code lives under `dpone.contracts`; runtime catalog models remain adapter
inputs converted at a later approved boundary. No reverse contracts-to-runtime
import, generic plugin registry or facade-only module is allowed. Each module
targets 200–300 SLOC and must remain below the repository hard limit.

The immutable `PostgresMssqlTypeDecisionAuthorityV1` and exact
`derive_type_decision` algorithm share the existing
`dpone.contracts.postgres_mssql_type_derivation` owner. Type authority retains
policy coverage, ordering, catalog resolution and source-column issuance. This
internal ownership change preserves the decision matrix, field order, canonical
bytes and failure precedence; it adds no runtime dependency.

Implementation requires a separate path-scoped task contract after this exact
specification is approved. Renderer, migration and binding code is forbidden in
that task.

## Test and certification plan

Hermetic tests cover:

- every enum, field, union, optional arm, bound and domain;
- exact numeric/temporal/text mapping vectors;
- both temporal-precision-six shapes (`source_typmod=-1` and `6`) in one
  policy, stable ordering across input permutations, and same-shape reuse by
  two columns;
- all sixteen required width-specific Ryu golden vectors listed by this spec;
- exact-expiry rejection after the already projected microsecond instant;
- rejection of caller/local/environment clock input and persisted verification
  payload, policy-digest and receipt-digest mismatches;
- every unsupported/lossy shape;
- target column order, duplicate and casefold collisions;
- every forbidden target feature;
- verification/payload/catalog/physical-identity anti-splice mutations;
- rotation-stable equality across two fresh registrations;
- canonical round-trip, unknown/trailing bytes and V1/domain rejection;
- explicit `must_reject` versus `valid_distinct` registries;
- differential compatibility with the existing mapper.

The executable mutation registry includes every dataclass field, enum member,
optional arm and cross-authority reference. Each case is named
`must_reject` or `valid_distinct`; every valid-distinct mutation must change its
owning canonical digest. Boundary vectors include numeric precision/scale
`0/1/38/39`, negative and greater-than-precision scale; temporal precision
`-1/0/6/7`; varchar `0/1/2000/2001/10485760/10485761`; SQL MAX `-1`;
identifier `1/128/129` UTF-16 units with supplementary characters; integer
`0/1/SQL-int-max/overflow`; and every value-guard rejection.

The later renderer/adapter task owns the not-yet-implemented SQL projection and
must cover values immediately below, at and above the half-microsecond
boundary, next-second carry and exact-expiry rejection as exact SQL golden and
mocked ODBC result vectors. The exact SQL Server 2022/CU driver matrix repeats
them live; until that succeeds, timestamp-source fidelity stays `UNVERIFIED`
and cannot promote the provider. This SQL-free task cannot claim those future
renderer vectors as implementation evidence.

Focused implementation checks are frozen in the later task contract and must
include:

```bash
.venv/bin/pytest tests/test_postgres_mssql_r1_v3_type_target_authority_contract.py -q
.venv/bin/pytest tests/test_postgres_mssql_type_mapping.py \
  tests/test_postgres_mssql_r1_v3_contracts.py -q
```

The pure task writes no release evidence. If its approved task contract elects
to retain a hermetic report, the authoritative JSON and deterministic Markdown
projection live under
`test_artifacts/postgres-mssql-r1-v3/type-target-authority/<exact-commit>/`
and include dependency SHAs, mutation counts and raw check status.

Runtime catalog reads, SQL Server metadata fidelity and target-feature queries
remain `UNVERIFIED` until a later approved adapter/live task.

External market comparators are `N/A`: this child freezes an internal canonical
authority and makes no product-facing performance or feature claim.

## Documentation and rollout

Developer documentation records dependency pins, focused commands and failure
recovery. Public tutorials remain unchanged: the pure-contract implementation
is still an integration candidate, while SQL persistence is absent,
certification is unverified and activation is blocked.

No current CLI, manifest or exported Python API changes. These are new durable
canonical artifact/digest contracts for the unreleased, inactive R1 provider;
compatibility is additive only because no production bytes exist. Rollback
removes unused internal V1 models. Once retained, successor bytes use a new
domain/version; V1 bytes are never reinterpreted.

## Approval checklist

- [x] Existing mapper is compatibility reference, not canonical authority.
- [x] Exact source, target, codec, loss, equality and hash authority is closed.
- [x] Registration verification resolves to exact target-column authority.
- [x] Registration rotation does not change stable binding-pack input.
- [x] Target feature profile fails closed.
- [x] Public compatibility impact is none.
- [x] Fresh architecture and test reviews approve this exact specification.
- [x] Maintainer changes status to `APPROVED`.
