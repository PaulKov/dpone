# Immutable SQL Server physical plans

This reference is for dpone runtime and adapter developers handling the
unreleased managed SQL Server plan format. The pure records and codec establish
representation and internal consistency. They perform no SQL, resource lookup,
credential access, original authentication, or dispatch.

The SQL enrollment and admission consumer are not implemented by this slice.
`runtime_registration_id` is a locator; neither constructing a plan nor decoding
valid canonical bytes establishes a registered runtime or execution authority.

## Records and public functions

Import immutable records from `dpone.contracts.dbt_mssql_physical`:

| Record | Purpose |
| --- | --- |
| `PhysicalRelation` | Exact database, schema and table spellings. |
| `PhysicalFilegroup` | Planned positive data-space ID and name. |
| `AbsentPredecessor` | An absence claim requiring later fresh observation. |
| `ManagedPredecessor` | Prior object ID, creation token and unauthenticated receipt reference. |
| `PhysicalModelSpec` | Model ID, graph digest, ordered columns, relation, layout, filegroup and resource-bounds reference. |
| `PhysicalModelPlan` | Generation-specific spec and predecessor, with derived object names and plan digest. |
| `PhysicalPlanSet` | Ordered plans, generation and registration IDs, existing attempt/guard carriers, profile reference and database pin. |

Constructors derive `model_spec_sha256`, `model_plan_sha256`, candidate/helper
names and conditional backup/index names. These fields are not constructor
arguments. `to_dict()` returns a detached wire projection; use the codec boundary
when exporting or accepting a complete document.

Import the following from `dpone.contracts.dbt_mssql_physical_wire`:

```python
from dpone.contracts.dbt_mssql_physical_wire import (
    decode_physical_plan_set,
    encode_physical_plan_set,
    physical_plan_set_digest,
)


def validate_and_retain(payload: bytes) -> tuple[bytes, str]:
    plan_set = decode_physical_plan_set(payload)
    return encode_physical_plan_set(plan_set), physical_plan_set_digest(plan_set)
```

The decoder returns `PhysicalPlanSet` or rejects the document with
`PhysicalPlanError`, defined in `dbt_mssql_physical_validation`. Encoding verifies
the projection through the same strict decoder. The example retains canonical
bytes and their digest; it does not publish an original or authenticate its
references. Callers must bound acquisition before materializing input bytes.

`PHYSICAL_PLAN_SET_KIND` is the local literal `mssql_physical_plan_set_v1`.
It is deliberately absent from the global original-kind registry until an actual
authenticated admission consumer is integrated.

## Wire and deterministic identity

The top-level schema is `dpone.mssql-physical-plan-set.v1`. Its closed fields are
`schema`, `generation_id`, `runtime_registration_id`, `workspace_attempt`,
`guard`, `profile`, `model_database`, and `models`. It contains no self digest,
reservation digest, command, executor binding, or future completion receipt.

The implementation reuses the native-delivery JSON canonicalizer and its
document, token, depth and UTF-8 string limits. Alternate encodings, whitespace,
unknown keys, malformed variants and inconsistent derived fields are rejected;
the decoder does not silently normalize received bytes.

There are three distinct digest inputs:

1. A model spec omits only its own `model_spec_sha256`.
2. A model plan omits only its own `model_plan_sha256`, retaining its complete
   spec and spec digest.
3. The external plan-set digest includes the complete document and all nested
   digests.

Object names hash the canonical object containing schema
`dpone.mssql-physical-object-name.v1`, generation ID, model ID and role.
The full 64-character lowercase SHA-256 hex digest follows `dpone_c_` for
`CANDIDATE`, `dpone_h_` for `HELPER`, `dpone_b_` for `BACKUP`, or `dpone_i_` for
`CCI`. A backup name exists only for a managed predecessor; a CCI name exists
only for `columnstore`. Other conditional values are JSON null. Incoming names
are recomputed, even when their supplied hashes are internally consistent.
There is no suffix search or collision repair.

## Validation and limits

Models must be nonempty, unique and sorted by the UTF-8 bytes of their model IDs.
Generation IDs and database spellings must agree within the plan set. UUID text
is canonical lowercase hyphenated form. Positive SQL integers reject booleans
and overflow. Identifiers use 1–128 UTF-16 code units and exclude control
characters. Database/object creation timestamps retain exactly seven fractional
digits; Gregorian date and clock validity are checked without losing precision.

The four layout tags are `rowstore_none`, `rowstore_row`, `rowstore_page`, and
`columnstore`. Column order and strict boolean nullability are preserved; dtype
spelling must already match the existing physical type normalizer. Character
columns require a collation and noncharacter columns require null collation.

These are representation checks. The codec does not enforce the later
256-column physical admission policy, deduplicate columns, qualify data types,
authenticate selected graph membership or predecessor receipts, compare current
catalog state, resolve SQL-collation aliases, or verify a registered database.
A decoded plan is not a physical-layout certificate.

## Admission ordering and recovery boundary

The planned sequence is preallocated IDs → plan original → command original →
retained admission request/reservation → executor binding → immutable enrollment
and independent readback → one launch. Plans therefore cannot include command
or reservation identities that would create a digest cycle. Fresh validation
after reservation must reject predecessor or namespace drift before mutation;
it cannot silently replace the plan. Metadata replay must not restore launch
entitlement.

See [ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md) for the
identity and permission boundary, and [physical catalog wire](dbt-mssql-physical-catalog-wire.md)
for observation transport. Existing legacy codecs and admission command bytes
are unchanged. Return to the [dbt integration overview](dbt.md).
