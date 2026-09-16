# Physical runtime registration records

This reference is for dpone developers preparing the unreleased SQL Server
physical runtime registration format. The immutable records and canonical codec
validate data representation and internal consistency. They do not register a
runtime, query a server, resolve originals, provision permissions or authorize
execution. A separate privileged [SQL storage adapter](dbt-mssql-physical-registration-provisioning.md)
installs and retains registrations. The separate [signed source-identity
bridge](dbt-mssql-physical-source-bridge.md) supplies provisioning and read-only
current-owner checks. Complete original authentication, physical model admission
and route qualification remain unfinished.

## Python interface

The root record is `MssqlPhysicalRuntimeRegistration` in
`dpone.contracts.dbt_mssql_physical_registration`. Nested records live in
`dpone.contracts.dbt_mssql_physical_registration_values`:
`PlatformSelection`, `ProgramAuthority`, `RegisteredLimits`, `DatabasePrincipal`,
`DatabaseRoleMapping`, `DedicatedObserver`, `SharedObserver` and
`RegisteredPrincipals`. Constructors require explicit inputs; `to_dict()` returns
a detached projection. Use the bounded codec when exporting complete records.

```python
from dpone.contracts.dbt_mssql_physical_registration_codec import (
    decode_physical_runtime_registration,
    encode_physical_runtime_registration,
    physical_runtime_registration_digest,
)


def retain_registration(payload: bytes) -> tuple[bytes, str]:
    registration = decode_physical_runtime_registration(payload)
    return (
        encode_physical_runtime_registration(registration),
        physical_runtime_registration_digest(registration),
    )
```

The codec rejects malformed records or noncanonical bytes with
`PhysicalRegistrationError`, a `ValueError` subclass in the values module.
Constructor validation also reuses existing contract validators; the codec is
the unified boundary for external documents. It uses the existing native JSON
encoder/decoder and inherited document, string, depth and token limits. Bound
acquisition before materializing payload bytes. The example performs no write
or authentication; retaining a digest does not establish an installed record.

## Closed document and identities

The wire schema is `dpone.mssql-physical-runtime-registration.v1`. The seventeen
constructor inputs plus this fixed `schema` form eighteen required keys:

| Fields | Meaning |
| --- | --- |
| `registration_id` | Independently allocated canonical UUID; not derived from a digest. |
| `platform_subject` | Exact existing PLATFORM subject with runtime authority and policy digest. |
| `control_authority`, `capacity_authority` | Retained original references. Capacity authority describes generation capacity only. |
| `trusted_profile`, `trusted_toolchain` | Explicit reference plus non-null PLATFORM subject for each selection. |
| `qualification_policy_id` | Selected policy label, not proof of qualification. |
| `control_connection_ref`, `model_connection_ref` | Connection labels, not connection credentials or discovered endpoints. |
| `service_authority_sha256` | Expected service fingerprint; actual same-service observation belongs to provisioning. |
| `control_database`, `model_database` | Existing database pin carriers with exact name, positive SQL ID, seven-fraction creation token and UUID. |
| `control_schema`, `local_schema` | Names validated by the existing native control-schema contract. |
| `program` | Program/package/macro hashes with fixed control-program and physical-policy tags. |
| `limits` | Explicit generation, metadata and physical acquisition bounds. |
| `principals` | Database-scoped metadata, build and observer mappings. |

The external digest hashes every canonical byte, including the independent UUID.
There is no self-digest field or omission rule. No generation, command,
reservation, executor or future receipt is included. Upstream selections and
program hashes must precede registration; downstream plans may identify it.

`ProgramAuthority` takes three digests and projects fixed tags
`dpone.mssql-physical-control.v1` and `sqlserver-table-physical-v1`. Digest-shaped
values do not authenticate package or program bytes. The codec introduces no
new global original kind and does not manufacture missing upstream originals.

## Units and limits

All numeric limits are explicit exact integers, with booleans rejected:

| Limit | Representation constraint |
| --- | --- |
| `max_metadata_bytes` | 1–1,048,576 bytes, capped by the inherited JSON document ceiling. |
| `max_generation_bytes` | 1–9,223,372,036,854,775,807. |
| `max_catalog_rows` | 1–2,147,483,647. |
| `max_definition_utf16_bytes` | 1–2,147,483,647 UTF-16LE bytes, independent of metadata JSON bytes. |
| `max_dependency_rows` | Positive and no greater than `max_catalog_rows`. |
| `max_columns` | Exactly 256, recording the approved physical admission ceiling. |

Representation ceilings are not allocation defaults or observed SQL capacity.
Provisioning must choose and qualify actual bounds. The definition-byte value
matches the [catalog decoder](dbt-mssql-physical-catalog-wire.md) and must not be
silently converted from UTF-8. Recording a 256-column ceiling does not apply that
policy to any model: the generic plan codec still performs representation checks.

`PhysicalModelSpec.resource_bounds` is not compared with `capacity_authority`. Generation
capacity does not establish physical catalog or dependency limits. Actual
admission must reject execution until an authenticated physical-bounds original
or explicit authenticated projection mapping is available.

## Principal namespaces

`DatabasePrincipal` contains an ID from 5 to 2,147,483,647 and `sid_hex`: 1–85
bytes encoded as lowercase even-length hex without a prefix. Each
`DatabaseRoleMapping` has `control` and `model` principals, scoped by the
corresponding database pin. Metadata and build require different IDs and SIDs
within each namespace.

`DedicatedObserver` adds a mapping distinct from both roles in each database.
`SharedObserver` carries exactly `SHARE_METADATA` or `SHARE_BUILD` and a
permission-contract digest. The latter represents an explicit permission choice;
the codec does not verify that the reviewed contract exists or that grants match
it. A shared identity has the actual union of permissions and is not inherently
a restricted final-quality credential.

Within the asserted service pin, equal database IDs or equal database GUIDs
require complete equal pins and equal per-role mappings. Otherwise the records
use separate namespaces. This is a conservative consistency rule, not a global
GUID uniqueness guarantee or live identity check. No case folding or SQL
collation equivalence is inferred.

## Provisioning and recovery boundary

The codec has no provisioning or recovery command. The separate privileged
storage API binds the exact UUID, payload, digest and projected columns
atomically. An identical duplicate permits readback; conflicting bytes fail.
Lost acknowledgment requires independent readback of the same UUID rather than
replacement or permission changes in place. Both insertion and readback recheck
the model database pin, role identities and required catalog/schema DENYs.
Replaying metadata never restores launch entitlement.

Complete provisioning still requires actual original resolution and program authority.
The source bridge observes same-instance database pins and installs finite
caller-preserving signatures under those external platform prerequisites. Codec,
storage or source-observation success cannot substitute for model admission or
route qualification. Existing legacy codecs and admission
behavior are unchanged. See [ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md),
[physical plans](dbt-mssql-physical-plans.md), or return to the
[dbt integration overview](dbt.md).
