# Provision physical runtime registration storage

This guide is for platform engineers installing the immutable SQL registration
catalog in the selected model database. It supplies storage and exact readback;
it does not install signed source-read procedures or enable physical model
execution. Start with the [dbt overview](dbt.md) and the existing
[native-original provisioning boundary](native-originals-mssql.md).

## Prepare authenticated inputs

The platform provisioning owner must supply a previously authenticated
`MssqlPhysicalRuntimeRegistration`, a privileged fresh connection factory with
finite connection and statement timeouts, and the existing `dbo`-owned local
schema. The factory selects the exact authenticated model database. Credentials
remain outside the payload and repository.

Before insertion, provisioning authenticates the complete selected policy and
actual retained profile/toolchain bytes and subjects, external control/capacity
trust anchors, package/program/macro inventory, connection registry, observed
same-service database pins, role mappings and any observer-sharing permission
contract. A hash-shaped value or correct hash alone is not authority. This API
does not resolve those inputs or create a new native-original kind.

Runtime principals must be ordinary restricted users. Provisioning must exclude
sysadmin, database ownership, impersonation, covering administrative privileges
and alternative procedures that expose the protected catalog. The migration
checks actual database principal IDs/SIDs and rejects `db_owner` membership.
It denies direct catalog SELECT/INSERT/UPDATE/DELETE/ALTER/TAKE OWNERSHIP and
local schema ALTER/TAKE OWNERSHIP. This is not a general effective-permission
certifier: server roles and all cross-database/signing paths remain upstream
provisioning responsibilities. A shared observer has its selected role's actual
union of permissions; it is not an independently restricted QUALITY credential.

## Install and retain one registration

The variables in this example are outputs of that authenticated platform
provisioning process. `model_runtime_principals` includes metadata, build and
the actual dedicated or inherited observer identity in the model database.

```python
from dpone.adapters.dbt_mssql_physical_registration_schema import (
    MssqlPhysicalRegistrationSchemaMigration,
)
from dpone.adapters.dbt_mssql_physical_registration_store import (
    MssqlPhysicalRegistrationStore,
)

MssqlPhysicalRegistrationSchemaMigration(
    connection_factory=privileged_model_connection,
    local_schema=authenticated_registration.local_schema,
    runtime_principals=model_runtime_principals,
).apply()
store = MssqlPhysicalRegistrationStore(
    connection_factory=privileged_model_connection,
    local_schema=authenticated_registration.local_schema,
)
retained = store.register(authenticated_registration)
assert store.resolve(authenticated_registration) == retained
```

Installation is additive and transactional. Exact repeated installation verifies
the retained table; incompatible ownership, columns, timestamp precision,
constraints, indexes, triggers, foreign keys or column-level grants reject without destructive repair.
No runtime procedure or runtime registration capability is granted.

The versioned table is `physical_runtime_registrations_v1`. Its complete
53-column layout is frozen in
`packages/dbt-dpone/control/sqlserver/physical-v1/schema.sql`, using the reference
schema `dpone_physical`. The deterministic `registration_table_sql` producer
substitutes a validated deployment schema; a golden test checks the package copy.
The complete canonical UTF-8 payload and external digest remain authoritative.
All scalar authority, reference, pin, program, limit and role fields have named
typed projections; nested platform subjects retain their canonical bytes.
UUIDs use `uniqueidentifier`, pins use `datetime2(7)`, SQL ranges use `int` or
`bigint`, identifiers use `nvarchar(128)`, and exact text/reference/digest/SID
values use binary storage. Full names and types are in the package SQL.

Dedicated observers store their own mappings and an empty binary permission
contract; shared observers store the inherited mappings, exact mode and explicit
permission-contract digest. Empty bytes never represent a shared permission
grant. Timestamp reads construct all seven fractional digits on the server,
including zero fractions, to avoid Python datetime microsecond truncation.

## Observe and recover

`register` returns the canonical decoded registration only after an independent
fresh connection reads and compares payload, digest and every projection. SQL
checks the bound payload hash and atomically inserts or compares the complete
tuple under key-range locking. The independently allocated UUID never changes.
Exact duplicate replay performs no row update; any difference conflicts.

An execute or commit failure causes one independent read-only reconciliation of
that same UUID and full expected tuple. Exact visible state can resolve a lost
acknowledgement. Missing, different or unreadable state raises
`PhysicalRegistrationStorageError`. The adapter never blindly retries insertion,
allocates a replacement UUID, repairs a row, changes generation state, releases
capacity or authorizes dispatch. Direct `resolve` SQL/connection failures propagate
without retry. Both paths recheck the actual model database pin and model role
IDs/SIDs, so an unchanged row cannot conceal a renamed database or replaced user.
Keep the expected bytes and investigate using
privileged read-only tooling. Provision a new UUID only for an explicitly
authenticated new registration, preserving historical rows and references.

## Validation and remaining integration

Run the focused schema/store tests for deterministic projection and recovery
checks. The opt-in `tests/test_dbt_mssql_physical_registration_live.py` suite uses
`DPONE_RUN_PHYSICAL_REGISTRATION_LIVE=1`, `DPONE_NATIVE_SQL_TEST_HOST` and
`DPONE_NATIVE_SQL_TEST_PASSWORD` against an explicitly authorized isolated SQL
Server. It creates uniquely named test databases/logins and removes its own
objects afterward. Never point it at corporate, shared or production systems.

Its synthetic provisioner retains real canonical source bytes and digests as
explicit test inputs. Those bytes are not published native originals, reviewed
release inventory or qualification evidence. Successful storage tests cover
immutable replay, conflicts, lost acknowledgements, schema drift and actual
restricted-user attempts; they do not qualify authority resolution, signing,
source reads, physical bounds, model mutation or commit receipts. These remain
separate implementation and qualification work before any runtime admission.
