# Observe a registered SQL Server source identity

Platform engineers can provision a caller-preserving read between the registered
model database and native control database on one SQL Server instance. Runtime
METADATA and BUILD callers can then observe the exact current source executor.
This is the next bounded step after [immutable registration
provisioning](dbt-mssql-physical-registration-provisioning.md).

The observation proves a registered source is currently BUILDING and ACTIVE under
its existing physical owner. It grants no model mutation, launch, quality result,
capacity release or replay. Physical bounds admission and model transactions
remain separate prerequisites.

## Prepare the protected boundary

Use distinct ordinary SQL-authenticated logins for METADATA and BUILD, each mapped
to its own database user in both databases. Register the actual per-database IDs
and SID bytes; numeric IDs need not match across databases. The connection must
authenticate as that login. Administrator impersonation is not a replacement for
a real runtime connection. Keep guest, TRUSTWORTHY and database/server ownership
chaining disabled. Protected schemas and their local objects retain dbo ownership.

The privileged platform owner authenticates retained policy, profile, toolchain,
program, package and macro bytes before enrollment. Hash-shaped strings alone do
not authenticate those documents. Profile/toolchain subjects are PLATFORM subjects.
The existing [native original index](native-originals-mssql.md) does not bootstrap
its own trust root. The registration references the independently authenticated
control/capacity authority and actual database/service pins.

Provision exactly two modules from the retained
`packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql` bytes:

| Database | Module | Certificate use | Certificate principal permissions |
|---|---|---|---|
| Model | `physical_require_source_v1` | Signed entry | VIEW DEFINITION on this entry only |
| Control | `physical_control_require_source_v1` | Matching countersignature | EXECUTE and VIEW DEFINITION on this helper only |

The certificate public identity must match in both databases. Key material belongs
to privileged provisioning and never enters registration bytes or runtime
configuration. Each certificate user has no login and no CONNECT grant. Its exact
object permissions are listed above; a same-database installation uses their
finite union. Runtime receives EXECUTE on the model entry only; no direct helper grant is added.
Do not DENY helper EXECUTE to runtime: provisioning rejects that drift. In the
two-database layout it also blocks the signed indirect call. In a same-database
layout the local ownership chain can bypass the helper permission check; neither
revoking its certificate-user EXECUTE nor adding a caller DENY is a runtime
revocation mechanism. Both layouts still enforce exact module signatures at runtime.
Missing expected certificate permissions are restored only by an explicit
privileged provisioning call; unexpected grants or DENYs are rejected. Runtime
reads never repair permissions. Use the native source/owner state boundary to
stop source observations rather than relying on a topology-specific permission hop.
Existing native table DENYs and metadata authority checks stay in place.

Deployment renders fixed coordinates, the verified certificate thumbprint and the
unchanged native physical-owner predicate. It verifies exact installed definitions, caller context, ownership,
finite signatures, principal mappings and effective permissions before storing
the registration. Producer/package hashes and deployed expansion hashes have
different meanings; neither contains this registration's digest.

Both runtime modules check their own exact signature inventory before reading
protected state: one expected signature or countersignature and no extra entries.
This is necessary because the SQL2022 fixture demonstrated that removing only the
helper countersignature can otherwise leave its local ownership-chain read usable.
An unrelated countersignature also retained the expected caller certificate token
in the measured engine cell, so token membership alone could not prove the exact
counter inventory. The scoped VIEW DEFINITION grants make only each module's own
cryptographic metadata visible while the signed context is active. They add no
standing runtime/public grants, database/schema visibility, certificate metadata
visibility, table access or DDL authority. Outside the signed entry, ordinary
runtime callers still cannot directly execute the helper or inspect its inventory.
The installer first checks actual `CERTENCODED` bytes in both databases, then reads
the actual 20-byte SHA-1 thumbprint for deterministic expansion. It does not guess
a fingerprint in Python. Certificate principal SIDs are a separate catalog value;
see [Microsoft's certificate catalog reference](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-certificates-transact-sql?view=sql-server-ver17).

After the immutable registration storage migration and external authentication,
the platform invokes the concrete installer. Its connection factory selects the
model database with privileged authority and opens existing protected keys when
required. `certificate_public_bytes` is the actual expected `CERTENCODED` public
value, not a private key or thumbprint substituted for that value:

```python
from dpone.adapters.dbt_mssql_physical_source_schema import MssqlPhysicalSourceSchemaProvisioner


def install_source_bridge(platform_connection_factory, registration, admission_bytes,
                          certificate_name, certificate_public_bytes, certificate_user):
    provisioner = MssqlPhysicalSourceSchemaProvisioner(
        connection_factory=platform_connection_factory,
        admission_sql=admission_bytes,
        certificate_name=certificate_name,
        certificate_public_bytes=certificate_public_bytes,
        certificate_user=certificate_user,
    )
    return provisioner.apply(registration)
```

The installer verifies SQL inventory and immutable storage readback; the caller
has already authenticated upstream original/package bytes. Keep the observed
expansion/signature/grant inventory in platform provisioning evidence. An
ambiguous installation commit prevents registration and requires independent
operator inspection before another installation attempt.

## Read from an actual runtime connection

The platform supplies an authenticated registration and a fresh connection factory
for its model database. Configure finite connection and statement timeouts. The
following function is a complete composition example; its inputs come from those
existing platform boundaries:

```python
from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader


def observe_source(model_connection_factory, registration, generation_id, invocation_id):
    reader = MssqlPhysicalSourceReader(
        connection_factory=model_connection_factory,
        registration=registration,
    )
    return reader.read(generation_id, invocation_id)
```

`generation_id` and `invocation_id` are canonical UUID strings from the existing
[reserved and bound generation](native-generation-admission.md). The reader owns
one bounded transaction and closes its connection. A later model transaction must
perform its own current checks; this result is a point-in-time observation.

The source-identity contract owns the closed fourteen-field decoder shared by
source and catalog readers. Readers retain rowset cardinality, current identity
matching, transactions and cleanup. Decoding a row alone is neither authenticated
observation nor permission to execute a model; the existing reader error import
continues to identify the same shared exception class.

The public SQL entry accepts exactly registration UUID, generation UUID and expected
invocation UUID. It reads the real protected registration, verifies its complete
digest and all 53 typed projections, checks the actual model pin and caller, then
calls the literal control helper. The helper separately observes the control
caller ID/SID and original-login continuity. It compares registered metadata
authority without treating BUILD as the metadata principal.

The control helper reads the immutable request preimage without taking a G update
lock, executes the shared P ownership predicate, then locks and rechecks G. It
requires the exact retained executor, reservation, profile, epoch, authority,
BUILDING/ACTIVE state and absence of completion/frozen values. An already admitted
executor remains readable when writer admission is CLOSED; this cannot launch a
new executor. Neither procedure starts, commits or rolls back the caller's
transaction, mutates rows or emits a helper rowset.

## Interpret the observation and recover

`PhysicalSourceIdentity` contains wire version 1, registration UUID/digest,
generation and invocation UUIDs, guard epoch, source revision, reservation
reference, complete executor bytes and both observed database principals. The
closed SQL row has 14 columns; Python groups locator/digest and ID/SID pairs into
their existing value types. There is no success flag, timestamp authority,
qualification or commit receipt.

`PhysicalSourceReadError` means no accepted observation. Investigate the underlying
SQL error with the platform owner. A missing registration, changed principal/SID,
unreadable pin, dropped signature, direct helper call, stale owner, wrong executor,
non-ACTIVE source, missing transaction or transport timeout all fail closed.
Errors never trigger dispatch, source cleanup, registration repair or an automatic
retry. Repeating this read after diagnosis is read-only; it does not establish
that an uncertain writer stopped.

Changing module text removes its signature. Reprovision the exact authenticated
inventory before use; do not grant broader rights to bypass a failure. Changing a
registered identity requires a newly provisioned registration UUID, preserving the
old row for outstanding generations. See [registration recovery and
compatibility](dbt-mssql-physical-registration.md).

## Validation boundary

Focused tests cover closed fact identities and deterministic SQL expansion.
The isolated SQL2022 integration test must use independent real METADATA/BUILD
connections in both same-database and two-database layouts, genuine reserve/bind
state and retained platform fixture bytes. The two-database case uses deliberately
different principal IDs; the same-database case uses one certificate user with
exactly three object grants (entry VIEW DEFINITION; helper EXECUTE and VIEW DEFINITION).
It checks missing/foreign/extra signatures, direct helper
calls, effective permissions, changed registration/principals/state and identical
native rows/capacity before and after reads. Synthetic SQL acceptance does not
qualify a production deployment, model admission or a complete native route.

This additive API does not rename or alter existing native procedures. Existing
consumers require no migration. The next implementation stage is authenticated
physical bounds/model admission under [ADR0065](adr/0065-trusted-isolated-native-generation-execution.md).
