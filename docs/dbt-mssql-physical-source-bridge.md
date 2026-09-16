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

| Database | Module | Certificate use |
|---|---|---|
| Model | `physical_require_source_v1` | Signed entry |
| Control | `physical_control_require_source_v1` | Matching countersignature |

The certificate public identity must match in both databases. Key material belongs
to privileged provisioning and never enters registration bytes or runtime
configuration. The control certificate user receives EXECUTE on the helper only.
Runtime receives EXECUTE on the model entry only; no direct helper grant is added.
Do not DENY helper EXECUTE to runtime: that also blocks the signed indirect call.
Existing native table DENYs and metadata authority checks stay in place.

Deployment renders only fixed coordinates and the unchanged native physical-owner
predicate. It verifies exact installed definitions, caller context, ownership,
finite signatures, principal mappings and effective permissions before storing
the registration. Producer/package hashes and deployed expansion hashes have
different meanings; neither contains this registration's digest.

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
connections in two databases, different principal IDs, genuine reserve/bind state,
and retained platform fixture bytes. It checks missing signatures, direct helper
calls, effective permissions, changed registration/principals/state and identical
native rows/capacity before and after reads. Synthetic SQL acceptance does not
qualify a production deployment, model admission or a complete native route.

This additive API does not rename or alter existing native procedures. Existing
consumers require no migration. The next implementation stage is authenticated
physical bounds/model admission under [ADR0065](adr/0065-trusted-isolated-native-generation-execution.md).
