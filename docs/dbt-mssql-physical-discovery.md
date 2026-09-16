# Observe an unused physical namespace under P

This platform Python boundary checks that proposed TARGET, CANDIDATE and HELPER
names are absent and that an explicitly selected SQL Server filegroup is an
ordinary writable filegroup. It requires an already admitted workspace attempt
and its real physical guard (P). It returns a temporal observation after the read
transaction commits successfully. It does not reserve names or retain a generation.

This is a preparation prerequisite for [physical plans](dbt-mssql-physical-plans.md),
not a complete self-service launch command. The platform must authenticate the
whole selected project policy and write footprint separately. A returned
filegroup name proves its observed database state, not its policy provenance.

## Prerequisites and installation

The platform owner supplies an authenticated runtime registration, protected
catalog binding, installed `physical_catalog_v2`, complete PLATFORM subject and
actual workspace attempt/guard identity. The first cell requires a dedicated
observer and a separate model schema owned by dbo. It supports SQL Server 2022.
The connection factory supplies a fresh ordinary METADATA connection with a
finite connect timeout, statement timeout and reliable `nextset()` support.

`MssqlPhysicalDiscoverySchemaProvisioner` deploys two fixed modules from retained
`physical-v1/discovery.sql` package bytes. Its independent matching certificate
has database VIEW DEFINITION in the model database and EXECUTE plus VIEW DEFINITION
on the control helper in the control database. A same-database installation uses
the exact union. METADATA receives EXECUTE on the model entry only; BUILD and the
observer receive no discovery grant. Existing source/catalog certificates and
modules are preserved. The installer verifies exact replay instead of repairing
a partially installed or changed trust chain.

The model entry is `physical_discover_absent_v1`; the countersigned control helper
is `physical_control_require_owner_v1`. The helper has OUTPUT parameters only.
Installation coordinates are authenticated by the platform owner; passing typed
Python values to the renderer does not authenticate them.

## Construct and read a request

The following function accepts already authenticated application inputs. Its
parameters deliberately carry the existing registration, binding and actual P
identity; constructing them from arbitrary caller data is not an admission path.

```python
from time import monotonic

from dpone.adapters.dbt_mssql_physical_discovery import MssqlPhysicalDiscoveryReader
from dpone.contracts.dbt_mssql_physical_discovery import (
    DiscoveryObject,
    PhysicalDiscoveryRequest,
)


def observe_orders(connect, registration, binding, attempt, guard,
                   generation_id, invocation_id, selected_filegroup):
    request = PhysicalDiscoveryRequest(
        subject=registration.platform_subject,
        workspace_attempt=attempt,
        guard=guard,
        generation_id=generation_id,
        invocation_id=invocation_id,
        filegroup_name=selected_filegroup,
        objects=(
            DiscoveryObject("model.example.orders", "TARGET", "orders"),
            DiscoveryObject("model.example.orders", "CANDIDATE", "candidate_orders"),
            DiscoveryObject("model.example.orders", "HELPER", "helper_orders"),
        ),
    )
    reader = MssqlPhysicalDiscoveryReader(
        connection_factory=connect,
        registration=registration,
        binding=binding,
        operation_timeout_seconds=30,
        clock=monotonic,
    )
    return reader.read(request)
```

The preallocated generation and invocation UUIDs correlate preparation only. No
G record or executor is read or created. Production names come from the approved
plan builder. Models must be strictly sorted by UTF-8 model ID; each model has
exactly the ordered TARGET, CANDIDATE, HELPER triple. The request has no database,
schema, numeric budget, executor or generation override. Its canonical JSON
schema is `dpone.mssql-physical-discovery-request.v1`.

Names preserve exact spelling and are bounded to 128 UTF-16 code units without
control characters. SQL checks pairwise aliases under its actual catalog
collation and rejects collisions with every `sys.objects` type in the pinned
schema. It uses `CATALOG_DEFAULT`, which follows metadata collation in both
contained and noncontained databases; data collation can differ. See Microsoft's
[contained database collation reference](https://learn.microsoft.com/en-us/sql/relational-databases/databases/contained-database-collations).

## Transaction and returned facts

The entry validates all existing registration projections, canonical binding,
actual METADATA caller and original login, database identity and its own signature.
The binding's module digest refers to the installed `physical_catalog_v2`, not the
discovery entry. The control helper independently validates its caller mapping,
protected native authority and the shared `physical_owner` predicate. Its real
workspace/attempt/guard locks precede namespace and filegroup reads in the same
transaction. Effective metadata visibility and relevant token DENYs are checked
before absence can be reported.

The only accepted wire result is one version-1 row with 18 scalars: registration
UUID/digest, request digest, database ID/GUID, schema ID/name/owner, P epoch,
object count, filegroup ID/name/type and both actual caller ID/SID pairs. Python
compares all expected identities, enforces registered metadata/object bounds and
a finite operation deadline, rejects extra rows/result sets, then commits.
The setup cursor is closed before the remaining statement timeout is applied to
a fresh discovery cursor on the same connection and transaction. An expired
setup budget prevents discovery dispatch; it never becomes an infinite timeout.
Only an acknowledged commit returns an observation. Cursor and connection cleanup
is best effort. The commit ends this read transaction; it does not release or
renew the separately managed durable P lease. Upstream must continue to own P and
perform later admission checks. Absence is not guaranteed after this transaction.

## Failures and recovery

A name collision, nonordinary/read-only/missing filegroup, schema or caller drift,
metadata DENY, changed registration/binding/module, lost P, malformed request or
exceeded budget produces no accepted observation. Filegroup lookup has no PRIMARY
or default fallback. Restore the authoritative prerequisites or choose a policy-
authorized preparation request; never guess a different name after a collision.

`PhysicalDiscoveryReadError` reports acquisition failure without exposing raw SQL
in its public message. Preserve the private exception cause for restricted
operator diagnostics. Commit uncertainty and cancellation trigger best-effort
rollback and cleanup; there is no automatic retry. The shared P validator retains
its existing `DPONE_NATIVE_GENERATION_*` diagnostic codes even though discovery
performs no generation-table query.

Focused tests cover canonical request closure, strict rows, identity mismatches,
settlement and finite SQL producer structure. Mocked tests do not certify SQL
Server permissions, locks, collation or live absence. Live certification requires
the separately approved isolated environment, including genuine P without G,
same/two database execution and adversarial caller/visibility/lease checks.
