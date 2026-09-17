# Observe an existing managed SQL Server table

Use the physical catalog reader to compare an existing table with a local
physical model plan before enrollment. It reads the real SQL Server catalog and
an exact row count in one transaction, rejects incomplete or changing facts, and
returns a detached observation only after the comparison and commit succeed.

This is the post-generation revalidation component. Initial predecessor discovery
under the workspace guard must happen before a generation is reserved and needs
its own preparation boundary. This reader requires an already reserved and bound
generation; it cannot prepare that generation's initial plan.

An observation is a match at observation time. It is not authenticated model
membership, an enrollment receipt, a launch grant, or route qualification. Locks
are released when the read transaction ends. Mutation requires fresh admission.

## Prerequisites

The platform owner must authenticate the retained profile, registration and
model-schema deployment binding before installation or acquisition. Passing a
`MssqlPhysicalRuntimeRegistration` or an `OriginalRef` does not authenticate it.
The direct reader and v1 installer do not resolve these originals on the caller's
behalf. The v2 lifecycle below uses the actual native verifier for its selected
policy projection. See [registration provisioning](dbt-mssql-physical-registration-provisioning.md)
and the [source bridge](dbt-mssql-physical-source-bridge.md).

The first cell requires a dedicated observer registration. Shared-observer modes
remain unsupported because adding catalog execution to their shared identity would
change its retained permission contract. No catalog grant is added to the
dedicated observer.

The initial engine cell is SQL Server 2022 on Linux x86-64 with the qualified ODBC
connection boundary. Other catalog shapes have no fallback. Live certification
must use the exact candidate, server build and driver versions; mocked tests do
not qualify a route.

The source bridge must already admit the actual METADATA or BUILD login for the
reserved generation and executor invocation. Do not use an administrator,
impersonated session or dedicated observer for the read API. Supply a fresh
connection with a finite connect timeout and statement-timeout/`nextset`
capabilities. The caller owns the connection factory and monotonic clock.

The model-data schema must be separately selected by the authenticated deployment.
It is not `registration.local_schema`, which names protected control storage.
Pin the schema name, ID and dbo owner. System, dbo and control schemas are excluded
from the first cell. The platform owner excludes concurrent privileged deployment
changes while installing and verifying the module.

## Install the separate catalog permission boundary

`MssqlPhysicalCatalogSchemaProvisioner` takes authenticated `catalog.sql` package
bytes, a fresh privileged connection factory, and an existing catalog certificate's
name, public bytes and certificate-user name. The private key remains on the
server; it is not supplied through the API. The canonical template is
`packages/dbt-dpone/control/sqlserver/physical-v1/catalog.sql`.

Call `apply(registration, model_schema=..., model_schema_id=...,
model_schema_owner_id=1)` only after authenticating the profile-to-schema binding.
Installation observes the database pin and schema identity, creates the fixed
module if absent, and verifies its exact definition, signature and permissions.
An existing deployment with drift is rejected rather than repaired. The returned
`CatalogDeploymentObservation` records the committed module digest, schema pin
and certificate thumbprint; it is not an authentication receipt.

The separate catalog certificate user receives exactly:

| Scope | Permission |
| --- | --- |
| Model database | `VIEW DEFINITION` |
| `sys.sql_expression_dependencies` | `SELECT` |
| Pinned model-data schema | `SELECT` |

METADATA and BUILD receive only `EXECUTE` on the new catalog procedure. The source
bridge retains its own signatures and grants. The catalog certificate does not
sign or countersign source modules. No new standing data permissions are granted
to runtime users or `public`. The installer rejects unexpected signatures,
certificate grants, role membership, ownership and visibility conflicts.

## Read and compare

The following application function consumes already authenticated deployment
inputs and a local plan. It performs no credential discovery or implicit grants:

```python
from time import monotonic

from dpone.adapters.dbt_mssql_physical_catalog import MssqlPhysicalCatalogReader


def inspect_existing_table(
    connection_factory, registration, plan, invocation_id,
    object_id, object_name, object_create_time,
):
    reader = MssqlPhysicalCatalogReader(
        connection_factory=connection_factory,
        registration=registration,
        operation_timeout_seconds=30,
        clock=monotonic,
    )
    return reader.read(
        plan=plan,
        executor_invocation_id=invocation_id,
        object_id=object_id,
        expected_object_name=object_name,
        expected_object_create_time=object_create_time,
    )
```

Supply exact creation timestamp text with seven fractional digits. Object names,
column names and collation spellings are compared exactly. The read result exposes
`source` and a read-only `results` mapping containing typed tuples for the nine
[wire kinds](dbt-mssql-physical-catalog-wire.md).

The first comparison cell requires no observed dependencies and no forbidden
properties. It compares ordered column types and dimensions, nullability and
collation, one heap or ordinary CCI, one nonpartitioned filegroup placement, and
NONE, ROW, PAGE or COLUMNSTORE compression selected by the plan. Unknown property
codes, aliases, widening-compatible differences and case-only differences reject.
A future dependency-bearing cell needs an authenticated closure consumer.

## Transaction and resource boundaries

```mermaid
sequenceDiagram
    participant Caller
    participant Reader
    participant SQL as Signed catalog module
    Caller->>Reader: Existing-object plan and registered generation
    Reader->>SQL: Source guard in one fresh transaction
    Reader->>SQL: COUNT request
    SQL->>SQL: Verify identity and visibility; reject RLS
    SQL->>SQL: COUNT_BIG with TABLOCK,HOLDLOCK
    SQL-->>Reader: Exact count retained by reader
    Reader->>SQL: First HEADER request
    SQL-->>Reader: Complete bounded HEADER
    loop TABLE and six collections
        Reader->>SQL: Fixed kind request, same transaction
        SQL-->>Reader: One complete bounded rowset
    end
    Reader->>SQL: Final HEADER
    Reader->>Reader: Header/count equality and exact comparison
    Reader->>SQL: Commit read transaction
    Reader-->>Caller: Detached observation
```

The SQL producer revalidates the source on each request and keeps the count's
shared table lock until transaction settlement. COUNT runs once per acquisition;
other kind calls use a real TOP(1) assignment with TABLOCK/HOLDLOCK to retain a
safe lock when called independently, including on an empty table. Only HEADER
materializes all collection counts; detail calls materialize their own collection.
It checks every finite forbidden
predicate before an empty marker can mean zero. Constraints, triggers, permissions,
extended properties, user statistics, fulltext, change tracking, row security,
legacy defaults/rules and unsupported table/column/index/partition state are
included. RLS, including disabled policies, rejects before counting.

Registered row, dependency, column and definition limits are representation
ceilings, not physical data-row capacity or `PhysicalModelSpec.resource_bounds`.
The SQL producer rejects a limit-plus-one result instead of returning a truncated
collection. Definition lengths are checked using `DATALENGTH` before projection.
The reader incrementally fetches, rejects advertised overflow and extra rowsets,
and validates the existing strict wire format.

For the acquisition's aggregate detached payload, each field is charged eight
framing bytes, plus UTF-16LE bytes for text and sixteen bytes for UUID values.
Both headers and empty markers count against `max_metadata_bytes`. This is a
conservative retained-payload ceiling; it does not measure Python object overhead
or prove bounds on driver/network buffering. Server-side bounds remain mandatory.
The definition-byte limit applies to the index filter definition, independently
of names and other fixed-width text.

One absolute observation deadline is checked before calls and fetches and after
commit. Each statement receives a new cursor on the same connection, with the
remaining positive timeout set before cursor creation. This matters because
[pyodbc 5.3.0 sets SQL_ATTR_QUERY_TIMEOUT during Cursor_New](https://github.com/mkleehammer/pyodbc/blob/5.3.0/src/cursor.cpp).
Connect timeout is a factory prerequisite. Deadline checks reject late results;
they do not prove hard termination of a blocked driver, commit, rollback or close,
or server quiescence after cancellation. There is no automatic retry.

## Diagnose and recover

| Failure | Meaning and next step |
| --- | --- |
| Source or visibility could not be verified | No absence or layout verdict exists. Restore the authenticated identity, deployment and permissions; repeat the read under retained ownership. |
| Acquisition limit exceeded | Partial rows are rejected. Reduce scope or provision a new authenticated bounds selection; never reuse partial evidence. |
| Header or collection counts changed | No comparison is accepted. Re-observe under the ownership boundary; do not replace immutable plan bytes silently. |
| Layout differs | Preserve existing objects and resolve drift before preparing a new plan. |
| Timeout, cancellation or uncertain commit | No accepted observation is returned. Cleanup is best effort and is not rollback proof or permission to replay mutation. |

The reader raises `PhysicalCatalogReadError` with a generic public diagnostic.
Underlying driver exceptions are available as exception causes for restricted
operator diagnostics; do not expose credentials or raw sensitive SQL in user logs.

## Validate a deployment

Run focused acquisition, comparison, query and provisioning tests first. The
opt-in live suite must establish a successful real source/catalog baseline, then
exercise all four layouts, exact known row counts, permission/signature drift,
RLS and deadline behavior in isolated synthetic databases. Record exact source
commit, expanded module identity, SQL build, ODBC/pyodbc versions, authentication
role and cleanup. A skipped or unavailable live check remains UNVERIFIED.

The complete journey still requires initial P-only preparation, authenticated
bounds and plan membership, enrollment/materialization and independent receipts.
See [physical plans](dbt-mssql-physical-plans.md) and
[ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md).

## V1 deployment lifetime limitation

The fixed catalog module embeds one immutable registration ID/digest and its
bounds. Repeating the same installation verifies it without repair. A second
registration, even with the same policy, produces a different module definition
and is rejected in the same control namespace. Changing the selected profile
also rejects. The installer does not silently ALTER the module or create a
schema per run.

Consequently this component does not yet provide reusable multi-registration
profile upgrades in one namespace. Do not prescribe namespace creation as an
upgrade workaround. Closing the complete self-service journey requires an
approved-compatible protected per-registration deployment projection or a finite
versioned program/deployment lifecycle, with actual authentication and migration
consumer tests. That integration decision is separate from this bounded reader.

### Temporal column comparison

The catalog comparator uses native `sys.columns.max_length` for each declared
scale, with exact type identity, precision and scale. For scales 0 through 7,
SQL2022 reports these byte lengths:

| Type | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `time` | 3 | 3 | 3 | 4 | 4 | 5 | 5 | 5 |
| `datetime2` | 6 | 6 | 6 | 7 | 7 | 8 | 8 | 8 |
| `datetimeoffset` | 8 | 8 | 8 | 9 | 9 | 10 | 10 | 10 |

These are column catalog dimensions. A fixed-width in-memory or serialized
representation must not substitute for them. The opt-in temporal live probe
checks all 24 combinations; unit vectors alone do not qualify a SQL route.

### Authenticate the catalog policy projection

Platform provisioning can use `MssqlPhysicalCatalogPolicyReader` from
`dpone.adapters.dbt_mssql_physical_catalog_policy` to authenticate the selected
release, archive, full policy member and intent before persisting a catalog
binding. Supply the application's real `NativeOriginalVerifier` and
`NativeProjectDocumentReader`; keep the verifier open while reading its extracted
project. The returned projection records policy membership, profile selection
and model schema. It does not authenticate SQL registration storage or install
the catalog module.

The binding contract owns the detached projection record and pure selected-policy
comparisons. The adapter retains original authentication, archive acquisition and
exact member-byte comparison, in that order. Calling the pure projection helper
directly establishes no authority and cannot replace that acquisition boundary.

The selected v4 profile needs an explicit `authoring_template.invocation_target`
and this optional section under `native_execution`:

```yaml
physical_catalog_limits:
  max_catalog_rows: 1000
  max_definition_utf16_bytes: 65536
  max_dependency_rows: 1000
  max_columns: 256
```

These are illustrative platform choices, not defaults or qualified capacity
recommendations. The platform must select and validate limits for its deployment.
All four values are mandatory when the section exists. Values must be positive
SQL integers, dependency rows cannot exceed catalog rows, and columns are fixed
at 256 for this cell. The consumer compares all four and the existing metadata
and generation limits exactly with the registration. It also checks the selected
PLATFORM subject, retained profile selection and database/schema mapping.

Older v4 policies remain structurally valid without this section. The new
consumer rejects missing catalog configuration before SQL writes; obtain an
updated platform policy and create a new immutable registration when authority
facts change. It applies no fallback from the control schema or ambient profile.
`resource_bounds` identifies the selected profile projection, while
`capacity_authority` continues to identify generation allocation authority.

The policy projection alone does not install a binding or admit model execution.
Use the explicit v2 lifecycle below to reuse the installed module.

### Reuse a v2 deployment

`MssqlPhysicalCatalogRegistrationLifecycle` composes the policy reader, existing
`MssqlPhysicalRegistrationStore`, new binding schema/store, and
`MssqlPhysicalCatalogV2SchemaProvisioner`. The platform application supplies
these dependencies and fresh bounded privileged connections to the model
database. It authenticates the package `physical-v1/catalog-v2.sql` and prepares
a **separate v2 certificate and certificate user**. Reusing the v1 certificate
would violate its exact one-module signature inventory. No credentials belong
in the registration, policy, binding or evidence.

```python
# Dependencies are composed by the trusted platform application.
binding = lifecycle.apply(original_refs, registration=registered_claim)
```

The operation performs actual release/archive/member authentication, checks the
effective execution adapter, connection alias, database and schema, then
independently resolves the existing protected registration. It observes schema
ID/dbo ownership, installs or verifies companion storage and the v2 module,
writes the immutable binding, and independently reads back binding and module
inventory before returning. A caller-created projection is not an alternative
input. The registration and complete source deployment must already exist through
their source provisioning path. The lifecycle verifies catalog deployment
inventory; it does not independently verify every source module, signature or
permission. Preserve that prerequisite and the source guard's own runtime checks.

The companion `physical_catalog_bindings_v1` has a registration UUID, exact
registration digest, canonical payload and payload digest. It adds no foreign key
and no duplicate numeric limit authority. All six limits remain in the existing
registration. Runtime principals have explicit direct-access DENYs; the signed
module accesses protected rows through the existing ownership boundary.

For a second registration in the same supported cohort, run the same operation
with its own authenticated originals and immutable registration. Existing schema,
module text, signatures and grants must match exactly; the second registration
adds only its binding row. The cohort retains the same database/schema pins,
source deployment and METADATA/BUILD/dedicated-observer principals. Changes to
that cohort reject and require an explicitly provisioned deployment. Existing
registrations and bindings remain immutable and independently readable. Generation
execution still obeys existing activation, owned-footprint and exclusion rules;
reusable provisioning does not authorize concurrent uncontrolled writes.

Select v2 explicitly when constructing the catalog reader:

```python
reader = MssqlPhysicalCatalogReader(
    connection_factory=runtime_connection_factory,
    registration=registration,
    operation_timeout_seconds=operation_budget,
    clock=time.monotonic,
    catalog_version=2,
)
```

The default remains v1. V2 rejects missing/substituted bindings before observing
the model object and requires `plan.spec.resource_bounds` to identify the
registered trusted-profile projection. It verifies source identity first, then
registration/binding bytes, selected policy/profile linkage, schema pins and the
actual executing module hash within the same transaction. It reads all numeric
bounds from the protected registration. This does not grant plan membership,
model enrollment, publication authority or qualification of a live route.

Installation steps commit separately. A failure can leave protected storage or
a module installed but returns no accepted binding. Retry the same authenticated
inputs explicitly: exact existing state is verified, conflicting state is never
repaired. An uncertain binding write gets one independent read-only reconciliation
with the same UUID and complete expected bytes. Missing, unreadable or different
state remains failure; there is no automatic second insert or replacement UUID.
Final module verification is read-only and never creates a missing procedure.

The canonical binding must fit the registered metadata ceiling, including both
full PLATFORM subjects. A too-small ceiling rejects; the lifecycle does not raise
it implicitly. Obtain a new platform-authorized policy/registration when limits
need to change.
