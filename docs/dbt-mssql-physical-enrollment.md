# Physical plan enrollment and session attachment

The physical path retains one complete plan set for a generation before a dbt
BUILD connection can attach to a model. Enrollment preserves the original plan,
command, reservation and executor identities. Its observation does not launch dbt,
reserve capacity, or grant permission to execute a model transaction.

This implementation slice requires a previously installed physical source bridge,
protected catalog registration/binding, and actual managed model membership.
Existing legacy materialization does not supply that membership. End-to-end
managed execution and the new signing path remain unqualified until the final
assembled source passes the approved live SQL Server 2022 environment. Mocked
transport tests and generated SQL comparisons do not establish that qualification.

## Application flow

1. Produce the existing canonical `PhysicalPlanSet`, with every predecessor
   `ABSENT`, under the complete workspace attempt and guard. Retain its original
   using the `mssql_physical_plan_set_v1` kind.
2. Record the actual parse, ls and build argv with exactly one literal `--vars`
   argument each. Their vars text must be identical. Its reserved
   `__dpone_managed` object contains only `generation_id`, `invocation_id`,
   `plan_set` (`locator`, `sha256`) and `runtime_registration_id`. No reservation
   or command reference is inserted later, and argv is never rewritten here.
3. Use the existing generation admission and executor binding. The enroller
   reads the actual bound plan, command and reservation versions. It compares the
   complete retained G request with the plan's workspace attempt, including
   activation ID, guard, subject, profile and command before checking membership.
4. The concrete `MssqlPhysicalPlanMembershipReader` authenticates the indexed
   source and complete selected model set. A DTO, digest or approval callback
   cannot substitute for this reader.
5. Call `MssqlPhysicalPlanEnroller.enroll` with the existing preparation deadline
   budget. SQL checks current source/P/G, protected registration and catalog
   binding, retained original projections and the complete absent namespace.
   It commits one immutable enrollment and returns its complete eight-field row.
   Python then opens a fresh connection and independently reads the same bytes.
6. The actual BUILD connection calls `physical_attach_session_v1`. SQL rechecks
   current source and enrollment, validates only that model's target, candidate
   and helper names, observes the actual connection, and commits one durable
   model-session row. The fifteen-field response includes the full executor and
   selected model-plan JSON required by the managed consumer.

The managed local schema is exactly `dpone_physical`; this does not change the
configurable schema of the legacy path. A model schema remains separate and
owned by dbo. The selected writable ordinary filegroup must match its retained
name and numeric ID; there is no PRIMARY or suffix-search fallback.

## Python composition and deadlines

`MssqlPhysicalPlanEnroller` accepts the exact registration and catalog binding,
original reader/binding ports, concrete membership reader, and original source
references. These values are comparison inputs; their construction grants no
SQL or archive authority. The application supplies all dependencies explicitly.

Its connection factory accepts the remaining timeout in seconds and returns a
fresh METADATA connection configured with that finite login timeout. Each SQL
phase creates its cursor after setting the remaining statement timeout. The
caller passes the preparation operation's existing `CatalogReadBudget` to
`enroll(plan_reference=..., executor=..., budget=...)` or the read-only
`read(...)` reconciliation method. The enroller never renews that deadline.
Original providers must themselves implement finite I/O; deadline checks bracket
acquisition and reject a response that arrives after the operation expires.

SQL owns these short transactions, so these endpoints require no ambient caller
transaction and the Python connection uses autocommit. Every response requires
exactly one row and no additional result set. Complete payload bytes, digest,
registration, generation, invocation and guard must match. Connection cleanup is
best effort and is not represented as an observed authority fact.

## Replay and uncertainty

The generation is the sole enrollment key. Identical replay can return the same
metadata only after current authority and namespace checks; differing complete
bytes or another invocation conflict. Enrollment does not update or delete an
existing row. `read` requires current P and is not a historical-read bypass.

A lost enrollment response, failed independent readback, expired deadline or lost
P produces `PhysicalEnrollmentError`; retain the same generation and originals
for read-only reconciliation. There is no automatic mutation retry, replacement
generation, capacity release, or cleanup of an uncertain operation.

Attachment admits at most one durable row per generation and model identity.
SQL compares complete UTF-8 model bytes on a hash lookup. A repeated attachment
rejects, including from the same connection. The row survives disconnect and
later model rollback. Connection ID, connection creation time, SPID and login
time come from SQL Server, not client labels or caller-supplied session IDs.
This row alone does not solve transaction binding, pool reset or model execution
replay. No bind, require-transaction, reset or receipt procedure is supplied by
this slice.

## SQL ownership and signing

The canonical `physical-v1/enrollment.sql` resource owns the two immutable table
definitions, three enrollment modules and shared validation fragments.
`physical-v1/session.sql` owns the BUILD attach and self-only connection observer.
The finite Python producers receive authenticated resource bytes and observed
certificate thumbprints; neither input type proves deployment authenticity.

A new E certificate signs enroll/read/attach and countersigns the control helper.
Its inventory does not change existing source/catalog/discovery certificates or
grants. Static access to existing protected tables follows the verified dbo
ownership chain; runtime direct DENYs remain. Existing callers retain their
source-entry permissions. Namespace assertions use the actual shared discovery
producer inline, with the new E metadata capability.

A distinct C certificate signs only the current-connection observer. Its server
certificate login has the exact SQL Server 2022 `VIEW SERVER PERFORMANCE STATE`
capability. Runtime users receive no direct DMV or observer-helper grant. The
helper rejects missing/multiple/MARS connection evidence and authenticates the
original SQL login. Installation must verify exact table, module, certificate,
login, signature and permission inventories in both database layouts before any
privilege claim. Provisioning and live evidence are separately integrated work.
