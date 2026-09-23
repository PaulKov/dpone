# SqlClient CREATE departure evidence

Runtime maintainers and operators use this explanation to distinguish a finished
CREATE connection from a new observer connection with the same SQL Server session
number. This is a design under implementation. It does not enable SqlClient bulk
execution; see the [current transport status](../mssql-native-transport.md#sqlclient-transport-development-status).

## Why the session number is insufficient

After CREATE closes, SQL Server can reuse its numeric session ID (SPID). The
dedicated observer can receive that number when it connects. A check for any row
at the old SPID then observes the observer itself and cannot return zero.

Version 1 deliberately rejects every live row at the old SPID, including a reused
incarnation. Its six counters mean literal raw zero observations. Neither a retry
nor removal of the observer row from the query may change that evidence's meaning.

The version 2 design retains raw counts and separately identifies the observer's
own contribution. It requires a different connection UUID, later connect and
login times, the same continuously held physical connection, and unchanged
server, database, login and transport context. Unrelated SPID reuse still fails.
Original connection UUID matches and its MARS children must remain absent.

## What a successful observation must contain

The observer samples connections, sessions, requests, transactions, connections
and sessions, in that order. For an observer using the original SPID, the admitted
target pattern is:

| Sample | Raw rows | Proven observer rows |
| --- | ---: | ---: |
| Connections, before and after | 1 | 1 |
| Sessions, before and after | 1 | 1 |
| Requests | 1 | 1 |
| Session transactions | 0 | 0 |

The request must belong to the actual observer connection and session. A second
request, NULL or unrelated connection UUID, any transaction row, or an additional
unclassified row prevents success. Transaction rows are never subtracted. When
the observer has a different SPID, all six raw and own counts must be zero.
These acceptance rules require live qualification on the admitted driver/server
profile; a constructed record cannot establish that SQL returned them.

One complete observer incarnation is retained. Before and after each of the six
samples, the helper reacquires the complete identity and context from its actual
connection. Twelve separate digests bind those observations to the retained
incarnation. This requires thirteen full identity/context acquisitions in total:
the initial acquisition plus twelve guards, separate from the six sweep samples.
One full acquisition may require several SQL statements. Reusing a cached digest
without reading SQL is not an observation.
Any change permanently invalidates the sequence, even if a later read matches.

The approved pure-record slice uses a closed
`dpone.sqlclient.create-departure.v2` record capped at 16 KiB. Canonical codecs
must reject malformed original types before normalization or hashing. Raw and
own counts remain distinct fields; v2 does not provide a synthetic legacy
six-zero `counts` field. The internal v2 composition explicitly selects the
version before effects and requires a separately configured observer admission.
It snapshots that expectation and uses the existing CREATE and helper lifecycle;
an optional field or failed decode never selects another protocol.

Each acquisition samples `XACT_STATE()` into a fresh batch-local variable before
its metadata SELECT and requires the actual sampled value to be zero. The SELECT
independently checks transaction nesting, implicit mode and session transaction
associations. This separates entry-state measurement from work performed inside
a read statement; it never converts an observed nonzero state to zero. The fixed
batch returns one bounded result set. Thirteen such batches and the other checks
use 27 execute calls and 40 SQL statements, without a measured speed guarantee.

## Evidence is not execution authority

Record validation establishes structural consistency. The parent must still
retain the result of its actual original CREATE call, acknowledged CREATE and
helper records, matching process identities, actual zero exits and completed
local cleanup. SQL snapshots are sequential, not an atomic cross-DMV snapshot.

Successful departure does not authorize `Prepared`, a bulk grant, publication or
a checkpoint. Current staging identity, emptiness, restricted-writer rights,
durable preparation and subsequent writer settlement remain required by the
[TDS recovery decision](../adr/0072-bounded-tds-importer.md).

## Current stage checks after departure

The separate internal stage reader now compares the original identity with two
actual catalog observations around an emptiness query. Its intermediate profile
covers SQL Server 16–17 and at most 100 columns using `BIGINT`, `FLOAT(53)`,
`NVARCHAR(MAX)` and `DATETIME2(6)`. Management requires database `VIEW DEFINITION`, database `ALTER ANY SECURITY POLICY` and
object `VIEW DEFINITION`; missing permissions and security predicates reject
admission. It does not require sysadmin.

This reader returns observations, not launch permission. Blocking SQL needs the
external bounded helper, and sequential reads do not protect future consumption.
Live metadata and RLS qualification, preparation and full writer integration
remain required before the route can be used.

## Admit a fresh v2 attempt

Internal callers first acquire the target lease, call
`admit_sqlclient_state_domain`, then pass its returned factory to
`create_tds_attempt` with `backend="mssql_sqlclient"`. Reserve the original CREATE
operation through that attempt, then retain the exact factory for
`run_sqlclient_create_departure_v2`. A second wrapper over the same store or
an admission obtained after creating the attempt cannot replace it.

The composition acknowledges an immutable stage locator after coordinator
intent and completes the locator actor before starting CREATE. The caller's
existing actor pool must have capacity for this transient actor. Failure or
uncertain teardown prevents CREATE and retains the actual actor for cleanup
within the original budget. A saved locator supplies discovery coordinates;
it does not authorize retry or preparation.

Existing internal v2 callers must adopt this order when creating new attempts.
Do not backfill origin bindings or locators for historical attempts. V1 and the
default BCP route keep their existing behavior. Full SqlClient writer integration
and route qualification are still pending.

## Authenticate original CREATE evidence

Internal preparation callers can use `collect_authenticated` with an
independently configured `PinnedEvidenceReadFactory` and the already retained
OBSERVE handle. The method returns historical observations only after fresh
store/file actors and closing journal/SQL checks succeed. It does not accept an
in-memory original outcome or caller-provided receipt tuple. It shares the
collector's one-use guard with raw `collect`; choose one method for that instance.

A v2 producer saves the original snapshot and all six acknowledged receipts
before departure. Retain the authoritative store and evidence directory
together. If a seal or file is unavailable, keep the original table and
investigate its operation. The reader cannot backfill, repair evidence or
authorize CREATE replay. Existing unsealed attempts cannot use this new
authenticated path.

Configure an absolute evidence path under trusted deployment custody, including
its ancestors. Required no-follow/directory/nonblocking file operations must be
available. Symlinks, special files, changed identities, unexpected sizes/hashes
and current coordinator errors reject acquisition. On UNKNOWN retain the actual
failed gateway; bounded `close_gateway` is available, and `close_locator` remains
a compatibility alias. A historical CREATE observation is not Prepared, current
permission acceptance, emptiness or remote settlement. Full writer integration
and live qualification remain pending.

## Diagnose and recover

Retain the original failed attempt and its last acknowledged evidence. A helper
failure after credential intent means SQL access may have occurred; missing
RESULT, LOCAL_EXIT or EXCLUSION records cannot be reconstructed as success from
a later observation. Containment does not renew the operation budget.

Do not retry a consumed v1 sequence as v2, replace its backend, or rewrite its
records. Version 1 bytes and interpretation remain unchanged. Version 2
selection applies only to fresh, explicitly admitted attempts. Older readers
must reject unsupported v2 records; there is no down-conversion to v1.

Cleanup requires independent confirmation of original-session departure and the
exact owned table incarnation. A table name alone cannot authorize DROP. Keep
unknown resources until that reconciliation succeeds. Reconciliation may remove
owned resources without changing the original failed run into a passing run.

Return to [delivery acceleration](index.md). The
[existing BCP operations and recovery guide](operations.md) covers the established
route; it is not an execution or recovery recipe for unfinished SqlClient wiring.


## From original CREATE evidence to Prepared

The internal CREATE/departure result is not automatically a settled directory
slot. Its trusted owner must explicitly acknowledge local containment and remote
settlement through `settle_sqlclient_create_departure` before reserving OBSERVE.
The entry point consumes the registered original result; accepting an equivalent
caller-created object or retrying a lost acknowledgement is not supported.

The subsequent preparation component authenticates original CREATE evidence and
current inventories, verifies effective permissions and management restoration,
and validates the borrowed input descriptor, policy and managed build. It writes
create-only preparation evidence before acknowledging PREPARED. The original
management identity, actual child exit and evidence remain available after
cleanup for the next independent departure check. PREPARED alone does not allow
a bulk grant, writer launch, publication or checkpoint advancement.

Real permission-baseline admission is still closed pending independent platform
qualification. The public optional writer route remains unavailable; use the
default BCP route for supported execution. Synthetic component checks establish
ordering and rejection behavior, not live SQL or performance certification.

For exact identity domains, acknowledgement ordering and UNKNOWN behavior, see
[the preparation decision](../adr/0072-bounded-tds-importer.md#original-create-settlement-and-durable-preparation).

## Record the OBSERVE verifier intention before startup

The original preparation observer and the later departure verifier are different
processes. The verifier's launch intention contains its immutable plan and the
independently configured verifier admission. Persist and acknowledge that
intention before starting the verifier; actual startup identity belongs in the
subsequent registration and request. Never construct a placeholder startup to
serialize the earlier intention.

The plan-only codec path and the full-request validation path must produce the
same canonical launch-intent bytes. Both reject malformed nested values and a
mismatched verifier admission. A codec receipt describes those bytes; only the
original evidence actor's acknowledged write establishes persistence.

The internal OBSERVE settlement composition enforces this ordering. It does not
grant SQL permissions or start bulk copy. The full writer and live route
qualification remain pending.

## Preserve original evidence through preparation cleanup

The pending OBSERVE settlement consumes the original preparation owner. Retain
the actual ADMISSION, REGISTRATION and AUTHORITY payloads with their acknowledged
receipts at the producer, together with the actual preparation payload and
receipt. Saving only the last evidence observation loses earlier acknowledgements.
Re-encoding a matching record later cannot restore that execution history.

Retain pending preparation bytes before attempting the evidence write. A write
may persist the file and lose its acknowledgement; keep that outcome unknown
without replaying the write or accepting a later matching file as success.
Original gateway, factory and pool references are pinned separately from their
value snapshots. Validation after cleanup uses retained observations and does
not reopen closed coordinator or evidence actors.

The payload limit is per attempt: at most 8 MiB of preparation bytes and 64 KiB
across the three coordinator payloads. Immutable bytes remain parent-local and
are not copied into the helper request. These limits do not establish an
aggregate memory or throughput guarantee for concurrent attempts.

## Keep parent expectations separate from helper input

The private OBSERVE helper envelope uses the existing three pipes. The trusted
parent retains its expected request and independently configured verifier
admission before sending the envelope. It must validate the returned result
against those originals, including the actual registered startup.

The helper checks the envelope against its fixed startup, build, connection
profile, resource limits and original deadlines. It executes the supplied plan
and validates actual SQL observations against that plan. This is consistency
checking in the helper; it is not a second independent configuration channel.
Never derive the parent's expected identity or admission from the result.

The helper performs one connection attempt and uses the existing guarded
connection/session/request/transaction census. SQL closure must succeed before
the result frame is sent. A result followed by failed local cleanup or a nonzero
helper exit cannot authorize settlement. The parent settlement composition
checks those outcomes; writer integration and live qualification remain required
before public route use.


## Complete the original preparation settlement

The trusted internal `settle_prepared_observe` entry consumes the actual original
successful preparation with the same admitted factory and actor pool, separately
configured verifier admission and bounded deadlines. It accepts neither caller
proofs nor substituted results.

The internal settlement owner retains the original prepared attempt before
entering its guarded sequence. Registration consumes that continuation before
any clock, actor or launcher callback. An unentered owner is not permission to
run a writer; a failed or interrupted continuation cannot be replaced or retried
to recover forward authority.

The owner records the original observer's containment evidence before the local
directory acknowledgement. Each helper phase separately retains attempted bytes,
the actual write return, the actual observation and the validated acknowledgement.
A returned receipt followed by a failed observation remains an unknown outcome;
matching bytes found later cannot repair that execution.

A remote directory acknowledgement is not the final success boundary. Evidence
actors must close, and the final guard must still match the original prepared
lifecycle, directory snapshots, owner, factory, process and helper observations.
It checks both journals again after the last actor callback. Only successful
context exit releases the pending continuation for subsequent work. Changing a
cached journal snapshot or retained helper after an earlier successful check
must prevent completion, even when all evidence files remain unchanged.

On failure, retain the same resources and their actual observations for bounded
cleanup. The first cleanup bound is consumed even if invalid; a later call cannot
supply a fresh allowance. Repeated bounded waits for the same actor may finish
shutdown within that original allowance. This does not permit another raw child
close, replay an evidence write or establish remote SQL settlement. Explicit
journal teardown remains available after the attempt is poisoned.

`SqlClientObserveDepartureUnknown.retained` exposes the original retained
observations and resources for diagnosis. Its `close(deadline=...)` performs
bounded cleanup on those same resources; it cannot retry settlement, clear the
pending or poisoned state, or replay a raw child close. A successful outcome and
its receipts report historical observations, not reusable execution authority.

These internal guarantees do not enable the public SqlClient writer. Permission
and bulk-grant delivery, verified target receipts and
live route qualification remain separate acceptance work. Continue to use the
[current transport status](../mssql-native-transport.md#sqlclient-transport-development-status)
for supported execution choices.
