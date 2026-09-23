# SqlClient companion development

This directory contains the implementation in development for the optional
`mssql_sqlclient` transport. It is not yet an installable or certified transport.
BCP remains the default. See [the transport reference](../../docs/mssql-native-transport.md)
for current execution availability.

The production-preparation tools can create an unsigned, deterministic Linux
Arm64 companion bundle from two byte-identical locked offline builds. The bundle
is a separate artifact and is never embedded in a Python wheel. See
[BUILD.md](BUILD.md) for the exact producer, independent admission, SBOM/license
inputs, versioned read-only installation, upgrade and rollback procedure. A
packaged artifact is still not publication, runtime admission or route
certification.

## Native input checks

Run from the repository root with the repository Python dependencies installed
and the admitted .NET 8 SDK available. Generate the synthetic corpus outside the
checkout, then run the C# checks against that directory:

```sh
corpus_dir="$(mktemp -d)"
PYTHONPATH=src uv run python \
  packages/dpone-mssql-sqlclient/tests/Input/generate_corpus.py \
  --output "$corpus_dir"
dotnet run --project packages/dpone-mssql-sqlclient/InputChecks.csproj \
  -- "$corpus_dir"
dotnet run --project packages/dpone-mssql-sqlclient/BulkChecks.csproj \
  -- "$corpus_dir"
```

The producer creates 105 valid and invalid native-format cases. It records an
independent original-value oracle, the actual Python decoder's results, and the
paths and hashes of imported reference modules. The C# checks compare scalar
values, complete-input receipts, and rejection behavior, then exercise fragmented
reads, allocation boundaries, cancellation, lookahead, and stream ownership.
The corpus contains synthetic values only and requires no database or credentials.

For a read-only source mount, use `dotnet build` with
`-p:BaseIntermediateOutputPath=...` and `-p:OutputPath=...` pointing to writable
temporary directories, then run `dotnet <output-directory>/InputChecks.dll`
with the corpus directory as its argument.
The evidence directory must be writable for `corpus-results.json`.

`BulkChecks.csproj` restores the pinned SqlClient and Arrow packages. Its default
checks require no database: they exercise both input representations against the
same corpus, byte and row budgets, deadlines, and one-shot outcome retention.
The live harness is intended for an explicitly approved disposable SQL test
environment and performs schema/principal creation and cleanup. It is not a
production loading command.

These checks prove input-component behavior. They do not prove SQL delivery,
remote writer settlement, recovery, or end-to-end throughput. The caller owns
the authenticated input descriptor and stream; the reader does not open paths
or close the caller's stream. A receipt requires natural EOF and matching row
count, byte count, and hash.

## Input buffer accounting contract

The shared input budget counts owned buffer capacities for the admitted Arm64
runtime: eight bytes per row reference slot or retained numeric scalar, actual
byte-array lengths, and two bytes per UTF-16 code unit in retained strings.
Scratch input and its decoded result count simultaneously during conversion.
Arrow additionally counts values, offsets, validity, and simultaneous old and
new buffers during conversion or growth. Reservations precede allocation and
remain held for the lifetime of the owning row or batch.

This is a defined input-buffer metric. CLR object headers, garbage awaiting
collection, fixed configuration, hashing internals, and SDK allocations are
outside it; process address-space and GC limits address separate resources.
The encoded row limit remains independent. A large configured row limit alone
does not reject a small actual row. An actual conversion whose simultaneous
buffers exceed the budget fails without granting a complete-input receipt.

The native reader enforces this contract when constructed with an explicit
`InputBufferBudget`; the production writer must supply that shared budget.
Legacy constructors remain available without a decoded-buffer budget. The row
and Arrow components share this ledger, including Arrow conversion and retained
batch buffers. Their guarded worker integration remains in progress. These
component changes do not enable the transport execution described above.

`SealedInputStream.Admit` reserves a fixed 65,536-byte read-ahead buffer from
that same ledger before allocation. Pass the ledger to the five-argument
`RowsInput` constructor or the budget-aware Arrow reader. Disposing a reader
releases its own reservations; the transport reservation remains until the
stream is disposed. Admission checks the inherited read-only regular-file
descriptor, original file identity and initial offset. Natural EOF checks the
identity again before a complete-input receipt can be produced.

This accounting covers the native decoder's `Read(byte[], offset, count)` path.
Inherited span and asynchronous stream APIs are not certified under this budget.
`SealedInputChecks.csproj` checks descriptor admission, block reads, content,
EOF, drift and ownership; `RowsBudgetChecks.csproj` checks shared reservations,
allocation denial and disposal. These are component checks, with no measured
end-to-end throughput or completed SQL delivery claim.

## Startup and session control checks

`StartupComponent.csproj` builds the guarded startup library.
`StartupChecks.csproj` runs closed launch parsing and Linux pipe framing controls
without SQL or credentials. Run its executable with the repository's
`tests/fixtures/mssql_sqlclient` directory as the sole argument. With a read-only
source mount, build to writable intermediate/output directories as above, then
invoke `dotnet <output-directory>/StartupChecks.dll <fixture-directory>`.

The frozen launch vectors check canonical digests, nonzero UUIDs and retained
attempt/input bindings. Pipe controls require actual EOF and reject oversized,
truncated, trailing or stalled frames. These checks do not establish parent/thread
death behavior or successful guarded startup; those require separate process
integration evidence. A startup timeout does not become a pass because the
worker was successfully contained afterward.

The additive Python session-control contract binds a bulk grant to its original
launch, owner/fence, process, input, build, object, full independently observed
SQL session and original operation deadline. The observation reference identifies
immutable evidence bytes. The separately named grant digest identifies canonical
wire content. A parsed announcement or grant is not SQL authority: the supervisor
must establish the observation, acknowledge durable intent and send once. Managed
session integration and full-route certification remain pending.

After sending a job, the supervisor uses `receive_session_or_result` to wait for
an announcement or an early failure under the original operation deadline. An
empty job can receive its result directly. Started result bytes take priority
through EOF; receiving a result permanently disables the grant phase. A complete
announcement triggers a final nonblocking result check before being returned.
This arbitration is not an atomic SQL grant: the supervisor and worker must still
validate their phases and original authority before mutation. Raw result bytes
remain available even if closing their pipe subsequently fails.

`SessionComponent.csproj` builds the typed session-control component with startup
bindings. `SessionChecks.csproj` checks the Python wire fixtures; its first
argument is the same fixture directory used by startup checks. An optional second
argument supplies a synthetic Unicode differential-vector JSON file. Canonical
string values use literal UTF-8 for valid Unicode scalars and only JSON-required
escapes, matching Python's `ensure_ascii=False`; unpaired UTF-16 surrogates fail.
These component checks still perform no SQL operations or credential delivery.

The managed session component also encodes the closed result envelope. The four
shared fixtures cover nonempty success, empty success, failure before a grant and
failure after a grant. A nonempty successful result requires the original grant
and exact input receipt. Failed bulk diagnostics never become successful input
receipts. Result validation alone does not prove remote settlement or publication.

The Python `SqlClientCredentials` record is memory-only and hides its fields from
`repr` and validation errors. TLS selection is mandatory; `disposable_test` must
be authorized separately by trusted synthetic-environment composition. The record
performs no connection or authorization. Never log or persist credential/job
bytes, or derive evidence from a password hash. Python cannot guarantee secret
zeroization. Full job delivery, worker orchestration and route activation remain
pending; BCP remains the default.

The Python SqlClient job codec preserves the full attempt, ordered input metadata
and declared file identity. Admission compares these with the original launch,
owner, object, transport policy, nonce and TLS profile under the original operation
deadline. Empty jobs require null credentials and nonce. The evidence binding
projects only nonsecret fields and a credentials-present flag; it never hashes
credential bytes. Trusted composition must separately establish policy identity,
TLS authorization, actual input observations and one-shot delivery.


The unreleased grant now requires `resolved_database_principal` with the exact
principal ID, name and canonical hexadecimal SID. A catalog mapping is distinct
from the worker's effective context: the worker must compare its initial and
immediately rechecked database-principal tuple before copying. This field conveys
no extra privileges. Missing principal data rejects the grant; the older unrefined
synthetic fixture is retained as a rejection control. Coordinator and restricted
writer credentials remain separate.

`JobChecks.csproj` exercises the independent rows, Arrow/Unicode/UInt64 and empty
vectors in `tests/fixtures/mssql_sqlclient/jobs`. `ClockChecks.csproj` checks the
same Linux nanosecond epoch used by startup and bulk deadlines, with no arguments.
These checks do not establish writer-session authority or complete-route success.

## Restricted writer session component

`SingleWriterSession` owns one nonpooled `SqlConnection`, disables retries, MARS
and ambient enlistment, and requires encrypted transport. Certificate validation
can be relaxed only by both an explicit disposable-test credential profile and
trusted test composition. Opening, nonce initialization and subsequent context
checks use the original operation deadline. Repeated or overlapping operations
permanently invalidate reuse; a failed connection is never reopened.

The self-observation query reads only the current session and principal. Its
integer projection explicitly returns SQL `int`; the managed decoder does not
silently coerce smaller or unrelated types. The exact stored nonce comes from
the session DMV because the scalar `CONTEXT_INFO()` representation can be padded.
The session compares current and original login context and the database
principal separately. Its client connection ID is a local continuity diagnostic,
not the parent observer's server connection identity or authority digest.

`WriterSessionChecks.csproj` runs hermetic snapshot, policy, cardinality,
concurrency and deadline checks by default. Its explicit `synthetic-live` mode
accepts a bounded credential DTO through stdin and checks opening, same-session
observation and disposal against an approved disposable SQL environment. Never
persist that input. This mode does not copy rows or establish remote settlement.

Composition must admit the grant and serialize all use of the borrowed connection
before invoking the existing bulk writer. Retain result bytes before disposing
the session; a blocked provider cleanup still requires external process
containment. Worker execution, durable parent evidence and full-route activation
remain unfinished.

## Post-startup execution component

`WorkerRun` owns one post-startup context and executes one bounded Job. It uses
one shared input budget and the original operation deadline. Empty input must
reach verified EOF without opening SQL, announcing a session or reading a grant.
Nonempty input opens one restricted session and accepts one grant before bulk
copy. Grant admission checks launch, attempt, input, process, stage, owner/fence
and the writer's observable session/principal identity. Privileged server facts
remain authenticated parent attestations; the restricted worker does not invent
an independent observation of them.

The worker retains exact result bytes before delivery and fallible cleanup.
Failed delivery produces an unsuccessful exit without replacing or resending
the result. An expired frame deadline is reported as `operation_timeout`; no
fresh delivery allowance is introduced. The parent still owns durable evidence,
process reaping, remote settlement, verification and checkpoint promotion.

`WorkerRunChecks.csproj` accepts the jobs fixture directory as its sole argument.
It tests declaration/grant mutations and executes empty rows/Arrow jobs through
actual Linux pipes, including malformed input, withheld EOF, concurrent reuse
and broken result delivery.

Its explicit `synthetic-live` mode exposes a bounded interactive test-controller
protocol on stdin/stdout. It freezes nonsecret Job/launch bindings before running
the actual worker; credentials go only through the private Job pipe. The test
controller must independently observe the SQL session, provide one grant, inspect
SQL values and require both the completion event and terminal process status.
The worker continues to use separate real session, grant and result pipes.

Local nonempty component cases cover rows and Arrow with typed value checks,
rejected grants, partial committed batches, broken result delivery and withheld
grant deadlines. These are post-startup component checks, not guarded production
startup, durable parent evidence or complete-route certification. SDK disposal
exception injection remains unverified. The seven-day TDS comparison has not run.

P10f verification after local writer exit keeps its existing exact typed
multiset contract but computes row hashes on SQL Server. The management helper
receives one aggregate row with count and fixed word sums; it never fetches
business rows. A temporary hash heap bounds work at expected rows plus one and
requires `tempdb` capacity for 32 hash bytes per expected row plus heap overhead.
Python/T-SQL differential tests for every admitted type are a deployment
requirement. `count/min/max` is not publication authority.


## Fixed production entry candidate

`Worker.csproj` builds `Dpone.Mssql.SqlClient.Worker.dll` from the existing
startup, Job, session, input, bulk and execution components. It excludes test
sources. The entry accepts exactly the four argument pairs supplied by the fixed
Python launcher: `--launch`, `--companion-root`, `--runtime-root` and
`--deployment-manifest`. It rejects duplicate or unknown keys and bounds the
launch to 16,384 UTF-8 bytes before encoding. Startup owns deployment admission
and readiness; the existing `WorkerRun` owns Job, session, grant and result
handling. Production always passes `allowDisposableTest: false`. Ordinary entry
failures return exit 70 without exception text on stdout or stderr.

This framework-dependent candidate targets Linux Arm64, .NET runtime 8.0.31,
Microsoft.Data.SqlClient 7.0.2 and Apache.Arrow 23.0.0. BCP remains the default.
Deployment supplies the separately admitted runtime; the SDK is a build
prerequisite only. `Worker.packages.lock.json` is the dedicated NuGet-generated
production lock. Build with the approved SDK 8.0.425, an explicitly supplied
package feed, locked restore, Release configuration and isolated intermediate
and output paths, followed by publish with `--no-restore`. Do not disable locked
restore or rewrite generated dependency/runtime configuration to admit outputs.
The build pins the runtime with roll-forward disabled, server GC disabled and a
536,870,912-byte GC heap hard limit. Publish XML/PDB diagnostics must remain
outside the eventual runtime inventory.

`EntryChecks.csproj` checks the fixed argument contract, strict UTF-8 bounds and
entry failure exits without SQL. Its optional sole argument is a publish output
directory: the checks then validate actual SDK-generated runtime configuration
and dependency closure through the existing managed validators. Existing startup
and WorkerRun checks remain
separate regression targets. A build or empty-input success does not establish
restricted-writer authority, durable parent evidence, remote settlement or
successful SQL delivery. See [BUILD.md](BUILD.md) for the fixed offline build and admission producers.
Companion installation, full writer integration and guarded production process
tests remain separate qualification work. The seven-day TDS comparison remains unexecuted.


The Linux host also needs the operating-system dependencies of .NET 8, including
ICU for this normal-globalization profile. Copying the runtime directory alone
does not supply those native libraries. Follow Microsoft's
[.NET 8 OS package requirements](https://github.com/dotnet/core/blob/main/release-notes/8.0/os-packages.md)
for the chosen distribution; Debian 12 uses `libicu72`. Invariant globalization
is not an admitted substitute in this profile. OS dependencies belong to the
separately admitted deployment platform, while the companion and runtime retain
their own fixed inventories. A successful dependency check still requires guarded
startup qualification on that actual platform.
