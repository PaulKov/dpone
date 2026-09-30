# MSSQL SqlClient target-local verification v2: certification and rollout

This normative appendix belongs to the [approved feature specification](mssql-sqlclient-target-local-verification-v2.md). It owns market comparison, measurable targets, privacy, test evidence, documentation, and rollout.

## Market comparison

Facts below were checked from official primary documentation on 2026-09-27.

| System/version | Relevant capability | Observed design | Strength | Limitation | Adopt/reject | Source/date |
|---|---|---|---|---|---|---|
| dlt 1.30 | MSSQL loading | Insert-values default; optional ADBC/Parquet path; staging-optimized replace uses transactional schema transfer | Capability-based faster path and atomic replace | Its documented type limits and transaction/file model are not dpone's native-wire recovery proof | Adopt explicit capability; reject equivalence claims | [dlt MSSQL](https://dlthub.com/docs/dlt-ecosystem/destinations/mssql), 2026-09-27 |
| Informatica | Managed bulk integration | N/A for this narrow open-source connector adapter decision | N/A | Managed product layer is outside the selected writer-boundary axis | N/A | No source consulted; excluded from evidence, 2026-09-27 |
| Airbyte | Connector-based replication | N/A for SQL Server native-wire digest and bounded publication | N/A | Different route contract | N/A: compare only with a future reproducible MSSQL benchmark | No source consulted; excluded from evidence, 2026-09-27 |
| Fivetran | Managed incremental replication | N/A for user-operated bulk writer and recovery journal | N/A | Managed service is outside this implementation axis | N/A | No source consulted; excluded from evidence, 2026-09-27 |
| Pentaho | Bulk-load ETL | N/A for the current dpone route contract | N/A | No comparable digest/recovery contract was evaluated | N/A | No source consulted; excluded from evidence, 2026-09-27 |
| Microsoft SSIS / SQL Server 2022+ | OLE DB fast load | Supports table lock, keep-null, constraint, rows-per-batch, and maximum commit-size controls | Mature SQL Server bulk-load controls | Batch commit choices can expose partial progress and do not provide dpone receipts | Adopt explicit mappings/nulls/lock and bounded batches; retain dpone authority | [Microsoft OLE DB destination](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination?view=sql-server-ver17), 2026-09-27 |
| Microsoft.Data.SqlClient 6.1.7 | Native SQL Server bulk copy | `SqlBulkCopy` exposes streaming, batch size, timeout, mappings, rows copied, and transaction choices | Direct supported TDS bulk path | Writer completion alone is not end-to-end content proof | Pin 6.1.7 for P2 and retain independent digest | [NuGet 6.1.7](https://www.nuget.org/packages/Microsoft.Data.SqlClient/6.1.7), [SqlBulkCopy API](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlbulkcopy?view=sqlclient-dotnet-core-6.0), checked 2026-09-28 |
| .NET 10 LTS | Companion runtime | Supported through 2028-11-14; .NET 8 support ends 2026-11-10 | Long operating horizon for a new backend | Adds a newer runtime prerequisite | Target `net10.0`; reject .NET 8 as the GA baseline | [.NET support policy](https://dotnet.microsoft.com/en-us/platform/support/policy), checked 2026-09-28 |
| gusty | DAG authoring | N/A | N/A | Does not implement this data-plane writer | N/A | No source consulted; excluded from evidence, 2026-09-27 |
| Astronomer Cosmos | dbt orchestration | N/A | N/A | Does not implement this data-plane writer | N/A | No source consulted; excluded from evidence, 2026-09-27 |
| Apache Beam | Distributed data processing | N/A for a first SqlBulkCopy adapter | N/A | A Beam runtime would be a different architecture | N/A | No source consulted; excluded from evidence, 2026-09-27 |

## Measurable differentiation

```yaml
axis: confirmed visibility latency with recoverable versioned verification
scenario: fixed UTC half-open seven-day window partition-pruned by technical load time
baseline: current released bounded native BCP route on the same commit-compatible environment
metric:
  - confirmed_visibility_seconds
  - business_rows_read_back
  - rows_per_second
  - peak_rss_bytes
  - recovery_matrix_status
target:
  hard_fail_at_or_above_seconds: 3600
  median_at_or_below_seconds: 1200
  worst_of_three_at_or_below_seconds: 2700
  p90_at_or_below_seconds: 2700
  p90_minimum_measured_runs: 10
  business_rows_read_back: 0
  atomic_visibility_boundaries: 1
procedure: one warmup plus at least three measured runs per unchanged commit, environment, schema, configuration, and source authority; correctness and recovery gates precede performance
artifact: private versioned qualification receipt outside the repository; synthetic public receipts contain no private identities
limitations: establishes only the measured bounded route and workload; it is not a universal product ranking
```

Candidate median should additionally be no more than half the released BCP
baseline median on the same environment. This ratio is advisory until both
receipts bind the same environment/workload digest; the absolute gates remain
mandatory.

## Security, privacy, and operations

- Credentials are provided to the child over an inherited anonymous pipe after
  process creation. The payload is length-bounded, never placed in argv or the
  environment, read once, zeroed where the runtime permits, and both pipe ends
  are closed before result emission. No credential file is created. Credentials
  never enter plans, receipts, logs, or tracebacks.
- A grant is issued only after revalidating exact database-independent target
  identity, object ID, owner binding, schema/layout digest, attempt, and sealed
  artifact identity. Connection values are not part of public identity.
- The companion has one job, one bounded reader, one destination, and one
  absolute deadline. It emits a closed protocol with size limits.
- `FireTriggers` is disabled. Computed/service columns are excluded from
  writable mappings. `KeepNulls` is required. Table locking is used only after
  capacity and ownership checks.
- Local privileged operational records may contain exact database object
  coordinates needed for recovery and are protected by the existing journal
  permissions. Shareable evidence contains only opaque invocation/stage IDs and
  digests. Sanitization occurs when the sidecar is produced; it does not erase
  the local recovery authority.
- Operational metrics include phase latency, throughput, peak RSS, allocated
  and log bytes, retry count, aggregate verification rows, and outcome class.
- Alerts fire on unknown outcome, capacity stop, digest drift, source-authority drift,
  deadline exhaustion, and evidence/checkpoint failure.
- Shareable and private-export certification artifacts forbid host, port,
  login, database, schema, table, query text, connection string, row samples,
  absolute paths, and vendor exception text. Privileged recovery records may
  retain exact object coordinates under their separate access policy.

## Test and certification plan

| Layer | Scenario | Environment | Expected artifact |
|---|---|---|---|
| Unit | SQL/Python canonical bytes and 256-bit limb/carry parity for every allowed raw/prepared type, NULL, empty, duplicates, reorder, and extrema | Hermetic | Differential report |
| Unit | Streaming native decoder, mappings, timeout/cancel, malformed protocol, bounded buffering | .NET/Python hermetic | Test report |
| Contract | Omitted selector remains BCP; explicit SqlClient has no fallback; v1/v2 journal separation | Hermetic | Contract tests |
| Contract | Evidence rejects secrets, endpoints, object/query identities, absolute paths, and raw exceptions | Hermetic | Privacy-lint report |
| Integration | Partial write, lost ACK, worker crash, deadline, digest/schema mutation, EOF boundaries, unknown commit | Mocked target/process | Recovery matrix |
| Docker live | Real SqlBulkCopy narrow 10k/1m and versioned wide100 10k/1m; existing 200-column profile remains stress coverage | Native x86-64 Linux Docker | Versioned synthetic receipt |
| Docker live | Force-kill real SqlBulkCopy during a batch; prove app-lock/table-lock barrier waits for rollback, repeated digest stability, `UNKNOWN` retention, and no premature drop/retry/prepare | Native x86-64 Linux Docker | Quiescence recovery receipt |
| Runtime/live | Lose publication acknowledgement after commit and reconcile the exact durable receipt before any replay | Synthetic SQL transaction harness and full runtime live matrix | Receipt-first recovery receipt |
| Docker live | Differential SQL/Python parity for every generated admitted raw and prepared layout, nullable/fixed/max framing, precision/scale, collation, and boundary value | Native x86-64 Linux Docker | Layout matrix receipt |
| Docker live | Empty interval, duplicates, late change, outside-window invariance, rollback, receipt-first recovery | Native x86-64 Linux Docker | Correctness/recovery receipt |
| Performance | Warmup plus three measured narrow and wide100 runs; ten runs before p90 claim | Stable Docker profile | Benchmark receipt |
| Private live certification | Fixed UTC half-open seven-day technical-load window, rerun idempotency, failure injection, exact commit | Approved private environment | External private receipt only |
| Compatibility | Released BCP manifests, plans, journals, examples, and import paths | Hermetic and Docker | Compatibility report |

The public Docker campaign uses two separate immutable receipts. First,
`tools/mssql_sqlclient_certification_image.py` builds an exact clean Git archive
and records the inspected image digest. Then
`tools/mssql_sqlclient_certification_runner.py` resolves the requested tag back
to that digest, creates every cell from the immutable `sha256:` image ID, checks
each container's configured image before start, and binds the seven artifact
hashes into one runner receipt. The campaign closer accepts this execution
receipt; a caller-provided image digest or alternate baked identity path is not
sufficient.

The approved certification environment requires a native x86-64 Linux Docker
daemon. ARM emulation remains useful for diagnosis but cannot certify this
route. Runner receipt v4 records the daemon architecture; campaign v3 requires
`amd64`. The exact-master GitHub workflow runs the seven transport cells and
fifteen full-route cases in the same immutable image, rejects skipped route tests,
and retains their receipts. SQL Server uses a 3 GiB engine cap on a host with at
least 8 GiB available. This changes certification authority only; the selected
writer and runtime invocation contracts are unchanged.

The versioned runner resource envelope is part of the certification authority:
48 MiB maximum encoded/IPC frames, one additional pending slot, two encoder
workers, up to two import workers, and a 2 GiB runner cgroup. The wide100 profile
also caps frames at 8,192 rows, giving at most 123 raw stages for one million
rows plus the prepared-stage reservation under the 128-table ceiling. Before
execution, the runner inspects Docker's applied memory, memory-plus-swap, and
OOM-killer settings. Its receipt records them with the largest sampled Docker
CLI cache-adjusted memory value; this polling metric is not a cgroup high-water
mark. Setting drift, an unavailable sample, or an OOM kill fails closed.

The force-kill cell has no time-based injection. Its independent SQL Server
observer waits for an active `INSERT BULK` request whose per-attempt application
name, exact grant application lock, and exact stage lock all match the supplied
process request, then kills that companion process group. An unrelated concurrent
SqlClient session cannot satisfy the observer. Recovery
must acquire the same lock barrier and obtain two identical count/digest
observations before it may persist `PARTIAL_PROVED` and retire the exact stage.

Small synthetic fixtures additionally compare the complete typed multisets on
both sides and therefore provide exact equality for those finite fixtures.
Large Docker and private workloads use exact identity/schema/count checks plus
the versioned probabilistic multiset digest; their status and documentation
must not label content equality as mathematically exact.

Two wide contracts are generated deterministically from checked-in seeds and
publish schema digests. `wide100-verifier-v1` has exactly 100 business columns
before service columns and cycles through every current BCP scalar, text,
binary, decimal, and temporal family. `wide100-sqlclient-v1` has exactly 100
business columns and deterministically repeats the four SqlClient v1 types.
Both mix nullability, exercise identifier boundaries, and fix byte-size,
null, duplicate, and skew distributions. The existing 200-column profile is an
additional stress case, not a substitute.

Checked-in fixture descriptors are authoritative, not prose-generated at test
time. `narrow-sqlclient-v1` uses seed `20260927`, four business columns in order
(`bigint`, `float(53)`, `nvarchar(max)`, `datetime2(6)`), alternating
nullability where legal, and 10k/1m row sizes. `wide100-sqlclient-v1` uses the
same seed, 25 columns of each SqlClient v1 type, round-robin order, alternating
nullability, and 10k/1m rows. `wide100-verifier-v1` uses seed `20260928`, 100
business columns allocated round-robin across the generated admitted type
families, and 10k/1m rows. Column names exercise lengths 1, 64, 127, and the
current SQL identifier maximum without collision.

Across descriptors, row `i` is NULL in nullable column `j` when
`(i + j) % 11 == 0`; every 13th generated business row duplicates the prior
row; text/binary lengths cycle through `0, 1, 31, 255, 4095, 65535` within the
column and row byte limits; one max-row case occurs every 65536 rows; numeric
and temporal boundary vectors occur every 257 rows; remaining values come from
the named deterministic generator. Each descriptor freezes generator version,
column list, type parameters, distributions, row-byte limit, and expected
schema SHA-256.

Private qualification freezes `[interval_start, interval_end)` in UTC from the
invocation data interval and prunes source partitions by technical load time.
Before and after extraction it records a private source authority consisting of
partition identity, count, and versioned digest. Mutation/merge drift makes
correctness and performance `UNVERIFIED`; that run is excluded from all
statistics. The authority never enters public artifacts.

The performance status function excludes warmup and retains failed/timeout
samples. Any measured run at or above 3600 seconds is `FAIL`. With three valid
runs, median above 1200 seconds or worst above 2700 seconds is `FAIL`. p90 is
computed only with at least ten valid independent runs; otherwise p90 is
`UNVERIFIED`. Missing phase metrics, identity mismatch, source-authority drift,
correctness/recovery failure, partial publication, or ambiguous retry makes the
run `FAIL` when safety is violated and otherwise `UNVERIFIED`. Release
acceptance requires the absolute latency gate, all correctness and recovery
cells, `business_rows_read_back=0`, `bcp_process_count=0` for SqlClient, one
visibility boundary, and complete phase metrics. An unavailable private run
remains `UNVERIFIED`.

## Documentation plan

- Keep the ClickHouse-to-MSSQL overview short: backend choice, first-success
  sequence, and links to focused pages.
- Add focused pages for SqlClient install/verify/upgrade/removal; manifest, CLI,
  evidence and compatibility reference; recovery runbook; architecture/trust
  boundaries; and certification/limitations.
- Update native transport reference with the writer port, aggregate verifier,
  identity v2, process/security boundary, journal/receipt stores, aggregate-only
  return path, and failure reconciliation diagrams.
- Add an operator decision table for every diagnostic token. It gives exact
  `inspect`, `reconcile`, `resume`, or `retire` command, required opaque IDs,
  expected result, cleanup authority, and escalation rule.
- Provide complete schema-valid BCP-v1, BCP-target-local, and SqlClient manifest
  examples plus success, blocked, and unknown-outcome JSON examples.
- Update schema-generated references, source-sink matrix, certification matrix,
  examples, ADR index, and changelog in their owning release phase.
- Keep private benchmark inputs and results outside all documentation.

## Rollout and rollback

Delivery is intentionally phased so each phase is an independently reviewable
patch or minor release:

1. **P1: target-local verification.** Keep BCP plus Python readback as v1
   default. Add explicit `target_local` identity v2, certify full admitted type
   parity plus narrow/wide100 behavior, and remove business-row readback only
   for that optimized path. Rollback may start a new Python-readback invocation
   only after reconciliation proves the prior v2 publication state terminal
   and non-overlapping; evidence and unresolved custody are preserved.
2. **P2: optional SqlClient writer.** The companion, explicit selector,
   identity/journal v2, readiness, failure recovery, documentation, and Docker
   certification ship in 0.88.0. BCP remains default. Publication uses the
   independently controlled manifest-bound release set; PyPI uploads remain
   non-transactional and no source-repository publisher is added.
3. **P3: private production qualification.** Run the exact released candidate
   privately, tune bounded operational values without publishing private data,
   and decide GO/NO-GO from the stated SLO.
4. **P4: persisted hash layout.** Release 0.88.0 exposes it only through explicit
   `layout_version: 2`. The layout has a distinct identity, stores the canonical
   sealed-row hash and SQL Server mutation watermark, and is covered by the same
   narrow/wide100 and source-free recovery gates. Layout v1 remains available;
   invocations never change layout during recovery.
5. **P5: direct streaming research.** Consider eliminating sealed files only in
   a separate approved design that replaces their replay and custody proof.

Rollback never mutates an in-flight identity. For P1, disable `target_local`,
preserve its evidence/custody, prove non-overlap at the publication boundary,
and then start a new Python-readback invocation. For P2, disable SqlClient,
reconcile existing attempts to a proved terminal state, prove non-overlap, and
then start a new BCP invocation. Trigger rollback on silent mismatch,
unclassifiable outcome,
credential leakage, unbounded memory, compatibility regression, or missed hard
latency gate after controlled tuning.
