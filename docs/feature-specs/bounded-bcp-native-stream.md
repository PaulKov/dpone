# Feature design: bounded BCP native physical chunks

- Status: APPROVED
- Owner: transport maintainers
- Issue: bounded source-native transfer
- Target release: TBD
- Last verified: 2026-09-28

## Problem and journey

Bulk operators need typed SQL Server extraction without SQL-side TSV escaping
or a whole-source local spool. The user has authorized implementation of the
best measured transport and retirement of costly implicit encoding choices.
This specification covers the additive bounded native capability; automatic
route preference changes require separate measured evidence.

Authors select the existing native BCP wire profile and physical chunk policy,
run validation, inspect selected transport and byte limits, run a canary, then
execute a manual bulk load. Operators inspect chunk receipts and terminal
producer status. A failed run remains unpublished; retry requires the existing
staging/recovery contract, never blind replay of acknowledged chunks.

## Public contract and compatibility

No new CLI flags or manifest keys. Previously rejected native BCP with required
physical chunks becomes supported for existing validated native layouts.
Text mode remains compatible. There is no silent fallback from binary to text
or to a whole-source file. Existing native scalar/type validation is preserved.
Each child artifact carries the parent's native wire and bulk wire contracts.
Existing chunk evidence records byte counts, rows, checksums and lifecycle.
Eager cleanup bounds retained source chunks; retention modes still retain files
and are not advertised as bounding total disk. Source and transcoded files plus
encoder buffers must fit the configured worker budget.

When physical chunks are `required`, the `mssql-bcp-native` physical-chunk
transport fails closed before source I/O for `snapshot_diff`, `scd2`, or any
nonempty `schema_contract`. In `auto`, those combinations make physical chunks
ineligible and preserve the existing range-partitioned or whole-file BCP Native
route. Those strategies remain route-wide capabilities through transports that
can apply their metadata and contract projections; this specification does not
add those projections to the opaque BCP Native chunk wire.

## Algorithm and failure semantics

1. Build and validate the native layout before spawning BCP.
2. Launch one supervised queryout into the existing FIFO.
3. Incrementally identify fields from fixed lengths and signed length prefixes.
   Preserve bytes, including prefixes, without constructing scalar values.
4. Reject invalid lengths, forbidden NULLs, oversized rows and truncated EOF.
   Check declared row size before buffering a field payload.
5. Seal files only at complete row boundaries using the existing physical byte
   limits, checksums and monotonically increasing chunk indexes.
6. Load each file through the existing native transcoder and sink. Release it
   after acknowledged staging load when eager cleanup is configured.
7. Require successful producer exit after EOF; only then complete extraction.
   A late source failure invalidates the attempt and prevents publication.
8. Generator closure or load failure aborts/reaps BCP and removes the FIFO.

States remain planned -> extracting/loading -> completed or failed. Files are
owned by one invocation. No new resume protocol, parallel writer, transactions,
or publication mechanism is introduced. Empty input succeeds with zero rows;
malformed or partial data fails closed. Native scalar validation remains in the
transcoder. Binary payload newlines never represent row boundaries.

## Architecture and alternatives

A native row framer consumes bounded byte segments. The reusable physical
writer gains a complete-row entry point. MSSQL source composition injects the
validated contract; child artifacts preserve it. Existing sink dispatch and
accelerator remain authoritative. No vendor driver import is introduced.

BCP native is selected as the smallest extension of the existing certified
transport. SqlDataReader remains a benchmark alternative, not an assumed slower
option. Direct raw TDS parsing is rejected: use supported drivers instead.
Text export is a compatibility path. Object-store Parquet staging is useful for
replay but adds storage and does not solve this specific local-spool gap.

## Market comparison

Sources checked 2026-09-28:

- Microsoft BCP native avoids text representation for supported scalar values:
  https://learn.microsoft.com/en-us/sql/relational-databases/import-export/use-native-format-to-import-or-export-data-sql-server
- Microsoft SqlClient supports sequential streaming readers:
  https://learn.microsoft.com/en-us/sql/connect/ado-net/sqlclient-streaming-support
- ClickHouse recommends batched inserts and efficient Native formats:
  https://clickhouse.com/docs/concepts/best-practices/selecting-an-insert-strategy

Adopt bounded buffering and typed transport. Do not claim universal performance
superiority. Compare identical synthetic inputs, source CPU, elapsed time,
resident memory, disk high-water and decoded target values. SSIS is relevant as
a streaming integration alternative but is not a required runtime dependency.
dlt, Informatica, Airbyte, Fivetran, Pentaho and Beam: N/A to the narrow BCP
framing implementation; no comparative performance claim. gusty and Cosmos:
N/A because they orchestrate rather than define this binary wire format.

## Validation, documentation and rollout

Red-green tests cover every byte split, mixed fixed/prefixed values, nullable
and empty fields, embedded delimiters, truncated fields, oversized declarations,
empty input, contract propagation, eager cleanup and late source failure.
Synthetic native decoder fixtures establish fidelity. Focused integration must
prove bounded native extraction cannot silently select full-file spooling.
Measure full export/transcode/load separately before changing default routes.
Run repository quality gates and fresh independent review before merge.
Add user guidance for native chunk selection, limitations and recovery. Update
changelog. Roll out opt-in, canary, then bulk; rollback selects the prior explicit
transport only after existing attempt outcome is resolved.

## Non-goals and risks

No new range planner, CDC, consistency guarantees, globally atomic cluster
publication, package pin or release in this change. Framing adds per-field work;
benchmark it before claiming throughput. A row may approach the chunk limit;
retained input, row output copies and transient buffers must remain bounded. Source-native formats are not
ClickHouse Native and always require validated conversion.
