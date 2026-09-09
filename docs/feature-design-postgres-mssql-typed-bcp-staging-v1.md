# Feature design: PostgreSQL to MSSQL typed length-prefixed BCP staging v1

- Status: APPROVED
- Owner: dpone maintainers
- Target release: 0.74.11; XMin initial extension: 0.74.19
- Last verified: 2026-08-22

Approval: the maintainer explicitly requested a systemic, market-grade loading
strategy without compatibility monkey patches on 2026-08-20.

## Problem and outcome

The governed PostgreSQL XMin route already extracts one repeatable-read delta
and complete key image, stores immutable file receipts, and mutates only new,
changed, reactivated, or deleted target rows. Its bottleneck was the MSSQL
landing boundary: character BCP first populated an all-`nvarchar(max)` table,
then SQL Server scanned it with `TRY_CONVERT` and copied every row into a second
native table.

The approved path keeps PostgreSQL COPY and the immutable artifacts, but reads
each artifact exactly once, verifies its SHA-256 and per-row checksum, decodes
the reversible text codec, and writes a private length-prefixed UTF-8 BCP host
file. Microsoft BCP converts those fields directly into the final native SQL
Server staging table. There is no delimiter ambiguity, raw SQL staging,
`TRY_CONVERT` scan, SQLite index, ODBC row loop, or raw-to-native table copy.

Observed on the disposable SQL Server 2022 route runner:

- obsolete pyodbc array prototype: about 3,200 rows/s;
- direct BCP engine: about 281,000 rows/s;
- complete verified pipeline (read, SHA, row checksum, decode, prefix file,
  BCP, count/evidence): 100,000 rows in 1.91 seconds, about 52,000 rows/s.

The exact production SLO remains environment-dependent and is certified by a
full-volume DEV run plus an immediate no-change successor.

## Scope

In scope:

- PostgreSQL XMin `key_snapshot` routes targeting MSSQL;
- PostgreSQL XMin `initial` backfill chunks whose wire and target business
  column identities are unchanged;
- direct native delta and key staging;
- reversible NULL/empty/text/binary/temporal representation;
- one descriptor-bound read per source artifact;
- existing changed-hash update, missing-key insert, reactivation and soft
  delete logic;
- exact BCP, physical staging and consumed-payload evidence.

Non-goals:

- feeding PostgreSQL binary COPY bytes to SQL Server (the vendor formats are
  incompatible);
- SQL Server `MERGE`;
- `DELETE + INSERT` for changed rows;
- skipping the complete key image used to detect physical source deletes;
- weakening source/database/file/state/receipt authority;
- silently falling back to raw `nvarchar(max)` normalization.

## Public and compatibility contract

No public manifest key, state schema, target DDL, or CLI changes. Existing
certified PostgreSQL XMin + MSSQL key-snapshot and explicit XMin `initial`
manifests select this transport automatically when the runtime proves an exact
identity projection and supported native types. Generic MSSQL routes and
renamed/unsupported projections retain their existing validated character-wire
normalizer.

The internal transport receipt is
`mssql.bcp.length_prefixed_utf8.v1`. It records exact source file evidence,
native schema digest, declared/consumed/native row counts, BCP batch count and
staging cardinality. No values or credentials enter progress logs.

Once the direct transport is selected, an unavailable format-file BCP, lossy
scalar, or count/digest mismatch fails before business DML. Snapshot artifacts
do not fall back. XMin initial eligibility is resolved before staging DDL; an
unsupported type or renamed business projection remains on the already
certified generic normalizer and never starts a partial direct materialization.

## Algorithm

1. Resolve exact native MSSQL types, nullability and BIN2 equality-key
   collations before staging creation.
2. Create only the native delta and key tables.
3. Open the immutable source artifact without following symlinks; require its
   captured scope, device, inode, size, mtime and wire contract.
4. In one sequential pass:
   - update the file SHA and byte/row counters;
   - require the exact field count;
   - recompute the producer UTF-16LE row checksum in constant-time comparison
     for snapshot artifacts; business-only XMin initial artifacts instead rely
     on the independently captured whole-file receipt because no target-only
     hash field exists on their immutable wire;
   - decode NULL versus empty, codec controls and every supported scalar;
   - reject width, range, precision, temporal, UUID, binary and float loss;
   - write each decoded field as signed little-endian four-byte length followed
     by UTF-8 bytes (`-1` is SQL NULL, `0` is an empty value).
5. Require EOF, digest, identity, byte and row receipts before BCP starts.
6. Invoke Microsoft BCP once with a deterministic non-XML format file:
   `SQLCHAR`, four-byte prefixes, no field or row terminators, UTF-8 code page,
   bounded batch size, encrypted-safe packet size, `TABLOCK`, `KEEPNULLS`, and
   a private reject file.
7. Require vendor rows copied, physical `COUNT_BIG`, declared rows and native
   evidence to agree exactly.
8. Validate key uniqueness/parity and target shape, then under the existing
   transaction and applock execute separate reactivation, changed-only update,
   absent-key insert and missing-key soft delete.
9. Commit target, checkpoint and receipt atomically. On any earlier failure,
   remove owned files/staging and retain the old checkpoint.

For XMin initial chunks, the native staging table contains the business wire
prefix followed by physically nullable framework columns. The non-XML format
file maps only the existing business fields; SQL Server therefore loads native
business values directly and leaves the omitted suffix `NULL`. One set-based
statement then projects lineage and the canonical row hash in that same table,
after which native evidence is issued. There is no raw table, second staging
table, or raw-to-native `INSERT ... SELECT`.

An externally owned repeatable-read lease deliberately remains open until the
target acknowledges the chunk. Direct-staging admission therefore uses the
already acquired, immutable snapshot boundary for planning; it must not wait
for the completion timestamp and silently choose the generic raw route. After
the single artifact pass, target finalization still requires a completed source
receipt and proves that snapshot identity, lineage projection and native schema
are identical to the admitted plan. Artifact integrity remains inside that one
pass—planning never pre-hashes the file.

Schema-contract enforcement may wrap that file before it reaches the sink.
Only a wrapper holding a valid file-contract receipt may expose its exact inner
file capability to direct-staging admission. Streaming and opaque wrappers are
not unwrapped, and the wrapper still owns materialization and cleanup.

## Why length prefixes

Delimiter-based BCP cannot represent arbitrary tabs, CR/LF, control markers,
Unicode and empty values without an escape/decode stage. Four-byte field
lengths make every record self-delimiting. BCP still owns native type conversion
and high-throughput loading; dpone owns only the documented character host
format and finite scalar validation. A private intermediate host file is
required because BCP treats FIFOs and `/dev/stdin` as empty regular inputs on
the supported Linux utility. It is written once, read once by BCP, and removed
in the same staging operation.

## DML strategy

The transport does not alter reconciliation semantics:

- `UPDATE` only when the stored business hash differs;
- `INSERT` only when the key is absent;
- soft-delete only when an active target key is missing from the complete key
  image;
- reactivation only when a previously deleted key returns;
- no-op runs do not rewrite target rows.

`DELETE + INSERT` is rejected because it changes physical row identity, can
break foreign keys, doubles transaction-log/index work, and widens concurrency
windows. SQL Server `MERGE` is also rejected; separate deterministic statements
have clearer locking, metrics and retry semantics.

## Components

| Component | Responsibility |
|---|---|
| `VerifiedArtifactLineReader` | one physical artifact read and exact receipt proof |
| `MssqlTypedValueDecoder` | pure finite wire-to-native scalar validation |
| length-prefixed BCP writer | delimiter-free UTF-8 host records and deterministic format file |
| `BcpRunner.import_format_file` | redacted/authenticated Microsoft BCP invocation |
| typed staging evidence | source/native schema and row-count authority |
| snapshot finalizer | key validation and changed-only atomic target DML |

Each policy remains outside the connector and each new module stays below the
hard size budget.

## Alternatives

| Alternative | Decision | Reason |
|---|---|---|
| raw BCP + `TRY_CONVERT` | reject | two tables and repeated full scans |
| pyodbc `fast_executemany` | reject for this route | measured about 19x slower end-to-end |
| PostgreSQL binary COPY as BCP native | reject | incompatible vendor binary formats |
| delimiter BCP directly to native | reject | cannot preserve arbitrary text and NULL/empty exactly |
| length-prefixed character BCP | adopt | native conversion, lossless framing, high throughput |
| one INSERT per row | reject | network and log amplification |

## Tests and rollout

- unit scalar/null/empty/range/format/receipt contracts;
- real SQL Server Unicode, tab/newline, binary and temporal fidelity;
- full XMin baseline/no-op/change/delete/reactivate/rollback/receipt lifecycle;
- 100k integrated benchmark without a wall-clock assertion;
- full-volume three-table DEV run and immediate no-op successor;
- synchronous runtime and Airflow provider pin;
- identical artifact promotion to PROD and bounded acceptance.

Rollback is the previous immutable runtime/provider image. No persistent state
or target migration is required.
