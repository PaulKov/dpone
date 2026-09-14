# ADR 0064: Explicit validated character-file staging in ClickHouse

Status: Accepted

Date: 2026-09-14

## Context

A genuine default BulkTextCodec character export contains marker-encoded logical
values. Forwarding those bytes as ClickHouse TSV, or applying CSV quote rules,
can change values despite matching source hashes and row counts. Existing generic
runtime dispatch has different lifecycle and schema-evolution boundaries.

## Decision

Provide one additive `ClickHouseSink.stage_validated_file` Python API for existing
validated exports. Keep generic dispatch and automatic routes unchanged. Share the
producer's logical reader, preserve source receipt authority, and prepare a separate
bounded RowBinary spool for an explicitly admitted finite type/transport profile.

Admit controlled client and HTTP adapters on one node/Atomic database. Fully prepare
before CREATE; bind endpoint/table identity, verify exact transmitted bytes, observe
one canonical COUNT, and recheck source/derived identities before returning an
existing staging handle. Use constructor DI and a narrow runtime query port.

Place pure request construction and credential/option models in transport-specific
request modules shared by legacy and controlled adapters. Keep the producer codec
and logical reader together. Put finite admission policy, errors, canonical JSON
and the narrow journal resource port alongside the runtime query contract. The sink
binds storage to the journal factory; orchestration and preparation depend on its
resource port. The internal service requires this bound factory rather than a
storage argument or concrete default. Keep bounded HTTP response parsing in a
standard-library module with injected metadata/body limits and remaining deadline.
These ownership changes preserve historical aliases, metadata, pickle globals,
constructor defaults and public transport dispatch. They introduce no new public
staging policy or planner/preparer injection point.

Publish immutable query intents before CREATE/INSERT/DROP and durable final evidence
after successful verification and spool cleanup. Separate local sender termination
from exact remote-query completion. Unknown execution retains owned resources for
manual investigation. Do not retry, automatically resume partial stages or grant
source checkpoint/target promotion authority.

Thread one optional immutable verification budget through existing authorities.
Preserve no-budget compatibility, original errors, byte/descriptor/path/receipt
checks and file position. Deadlines bound cooperative work between reads and lock
acquisition; they cannot preempt stalled filesystem syscalls. Probes observe the
controlled endpoint, not an already-open finalizer socket. Exclude concurrent DDL
in the private staging namespace because UUID checks and DROP are not atomic CAS.

## Alternatives and consequences

Rejected raw forwarding and marker-free/integer-only exceptions do not prove full
value fidelity. A new ClickHouse SQL codec duplicates the producer grammar; automatic
fallback changes capabilities after admission. A general plugin mechanism is not
needed for two controlled adapters. Broader types, Python transport and automatic
runtime integration require separate contracts and evidence.

Full scans and local spool/journal storage cost time and capacity. Strict unsupported
profiles and manual unknown-state recovery are explicit operational costs. Existing
imports, manifests, source selection and finalizers remain compatible. There is no
exactly-once, crash-recovery, live-certification or throughput claim.

## Evidence and related contracts

The accepted design, verification-budget amendment and responsibility amendment are tracked by
`docs/agent-task-contracts/b02-validated-clickhouse-file-staging.yml`. Validation uses
genuine receipts, independent value oracles and real local transport fixtures;
external live checks require separate authorization. See the
[user journey and runbook](../validated-clickhouse-file-staging.md) and
[developer architecture and evidence](../developer-validated-clickhouse-file-staging.md).

Official ClickHouse references inspected on 2026-09-14:
[database identity](https://clickhouse.com/docs/reference/system-tables/databases),
[table metadata](https://clickhouse.com/docs/reference/system-tables/tables),
[column metadata](https://clickhouse.com/docs/reference/system-tables/columns),
[exact-query cancellation](https://clickhouse.com/docs/reference/statements/kill),
[HTTP behavior](https://clickhouse.com/docs/concepts/features/interfaces/http), and
[JSON integer format setting](https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Core/FormatFactorySettings.h).
These establish server semantics; they are not execution evidence for this version.
