# dbt diagnostics and execution context handoff

Status: DRAFT. This is an analysis handoff, not an approved feature specification.
Only the separately assigned existing-contract redaction correction may be
implemented. Live logs, diagnostic excerpts, debug configuration, public
composition changes and manual interval policies remain unapproved.

## Assessed source and boundaries

Baseline: `46830976b214262c7772800523e832a5a6f6d78f` (dpone 0.79.2).
The assessment uses public source, synthetic values and injected process
capabilities. No corporate material, credentials or live services are involved.
[Synthetic observations](synthetic-observations.json) record source hashes and
unresolved properties; [the producer](synthetic_probe.py) requires that exact
production tree. Run it on the baseline, not a source tree containing fixes.

## Current behavior and proposed interfaces

### Output, failure diagnosis and public composition

`SubprocessDbtCommandRunner` already accepts process construction and supervision
dependencies, drains both streams and returns bounded redacted text. It has no
live output sink. `DbtExecutionService` accepts its dependencies explicitly but
discards build stdout/stderr and truncation indicators. Failed preflight parse
and selection commands become generic selection-drift diagnostics. The public
`execute_dbt_pack` entry point exposes environment and path arguments; injected
runner/preflight/interval values exist only on its private composition function.
Semantic refresh has a separate runner construction site.

Candidate interfaces, pending stage 01 agreement:

1. An optional output observer at the runner constructor, receiving only
   sanitized, size-bounded chunks with stream and invocation correlation.
   The observer must never receive raw secret-bearing chunks. Its absence keeps
   current behavior. Delivery requires bounded redaction state and UTF-8
   decoding across arbitrary chunk boundaries; the current completion-time
   capture fix does not establish streaming safety.
2. An optional lifecycle observer at the execution-service composition boundary,
   with an injected monotonic clock, bounded events and explicit overflow.
   Observer failures must not replace the business failure or permit success.
   Reuse the existing bounded native-observation patterns, without passing dbt
   phases into that subsystem's closed native-delivery phase enumeration.
3. A typed public composition input forwarding approved dependencies through
   both normal and semantic-refresh entry points. Do not introduce a generic
   plugin registry or require private-method/global replacements. The precise
   port and argument names are intentionally not approved by this draft.

Preserve one JSON stdout document and the real exit code. Default stderr remains
as documented in the [CLI reference](../../../docs/dbt-self-service-reference.md).
Any optional live stderr mode must be explicitly approved and documented.
`DBT_DEBUG` and `DBT_LOG_LEVEL` are currently excluded by the canonical environment;
do not introduce arbitrary environment or argv passthrough. Stage 01 must decide
whether a bounded debug level changes invocation identity and which owner sets it.

Separate the primary operation failure from secondary cleanup failures in any
future diagnostics contract. A build-side timeout may still mean `COMMIT_UNKNOWN`:
more informative diagnostics do not establish absence of a target mutation or
authorize automatic retry. The assigned redaction correction does not alter
current timeout/cleanup precedence.

### Artifact excerpts

Current paths distinguish preflight `target`/`logs`, final target results and
final logs under an attempt directory. No safe excerpt reader was found in this
runtime path. Raw dbt-owned files bypass captured-output redaction; their live
contents, redaction and disk growth remain UNVERIFIED.

A future excerpt contract must define allowed files, confinement/no-follow
reads, source identity, maximum read and emitted bytes, line/UTF-8 handling,
redaction before persistence and explicit truncation. It must decide bounded
head/tail retention for late failure causes and distinguish incomplete capture
from an empty file. Excerpts are diagnostic only. They cannot alter existing
immutable execution evidence or its strict readers without schema/version and
compatibility approval. A separate bounded diagnostic sidecar is a candidate,
not a decision made by this handoff.

### Phase/progress and resources

Existing dbt evidence already includes `started_at`, `finished_at`, preflight
status, build-started state, exit code and node `execution_time`. Preserve these
fields. `finished_at` currently precedes evidence persistence; its wall-clock
delta does not measure monotonic end-to-end duration. Raw dbt `timing[]` and
top-level `elapsed_time` are validated but not normalized into node evidence.

Candidate observational boundaries are artifact verification, activation
readiness, profile/preflight work, target invocation, result validation and
evidence persistence. These are distinct observations, not interchangeable
success states. The coordinator must bind each boundary to the actual owner:

| Observation | What it establishes | What it cannot establish |
| --- | --- | --- |
| Artifact verification | The designated verifier accepted exact bytes/identity | Activation readiness or execution |
| Activation readiness | Required readiness producer accepted its own boundary | Target mutation or durable terminal evidence |
| Invocation started | Control entered a potentially mutating operation | Commit, rollback or retry safety |
| Uncertain mutation | Outcome requires reconciliation | Failure with no side effects |
| Terminal evidence persisted | The designated evidence writer completed its write | Independent verification unless that verifier actually ran |
| Independently verified terminal evidence | The designated independent verifier accepted its evidence | Unmeasured performance or other routes |

Do not infer percent complete from elapsed time, emitted log lines, or configured
thread counts. Monotonic timings need a process/clock domain, phase, attempt,
outcome, unit and collection status. Missing measurements remain unavailable,
not zero. Overflow makes an aggregate incomplete; it must not silently retain a
partial sum as complete. Retries require separate attempt correlation.

Stage 04 must supply the concrete resource interface before implementation.
Configured limits, effective admitted limits and actual measurements are
different quantities. A configured/effective worker count cannot substitute for
measured CPU, memory, bytes or concurrency. Resource observations grant no
artifact, activation, mutation, retry or terminal-evidence authority. Stage 03
must supply exact terminal/recovery states; its connector retry correction does
not establish durable safety of outer task retries.

### Manual intervals, timezone and DST

Current dbt behavior requires two aware timestamps with `start < end` by absolute
instant. Missing, partial, naive, zero-width and reversed intervals are rejected.
Explicit offsets support 23-hour spring days, 25-hour autumn days and repeated
local hours. The wire preserves authored strings; regional IANA-zone consistency
and nonexistent local wall times are not validated by this two-string contract.
Scheduler identity alone makes the generic `RunInterval` nonempty even when its
bounds are missing; this does not produce a valid dbt execution interval.

Proposed default: retain rejection for empty/manual windows. Permit an explicit
aware pair only through an approved public input, or an approved scheduler-derived
policy. Never infer a window from DAG/model names, runtime wall time, or a fixed
24-hour subtraction. Use half-open windows `[start, end)`. A future regional
calendar policy must pin an IANA zone and distinguish date-only localization,
aware timestamp conversion, ambiguous local times and nonexistent local times.
Require explicit disambiguation rather than silently selecting a DST fold/gap.
Semantic refresh separately requires one UTC day; do not replace that contract
with a generic local-calendar day.

Resolve any future interval override exactly once before credentials/output
materialization. Stage 01 must define explicit-vs-environment precedence,
conflict handling, effective-window evidence and retry equivalence. Current
attempt identity does not include the interval; new override behavior must not
allow different effective windows under indistinguishable attempt evidence.

The provider creates `CronTriggerTimetable(cron, timezone=...)` without an
interval. [Official Airflow 3.1.1 source](https://airflow.apache.org/docs/apache-airflow/3.1.1/_modules/airflow/timetables/trigger.html)
defaults that interval to zero for both manual and automated runs (checked
2026-09-13). Its incompatibility with dbt's positive-width requirement follows
from those contracts; real scheduler reproduction remains UNVERIFIED here.
Choosing a data-interval timetable or an explicit trigger window affects
scheduling/catchup and needs an approved decision, not a blanket conversion.

## Minimal path proposals and approval needs

These are requests for future assignments, not ownership claims:

| Slice | Minimal candidate paths | Required predecessor/approval |
| --- | --- | --- |
| Existing redaction defect | `src/dpone/adapters/dbt_subprocess.py`, internal `dbt_output_redaction.py`, one new boundary regression file, narrow threat-model text | Granted separately in ownership.yml |
| Authored start-date precision | `packages/dpone-airflow-pack/src/dpone_airflow_pack/dag_schedule.py`, one dedicated provider regression file | Separate existing-contract assessment and path grant |
| Output/lifecycle DI | Existing runner, execution service and bootstrap; narrow new port/contract only if needed | Stage 01 public API and channel agreement; coordinator shared-file assignment |
| Diagnostic excerpts | One bounded reader/writer and dedicated diagnostic contract | Artifact identity, retention, redaction, sidecar/schema decision |
| Resource projection | Observer adapter consuming stage 04 measurements | Concrete stage 04 units, limits, availability and authority boundary |
| Manual window policy | Existing interval contract and public bootstrap/provider boundary | Stage 01 identity/precedence, scheduler semantics and ADR decision |

Authored-time reproduction: public `parse_spec_start_date` with
`2026-09-13T15:45:30+00:00` and timezone `UTC` returns midnight on the baseline.
The authoring parser accepts datetime input, and the helper documents parsing a
start date with a catalog timezone; no date-only restriction was found. Minimum
regression proposal: aware timestamp preserved as an instant, naive datetime
localized without dropping time, and date-only current behavior unchanged.
Ambiguous/nonexistent times and invalid-value fallback need explicit contract
assessment before changing their behavior. Provider paths remain read-only.

## Documentation and regression handoff

After approval, document one synthetic parse failure, one failed dbt test and
one running task with observable progress. Show stable cause/phase, safe bounded
artifact location, truncation, real exit code and the permitted recovery action.
Provide scheduled/manual/DST/empty-window examples and a public Python DI example.
Update the CLI channel table and runbook links with the implementation; do not
publish examples for unsupported debug flags or implied safe raw-log collection.
Stage 01's current five documentation paths and compiler module are reserved.

Required tests include arbitrary chunk and UTF-8 boundaries, overlapping secret
representations, both pipes, bounded retention, callback failure/overflow,
default CLI compatibility, V1/V2 and semantic-refresh paths, each lifecycle failure
boundary, retry isolation and evidence-write uncertainty. Synthetic tests cannot
certify live dbt/Airflow behavior, database throughput or terminal publication.
Removal of downstream runtime patches is coordinator migration guidance only;
this programme does not access downstream private repositories.
