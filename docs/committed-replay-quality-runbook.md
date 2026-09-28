# Recover committed replay quality

This runbook is for operators handling a committed target whose quality replay
is blocked. Start with the [guide](committed-replay-quality.md) and identify the
error in the [reference](committed-replay-quality-reference.md#failure-reference).
The existing [publication runbook](clickhouse-cluster-publication-runbook.md)
continues to govern dispatch, partial cluster outcomes and predecessor cleanup.

## Diagnose before retrying

Preserve the original scheduler invocation, configuration, authority row and
Keeper version, candidate/predecessor ownership and distributed-DDL evidence.
Capture the safe error code and original/current run IDs. Compare policy,
admission/effective-plan, inventory, generation and immutable core digests through
the platform's approved read-only diagnostics. Restrict raw authority access:
acceptance observations may include column names and dataset labels.

A successful target publication does not establish successful quality governance.
Missing or inaccessible evidence is not a pass. Do not turn quality off, substitute
a current target count, clear the journal or replay `EXCHANGE` to obtain success.

## Recover within the supported boundary

- After an interrupted publication, reconcile the existing publication operation.
  The same invocation may consume a PREPARED capsule only after exact commit and
  generation checks; it evaluates the stored original probes and persists COMPLETE.
- After a completed publication, retry the same invocation and configuration.
  The fresh quality execution checks the original evidence, current generation
  and version continuity and issues its own process-local receipt. It does not
  read the source or reinsert data.
- A completed generation with unfinished governance cannot be replaced by a
  successor operation. Restore access to its original trusted store and resolve
  the original operation first.
- `FAILED`, malformed or missing original evidence has no automatic repair.
  Preserve it and escalate to maintainers with redacted evidence and exact source
  revision. A new independently approved operation is a separate decision.

A crash can leave `quality_reader` set. The guard has no lease or automatic expiry;
retries and successor publication remain blocked. There is no supported force-clear
command. The platform owner must quiesce all participating writers, prove that the
reader cannot resume, and obtain a reviewed recovery procedure for the specific
operation. Do not manually clear the field while workers might still be active.

## Upgrade, restore and retention

Quiesce old and new writers before switching authority implementations. Preserve
unfinished operations and their original authority. Provision/verify storage via
the platform's controlled procedure, then enable the explicit Python composition.
There is no automatic conversion, deletion or backfill. Older writers must not
share a slot with v2 quality records unless their compatibility has been established.

A completion version greater than the observed Keeper row version is rejected.
That detects an observed version rollback; it does not authenticate a Keeper
service or detect a privileged administrator restoring an old record with an
equal/higher version. Store replacement, restore and Keeper path changes require
quiescence and a separately reviewed continuity/retention procedure. No automatic
replay guarantee survives an unverified authority replacement.

Retain completed authority until the advertised replay window ends. The current
fixed-slot protocol permits a new operation after publication cleanup and COMPLETE
quality retirement; replacement ends the previous operation's replay window. Keep
an operational record of that boundary. Archival report files do not extend it.

## Distributed-DDL observation failures

A read-only retry for structured ClickHouse Code 999 with
`Coordination::Exception: No node` was evaluated separately. It is not enabled in
this implementation: the stock driver path does not supply an enforceable absolute
read deadline. A future bounded reader must distinguish that exact structured
failure, limit attempts and elapsed time, and preserve cancellation. No retry may
include publication, cleanup or authority mutations. An observation failure today
retains the operation for normal reconciliation; it never authorizes redispatch.

## Verify recovery

Confirm the safe processor result identifies the original `replayed_from` IDs,
quality and requested acceptance results, the same immutable core, and a released
reader guard. Confirm the publication dispatch count has not increased. Use the
[synthetic tutorial](committed-replay-quality.md#first-success-without-a-database)
for regression checks; it does not replace environment-specific live certification.
