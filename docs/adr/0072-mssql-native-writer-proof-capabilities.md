# ADR 0072: MSSQL native writers expose explicit proof capabilities

- Status: Accepted
- Date: 2026-09-27
- Related: [ADR 0057](0057-bounded-window-atomic-publication.md),
  [MSSQL target-local verification design](../feature-specs/mssql-sqlclient-target-local-verification-v2.md)

## Context

Target-local verification removes repeated business-row readback from the
bounded ClickHouse-to-MSSQL native route. Verification may create a receipt only
after dpone proves that the writer cannot mutate the exact owned stage.

The BCP CLI opens an independent SQL Server session. SQL executed by the Python
importer cannot acquire a session-owned application lock on that BCP session.
Treating the importer lock or a successful table scan as writer authority would
allow late writes after controller failure or a lost acknowledgement.

## Decision

Writer quiescence is a closed, identity-bound capability.

- `bcp-supervised-stage-barrier-v1` authorizes exactly one supervised BCP
  launch. Positive authority requires acknowledged success, a reaped child,
  exact vendor count, empty reject output, unchanged sealed-file identity, and
  a transaction-held `TABLOCKX, HOLDLOCK` barrier across identity checks and
  aggregate verification.
- `sqlclient-session-applock-v1` is reserved for the optional same-connection
  writer. It additionally binds the writer session to a nonce-derived
  application lock.

BCP failure, timeout, failed reaping, lost acknowledgement, or custody loss is
`UNKNOWN`. It cannot authorize retry, stage removal, preparation, publication,
or a new overlapping invocation. A barrier may be retried after restart only
when the journal already contains durable acknowledged-and-reaped success.

Target-local v2 claims an invocation-owned CAS custody record before source
I/O. Its key depends only on the target identity, so changing run, window,
writer, or verifier cannot bypass unresolved custody. Lease expiry does not
clear it. Every conforming v1 and v2 runtime must check the record before source
or writer I/O. Older binaries must be excluded operationally from targets with
v2 custody records.

Existing BCP plus Python readback remains journal v1 and preserves its public
behavior. Explicit target-local verification uses journal v2 and binds the
proof-capability identifier into invocation identity.

## Consequences

- The optimized successful BCP path can avoid business-row readback without
  claiming control of a SQL session it does not own.
- Ambiguous BCP failures favor retained custody and manual incident handling
  over availability.
- Empty target-local invocations claim custody but launch no writer process.
- A pre-EOF source failure may retire only fully verified exact-owned stages
  after durable non-publication proof. Any unknown attempt keeps custody held.
- Docker certification must exercise real BCP process loss, lock contention,
  late writes, object replacement, lease expiry, multi-chunk execution, and
  stable-custody admission. Mocked lifecycle tests are insufficient evidence.
- The optional SqlClient phase remains a separate change and certification
  boundary.
