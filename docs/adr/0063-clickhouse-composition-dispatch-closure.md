# ADR 0063: Close ClickHouse composition dispatch before proving quiescence

Status: Accepted implementation decision under the approved composition execution specification

Date: 2026-09-11

## Context

Composition transfers publish a complete generation with one Atomic database
EXCHANGE. A request may be admitted before it becomes visible in
`system.processes`. Disconnecting HTTP does not stop query execution. Disabling
a user and observing an empty process list therefore cannot prove that a delayed
request will never mutate the target. Repeating an uncertain EXCHANGE can restore
the old snapshot.

The initial supported participant is an exclusively enrolled single-node
ClickHouse service. Replication, distributed targets, asynchronous ingestion and
unmodeled background mutations remain outside this execution cell.

## Decision

A protected dispatcher holds the ClickHouse credentials. Workers submit closed
generation creation, exact payload insertion and exact snapshot exchange
operations. They cannot supply arbitrary SQL, endpoints, settings or credentials.
The deployed network boundary makes the dispatcher the only writer path.

Before opening a socket, the dispatcher commits an immutable unique claim in
the shared SQL ownership journal. That transaction independently checks the
current parent, RUNNING attempt, exact epochs, issued principal, enrollment and
effective budget. A duplicate claim or uncertain commit acknowledgement does
not authorize another send. Query IDs correlate observations; they do not supply
exactly-once execution.

Transport uses complete synchronous HTTP responses, no redirects, retries,
sessions or replacement queries. Full response framing and error checks precede
the protected terminal observation. Losing the transport or terminal-write
acknowledgement retains an unresolved claim.

Closure first journals CLOSING. This prevents new claims. Every earlier claim,
including one paused before opening its socket, must have a durable complete
terminal observation before closure can succeed. The dispatcher then revokes
the exact issued principal, reopens its identity and permissions, observes
server quiescence and journals CLOSED. No lease timeout releases ownership.

An ambiguous request remains blocking. Recovery requires a protected supervisor
barrier that terminates the old dispatcher and database process groups and
destroys their isolated network namespace while preserving the enrolled volume.
After restart under a new boot epoch, old principals remain disabled and the
original UUID orientation and complete data are reconciled. Restart is not
evidence of rollback or successful publication. EXCHANGE is never blindly
resent. Automated restart recovery is unavailable until separately implemented
and demonstrated; the initial failure path must retain the original reservation.

Stable service and database identity are separate from process boot identity.
The registry pins `composition_service_id` to the observed server UUID and
`database_authorities.<database>.database_uuid` to the Atomic database UUID.
Credentials, endpoint aliases, engine versions and catalog timestamps do not
change the physical guard identity.

## Concrete private namespace and principal policy

The first protected Linux deployment uses two pinned Docker containers.
ClickHouse owns a private network namespace and listens only on
`127.0.0.1:8123`. The dispatcher shares that exact network namespace, while
retaining separate process and mount namespaces. No host ports are published.
Workers reach only the dispatcher's constrained frontend; they receive neither
ClickHouse credentials nor access to Docker or the private namespace.

The protected supervisor independently reopens the immutable SQL enrollment and
actual container, image, boot, process, namespace, mount, configuration and
listener identities. Incomplete observations or drift prevent issuance and
closure. Repeated healthy observations retain identical stable facts; timestamps
and changing query counters are not enrollment identity.

ClickHouse `HOST LOCAL` recognizes every address local to its network namespace,
not just loopback. Loopback-only server listeners and the absence of forwarding
or proxy paths therefore establish the isolation boundary. An explicit protected
local-namespace observation selects `enable_local` and `observe_local`; the
catalog must contain no IP entries and exactly the hostname `localhost`, with
no regular-expression or LIKE hosts. Disabled principals have no allowed hosts.
Existing exact-IP mode continues rejecting loopback. There is no inferred mode,
IP-to-LOCAL fallback or caller-selected local policy. The immutable gate original
retains the policy and enrollment digest, while its issuance key remains the
same across policies, so changing policy cannot issue a second principal.

The bounded HTTP adapter accepts numeric IP endpoints; `localhost` maps to
`127.0.0.1`. Other hostnames require a separately implemented bounded resolver.
It verifies TLS certificates, complete response framing and exact catalog types,
and rejects truncated or overflowed results. No hidden DNS, redirect or retry
extends the dispatch deadline.

## Consequences and validation

Normal synchronous execution can close without restarting the service.
Ambiguity reduces availability and requires reconciliation; it never permits
another writer to assume the old request stopped. The same SQL claim boundary
orders dispatch and closure across independent callers.

Required fault tests pause before send, delay before process registration,
truncate responses, return an HTTP 200 error, lose claim and terminal
acknowledgements, duplicate submissions and attempt late dispatch after closure.
Only the isolated Linux campaign establishes live enforcement. Offline tests
are not evidence of network isolation or database quiescence.

This ADR defines the mechanism, not an implementation or certification claim.
See [composition execution](../feature-specs/composition-activation-execution.md)
and [scoped nonproduction authority](../feature-specs/nonproduction-composition-authority.md).

Primary sources: ClickHouse [HTTP interface](https://clickhouse.com/docs/interfaces/http)
and [KILL statement](https://clickhouse.com/docs/sql-reference/statements/kill).
