# One-shot native ClickHouse publisher

This reference is for platform integrators building the
[dpone-only authority](clickhouse-authority-journal.md). It provides explicit
Python building blocks, not a ready-to-run ETL route. Existing CLI/YAML defaults,
UUID-v1 publication and MSSQL ODBC availability are unchanged.

The publisher can prove transport closure. It cannot prove sealed input, complete
physical observation, publication commitment, checkpoint completion or permission
to admit another operation. Read the [method overview](clickhouse-publication-methods.md)
first; use the [closure runbook](runbooks/clickhouse-publication-closure.md) after a failure.

## Prerequisites and supported profile

- One host with a private persistent local Linux SQLite authority volume. Open
  the existing store; never replace it to recover availability. Docker Desktop
  host bind mounts, NFS/SMB, HA and automatic restore are not certified storage.
- One registered direct ClickHouse node, Atomic database and plain MergeTree.
  Every target writer must eventually use the same authority. This library does
  not inventory grants or prevent an independently credentialed writer.
- `dpone[clickhouse]`, which pins `clickhouse-driver==0.2.10`; this transport
  checks native handshake version `24.8.14`. The four-part package version and
  image digest require external evidence: the handshake does not attest them.
- A literal IPv4 address and one port. No DNS/alternate-host rotation, pool,
  proxy retries, generic connector, background dispatch or arbitrary settings.
- TLS verifies the server certificate by default. Explicit `secure=False` is
  only for isolated local test deployments; plaintext fixture evidence does not
  certify TLS configuration. Never disable certificate verification.
- Prepublication observation, writer admission/join/sealing and the complete
  eight-method backend are separate, unfinished components. Synthetic DTO flags
  do not substitute for those proofs.

## First observable result: offline no-send closure

This runnable example uses deliberately synthetic observations and a transport
that must never be called. It proves local closure and retained ownership only.
It neither connects to ClickHouse nor teaches a production candidate lifecycle.
All temporary demonstration files disappear when the block exits.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from dpone.adapters.clickhouse_authority_execution_lock import LocalPublicationExclusion
from dpone.adapters.clickhouse_authority_publisher import AuthorityPublicationPublisher
from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthoritySubject
from dpone.contracts.clickhouse_publication import PublicationObservation, PublicationTable, choose_publication

class NoNetwork:
    def execute(self, request):
        raise AssertionError("No-op must not call transport")

with TemporaryDirectory() as directory:
    path = Path(directory) / "authority.db"
    SQLitePublicationAuthority.provision(path, "demo")
    authority = SQLitePublicationAuthority(path, "demo")
    binding = authority.acquire("demo:first", AuthoritySubject("demo", "server", "db", "target"), "candidate")
    old = PublicationTable("old-uuid", "a" * 64, "b" * 64, 1, ("all",))
    new = PublicationTable("new-uuid", "a" * 64, "b" * 64, 1, ("all",))
    fixture = PublicationObservation(("server", "db", "target", "candidate"), "Atomic", old, new, True, True)
    entry = authority.prepare(binding, choose_publication("demo:first", fixture))
    grant = authority.claim(entry)
    assert grant is not None
    publisher = AuthorityPublicationPublisher(authority, LocalPublicationExclusion(authority), NoNetwork())
    publisher.execute_once(grant)
    publisher.close_and_drain("demo:first")
    diagnostic = authority.diagnostics("demo:first")
    assert diagnostic["owner_retained"] is True
    print(diagnostic["transport"], diagnostic["owner_retained"])
```

Expected output: `closed_without_send True`. The publication record remains
`claimed`; closure is not outcome classification. The same target still rejects
a second operation. The example is executed by the publisher test suite.

## Python API and dependency boundaries

| Import / API | Contract |
|---|---|
| `dpone.adapters.clickhouse_authority_execution_lock.LocalPublicationExclusion(authority)` | Resolve lock from the same protected authority and original subject |
| `hold(operation_id)` | Nonblocking context manager; returns invocation-local `ExecutionSession` |
| `ExecutionSession.assert_current()` | Validate active context, PID/thread, original binding, store and lock inode |
| `SQLitePublicationAuthority.execution_identity()` | Validated local path/device/inode/deployment, not a dispatch grant |
| `dpone.adapters.clickhouse_native_publication.NativePublicationEndpoint(...)` | Explicit endpoint/credentials; password excluded from repr |
| `DirectNativePublicationTransport(endpoint).execute(request)` | One synchronous request; returns only after successful EOS and teardown |
| `dpone.adapters.clickhouse_authority_publisher.AuthorityPublicationPublisher(authority, exclusion, transport)` | Inject the same authority into publisher and exclusion; no hidden construction |
| `execute_once(grant) -> None` | Load original intent, spend acknowledged grant, send once, validate and persist completion |
| `close_and_drain(operation_id) -> None` | Close unsent work or accept durable original closure; never send or observe source/target |

The exclusion port (`dpone.ports.clickhouse_publication_exclusion`) contains
only session/exclusion capabilities. Its local adapter consumes the two-method
`PublicationIdentityReader`: storage identity and original binding, without
dispatch rights. The publisher owns its narrow persistence and native execution
protocols; neither adapter imports the other. Immutable wire values live in
`dpone.contracts.clickhouse_native_publication`. The concrete publisher adapts
these infrastructure capabilities; method selection and guarded outcome recovery
remain in the existing runtime kernel.
`PublicationUnknown` is canonical in `dpone.contracts.clickhouse_publication`;
its existing runtime-kernel import remains compatible.

Endpoint required arguments are `server_id`, `host`, `port`, `user`, `password`.
Defaults: `connect_timeout=10.0`, `send_receive_timeout=30.0`, `secure=True`,
`ca_certs=None`, `server_hostname=None`. Timeouts must be finite and positive;
ports must be integers from 1 through 65535. Socket timeouts are not a total
wall-clock deadline. Endpoint identity comes from deployment registration, not
the handshake's display name. No environment variables are read by these APIs.

`NativePublicationRequest(binding, method, query_id, partition_id=None)`,
`NativePublicationCompletion` and `NativePublicationError` live in
`dpone.contracts.clickhouse_native_publication`. The consumer-owned
`NativePublicationTransport` protocol lives beside the publisher; the concrete
transport satisfies it structurally without importing that consumer. The publisher first
validates the original binding and intent, then copies method/query ID/partition
ID; no catalog/content evidence or grant crosses this boundary. Requests accept
no SQL/settings and confer no authority. Fixed rendering uses quoted simple identifiers and canonical
`REPLACE PARTITION ID`, including `'all'`; the transport profile accepts only
partition IDs matching `[A-Za-z0-9_-]+`. Unsupported IDs fail before send entry.
It never guesses a `tuple()` expression or performs partition loops.

The completion includes operation/query/server identity, statement SHA-256,
three-part server version, server protocol revision and driver version. Its
canonical digest uses profile `dpone.clickhouse.native-completion.v1`. Only the
digest is stored in the existing authority schema; the complete receipt cannot
be reconstructed from it. Caller-created receipts are not completion evidence.

## Ordering, concurrency and failure semantics

```text
original binding -> per-subject execution lock -> validate original intent
 -> acknowledged begin_send -> one native query -> successful EOS
 -> durable terminal digest -> release local lock (owner remains)

lost ACK / exception / disconnect -> original possible-send state -> quarantine
```

The lock file is a private persistent sibling of the authority database, keyed
by canonical subject rather than operation ID, hostname alias or changing table
UUID. It is never unlinked on release. A fresh descriptor per hold serializes
threads and processes without a mutable global registry. Sessions cannot cross
threads/processes or outlive their context. Forked children cannot unlock the
parent's shared descriptor. This is not protection against the owning OS user.

All network activity follows acknowledged `begin_send`; even connection failure
can therefore leave `may_have_sent`. SQLite write transactions remain short and
do not span network I/O. Successful EOS is accepted explicitly; progress/logs,
empty data and profile packets do not complete a request. Unexpected packets,
server exceptions, EOF, partial packets, timeout and cancellation never imply
successful closure. Neither process exit nor KILL permits replay.

| Condition | Public result and next action |
|---|---|
| Invalid endpoint constructor input | `ValueError`, before network; correct deployment configuration |
| Direct lock contention / invalid local files | `AuthorityConflict` / `AuthorityError`; inspect original, no takeover |
| Invalid/stale grant, missing original or publisher lock failure | `PublicationUnknown`, no permission to retry SQL |
| Missing optional driver or wrong driver/server version | Direct transport raises `NativePublicationError`; publisher raises `PublicationUnknown`; inspect original and installed environment |
| Lost send-entry ACK | Zero transport calls, but stored state may be `may_have_sent`; never infer no-send |
| Server error, partial send/response, disconnect or timeout | `PublicationUnknown`; retain possible-send state and resources |
| Cancellation | Original interruption propagates; later closure still obeys durable state |
| Lost terminal-write ACK | Call closure on original operation; only persisted terminal state permits success |

Both uncertainty exceptions have `safe_to_retry=False`. Public transport/publisher
messages suppress raw vendor error text. Library APIs do not print; CLI flags,
exit-code changes and manifest activation are N/A. For diagnosis use a new
redacted report path, not a fabricated receipt or arbitrary SQL probe.

The pinned driver otherwise logs raw server LOG rows, query text and socket
exceptions before the adapter can sanitize them. A private per-connection seam
rebinds its eight audited Python logging methods with copied globals and a
private disabled logger/log-block sink. It preserves the vendor decoder code,
defaults and closures; no shared SDK class, module or application logger is
changed. Unexpected method shape fails before connect. Tests exercise actual
LOG dispatch, query serialization and socket-error logging, and verify an
ordinary concurrent connection still logs normally. Re-audit this boundary
before any driver upgrade; application-wide log suppression is not a substitute.

## Validation and remaining work

Focused tests: `test_clickhouse_authority_execution_lock.py`,
`test_clickhouse_native_publication.py`, `test_clickhouse_authority_publisher.py`
and the existing authority/kernel tests. They cover exact statements, native
packet handling, driver serialization, real SQLite/process races, ACK loss and
retained ownership. They are not live certification.
`test_clickhouse_native_driver.py` covers the real-driver privacy boundary. The
restart fixture reports attempted transport calls separately from its outcome;
an exception translated into UNKNOWN cannot hide an attempted replay.

The opt-in `tests/integration/test_clickhouse_native_publication.py` requires
`DPONE_NATIVE_PUBLICATION_LIVE=1`, `DPONE_NATIVE_HOST` (literal IPv4), optional
`DPONE_NATIVE_PORT` (default 9000), and `DPONE_NATIVE_SOURCE_SHA` for evidence.
These variables belong to tests, not the library. Run only on a newly owned
isolated plaintext node with no host-published ports and empty default-user
credentials. Use a private local Linux volume for pytest's authority directories.
The fixture creates new `native_pub_*` databases and retains them for archiving;
do not point it at an existing shared server. The runner needs the pinned driver
and pytest; `--noconftest` isolates this dedicated fixture from unrelated services.

Run `pytest --noconftest tests/integration/test_clickhouse_native_publication.py
-m 'integration_live and integration_clickhouse'` inside that owned runner.
Record source SHA/tree, image digest, driver/Python/SQLite/server versions, JUnit,
original diagnostics and `assertions.json` before removing owned resources.
The relay tests prove mutation visibility before dropping the actual native
response; the relay itself is not a supported production transport.

Exact evidence belongs to its reported commit/environment, not this prose.
TLS, power-loss durability, ingress isolation, candidate sealing, protected
observation, controlled owner release and ODBC route certification remain separate
gates. See the [closure runbook](runbooks/clickhouse-publication-closure.md) and
[approved authority design](feature-design-clickhouse-dpone-only-authority.md).
