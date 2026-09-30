# ClickHouse authority journal: single-host foundation

Last verified: 2026-09-30

This guide is for platform engineers integrating the
[dpone-only authority design](feature-design-clickhouse-dpone-only-authority.md).
The journal is a building block, **not a production publication backend**. It
does not connect to ClickHouse, read source data, seal candidate writers or prove
transport completion. Ordinary CLI/manifest routing and the ODBC suspension are
unchanged. For publication semantics, start at the
[publication overview](clickhouse-publication-methods.md).

## Prerequisites and first use

Use a deployment-owned persistent local Linux filesystem, private to the service
OS user. SQLite WAL requires local storage; NFS/SMB and Docker Desktop host bind
mounts are not a certified production authority. Docker named-volume tests prove
process behavior, not power-loss durability. All future target mutation ingress
must use the same authority; the journal cannot enforce database grants itself.

The platform must first durably provision the private authority directory;
the journal does not create missing directories. Authority filenames cannot end
in SQLite's `-wal`, `-shm` or `-journal` suffixes. Provisioning refuses occupied
sidecar names and preserves those files for investigation.

Provision once, separately from ordinary startup. The example deliberately uses
a temporary private directory for learning, not production durability:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
import json

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthoritySubject

with TemporaryDirectory() as sandbox:
    root = Path(sandbox)
    private = root / "authority"
    private.mkdir(mode=0o700)
    path = private / "authority.db"
    SQLitePublicationAuthority.provision(path, "example-deployment")
    authority = SQLitePublicationAuthority(path, "example-deployment")
    subject = AuthoritySubject("example-deployment", "registered-server", "analytics", "orders")
    binding = authority.acquire("example-deployment:first", subject, "orders_candidate")

    reopened = SQLitePublicationAuthority(path, "example-deployment")
    assert reopened.binding("example-deployment:first") == binding

    reports = root / "reports"
    reports.mkdir()
    reopened.write_diagnostics("example-deployment:first", reports / "operation.json")
    report = json.loads((reports / "operation.json").read_text(encoding="utf-8"))
    assert report["owner_retained"] is True
    print(report["state"], report["transport"], report["owner_retained"])
```

Expected output is `registered not_started True`. The report has
`owner_retained: true`; no query was sent. Both sibling demonstration directories
are deleted on leaving the temporary block. Provisioning an existing file raises
`AuthorityError`; startup never initializes a missing file. Use the original
path and deployment identity on every restart. Do not put credentials in the
deployment/server/operation identifiers.

The subject key includes deployment, registered server identity, database and
target, not a DNS alias or changing table UUID. Supply a stable, independently
registered server identity rather than letting each caller invent its own alias.
Database, target and candidate use simple SQL identifiers. Operation IDs require
the exact deployment prefix followed by `:` and a nonempty operation suffix.

Direct `clickhouse_publication_codec.encode_record` / `decode_record` calls raise
`PublicationRecordCodecError` (a `ValueError`) for malformed records. The authority
adapter translates that validation failure into `AuthorityError`, preserving the
cause and retaining ownership. Serialization does not decide recovery policy.
`JournalEntry` lives in `dpone.contracts.clickhouse_publication`; invalid direct
construction raises `ValueError`. Invalid persisted snapshots and private storage
failures are translated to `AuthorityError` by the authority adapter.

## API and state reference

| API | Meaning and restrictions |
|---|---|
| `provision(path, deployment_id)` | Exclusive one-time initialization; does not reset existing files |
| Constructor | Existing private DB only; verify deployment, schema, inode and WAL/FULL/FK settings |
| `execution_identity()` | Validate local path/device/inode/deployment for exclusion; never grant SQL dispatch |
| `acquire(operation_id, subject, candidate)` | Retain one owner; exact retry returns original binding; no TTL or release |
| `binding(operation_id)` | Read protected prepublication registration; unknown operation raises |
| `prepare(binding, intent)` | Store immutable canonical PREPARED intent; reject divergent reuse |
| `read(operation_id)` | Return `JournalEntry(record, revision)`, or None before prepare; no grant |
| `claim(entry)` | Fresh exact PREPARED CAS returns a volatile `DispatchGrant`; losing CAS returns None |
| `begin_send(grant)` | Irreversibly mark possibly sent before any future network write |
| `close_without_send(operation_id)` | Compete with send entry; revoke grant only if sending has not begun |
| `record_terminal(grant, digest)` | Persist a trusted publisher's canonical completion digest; cannot verify EOS itself |
| `resolve(entry, state, observation)` | Exact revision CAS after durable closure; preserves intent/claim history |
| `transport_state(operation_id)` | Read original state; never permission to send |
| `diagnostics` / `write_diagnostics` | Redacted original metadata/history; reports cannot be imported as authority |

Every durable mutation increments the revision. Reload after closure before
resolution. Each resolution appends its canonical observation and digest to
immutable history, including when UNKNOWN is later resolved; the latest value
does not replace that history. Diagnostics expose only observation digests.
A terminal record is historical evidence, not proof of the current
contents of ClickHouse. Terminal resolution never releases ownership; this
increment therefore cannot perform a second operation on the same target.

`not_started` can transition to **either** `may_have_sent` **or**
`closed_without_send`. Only one durable CAS wins. Once possibly sent, closure
requires the [trusted native publisher's](clickhouse-native-publication.md) terminal completion; it cannot be
converted to no-send. Closed states never reopen. A matching terminal receipt
can be acknowledged idempotently; a different receipt is rejected.

The grant secret is generated for the acknowledged winner, excluded from repr,
and persisted only as a hash. Do not serialize, print, log or send the grant to
workers. A record readback after lost claim ACK cannot reconstruct the secret or
authorize sending. These checks protect cooperating service processes, not an
attacker with direct write access to the journal or publisher OS account.

## Failure and recovery runbook

| Symptom | Required action |
|---|---|
| `AuthorityConflict` for another operation | Keep original owner; inspect it, do not pick another ID to bypass it |
| Lost claim ACK | Reload original; no dispatch grant is returned or reconstructed |
| Crash after `begin_send` | Treat outcome as unknown and retain resources; no second send |
| Lost resolution ACK | Reload exact original; a stored terminal result can be finalized without source reads |
| Missing/corrupt/wrong-version journal | Stop admission; preserve files and database resources; do not provision over it |
| Changed inode or unsafe permissions | Stop admission; investigate deployment/storage replacement |
| Old backup restored | Startup alone cannot detect rollback; externally isolate credentials and reconcile offline before reuse |

This store has no force unlock, TTL expiry, cleanup, credential management or
automatic failover. Closing a process or SQLite connection does not release its
target. Do not directly delete owner rows. A later controlled release/finalization
protocol is required before repeatable production operation.

`record_terminal` and `resolve` are trusted composition boundaries, not public
operator recovery shortcuts. A fabricated digest or caller-supplied observation
does not prove ClickHouse completion. The native publisher validates EndOfStream
and request identity before terminal persistence; the future complete backend
must additionally prove sealed candidate and target evidence before resolution.
KILL, an empty process list or desired target rows
do not satisfy that obligation.

Diagnostics are versioned UTF-8 JSON, atomically created with no overwrite. Use
a separate report directory outside the private authority directory so reports
cannot occupy SQLite sidecar names. Failures before atomic publication leave no
partial destination; a failure after publication may leave a complete report.
Existing destinations remain unchanged. Reports omit grant secrets/hashes and
source rows, but contain operational identifiers and should remain access-controlled.

## Verification and remaining gates

The focused suite is:

```bash
uv run pytest tests/test_clickhouse_publication_codec.py \
  tests/test_clickhouse_authority_sqlite.py \
  tests/test_clickhouse_authority_processes.py \
  tests/test_clickhouse_guarded_publication.py -q
```

Codec tests reject malformed/noncanonical records, duplicated JSON keys, unknown
fields/versions and altered method selection. SQLite tests exercise real files,
claim/commit ambiguity, owner retention, exact revisions and atomic reports.
Spawned-process tests race claims and send/close transitions and terminate before
or after claim/send. Local Linux Docker uses a new owned volume and runner.

The [native publisher reference](clickhouse-native-publication.md) describes the
next executable transport increment and its separate evidence scope. Remaining
gates: protected observer, candidate-writer join/seal, backend composition,
durable ownership release, deployment certification, and all ODBC pre-read
memory/MSSQL/object-storage certification. No foundation test proves them.
