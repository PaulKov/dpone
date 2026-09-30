"""Certification-only pre-creation epoch producer and independent verifier.

SQLite FULL commits atomically append hash-chained epoch records and a durable
head. Opening existing evidence cannot acknowledge an uncertain CREATE. Original
inode checks reject replacement/copy; this does not resist a malicious host
administrator. No production capability, source or publication is created here.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import sys
from contextlib import closing, contextmanager, suppress
from pathlib import Path

from tests.integration.clickhouse_table_compatibility_support import CONFIG, IMAGE, OwnedClickHouse

SETTINGS = (
    "index_granularity",
    "index_granularity_bytes",
    "enable_mixed_granularity_parts",
    "min_index_granularity_bytes",
    "min_rows_for_wide_part",
    "min_bytes_for_wide_part",
    "storage_policy",
)


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def fingerprint(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class OwnedDockerEpochSource:
    """Collect test-deployment history before server/table creation, never after."""

    def __init__(self, root: Path, server: OwnedClickHouse) -> None:
        self.root, self.server = root.resolve(), server
        self.journal = self.root / "epoch.db"

    @classmethod
    def bootstrap(cls, root: Path, *, persistent: bool = False) -> OwnedDockerEpochSource:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root.chmod(0o700)
        server = OwnedClickHouse.prepare(root, persistent=persistent)
        source = cls(root, server)
        with source.journal.open("xb"):
            pass
        source.journal.chmod(0o600)
        stat = source.journal.stat()
        with source.connection() as db:
            db.execute("CREATE TABLE metadata (device INTEGER, inode INTEGER, head TEXT)")
            db.execute("INSERT INTO metadata VALUES (?, ?, ?)", (stat.st_dev, stat.st_ino, ""))
            db.execute(
                "CREATE TABLE events (sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL, digest TEXT NOT NULL)"
            )
        source.append("bootstrap", {"name": server.name, "image": IMAGE, "configuration": CONFIG})
        try:
            server.start()
            source.append("activated", {"identity": server.identity(), "globals": server.globals()})
        except BaseException:
            # A failed bootstrap cannot become usable history. Never delete it.
            # A second journal failure must not bypass stopping the real server
            # or replace the original failure reported to the test controller.
            try:
                with suppress(Exception):
                    source.append("invalidated", {"reason": "bootstrap_failed"})
            finally:
                with suppress(Exception):
                    server.stop()
            raise
        return source

    @contextmanager
    def connection(self):
        with closing(sqlite3.connect(self.journal)) as db, db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA journal_mode=DELETE")
            yield db

    def append(self, kind: str, facts: dict) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            head = db.execute("SELECT head FROM metadata").fetchone()[0]
            sequence = db.execute("SELECT count(*) + 1 FROM events").fetchone()[0]
            payload = canonical({"kind": kind, "facts": facts, "previous": head})
            digest = fingerprint([sequence, payload])
            db.execute("INSERT INTO events VALUES (?, ?, ?)", (sequence, payload, digest))
            db.execute("UPDATE metadata SET head=?", (digest,))

    def events(self) -> list[dict]:
        try:
            stat = self.journal.stat()
            # Read-only: missing/corrupt history must not create or repair a store.
            with closing(sqlite3.connect(f"{self.journal.as_uri()}?mode=ro", uri=True)) as db:
                device, inode, expected_head = db.execute("SELECT device, inode, head FROM metadata").fetchone()
                if (device, inode) != (stat.st_dev, stat.st_ino):
                    raise ValueError("original epoch file replaced")
                rows = db.execute("SELECT sequence, payload, digest FROM events ORDER BY sequence").fetchall()
            previous, result = "", []
            for index, (sequence, payload, digest) in enumerate(rows, 1):
                event = json.loads(payload)
                if sequence != index or digest != fingerprint([sequence, payload]) or event["previous"] != previous:
                    raise ValueError("epoch sequence/checksum mismatch")
                previous = digest
                result.append({**event, "sequence": sequence, "digest": digest})
            if previous != expected_head or not result or result[0]["kind"] != "bootstrap":
                raise ValueError("epoch tail mismatch")
            return result
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as error:
            raise ValueError("settings_provenance_unverified: original journal") from error

    @classmethod
    def open(cls, root: Path) -> OwnedDockerEpochSource:
        source = cls(root, OwnedClickHouse("", root.resolve() / "server.xml"))
        source.server.name = source.events()[0]["facts"]["name"]
        return source

    def table(self, name: str) -> dict:
        if not re.fullmatch("[a-z][a-z0-9_]*", name):
            raise ValueError("Unsupported test table name")
        rows = self.server.query(
            "SELECT toString(uuid) AS uuid, create_table_query AS ddl FROM system.tables "
            f"WHERE database='default' AND name='{name}'"
        )
        if len(rows) != 1:
            raise ValueError("settings_provenance_unverified: table missing")
        return rows[0]

    def create(self, name: str, settings: str = "index_granularity=4096", *, acknowledge: bool = True) -> None:
        if not re.fullmatch("[a-z][a-z0-9_]*", name):
            raise ValueError("Unsupported test table name")
        self.append("create_pending", {"table": name})
        self.server.query(f"CREATE TABLE default.{name} (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS {settings}")
        if acknowledge:
            self.append("create_completed", {"table": name, **self.table(name)})

    def invalidate(self, reason: str) -> None:
        self.append("invalidated", {"reason": reason})

    def observe(self, name: str) -> dict:
        events = self.events()
        activated = [event for event in events if event["kind"] == "activated"]
        coverage = [
            event for event in events if event["kind"] == "create_completed" and event["facts"]["table"] == name
        ]
        if len(activated) != 1 or len(coverage) != 1 or any(event["kind"] == "invalidated" for event in events):
            raise ValueError("settings_provenance_unverified: missing creation coverage")
        activation, creation = activated[0], coverage[0]
        if activation["sequence"] >= creation["sequence"]:
            raise ValueError("settings_provenance_unverified: creation ordering")
        if (
            self.server.identity() != activation["facts"]["identity"]
            or self.server.globals() != activation["facts"]["globals"]
        ):
            raise ValueError("settings_provenance_unverified: deployment changed")
        table = self.table(name)
        if table != {key: creation["facts"][key] for key in ("uuid", "ddl")}:
            raise ValueError("settings_provenance_unverified: table changed")
        explicit = dict(re.findall(r"(\w+)\s*=\s*('?[^,\s']+'?|\d+)", table["ddl"].split("SETTINGS", 1)[-1]))
        inherited = {key: activation["facts"]["globals"][key] for key in SETTINGS if key not in explicit}
        result = {
            "scope": "owned Docker prerequisite only; not production certification",
            "server_version": self.server.query("SELECT version() AS version")[0]["version"],
            "identity": activation["facts"]["identity"],
            "table_uuid": table["uuid"],
            "explicit_settings": explicit,
            "inherited_settings": inherited,
            "activation_sequence": activation["sequence"],
            "coverage_sequence": creation["sequence"],
            "epoch_head": events[-1]["digest"],
        }
        if self.server.identity() != activation["facts"]["identity"]:
            raise ValueError("settings_provenance_unverified: deployment changed during observation")
        return result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.server.stop()


if __name__ == "__main__":
    print(canonical(OwnedDockerEpochSource.open(Path(sys.argv[1])).observe(sys.argv[2])))
