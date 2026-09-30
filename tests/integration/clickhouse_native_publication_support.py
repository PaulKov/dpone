"""Owned live-fixture helpers, never a production observer or network proxy."""

from __future__ import annotations

import hashlib
import json
import os
import select
import socket
import threading
import time
import uuid
from pathlib import Path

from dpone.adapters.clickhouse_authority_execution_lock import LocalPublicationExclusion
from dpone.adapters.clickhouse_authority_publisher import AuthorityPublicationPublisher
from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.adapters.clickhouse_native_publication import DirectNativePublicationTransport, NativePublicationEndpoint
from dpone.contracts.clickhouse_authority import AuthoritySubject
from dpone.contracts.clickhouse_publication import PublicationObservation, PublicationTable, choose_publication


class PublicationCase:
    """Synthetic rows and registered evidence on a dedicated disposable server."""

    def __init__(self, root: Path, name: str) -> None:
        from clickhouse_driver import Client

        self.root, self.name = root, name
        self.database = "native_pub_" + uuid.uuid4().hex[:16]
        private = root / "authority"
        private.mkdir(mode=0o700)
        self.path = private / "authority.db"
        self.endpoint = NativePublicationEndpoint(
            "server",
            os.environ["DPONE_NATIVE_HOST"],
            int(os.environ.get("DPONE_NATIVE_PORT", "9000")),
            "default",
            "",
            secure=False,
            connect_timeout=5,
            send_receive_timeout=10,
        )
        self.client = Client(
            host=self.endpoint.host, port=self.endpoint.port, connect_timeout=5, send_receive_timeout=10
        )
        self.client.execute(f"CREATE DATABASE `{self.database}` ENGINE=Atomic")
        SQLitePublicationAuthority.provision(self.path, "deployment")
        self.store = SQLitePublicationAuthority(self.path, "deployment")
        self.publisher = AuthorityPublicationPublisher(
            self.store, LocalPublicationExclusion(self.store), DirectNativePublicationTransport(self.endpoint)
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # Retain owned fixture data and authority until the runner archives them.
        self.client.disconnect()

    def prepare(self, mode):
        old = [(1, 1, "old"), (1, 2, "old")]
        new = [(1, 3, "new"), (1, 3, "new"), (1, 4, "new")]
        if mode == "stale":
            old.append((2, 5, "stale"))
        if mode == "empty":
            new = []
        if mode == "same":
            new = old
        partition = "" if mode == "unpartitioned" else "PARTITION BY p"
        for name, rows in (("target", old), ("candidate", new)):
            if name == "target" and mode == "absent":
                continue
            self.client.execute(
                f"CREATE TABLE `{self.database}`.`{name}` (p Int32, id Int32, value String) "
                f"ENGINE=MergeTree {partition} ORDER BY (id, value)"
            )
            if rows:
                self.client.execute(f"INSERT INTO `{self.database}`.`{name}` VALUES", rows)
        before = self.snapshot()
        operation = "deployment:" + uuid.uuid4().hex
        subject = AuthoritySubject("deployment", "server", self.database, "target")
        self.binding = self.store.acquire(operation, subject, "candidate")
        observed = PublicationObservation(
            ("server", self.database, "target", "candidate"),
            "Atomic",
            self._table("target", before),
            self._table("candidate", before),
            True,
            True,
        )
        entry = self.store.prepare(self.binding, choose_publication(operation, observed))
        grant = self.store.claim(entry)
        assert grant is not None
        return operation, grant, before

    def _table(self, name, snapshot):
        identity = snapshot[f"{name}_uuid"]
        if identity is None:
            return None
        rows = snapshot[f"{name}_rows"]
        partitions = self.client.execute(
            "SELECT DISTINCT partition_id FROM system.parts WHERE database=%(db)s AND table=%(table)s "
            "AND active ORDER BY partition_id",
            {"db": self.database, "table": name},
        )
        # Fixture-only digests and flags: this is not a typed protected observer.
        digest = hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()
        return PublicationTable(identity, "a" * 64, digest, len(rows), tuple(row[0] for row in partitions))

    def snapshot(self):
        result = {}
        for name in ("target", "candidate"):
            identities = self.client.execute(
                "SELECT toString(uuid) FROM system.tables WHERE database=%(db)s AND name=%(name)s",
                {"db": self.database, "name": name},
            )
            result[f"{name}_uuid"] = identities[0][0] if identities else None
            result[f"{name}_rows"] = (
                self.client.execute(f"SELECT p, id, value FROM `{self.database}`.`{name}` ORDER BY p, id, value")
                if identities
                else None
            )
        return result

    def evidence(self, operation, before, after, facts):
        report = {
            "test": self.name,
            "fixture_observation_only": True,
            "source_sha": os.environ.get("DPONE_NATIVE_SOURCE_SHA", "UNVERIFIED"),
            "server_version": self.client.execute("SELECT version()")[0][0],
            "before": before,
            "after": after,
            "facts": facts,
        }
        with (self.root / "assertions.json").open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2, sort_keys=True)
        self.store.write_diagnostics(operation, self.root / "diagnostics.json")


class ResponseFaultRelay:
    """Forward real request; withhold server response after its unique query ID.

    No credentials or packet payloads are retained. A truncated varint is an
    explicit wire fault, not a fabricated successful response. This relay is
    test infrastructure only, not an admitted production topology.
    """

    def __init__(self, host, port, query_id, fault, effect_visible):
        self.destination = (host, port)
        self.query_id, self.fault = query_id.encode(), fault
        self.effect_visible = effect_visible
        self.activated, self.stop = threading.Event(), threading.Event()
        self.query_occurrences = self.held_response_bytes = 0
        self.error = None
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(15)
        self.port = self.listener.getsockname()[1]

    def __enter__(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.listener.close()
        self.thread.join(20)
        assert not self.thread.is_alive(), "Fault relay did not stop"

    def check(self):
        assert self.error is None, f"Relay failed: {type(self.error).__name__}"

    def _run(self):
        try:
            with self.listener.accept()[0] as client, socket.create_connection(self.destination, timeout=10) as server:
                pending, seen = b"", False
                while not self.stop.is_set():
                    ready, _, _ = select.select([client, server], [], [], 1)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        if source is client:
                            # Retain only the suffix needed to find a split query
                            # identity, never the complete handshake or SQL.
                            combined = pending + data
                            count = combined.count(self.query_id)
                            self.query_occurrences += count
                            seen = seen or bool(count)
                            pending = combined[-(len(self.query_id) - 1) :]
                            server.sendall(data)
                        elif seen:
                            self.held_response_bytes += len(data)
                            deadline = time.monotonic() + 5
                            while not self.effect_visible():
                                if time.monotonic() >= deadline or self.stop.wait(0.02):
                                    raise AssertionError("Mutation effect was not visible before response fault")
                            if self.fault == "truncated_packet":
                                client.sendall(b"\x80")  # unfinished packet-type varint
                            self.activated.set()
                            return  # drop socket before delivering server EOS
                        else:
                            client.sendall(data)
        except Exception as error:
            self.error = error
