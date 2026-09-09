"""PostgreSQL logical decoding CDC readers.

Production PostgreSQL logical replication normally uses the built-in ``pgoutput``
plugin, which emits binary logical-replication protocol messages scoped by a
publication. dpone supports that path by default through PostgreSQL's SQL binary
logical-decoding functions, so the reader does not require a Python replication
protocol client.

``test_decoding`` remains available as an explicit development fallback because
it is useful for diagnostics and works on very small local setups.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange
from dpone.runtime.cdc.postgres_pgoutput import (  # noqa: F401
    PgOutputColumn,
    PgOutputMessageParser,
    PgOutputRelation,
    _BinaryReader,
)
from dpone.runtime.cdc.postgres_test_decoding import TestDecodingMessageParser

PostgresLogicalPlugin = Literal["pgoutput", "test_decoding"]


@dataclass(frozen=True, slots=True)
class PostgresLogicalCDCReaderConfig:
    """Configuration for ``PostgresLogicalCDCReader``.

    ``plugin`` defaults to ``pgoutput`` because that is PostgreSQL's native
    logical-replication output plugin. Set ``plugin="test_decoding"`` only for
    local diagnostics or compatibility with very simple SQL decoding flows.
    """

    source_schema: str
    source_table: str
    slot_name: str
    plugin: str = "pgoutput"
    publication_name: str | None = None
    drop_existing_slot: bool = False
    create_slot_if_missing: bool = True
    create_publication_if_missing: bool = True
    proto_version: int = 1

    @property
    def resolved_publication_name(self) -> str:
        if self.publication_name:
            return self.publication_name
        safe_schema = re.sub(r"[^A-Za-z0-9_]", "_", self.source_schema)
        safe_table = re.sub(r"[^A-Za-z0-9_]", "_", self.source_table)
        return f"dpone_{safe_schema}_{safe_table}_pub"

    @property
    def is_pgoutput(self) -> bool:
        return self.plugin == "pgoutput"

    @property
    def is_test_decoding(self) -> bool:
        return self.plugin == "test_decoding"


class PostgresLogicalCDCReader:
    """Bounded SQL-polling reader for PostgreSQL logical replication slots."""

    def __init__(self, connector: Any, config: PostgresLogicalCDCReaderConfig):
        self.connector = connector
        self.config = config
        self._test_decoding_parser = TestDecodingMessageParser(
            source_schema=config.source_schema,
            source_table=config.source_table,
        )
        self._pgoutput_parser = PgOutputMessageParser(
            source_schema=config.source_schema,
            source_table=config.source_table,
        )

    def setup(self) -> None:
        """Create publication and logical replication slot if configured."""

        if self.config.drop_existing_slot:
            self.drop_slot(missing_ok=True)
        if self.config.is_pgoutput and self.config.create_publication_if_missing:
            self._create_publication_if_missing()
        if not self.config.create_slot_if_missing:
            return
        rows = self.connector.get_records(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
            (self.config.slot_name,),
            as_dict=True,
        )
        if rows:
            return
        self.connector.get_records(
            "SELECT * FROM pg_create_logical_replication_slot(%s, %s)",
            (self.config.slot_name, self.config.plugin),
            as_dict=True,
        )

    def drop_slot(self, *, missing_ok: bool = True) -> None:
        """Drop the replication slot owned by this reader."""

        if missing_ok:
            self.connector.get_records(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                (self.config.slot_name,),
                as_dict=True,
            )
            return
        self.connector.get_records("SELECT pg_drop_replication_slot(%s)", (self.config.slot_name,), as_dict=True)

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        """Consume up to ``max_changes`` messages from the logical slot."""

        del start_offset
        if self.config.is_pgoutput:
            return self._read_pgoutput_batch(max_changes)
        if self.config.is_test_decoding:
            return self._read_test_decoding_batch(max_changes)
        raise ValueError(
            f"Unsupported PostgreSQL logical decoding plugin: {self.config.plugin!r}. "
            "Supported built-ins: 'pgoutput', 'test_decoding'. "
            "Use a dedicated plugin adapter for decoderbufs/logical_buffers-style payloads."
        )

    def _read_pgoutput_batch(self, max_changes: int) -> CDCBatch:
        rows = self.connector.get_records(
            """
SELECT lsn::text AS lsn, xid::text AS xid, data
FROM pg_logical_slot_get_binary_changes(
    %s,
    NULL,
    %s,
    'proto_version',
    %s,
    'publication_names',
    %s
)
""",
            (
                self.config.slot_name,
                max_changes,
                str(self.config.proto_version),
                self.config.resolved_publication_name,
            ),
            as_dict=True,
        )
        changes: list[CDCChange] = []
        high_watermark: str | None = None
        for row in rows:
            lsn = str(row.get("lsn"))
            high_watermark = lsn
            payload = row.get("data")
            if payload is None:
                continue
            change = self._pgoutput_parser.parse(lsn, self._optional_str(row.get("xid")), payload)
            if change is not None:
                changes.append(change)
        return CDCBatch(changes=tuple(changes), next_offset=self._offset(high_watermark), high_watermark=high_watermark)

    def _read_test_decoding_batch(self, max_changes: int) -> CDCBatch:
        rows = self.connector.get_records(
            "SELECT lsn::text AS lsn, xid::text AS xid, data FROM pg_logical_slot_get_changes(%s, NULL, %s)",
            (self.config.slot_name, max_changes),
            as_dict=True,
        )
        changes: list[CDCChange] = []
        high_watermark: str | None = None
        for row in rows:
            lsn = str(row.get("lsn"))
            high_watermark = lsn
            change = self._test_decoding_parser.parse(
                lsn,
                self._optional_str(row.get("xid")),
                str(row.get("data", "")),
            )
            if change is not None:
                changes.append(change)
        return CDCBatch(changes=tuple(changes), next_offset=self._offset(high_watermark), high_watermark=high_watermark)

    def _create_publication_if_missing(self) -> None:
        publication = self.config.resolved_publication_name
        rows = self.connector.get_records(
            "SELECT pubname FROM pg_publication WHERE pubname = %s",
            (publication,),
            as_dict=True,
        )
        if rows:
            return
        self.connector.execute_query(
            f'CREATE PUBLICATION "{self._escape_identifier(publication)}" FOR TABLE '
            f'"{self._escape_identifier(self.config.source_schema)}"."{self._escape_identifier(self.config.source_table)}"'
        )

    def _offset(self, token: str | None) -> CDCOffset | None:
        if token is None:
            return None
        return CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token=token, snapshot_complete=True)

    def _optional_str(self, value: Any) -> str | None:
        return None if value is None else str(value)

    def _escape_identifier(self, value: str) -> str:
        return value.replace('"', '""')
