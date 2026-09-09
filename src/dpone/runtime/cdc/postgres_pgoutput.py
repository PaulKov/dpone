"""PostgreSQL pgoutput logical decoding parser."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from dpone.runtime.cdc.base import CDCChange, CDCOperation


@dataclass(frozen=True, slots=True)
class PgOutputColumn:
    name: str
    type_oid: int
    type_modifier: int
    flags: int


@dataclass(frozen=True, slots=True)
class PgOutputRelation:
    relation_id: int
    namespace: str
    name: str
    replica_identity: str
    columns: tuple[PgOutputColumn, ...]


class PgOutputMessageParser:
    """Parser for PostgreSQL ``pgoutput`` binary messages.

    The implementation covers the relation metadata and row-level insert,
    update, and delete messages needed for CDC loading. Values are decoded from
    pgoutput text tuples by PostgreSQL type OID where safe; temporal and unknown
    types are preserved as strings for downstream type mapping.
    """

    __test__ = False

    _int_oids = {20, 21, 23}
    _float_oids = {700, 701}
    _numeric_oids = {1700}
    _bool_oids = {16}

    def __init__(self, *, source_schema: str, source_table: str):
        self.source_schema = source_schema
        self.source_table = source_table
        self._relations: dict[int, PgOutputRelation] = {}

    def parse(self, lsn: str, xid: str | None, payload: bytes | bytearray | memoryview) -> CDCChange | None:
        reader = _BinaryReader(bytes(payload))
        tag = reader.read_char()
        if tag == "R":
            relation = self._parse_relation(reader)
            self._relations[relation.relation_id] = relation
            return None
        if tag == "I":
            return self._parse_insert(reader, lsn, xid)
        if tag == "U":
            return self._parse_update(reader, lsn, xid)
        if tag == "D":
            return self._parse_delete(reader, lsn, xid)
        return None

    def _parse_relation(self, reader: _BinaryReader) -> PgOutputRelation:
        relation_id = reader.read_uint32()
        namespace = reader.read_cstring()
        name = reader.read_cstring()
        replica_identity = reader.read_char()
        column_count = reader.read_uint16()
        columns: list[PgOutputColumn] = []
        for _ in range(column_count):
            flags = reader.read_uint8()
            column_name = reader.read_cstring()
            type_oid = reader.read_uint32()
            type_modifier = reader.read_int32()
            columns.append(PgOutputColumn(column_name, type_oid, type_modifier, flags))
        return PgOutputRelation(relation_id, namespace, name, replica_identity, tuple(columns))

    def _parse_insert(self, reader: _BinaryReader, lsn: str, xid: str | None) -> CDCChange | None:
        relation_id = reader.read_uint32()
        tuple_kind = reader.read_char()
        if tuple_kind != "N":
            return None
        relation = self._relation_for(relation_id)
        if relation is None:
            return None
        return CDCChange(
            operation=CDCOperation.INSERT,
            data=self._read_tuple(reader, relation),
            position=lsn,
            transaction_id=xid,
            source_schema=relation.namespace,
            source_table=relation.name,
            metadata={"plugin": "pgoutput"},
        )

    def _parse_update(self, reader: _BinaryReader, lsn: str, xid: str | None) -> CDCChange | None:
        relation_id = reader.read_uint32()
        relation = self._relation_for(relation_id)
        if relation is None:
            return None

        before: dict[str, Any] | None = None
        tuple_kind = reader.read_char()
        if tuple_kind in {"K", "O"}:
            before = self._read_tuple(reader, relation)
            tuple_kind = reader.read_char()
        if tuple_kind != "N":
            return None
        return CDCChange(
            operation=CDCOperation.UPDATE,
            data=self._read_tuple(reader, relation),
            before=before,
            position=lsn,
            transaction_id=xid,
            source_schema=relation.namespace,
            source_table=relation.name,
            metadata={"plugin": "pgoutput"},
        )

    def _parse_delete(self, reader: _BinaryReader, lsn: str, xid: str | None) -> CDCChange | None:
        relation_id = reader.read_uint32()
        tuple_kind = reader.read_char()
        if tuple_kind not in {"K", "O"}:
            return None
        relation = self._relation_for(relation_id)
        if relation is None:
            return None
        return CDCChange(
            operation=CDCOperation.DELETE,
            data=self._read_tuple(reader, relation),
            position=lsn,
            transaction_id=xid,
            source_schema=relation.namespace,
            source_table=relation.name,
            metadata={"plugin": "pgoutput"},
        )

    def _relation_for(self, relation_id: int) -> PgOutputRelation | None:
        relation = self._relations.get(relation_id)
        if relation is None:
            return None
        if relation.namespace != self.source_schema or relation.name != self.source_table:
            return None
        return relation

    def _read_tuple(self, reader: _BinaryReader, relation: PgOutputRelation) -> dict[str, Any]:
        value_count = reader.read_uint16()
        values: dict[str, Any] = {}
        for index in range(value_count):
            column = relation.columns[index]
            value_kind = reader.read_char()
            if value_kind == "n":
                values[column.name] = None
            elif value_kind == "u":
                values[column.name] = None
            elif value_kind == "t":
                raw = reader.read_bytes(reader.read_uint32()).decode("utf-8")
                values[column.name] = self._convert_value(raw, column.type_oid)
            else:
                raise ValueError(f"Unsupported pgoutput tuple value kind: {value_kind!r}")
        return values

    def _convert_value(self, raw: str, type_oid: int) -> Any:
        if type_oid in self._bool_oids:
            return raw.lower() in {"t", "true", "1"}
        if type_oid in self._int_oids:
            return int(raw)
        if type_oid in self._float_oids:
            return float(raw)
        if type_oid in self._numeric_oids:
            decimal = Decimal(raw)
            return int(decimal) if decimal == decimal.to_integral_value() else float(decimal)
        return raw


class _BinaryReader:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.offset = 0

    def read_char(self) -> str:
        return chr(self.read_uint8())

    def read_uint8(self) -> int:
        value = self.payload[self.offset]
        self.offset += 1
        return value

    def read_uint16(self) -> int:
        return int.from_bytes(self.read_bytes(2), "big", signed=False)

    def read_uint32(self) -> int:
        return int.from_bytes(self.read_bytes(4), "big", signed=False)

    def read_int32(self) -> int:
        return int.from_bytes(self.read_bytes(4), "big", signed=True)

    def read_bytes(self, length: int) -> bytes:
        end = self.offset + length
        value = self.payload[self.offset : end]
        self.offset = end
        return value

    def read_cstring(self) -> str:
        end = self.payload.index(0, self.offset)
        value = self.payload[self.offset : end].decode("utf-8")
        self.offset = end + 1
        return value


__all__ = ["PgOutputColumn", "PgOutputMessageParser", "PgOutputRelation"]
