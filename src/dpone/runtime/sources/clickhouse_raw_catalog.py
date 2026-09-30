"""Bounded read-only catalog authority for an explicitly pinned native session.

Vendor metadata and coordinates remain private in memory. Public descriptors
receive only canonical digests. The legacy connector logging/query path is not
used, so vendor errors cannot expose endpoints, principals or SQL predicates.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from uuid import UUID

from dpone.contracts.clickhouse_raw_read_policy import RAW_READ_SETTINGS
from dpone.contracts.strict_json import canonical_json_bytes

MAX_PARTS = 8192
_MAX_METADATA_BYTES = 8 * 1024 * 1024


def digest(value: Any) -> str:
    """Hash private canonical metadata without exposing its preimage."""
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class RawSnapshotCatalog:
    """One dedicated TLS socket; reconnects and native host rotation fail closed."""

    def __init__(self, connector: Any, database: str, table: str) -> None:
        self.client = connector.connection
        self.connection = self.client.connection
        self.params = {"database": database, "table": table}
        connection = self.connection
        if (
            getattr(connector, "driver", None) != "native"
            or any(
                getattr(connection, key, None) is not True for key in ("secure_socket", "verify_cert", "check_hostname")
            )
            or any(
                type(getattr(connection, key, None)) not in (int, float) or not 0 < getattr(connection, key) <= 86400
                for key in ("connect_timeout", "send_receive_timeout")
            )
        ):
            raise ValueError("mssql_native.source_read_profile_unsupported")
        if len(connection.hosts) != 1 or bool(getattr(self.client, "connections", None)):
            raise ValueError("mssql_native.replica_scope_unproven")
        connection.disable_reconnect = True
        connection.settings_is_important = True
        self.settings = {**RAW_READ_SETTINGS, "max_execution_time": max(1, int(connection.send_receive_timeout))}
        self._socket: Any = None

    def assert_session(self) -> None:
        """Do not let a lost session silently acquire a different query authority."""
        if self.client.connection is not self.connection or (
            self._socket is not None and (self.connection.socket is not self._socket or not self.connection.connected)
        ):
            raise ValueError("mssql_native.replica_scope_unproven")

    def read(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute bounded catalog reads with throwing limits and sanitized errors."""
        self.assert_session()
        try:
            rows, columns = self.client.execute(
                query,
                params or self.params,
                with_column_types=True,
                settings={**self.settings, "max_result_rows": MAX_PARTS, "max_result_bytes": _MAX_METADATA_BYTES},
            )
            self.assert_session()
            if self._socket is None:
                self._socket = self.connection.socket
            if self._socket is None or not self.connection.connected or len(rows) > MAX_PARTS:
                raise ValueError("metadata unavailable")
            result = [dict(zip((name for name, _ in columns), row, strict=True)) for row in rows]
            if len(canonical_json_bytes(result)) > _MAX_METADATA_BYTES:
                raise ValueError("metadata bound exceeded")
            return result
        except Exception:
            raise ValueError("mssql_native.source_read_profile_unsupported") from None

    def relation(self) -> tuple[dict[str, Any], tuple[tuple[str, str, str], ...]]:
        """Require one Atomic relation with exact immutable UUID and ordinary schema."""
        rows = self.read(
            "SELECT t.engine, t.engine_full, toString(t.uuid) AS uuid, d.engine AS database_engine, t.partition_key, t.sorting_key, t.primary_key FROM system.tables t INNER JOIN system.databases d ON t.database=d.name WHERE t.database=%(database)s AND t.name=%(table)s"
        )
        if len(rows) != 1 or rows[0]["database_engine"] != "Atomic":
            raise ValueError("mssql_native.replacing_raw_snapshot_required")
        table = rows[0]
        try:
            if UUID(table["uuid"]).int == 0:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError("mssql_native.source_uuid_required") from None
        columns = self.read(
            "SELECT name, type, default_kind FROM system.columns WHERE database=%(database)s AND table=%(table)s ORDER BY position"
        )
        if not columns or any(column["default_kind"] not in ("", None) for column in columns):
            raise ValueError("mssql_native.ordinary_columns_required")
        schema = tuple((column["name"], column["type"], column["default_kind"] or "") for column in columns)
        if len({name for name, _, _ in schema}) != len(schema):
            raise ValueError("mssql_native.duplicate_columns")
        if any(name in {"_part", "_part_offset"} for name, _, _ in schema):
            raise ValueError("mssql_native.source_provenance_incomplete")
        return table, schema

    def authority(self, scope: str) -> dict[str, Any]:
        """Bind effective settings, applicable policy and principal/replica identity."""
        settings = self.read(
            "SELECT name, value FROM system.settings WHERE name IN %(names)s ORDER BY name",
            {"names": (*self.settings, "additional_table_filters", "additional_result_filter", "filter")},
        )
        effective = {row["name"]: str(row["value"]) for row in settings}
        if any(effective.get(name) != str(value) for name, value in self.settings.items()):
            raise ValueError("mssql_native.source_read_profile_unsupported")
        filters = {
            name: effective.get(name) for name in ("additional_table_filters", "additional_result_filter", "filter")
        }
        if (
            filters["additional_table_filters"] not in ("{}", "[]")
            or filters["additional_result_filter"] != ""
            or filters["filter"] not in (None, "")
        ):
            raise ValueError("mssql_native.source_read_profile_unsupported")
        identities = self.read(
            "SELECT currentUser() AS principal, enabledRoles() AS roles, toString(revision()) AS revision, hostName() AS server, serverTimeZone() AS server_timezone, timezone() AS session_timezone"
        )
        if len(identities) != 1:
            raise ValueError("mssql_native.source_read_profile_unsupported")
        identity = identities[0]
        self.timezones = (identity.get("server_timezone"), identity.get("session_timezone"))
        policies = self.read(
            "SELECT toString(id) AS id, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except FROM system.row_policies WHERE database=%(database)s AND (table=%(table)s OR table='') ORDER BY id"
        )
        names = {identity["principal"], *identity["roles"]}
        applicable = [
            row
            for row in policies
            if (row["apply_to_all"] or names.intersection(row["apply_to_list"]))
            and not names.intersection(row["apply_to_except"])
        ]
        replica = None
        if scope == "connected_replica":
            replicas = self.read(
                "SELECT replica_name, zookeeper_path FROM system.replicas WHERE database=%(database)s AND table=%(table)s"
            )
            if len(replicas) != 1 or not all(replicas[0].get(key) for key in ("replica_name", "zookeeper_path")):
                raise ValueError("mssql_native.replica_scope_unproven")
            replica = digest(replicas[0])
        certificate = self._socket.getpeercert(binary_form=True)
        if not certificate:
            raise ValueError("mssql_native.source_read_profile_unsupported")
        return dict(
            server_revision=identity["revision"],
            endpoint_authority_sha256=digest(
                [identity["server"], self._socket.getpeername(), hashlib.sha256(certificate).hexdigest()]
            ),
            replica_identity_sha256=replica,
            principal_authority_sha256=digest([identity["principal"], sorted(identity["roles"])]),
            row_policy_sha256=digest([policies, filters]),
            policy_filtered=bool(applicable),
        )

    def physical(self, partition_ids: tuple[str, ...] | None, *, reject_mutations: bool) -> dict[str, dict[str, Any]]:
        """Bound base/patch parts and mask-bearing checksums; never truncate parts.

        All patch partitions are conservatively included for a bounded window,
        because an overlay can reference its base partition indirectly.
        """
        mutations = self.read(
            "SELECT mutation_id FROM system.mutations WHERE database=%(database)s AND table=%(table)s AND NOT is_done ORDER BY mutation_id"
        )
        if mutations and reject_mutations:
            raise ValueError("mssql_native.source_read_profile_unsupported")
        predicate = ""
        params: dict[str, Any] = dict(self.params)
        if partition_ids is not None:
            predicate = " AND (partition_id IN %(partitions)s OR startsWith(partition_id, 'patch-'))"
            params["partitions"] = partition_ids
        parts = self.read(
            "SELECT name, partition_id, rows, data_version, level, has_lightweight_delete, hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files FROM system.parts WHERE database=%(database)s AND table=%(table)s AND active"
            + predicate
            + " ORDER BY name",
            params,
        )
        result = {}
        for part in parts:
            if (
                not isinstance(part.get("partition_id"), str)
                or not part["partition_id"]
                or any(type(part.get(key)) is not int or part[key] < 0 for key in ("rows", "data_version", "level"))
                or type(part.get("has_lightweight_delete")) is not int
                or part["has_lightweight_delete"] not in (0, 1)
            ):
                raise ValueError("mssql_native.source_provenance_incomplete")
            if any(
                not isinstance(part.get(key), str)
                or re.fullmatch(r"[0-9a-fA-F]{32}", part[key]) is None
                or int(part[key], 16) == 0
                for key in ("hash_of_all_files", "hash_of_uncompressed_files", "uncompressed_hash_of_compressed_files")
            ):
                raise ValueError("mssql_native.source_provenance_incomplete")
            if (
                not isinstance(part.get("name"), str)
                or not part["name"]
                or part["name"] in result
                or type(part.get("rows")) is not int
                or part["rows"] < 0
            ):
                raise ValueError("mssql_native.source_provenance_incomplete")
            result[part["name"]] = part
        # Pending mutations change the physical observation even before new
        # immutable parts become visible. No mutation command text is retained.
        self.mutation_sha256 = digest(mutations)
        return result


def engine_signature(engine: str, full: str, scope: str) -> str:
    """Parse the exact engine and arguments; hash private replicated coordinates."""
    expected = {"ReplacingMergeTree": "single_server", "ReplicatedReplacingMergeTree": "connected_replica"}
    if engine not in expected:
        raise ValueError("mssql_native.replacing_raw_snapshot_required")
    if expected[engine] != scope:
        raise ValueError("mssql_native.replica_scope_unproven")
    literal = r"'(?:[^'\\]|\\.|'')*'"
    identifier = r"(?:[A-Za-z_][A-Za-z0-9_]*|`[A-Za-z_][A-Za-z0-9_]*`)"
    argument = f"(?:{literal}|{identifier})"
    match = re.fullmatch(
        re.escape(engine)
        + rf"(?:\(\s*({argument}(?:\s*,\s*{argument})*)?\s*\))?(?P<tail> (?:PARTITION BY|PRIMARY KEY|ORDER BY|SAMPLE BY|TTL|SETTINGS) .*)?",
        full,
    )
    if match is None:
        raise ValueError("mssql_native.replacing_raw_snapshot_required")
    args = re.findall(argument, match.group(1) or "")
    if engine.startswith("Replicated") and args and args[0].startswith("'"):
        if len(args) < 2 or not args[1].startswith("'"):
            raise ValueError("mssql_native.replacing_raw_snapshot_required")
        args = args[2:]
    if len(args) > 2 or any(re.fullmatch(identifier, value) is None for value in args):
        raise ValueError("mssql_native.replacing_raw_snapshot_required")
    return engine + ":sha256:" + digest(full)
