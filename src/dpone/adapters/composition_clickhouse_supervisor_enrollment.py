"""Exact protected supervisor policy and original enrollment, never auto-adoption."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from dpone.adapters.composition_clickhouse_principal import require_uuid
from dpone.adapters.composition_clickhouse_supervisor_linux import digest, require
from dpone.adapters.composition_clickhouse_supervisor_schema import require_clickhouse_supervisor_schema
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_identity import require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def container_id(value: object) -> str:
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None, "container_id")
    assert isinstance(value, str)
    return value


def absolute_path(value: object) -> str:
    require(
        type(value) is str
        and 1 < len(value) <= 1024
        and value.startswith("/")
        and str(PurePosixPath(value)) == value
        and ".." not in PurePosixPath(value).parts
        and not any(ord(char) < 32 for char in value),
        "path",
    )
    assert isinstance(value, str)
    return value


@dataclass(frozen=True, slots=True)
class ClickHouseSupervisorEnrollment:
    """Externally reviewed policy plus complete expected stable observation facts.

    policy.roles maps full Docker IDs to clickhouse/dispatcher/worker/control.
    Exactly one CH and dispatcher are mandatory. All running containers, even
    unrelated control services, are explicitly enrolled. config_roots maps the
    protected CH/dispatcher IDs to complete readonly configuration/code trees.
    """

    enrollment_sha256: str
    document: bytes = field(repr=False)

    def __post_init__(self) -> None:
        require_digest(self.enrollment_sha256)
        require(
            type(self.document) is bytes
            and 0 < len(self.document) <= 65536
            and digest(self.document) == self.enrollment_sha256,
            "enrollment_original",
        )
        body = strict_json_object(self.document)
        require(
            set(body)
            == {
                "schema",
                "service_id",
                "database_uuid",
                "boot_id",
                "isolation_id",
                "target_enrollment_sha256",
                "policy",
                "facts",
            }
            and body["schema"] == "dpone.composition-clickhouse-supervisor-enrollment.v1"
            and canonical_json_bytes(body) == self.document,
            "enrollment_shape",
        )
        for name in ("service_id", "database_uuid", "boot_id", "isolation_id"):
            require_uuid(body[name])
        require_digest(body["target_enrollment_sha256"])
        policy = body["policy"]
        require(
            type(policy) is dict and set(policy) == {"roles", "network_id", "frontend_port", "config_roots"},
            "enrollment_policy",
        )
        roles = policy["roles"]
        require(type(roles) is dict and 2 <= len(roles) <= 64, "enrollment_roles")
        for identifier, role in roles.items():
            container_id(identifier)
            require(role in {"clickhouse", "dispatcher", "worker", "control"}, "enrollment_role")
        require(
            list(roles.values()).count("clickhouse") == list(roles.values()).count("dispatcher") == 1,
            "enrollment_roles",
        )
        container_id(policy["network_id"])
        require(
            type(policy["frontend_port"]) is int
            and 1024 <= policy["frontend_port"] <= 65535
            and policy["frontend_port"] != 8123,
            "enrollment_frontend",
        )
        roots = policy["config_roots"]
        protected = {key for key, role in roles.items() if role in {"clickhouse", "dispatcher"}}
        require(type(roots) is dict and set(roots) == protected, "enrollment_configs")
        for key, paths in roots.items():
            require(type(paths) is list and 1 <= len(paths) <= 8 and paths == sorted(set(paths)), "enrollment_configs")
            for path in paths:
                absolute_path(path)
            if roles[key] == "clickhouse":
                require("/etc/clickhouse-server" in paths, "enrollment_ch_config")
        facts = body["facts"]
        require(type(facts) is dict and set(facts) == {"docker", "linux"}, "enrollment_facts")
        require(type(facts["docker"]) is dict and type(facts["linux"]) is dict, "enrollment_facts")

    @property
    def body(self) -> dict[str, Any]:
        return strict_json_object(self.document)

    @property
    def policy(self) -> dict[str, Any]:
        return self.body["policy"]

    def role_id(self, role: str) -> str:
        return next(key for key, value in self.policy["roles"].items() if value == role)


class SupervisorEnrollmentReader:
    """Pin the actual supplied transaction and inspect external enrollment only."""

    def __init__(
        self,
        context: CompositionMssqlLedger,
        enrollment_sha256: str,
        attempt: CompositionAttemptIdentity,
        target: SnapshotTarget,
    ) -> None:
        require(isinstance(context, CompositionMssqlLedger), "sql_context")
        require_digest(enrollment_sha256)
        self.context, self.reference, self.attempt, self.target = context, enrollment_sha256, attempt, target
        self._cursor, self._schema, self._service = context.cursor, context.schema, context.expected_service_id
        self._transaction = context.require_transaction()
        require_clickhouse_supervisor_schema(context.cursor, context.schema)
        self.check()

    def check(self) -> None:
        require(
            self.context.cursor is self._cursor
            and (self.context.schema, self.context.expected_service_id) == (self._schema, self._service),
            "sql_context_changed",
        )
        self.context.require_transaction(self._transaction)

    def read(self) -> ClickHouseSupervisorEnrollment:
        self.check()
        occurrence, receipt = require_existing_execution_in(
            self.context,
            self.attempt,
            expected_service_id=self._service,
            terminal_validator=self.context.terminal_validator,
        )
        require(
            occurrence.receipt.state in {"ACTIVE", "RETIRING"} and receipt.state in {"RUNNING", "COMMIT_UNKNOWN"},
            "attempt_scope",
        )
        workload = next(row for row in occurrence.request.workloads if row.workload_id == self.attempt.workload_id)
        resource = next((row for row in occurrence.request.resources if row.guard_id == self.target.guard_id), None)
        require(
            resource is not None
            and resource.connector == "clickhouse"
            and resource.service_id == self.target.service_id
            and resource.physical_subject_sha256 == self.target.physical_subject_sha256
            and self.target.guard_id in dict(self.attempt.guard_epochs)
            and workload.execution_cell == "mssql_clickhouse_full_refresh_v1"
            and self.target.write_subject_sha256 in workload.write_subjects
            and self.target.write_subject_sha256 in resource.write_subjects,
            "target_scope",
        )
        self.context.cursor.execute(
            f"SELECT TOP (2) LOWER(CONVERT(char(36),service_id)),LOWER(CONVERT(char(36),database_uuid)),"
            "LOWER(CONVERT(char(36),boot_id)),LOWER(CONVERT(char(36),isolation_id)),"
            "CASE WHEN DATALENGTH(enrollment_document) BETWEEN 1 AND 65536 THEN enrollment_document END "
            f"FROM {self.context.table('ch_supervisor_enrollments')} WITH (HOLDLOCK) WHERE enrollment_sha256=?;",
            self.reference,
        )
        rows = tuple(tuple(row) for row in self.context.cursor.fetchall())
        self.check()
        require(len(rows) == 1 and len(rows[0]) == 5 and type(rows[0][4]) is bytes, "enrollment_missing")
        value = ClickHouseSupervisorEnrollment(self.reference, rows[0][4])
        body = value.body
        require(
            rows[0][:4] == tuple(body[name] for name in ("service_id", "database_uuid", "boot_id", "isolation_id"))
            and (body["service_id"], body["database_uuid"], body["target_enrollment_sha256"])
            == (self.target.service_id, self.target.database_id, self.target.enrollment_sha256),
            "enrollment_subject",
        )
        return value


def read_service_enrollment(context: CompositionMssqlLedger, service_id: str) -> ClickHouseSupervisorEnrollment:
    """Reopen the exclusive supervisor enrollment original for one CH service."""

    require(isinstance(context, CompositionMssqlLedger), "sql_context")
    require_uuid(service_id)
    require_clickhouse_supervisor_schema(context.cursor, context.schema)
    context.cursor.execute(
        "SELECT TOP (2) enrollment_sha256,"
        "CASE WHEN DATALENGTH(enrollment_document) BETWEEN 1 AND 65536 THEN enrollment_document END "
        f"FROM {context.table('ch_supervisor_enrollments')} WITH (HOLDLOCK) WHERE service_id=?;",
        service_id,
    )
    rows = tuple(tuple(row) for row in context.cursor.fetchall())
    require(len(rows) == 1 and len(rows[0]) == 2 and type(rows[0][1]) is bytes, "enrollment_missing")
    value = ClickHouseSupervisorEnrollment(str(rows[0][0]), rows[0][1])
    require(value.body["service_id"] == service_id, "enrollment_subject")
    return value


def require_attempt_enrollment_original(
    context: CompositionMssqlLedger,
    enrollment_sha256: str,
    attempt: CompositionAttemptIdentity,
    target: SnapshotTarget,
) -> ClickHouseSupervisorEnrollment:
    """Reopen the exact SQL enrollment original for this attempt and target.

    Callers must invoke this before principal creation, before dispatch and
    before snapshot publication. A digest without the retained document is
    not authority, and a later drifted original is a new rejection.
    """

    return SupervisorEnrollmentReader(context, enrollment_sha256, attempt, target).read()
