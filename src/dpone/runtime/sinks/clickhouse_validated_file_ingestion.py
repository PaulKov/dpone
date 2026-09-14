"""Explicit validated-file stage orchestration, ownership and failure evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import asdict, replace
from typing import Any
from uuid import UUID, uuid4

from dpone.config.load_config import LoadConfig
from dpone.runtime.clickhouse_file_stage_contract import (
    CHUNK_BYTES,
    REMOTE_CONFIRMATION_SECONDS,
    ClickHouseFileJournalResource,
    ClickHouseFilePlan,
    ClickHouseFileStageRunner,
    ClickHouseValidatedFilePolicy,
    EndpointBinding,
    FileConsumptionError,
    IdentifiedStageQuery,
    QueryIdentity,
    QueryKind,
    StageQueryResult,
    response_rows,
    sql_identifier,
    sql_literal,
)
from dpone.runtime.etl.validated_file_artifact import ContractValidatedFileArtifact, FileValidationAttempt
from dpone.runtime.file_artifact_authority import FileVerificationBudget
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
from dpone.runtime.sinks.clickhouse_staged_evidence import staged_handle_metadata
from dpone.runtime.sinks.clickhouse_validated_file_preparation import (
    ClickHouseValidatedFilePreparer,
    PreparedClickHouseFile,
    config_digest,
    make_plan,
)
from dpone.runtime.sinks.load_payload import LoadPayload


class _StageSession:
    """One query sequence and one verified table; never a source terminal owner."""

    def __init__(
        self,
        runner: ClickHouseFileStageRunner,
        journal: ClickHouseFileJournalResource,
        plan: ClickHouseFilePlan,
        endpoint: EndpointBinding,
        clock: Callable[[], float],
        preparation_budget: FileVerificationBudget,
        verification_seconds: int,
    ) -> None:
        self.runner, self.journal, self.plan, self.endpoint, self.clock = runner, journal, plan, endpoint, clock
        self.preparation_budget = preparation_budget
        self.verification_seconds = verification_seconds
        self.verification_budget: FileVerificationBudget | None = None
        self.table = "__dpone_b02_" + journal.attempt_id
        self.qualified = f"{sql_identifier(plan.target_database)}.{sql_identifier(self.table)}"
        self.marker = "dpone-b02:" + journal.attempt_id
        self.owned_uuid: str | None = None
        self.last_mutation: QueryIdentity | None = None
        self.remote_known = True
        self.local_stopped = True

    def execute(
        self,
        kind: QueryKind,
        sql: str,
        *,
        tag: str = "",
        chunks: Iterable[bytes] | None = None,
        budget: FileVerificationBudget | None = None,
    ) -> StageQueryResult:
        identity = QueryIdentity(
            f"dpone-b02-{self.journal.attempt_id}-{kind}" + (f"-{tag}" if tag else ""), kind, self.endpoint
        )
        mutation = kind in {"create", "insert", "drop"}
        if mutation:
            if kind == "create":
                self.preparation_budget.check()
            elif self.runner.preflight(self.plan) != self.endpoint:
                raise FileConsumptionError("transport_changed")
            phase = {"create": "creating_staging", "insert": "loading", "drop": "cleanup"}[kind]
            self.journal.record(
                phase=phase,
                outcome="running",
                query_id=identity.query_id,
                query_kind=kind,
                endpoint_identity=asdict(self.endpoint),
                owned_staging=self.ownership(),
                local_sender_state="not_started",
                remote_execution_state="not_submitted",
            )
            if kind == "create":
                self.preparation_budget.check()
            self.last_mutation, self.remote_known = identity, False
        timeout = (
            self.plan.transport_timeout_seconds
            if budget is None
            else min(self.plan.transport_timeout_seconds, budget.remaining())
        )
        result = self.runner.execute(
            IdentifiedStageQuery(identity, sql),
            chunks=chunks,
            deadline_monotonic=self.clock() + timeout,
        )
        if budget is not None:
            budget.check()
        if result.query_id != identity.query_id or result.remote_state != "completed":
            raise FileConsumptionError("remote_execution_unknown", phase="loading")
        if mutation:
            self.remote_known = True
            if kind == "insert":
                self.begin_verification()
            self.journal.record(local_sender_state="stopped", remote_execution_state="completed")
        return result

    def ownership(self) -> dict[str, Any]:
        return {
            "database": self.plan.target_database,
            "table": self.table,
            "uuid": self.owned_uuid,
            "marker": self.marker,
        }

    def create(self) -> None:
        columns = ", ".join(f"{sql_identifier(name)} {dtype}" for name, dtype in self.plan.target_schema)
        self.execute(
            "create",
            f"CREATE TABLE {self.qualified} ({columns}) ENGINE=MergeTree ORDER BY tuple() COMMENT {sql_literal(self.marker)}",
        )
        self.inspect("created")
        self.inspect_columns("created")

    def inspect_columns(self, tag: str, *, budget: FileVerificationBudget | None = None) -> None:
        sql = f"SELECT name, type, position, default_kind, default_expression FROM system.columns WHERE database={sql_literal(self.plan.target_database)} AND table={sql_literal(self.table)} ORDER BY position FORMAT JSONCompactEachRow"
        rows = response_rows(self.execute("describe", sql, tag="columns" + tag, budget=budget).response)
        expected = [[name, dtype, index, "", ""] for index, (name, dtype) in enumerate(self.plan.target_schema, 1)]
        if rows != expected or any(type(row[2]) is not int for row in rows):
            raise FileConsumptionError("schema_changed", phase="creating_staging")

    def inspect(self, tag: str, *, allow_absent: bool = False, budget: FileVerificationBudget | None = None) -> bool:
        sql = f"SELECT toString(serverUUID()), database, name, toString(uuid), comment, engine, is_temporary FROM system.tables WHERE database={sql_literal(self.plan.target_database)} AND name={sql_literal(self.table)} FORMAT JSONCompactEachRow"
        rows = response_rows(self.execute("describe", sql, tag=tag, budget=budget).response)
        if not rows and allow_absent:
            return False
        if len(rows) != 1 or len(rows[0]) != 7:
            raise FileConsumptionError("schema_changed", phase="verifying")
        server, database, table, raw_uuid, marker, engine, temporary = rows[0]
        if (server, database, table, marker, engine, temporary) != (
            self.endpoint.server_uuid,
            self.plan.target_database,
            self.table,
            self.marker,
            "MergeTree",
            0,
        ) or type(temporary) is not int:
            raise FileConsumptionError("schema_changed", phase="verifying")
        table_uuid = str(raw_uuid)
        if UUID(table_uuid).int == 0 or (self.owned_uuid is not None and table_uuid != self.owned_uuid):
            raise FileConsumptionError("schema_changed", phase="verifying")
        self.owned_uuid = table_uuid
        self.journal.record(owned_staging=self.ownership())
        return True

    def begin_verification(self) -> None:
        self.verification_budget = FileVerificationBudget(
            self.clock, self.clock() + self.verification_seconds, self.preparation_budget.max_bytes
        )

    def load(self, prepared: PreparedClickHouseFile) -> FileVerificationBudget:
        if prepared.rows:
            columns = ", ".join(sql_identifier(name) for name, _ in self.plan.target_schema)
            chunks = iter(lambda: prepared.stream.read(CHUNK_BYTES), b"")
            result = self.execute("insert", f"INSERT INTO {self.qualified} ({columns}) FORMAT RowBinary", chunks=chunks)
            if (
                type(result.emitted_bytes) is not int
                or result.emitted_bytes != prepared.size_bytes
                or result.emitted_sha256 != prepared.sha256
            ):
                raise FileConsumptionError("transport_changed", phase="loading")
        else:
            self.begin_verification()
            if prepared.size_bytes != 0 or prepared.sha256 != hashlib.sha256(b"").hexdigest():
                raise FileConsumptionError("transport_changed", phase="loading")
            self.journal.record(
                query_id=None,
                query_kind="insert",
                local_sender_state="not_submitted_empty",
                remote_execution_state="not_submitted_empty",
            )

        assert self.verification_budget is not None
        self.verification_budget.check()
        return self.verification_budget

    def verify(self, expected_rows: int, budget: FileVerificationBudget) -> int:
        result = self.execute("count", f"SELECT count() FROM {self.qualified} FORMAT JSONCompactEachRow", budget=budget)
        rows = response_rows(result.response)
        if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] != expected_rows:
            raise FileConsumptionError("staging_count_mismatch", phase="verifying")
        self.journal.record(phase="verifying", rows_observed=rows[0][0])
        self.inspect("verified", budget=budget)
        self.inspect_columns("verified", budget=budget)
        budget.check()
        return rows[0][0]

    def _settle_mutation(self) -> bool:
        if not self.remote_known and self.last_mutation is not None:
            observation = self.runner.cancel_and_observe(
                self.last_mutation, deadline_monotonic=self.clock() + REMOTE_CONFIRMATION_SECONDS
            )
            self.local_stopped = observation.local_state == "stopped"
            self.remote_known = observation.remote_state in {"completed", "cancelled"}
            self.journal.record(
                local_sender_state=observation.local_state, remote_execution_state=observation.remote_state
            )
        return self.local_stopped and self.remote_known

    def cleanup(self) -> str:
        if not self._settle_mutation():
            return "retained_unknown"
        if self.last_mutation is not None and self.inspect("cleanup", allow_absent=True):
            try:
                self.execute("drop", f"DROP TABLE {self.qualified}")
            except BaseException:
                if not self._settle_mutation():
                    return "retained_unknown"
                if self.inspect("afterdrop", allow_absent=True):
                    raise
        return "failed_cleaned"


class ClickHouseValidatedFileService:
    """Explicit method owner; automatic runtime/schema phases are never invoked."""

    def __init__(
        self,
        *,
        runner_factory: Callable[[LoadConfig, ClickHouseValidatedFilePolicy], ClickHouseFileStageRunner],
        resolver: ClickHousePhysicalColumnTypeResolver,
        clock: Callable[[], float],
        journal_factory: Callable[[ClickHouseValidatedFilePolicy, str], ClickHouseFileJournalResource],
    ) -> None:
        self.runner_factory, self.resolver = runner_factory, resolver
        self.clock, self.journal_factory = clock, journal_factory
        self._retained_local: list[tuple[_StageSession, PreparedClickHouseFile]] = []
        self._retained_probes: list[ClickHouseFileStageRunner] = []

    def stage(
        self, load_config: LoadConfig, payload: LoadPayload, *, policy: ClickHouseValidatedFilePolicy
    ) -> StagedLoadHandle:
        if (
            not isinstance(payload, LoadPayload)
            or not isinstance(payload.artifact, ContractValidatedFileArtifact)
            or payload.target_projection is not None
        ):
            raise FileConsumptionError("source_type_unsupported")
        attempt_id = uuid4().hex
        preparation_budget = FileVerificationBudget(
            self.clock, self.clock() + policy.preparation_timeout_seconds, policy.max_source_bytes
        )
        with payload.artifact.file_validation_attempt(attempt_id, verification_budget=preparation_budget) as attempt:
            plan = make_plan(load_config, payload.schema, resolver=self.resolver)
            frozen = deepcopy(load_config)
            with self.journal_factory(policy, attempt_id) as journal:
                prepared, session = None, None
                try:
                    prepared = ClickHouseValidatedFilePreparer(clock=self.clock).prepare(
                        attempt, plan, policy, journal, verification_budget=preparation_budget
                    )
                    runner = self.runner_factory(frozen, policy)
                    try:
                        endpoint = runner.preflight(
                            replace(
                                plan,
                                transport_timeout_seconds=min(
                                    plan.transport_timeout_seconds, preparation_budget.remaining()
                                ),
                            )
                        )
                    except BaseException:
                        if not getattr(runner, "local_stopped", False):
                            self._retained_probes.append(runner)
                        raise
                    session = _StageSession(
                        runner,
                        journal,
                        plan,
                        endpoint,
                        self.clock,
                        preparation_budget,
                        policy.verification_timeout_seconds,
                    )
                    self._recheck(attempt, prepared, journal, load_config, preparation_budget)
                    session.create()
                    verification_budget = session.load(prepared)
                    observed = session.verify(prepared.rows, verification_budget)
                    self._recheck(attempt, prepared, journal, load_config, verification_budget)
                    prepared.stream.close()
                    journal.release_spool()
                    attempt.complete(observed, verification_budget=verification_budget)
                    final = journal.record(
                        outcome="staged", cleanup={"source": "unchanged", "spool": "released", "staging": "transferred"}
                    )
                    verification_budget.check()
                    staging = replace(frozen, target_table=session.table)
                    metadata = staged_handle_metadata(frozen, staging, None, None, payload)
                    metadata["validated_file_consumption"] = {**final, "journal_directory": str(journal.directory)}
                    return StagedLoadHandle(
                        staging_config=staging,
                        payload_schema=plan.source_schema,
                        staged_rows=observed,
                        metadata=metadata,
                    )
                except BaseException as error:
                    self._failure(error, journal, session, prepared)
                    raise

    def _recheck(
        self,
        attempt: FileValidationAttempt,
        prepared: PreparedClickHouseFile,
        journal: ClickHouseFileJournalResource,
        config: LoadConfig,
        budget: FileVerificationBudget,
    ) -> None:
        budget.check()
        attempt.verify_unchanged(verification_budget=budget)
        budget.check()
        if config_digest(config) != prepared.plan.config_sha256:
            raise FileConsumptionError("schema_changed")
        prepared.verify_transport(journal, check_deadline=budget.check)
        budget.check()

    def _failure(
        self,
        error: BaseException,
        journal: ClickHouseFileJournalResource,
        session: _StageSession | None,
        prepared: PreparedClickHouseFile | None,
    ) -> None:
        outcome = "failed_cleaned"
        try:
            if session is not None:
                outcome = session.cleanup()
            if prepared is not None:
                if session is None or session.local_stopped:
                    prepared.stream.close()
                else:
                    self._retained_local.append((session, prepared))
            if outcome == "failed_cleaned":
                journal.release_spool()
        except BaseException as cleanup_error:
            outcome = "cleanup_failed"
            if session is not None:
                session.local_stopped = bool(getattr(session.runner, "local_stopped", False))
                if prepared is not None and not session.local_stopped:
                    self._retained_local.append((session, prepared))
            if prepared is not None and (session is None or session.local_stopped):
                prepared.stream.close()
            add_exception_note(error, f"validated staging cleanup failed:{type(cleanup_error).__name__}")
        journal.attach_failure(error, outcome)
