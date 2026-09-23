"""Bind one retained native input to an exact prepared SqlClient attempt."""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation
from dpone.adapters.mssql_sqlclient_native_importer import SqlClientInputCustody, plan_sha256
from dpone.app.mssql_sqlclient_observe_composition import SqlClientRetainedObserve
from dpone.app.mssql_sqlclient_preparation_composition import prepare_sqlclient_attempt
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan
from dpone.contracts.mssql_permission_preparation_capabilities import NativeWireColumnLayout, TdsInputReceipt
from dpone.contracts.mssql_sqlclient_input import SqlClientFileIdentity, SqlClientInputDescriptor
from dpone.contracts.mssql_sqlclient_preparation_qualification import (
    AdmittedPreparationBaseline,
    PreparationBaselineAuthority,
)
from dpone.contracts.mssql_tds_api import TdsAttemptPhase, WindowLease
from dpone.services.mssql_tds_attempt import TdsAttempt

ERROR = "mssql_native.sqlclient_prepared_attempt_invalid"


class PreparedInputDeployment(Protocol):
    """Fields required to bind a retained native file descriptor."""

    @property
    def wire_columns(self) -> tuple[NativeWireColumnLayout, ...]: ...

    @property
    def max_row_bytes(self) -> int: ...


def prepared_input_descriptor(
    deployment: PreparedInputDeployment,
    file: EncodedNativeFile,
    descriptor: int,
) -> SqlClientInputDescriptor:
    """Bind an open descriptor to its exact retained file metadata."""
    observed = os.fstat(descriptor)
    identity = SqlClientFileIdentity(
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )
    expected = TdsInputReceipt(file.rows, file.encoded_bytes, file.file_sha256)
    return SqlClientInputDescriptor(
        1, descriptor, deployment.wire_columns, expected, deployment.max_row_bytes, identity
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPreparedAttemptContext:
    """Deployment-owned capabilities for one already-created attempt."""

    handle: SqlClientRetainedObserve
    input_fd: int
    parent_input: SqlClientInputDescriptor
    baseline: AdmittedPreparationBaseline
    baseline_authority: PreparationBaselineAuthority
    build: AdmittedSqlClientInstallation
    reader_factory: PinnedEvidenceReadFactory
    evidence_root: Path
    release_input: Callable[[], None]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPreparedAttempt:
    """PREPARED attempt plus custody retained until terminal writer settlement."""

    attempt: TdsAttempt
    release_input: Callable[[], None]


OpenPreparedAttemptContext = Callable[
    [NativeChunkPlan, EncodedNativeFile, str, WindowLease, SqlClientInputCustody],
    AbstractContextManager[SqlClientPreparedAttemptContext],
]


class SqlClientPreparedAttemptFactory:
    """Produce PREPARED only after revalidating every parent coordinate."""

    def __init__(self, open_context: OpenPreparedAttemptContext) -> None:
        if not callable(open_context):
            raise ValueError(ERROR)
        self._open_context = open_context

    def prepare(
        self,
        plan: NativeChunkPlan,
        file: EncodedNativeFile,
        attempt_id: str,
        lease: WindowLease,
        custody: SqlClientInputCustody,
    ) -> SqlClientPreparedAttempt:
        """Prepare the exact retained attempt, or fail without returning authority."""
        if (
            type(plan) is not NativeChunkPlan
            or type(file) is not EncodedNativeFile
            or type(attempt_id) is not str
            or not attempt_id
            or type(lease) is not WindowLease
            or type(custody) is not SqlClientInputCustody
            or plan.transport is None
            or plan.transport.backend != "mssql_sqlclient"
            or lease.target_id != plan.target_id
            or custody.plan_sha256 != plan_sha256(plan)
            or (custody.target_id, custody.run_id, custody.window_fingerprint)
            != (plan.target_id, plan.run_id, plan.window_fingerprint)
            or (custody.attempt_id, custody.ordinal) != (attempt_id, file.ordinal)
            or (custody.rows, custody.encoded_bytes, custody.file_sha256, custody.typed_digest)
            != (file.rows, file.encoded_bytes, file.file_sha256, file.typed_digest)
        ):
            raise ValueError(ERROR)
        with self._open_context(plan, file, attempt_id, lease, custody) as context:
            if type(context) is not SqlClientPreparedAttemptContext:
                raise ValueError(ERROR)
            handle, parent = context.handle, context.handle.request.parent
            if (
                type(handle) is not SqlClientRetainedObserve
                or type(context.input_fd) is not int
                or context.input_fd != context.parent_input.fd
                or context.parent_input.expected.rows != file.rows
                or context.parent_input.expected.encoded_bytes != file.encoded_bytes
                or context.parent_input.expected.file_sha256 != file.file_sha256
                or (parent.target_key, parent.run_id, parent.ordinal, parent.file_sha256)
                != (plan.target_id, plan.run_id, file.ordinal, file.file_sha256)
                or parent.plan_sha256 != custody.plan_sha256
                or parent.policy_sha256 != plan_sha256(plan.transport)
            ):
                raise ValueError(ERROR)
            prepare_sqlclient_attempt(
                handle,
                parent_input=context.parent_input,
                input_fd=context.input_fd,
                baseline=context.baseline,
                baseline_authority=context.baseline_authority,
                policy=plan.transport,
                build=context.build,
                reader_factory=context.reader_factory,
                evidence_root=context.evidence_root,
                expected_typed_digest=file.typed_digest,
            )
            attempt = handle.attempt
            if (
                type(attempt) is not TdsAttempt
                or attempt.lifecycle.state.phase is not TdsAttemptPhase.PREPARED
                or attempt.lifecycle.state.identity != parent
            ):
                raise ValueError(ERROR)
            if not callable(context.release_input):
                raise ValueError(ERROR)
            return SqlClientPreparedAttempt(attempt=attempt, release_input=context.release_input)


__all__ = (
    "SqlClientPreparedAttempt",
    "SqlClientPreparedAttemptContext",
    "SqlClientPreparedAttemptFactory",
    "prepared_input_descriptor",
)
