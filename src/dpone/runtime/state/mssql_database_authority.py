"""Pre-source verification of registry-pinned SQL Server databases."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, ClassVar

from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthoritySet
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.runtime.state import mssql_database_authority_support as authority_support
from dpone.runtime.state.mssql_database_authority_retry import (
    MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS,
    MssqlStagingSessionDeadline,
    MssqlStagingSessionRetryRunner,
    MssqlStagingVerificationStage,
    sanitize_staging_verification_error,
)
from dpone.runtime.state.mssql_database_authority_support import (
    MSSQL_DATABASE_AUTHORITY_CONNECT_TIMEOUT_SECONDS,
    MssqlDatabaseAuthorityVerificationError,
    MssqlStagingDatabaseLease,
    bounded_authority_connection,
    database_connector,
    master_connector,
    require_current_database,
    require_same_session_authority,
    session_identity,
    strict_close_connector_error,
    verify_pin,
)
from dpone.runtime.state.mssql_database_authority_verifier_support import (
    MssqlDatabaseAuthorityVerifierSupportMixin,
)

MasterConnectorFactory = Callable[[ResolvedBindingConnection], Any]
StagingConnectorFactory = Callable[[ResolvedBindingConnection, str], Any]
MSSQL_DATABASE_AUTHORITY_SHA256_OPTION = "mssql_database_authority_sha256"


def _raise_cleanup_error(connector: Any, stage: MssqlStagingVerificationStage) -> None:
    """Raise detached cleanup evidence so a failed close cannot permit a retry."""

    error = strict_close_connector_error(connector, stage=stage)
    if error is not None:
        raise error from None


@dataclass(frozen=True, slots=True)
class MssqlDatabaseAuthorityVerifier(MssqlDatabaseAuthorityVerifierSupportMixin):
    """Verify target, staging, and state databases without self-learning."""

    target_connection: ResolvedBindingConnection = field(repr=False)
    state_connection: ResolvedBindingConnection = field(repr=False)
    target_database: str
    staging_database: str
    state_database: str
    target_authorities: MssqlDatabaseAuthoritySet
    state_authorities: MssqlDatabaseAuthoritySet
    master_connector_factory: MasterConnectorFactory = field(
        default=lambda connection: master_connector(connection),
        repr=False,
        compare=False,
    )
    staging_connector_factory: StagingConnectorFactory = field(
        default=lambda connection, database: database_connector(connection, database),
        repr=False,
        compare=False,
    )
    staging_retry_runner: MssqlStagingSessionRetryRunner = field(
        default_factory=MssqlStagingSessionRetryRunner,
        repr=False,
        compare=False,
    )
    _query_timeout: ContextVar[int | None] = field(
        default_factory=lambda: ContextVar("dpone_mssql_database_authority_query_timeout", default=None),
        init=False,
        repr=False,
        compare=False,
    )
    _authority_support: ClassVar[Any] = authority_support

    @classmethod
    def from_connections(
        cls,
        *,
        target_connection: ResolvedBindingConnection,
        state_connection: ResolvedBindingConnection,
        target_database: str,
        staging_database: str,
        state_database: str,
        master_connector_factory: MasterConnectorFactory | None = None,
        staging_connector_factory: StagingConnectorFactory | None = None,
        staging_retry_runner: MssqlStagingSessionRetryRunner | None = None,
    ) -> MssqlDatabaseAuthorityVerifier:
        """Parse the already-verified registry descriptors exactly once."""

        target_descriptor = target_connection.descriptor
        state_descriptor = state_connection.descriptor
        if target_descriptor is None or state_descriptor is None:
            raise MssqlDatabaseAuthorityVerificationError("mssql_transaction.database_authority_descriptor_required")
        kwargs: dict[str, Any] = {}
        if master_connector_factory is not None:
            kwargs["master_connector_factory"] = master_connector_factory
        if staging_connector_factory is not None:
            kwargs["staging_connector_factory"] = staging_connector_factory
        if staging_retry_runner is not None:
            kwargs["staging_retry_runner"] = staging_retry_runner
        return cls(
            target_connection=target_connection,
            state_connection=state_connection,
            target_database=target_database,
            staging_database=staging_database,
            state_database=state_database,
            target_authorities=MssqlDatabaseAuthoritySet.from_connection_properties(
                target_descriptor.properties,
                capability="target",
            ),
            state_authorities=MssqlDatabaseAuthoritySet.from_connection_properties(
                state_descriptor.properties,
                capability="state",
            ),
            **kwargs,
        )

    def verify(self, *, target_connector: Any, state_connector: Any) -> None:
        """Verify all physical pins, access modes, and session topology."""

        with self._master_connectors(query_timeout=self._query_timeout.get()) as (target_master, state_master):
            self._verify_master_pins(target_master, state_master)
            target_identity = session_identity(target_connector, role="target")
            state_identity = session_identity(state_connector, role="state")
            require_same_session_authority(
                session_identity(target_master, role="target"),
                target_identity,
                code="mssql_transaction.target_database_session_topology_mismatch",
            )
            require_same_session_authority(
                session_identity(state_master, role="state"),
                state_identity,
                code="mssql_transaction.state_database_session_topology_mismatch",
            )
            target_pin = self.target_authorities.require(self.target_database, capability="target")
            state_pin = self.state_authorities.require(self.state_database, capability="state")
            require_current_database(target_connector, target_pin, role="target")
            require_current_database(state_connector, state_pin, role="state")

    def verify_pins(self) -> None:
        """Verify every database from master before endpoint construction."""

        with self._master_connectors(query_timeout=self._query_timeout.get()) as (target_master, state_master):
            self._verify_master_pins(target_master, state_master)

    @contextmanager
    def bounded_query_timeout(self, seconds: int) -> Iterator[None]:
        """Bind an explicit factory-session timeout for the current context."""

        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 1:
            raise ValueError("MSSQL database-authority timeout must be a positive integer")
        current = self._query_timeout.get()
        bounded = min(current, seconds) if current is not None else seconds
        token = self._query_timeout.set(bounded)
        try:
            yield
        finally:
            self._query_timeout.reset(token)

    def acquire_staging_lease(self, *, target_connector: Any) -> MssqlStagingDatabaseLease:
        """Open and verify a use-bound session to the signed staging database."""

        staging_pin = self.target_authorities.require(self.staging_database, capability="staging")
        return self.staging_retry_runner.run(
            lambda deadline: self._acquire_staging_lease_attempt(
                target_connector=target_connector,
                staging_database=staging_pin.database_name,
                deadline=deadline,
            )
        )

    def _acquire_staging_lease_attempt(
        self,
        *,
        target_connector: Any,
        staging_database: str,
        deadline: MssqlStagingSessionDeadline,
    ) -> MssqlStagingDatabaseLease:
        """Construct, verify, and either return or close one fresh lease."""

        try:
            staging_connector = self.staging_connector_factory(
                bounded_authority_connection(
                    self.target_connection,
                    connect_timeout_seconds=deadline.timeout_seconds(
                        maximum=MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS
                    ),
                ),
                staging_database,
            )
        except Exception as exc:
            raise sanitize_staging_verification_error(
                exc,
                stage=MssqlStagingVerificationStage.STAGING_CONNECTOR,
            ) from None
        lease = MssqlStagingDatabaseLease(
            verifier=self,
            target_connector=target_connector,
            staging_connector=staging_connector,
        )
        try:
            lease.assert_current(deadline=deadline)
        except BaseException:
            lease.close_strict()
            raise
        return lease

    def _verify_staging_lease(
        self,
        *,
        target_connector: Any,
        staging_connector: Any,
        deadline: MssqlStagingSessionDeadline | None = None,
    ) -> None:
        with ExitStack() as stack:
            query_timeout = self._staging_attempt_query_timeout(deadline=deadline)
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.STAGING_TIMEOUT_SCOPE,
                lambda: self._enter_query_timeout_scope(stack, staging_connector, query_timeout),
                deadline=deadline,
            )
            target_master = self._open_staging_master_connector(deadline=deadline)
            stack.callback(
                _raise_cleanup_error,
                target_master,
                MssqlStagingVerificationStage.MASTER_CLEANUP,
            )
            if deadline is not None:
                deadline.ensure_active()
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.MASTER_TIMEOUT_SCOPE,
                lambda: self._enter_query_timeout_scope(stack, target_master, query_timeout),
                deadline=deadline,
            )
            master_identity = self._run_staging_verification_stage(
                MssqlStagingVerificationStage.MASTER_IDENTITY,
                lambda: session_identity(target_master, role="staging"),
                deadline=deadline,
            )
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.TARGET_IDENTITY,
                lambda: self._enter_query_timeout_scope(
                    stack,
                    target_connector,
                    self._staging_attempt_query_timeout(deadline=deadline),
                ),
                deadline=deadline,
            )
            target_identity = self._run_staging_verification_stage(
                MssqlStagingVerificationStage.TARGET_IDENTITY,
                lambda: session_identity(target_connector, role="target"),
                deadline=deadline,
            )
            staging_identity = self._run_staging_verification_stage(
                MssqlStagingVerificationStage.STAGING_IDENTITY,
                lambda: session_identity(staging_connector, role="staging"),
                deadline=deadline,
            )
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.MASTER_TARGET_TOPOLOGY,
                lambda: require_same_session_authority(
                    master_identity,
                    target_identity,
                    code="mssql_transaction.staging_database_session_topology_mismatch",
                ),
                deadline=deadline,
            )
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.TARGET_STAGING_TOPOLOGY,
                lambda: require_same_session_authority(
                    target_identity,
                    staging_identity,
                    code="mssql_transaction.staging_database_session_topology_mismatch",
                ),
                deadline=deadline,
            )
            target_pin = self.target_authorities.require(self.target_database, capability="target")
            staging_pin = self.target_authorities.require(self.staging_database, capability="staging")
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.TARGET_CURRENT_DATABASE,
                lambda: require_current_database(target_connector, target_pin, role="target"),
                deadline=deadline,
            )
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.STAGING_CURRENT_DATABASE,
                lambda: require_current_database(staging_connector, staging_pin, role="staging"),
                deadline=deadline,
            )
            self._run_staging_verification_stage(
                MssqlStagingVerificationStage.STAGING_PIN,
                lambda: verify_pin(target_master, staging_pin, role="staging"),
                deadline=deadline,
            )

    def _staging_attempt_query_timeout(self, *, deadline: MssqlStagingSessionDeadline | None) -> int:
        configured = self._query_timeout.get()
        maximum = (
            MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS
            if configured is None
            else min(configured, MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS)
        )
        return deadline.timeout_seconds(maximum=maximum) if deadline is not None else maximum

    def _bounded_staging_connection(self, *, deadline: MssqlStagingSessionDeadline | None) -> ResolvedBindingConnection:
        """Return a master-factory connection constrained by the shared admission budget."""

        if deadline is None:
            return self.target_connection
        return bounded_authority_connection(
            self.target_connection,
            connect_timeout_seconds=deadline.timeout_seconds(maximum=MSSQL_DATABASE_AUTHORITY_CONNECT_TIMEOUT_SECONDS),
        )

    def _open_staging_master_connector(self, *, deadline: MssqlStagingSessionDeadline | None) -> Any:
        """Open a master connector while leaving its cleanup ownership observable.

        The caller registers strict cleanup before it checks whether this blocking
        factory call consumed the shared staging-attempt deadline.
        """

        if deadline is not None:
            deadline.ensure_active()
        try:
            return self.master_connector_factory(self._bounded_staging_connection(deadline=deadline))
        except Exception as exc:
            raise sanitize_staging_verification_error(
                exc,
                stage=MssqlStagingVerificationStage.MASTER_CONNECTOR,
            ) from None

    @staticmethod
    def _run_staging_verification_stage(
        stage: MssqlStagingVerificationStage,
        operation: Callable[[], Any],
        *,
        deadline: MssqlStagingSessionDeadline | None = None,
    ) -> Any:
        if deadline is not None:
            deadline.ensure_active()
        try:
            result = operation()
        except Exception as exc:
            raise sanitize_staging_verification_error(exc, stage=stage) from None
        if deadline is not None:
            deadline.ensure_active()
        return result

    @staticmethod
    def _enter_query_timeout_scope(
        stack: ExitStack,
        connector: Any,
        query_timeout: int | None,
    ) -> None:
        """Require a bounded query-timeout scope when a timeout is active."""

        if query_timeout is None:
            return
        scope = getattr(connector, "bounded_query_timeout", None)
        if not callable(scope):
            raise MssqlDatabaseAuthorityVerificationError(
                "mssql_transaction.database_authority_query_timeout_scope_required"
            )
        stack.enter_context(scope(query_timeout))


__all__ = [
    "MSSQL_DATABASE_AUTHORITY_SHA256_OPTION",
    "MasterConnectorFactory",
    "MssqlDatabaseAuthorityVerificationError",
    "MssqlDatabaseAuthorityVerifier",
    "MssqlStagingDatabaseLease",
    "StagingConnectorFactory",
]
