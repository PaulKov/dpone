"""Safe error contract for MSSQL database-authority verification."""

from __future__ import annotations

import re

from dpone._compat import StrEnum

_SQLSTATE_PATTERN = re.compile(r"^[0-9A-Z]{5}$")


class MssqlStagingVerificationStage(StrEnum):
    """A safe, non-secret boundary within one staging lease attempt."""

    STAGING_CONNECTOR = "staging_connector"
    STAGING_TIMEOUT_SCOPE = "staging_timeout_scope"
    MASTER_CONNECTOR = "master_connector"
    MASTER_TIMEOUT_SCOPE = "master_timeout_scope"
    MASTER_IDENTITY = "master_identity"
    TARGET_IDENTITY = "target_identity"
    STAGING_IDENTITY = "staging_identity"
    MASTER_TARGET_TOPOLOGY = "master_target_topology"
    TARGET_STAGING_TOPOLOGY = "target_staging_topology"
    TARGET_CURRENT_DATABASE = "target_current_database"
    STAGING_CURRENT_DATABASE = "staging_current_database"
    STAGING_PIN = "staging_pin"
    MASTER_CLEANUP = "master_cleanup"
    STAGING_CLEANUP = "staging_cleanup"
    RETRY_DEADLINE = "retry_deadline"


class MssqlDatabaseAuthorityVerificationError(RuntimeError):
    """The live database does not equal its signed deployment authority."""

    def __init__(
        self,
        code: str,
        *,
        stage: object | None = None,
        sqlstate: str | None = None,
        attempts: int | None = None,
        retry_exhausted: bool = False,
        deadline_exhausted: bool = False,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.stage = str(stage) if stage is not None else None
        self.sqlstate = sqlstate
        self.attempts = attempts
        self.retry_exhausted = retry_exhausted
        self.deadline_exhausted = deadline_exhausted


def sanitize_staging_verification_error(
    error: BaseException,
    *,
    stage: MssqlStagingVerificationStage,
    default_code: str = "mssql_transaction.staging_database_session_unavailable",
) -> MssqlDatabaseAuthorityVerificationError:
    """Copy one vendor failure into a detached, non-secret public value."""

    code = error.code if isinstance(error, MssqlDatabaseAuthorityVerificationError) else default_code
    return MssqlDatabaseAuthorityVerificationError(
        code,
        stage=stage,
        sqlstate=extract_sanitized_sqlstate(error),
    )


def detached_verification_error(
    error: MssqlDatabaseAuthorityVerificationError,
    *,
    attempts: int | None = None,
    retry_exhausted: bool | None = None,
    deadline_exhausted: bool | None = None,
) -> MssqlDatabaseAuthorityVerificationError:
    """Copy only the stable public fields, dropping every exception chain and note."""

    return MssqlDatabaseAuthorityVerificationError(
        error.code,
        stage=error.stage,
        sqlstate=error.sqlstate,
        attempts=error.attempts if attempts is None else attempts,
        retry_exhausted=error.retry_exhausted if retry_exhausted is None else retry_exhausted,
        deadline_exhausted=error.deadline_exhausted if deadline_exhausted is None else deadline_exhausted,
    )


def extract_sanitized_sqlstate(error: BaseException) -> str | None:
    """Read only canonical five-character SQLSTATE tokens from a causal chain."""

    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        attribute = _canonical_sqlstate(getattr(current, "sqlstate", None))
        if attribute is not None:
            return attribute
        for argument in getattr(current, "args", ()):
            candidate = _canonical_sqlstate(argument)
            if candidate is not None:
                return candidate
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return None


def _canonical_sqlstate(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    return candidate if _SQLSTATE_PATTERN.fullmatch(candidate) else None


__all__ = [
    "MssqlDatabaseAuthorityVerificationError",
    "MssqlStagingVerificationStage",
    "detached_verification_error",
    "extract_sanitized_sqlstate",
    "sanitize_staging_verification_error",
]
