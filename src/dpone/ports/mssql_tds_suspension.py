"""Process-local same-fence continuation capabilities."""

from dpone.contracts.mssql_tds_suspension import TdsAttemptResumeClaim as TdsAttemptResumeClaim
from dpone.contracts.mssql_tds_suspension import TdsAttemptSuspension as TdsAttemptSuspension
from dpone.contracts.mssql_tds_suspension import _issue_tds_attempt_suspension as _issue_tds_attempt_suspension

__all__ = ("TdsAttemptResumeClaim", "TdsAttemptSuspension", "_issue_tds_attempt_suspension")
