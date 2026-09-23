"""Compatibility exports for TDS attempt ownership contracts."""

from dpone.services.mssql_tds_attempt_retirement import AttemptDepartureViewMixin as AttemptDepartureViewMixin
from dpone.services.mssql_tds_attempt_retirement import ShutdownCapability as ShutdownCapability
from dpone.services.mssql_tds_attempt_retirement import TdsAttemptUnknown as TdsAttemptUnknown
from dpone.services.mssql_tds_attempt_retirement import assert_local_owner as assert_local_owner
from dpone.services.mssql_tds_attempt_retirement import assert_retirement_descendant as assert_retirement_descendant
from dpone.services.mssql_tds_attempt_retirement import same_state_tree as same_state_tree
from dpone.services.mssql_tds_attempt_retirement import validate_deadline as validate_deadline

__all__ = (
    "AttemptDepartureViewMixin",
    "ShutdownCapability",
    "TdsAttemptUnknown",
    "assert_local_owner",
    "assert_retirement_descendant",
    "same_state_tree",
    "validate_deadline",
)
