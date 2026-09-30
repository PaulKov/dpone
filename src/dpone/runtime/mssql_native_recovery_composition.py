"""Focused runtime dependencies for the native recovery application service.

This module is the composition boundary between the orchestration-only
application layer and concrete runtime services. It contains no recovery policy;
the application service owns decisions and consumes this stable implementation
surface.
"""

from dpone.runtime.etl.mssql_operation_lease import LEASE_OPTION, MssqlOperationLeaseHeartbeat
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION
from dpone.runtime.etl.mssql_transaction_request import live_target_coordinates
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot
from dpone.runtime.mssql_native_application import _digest, _NativeRuntimeAssembly
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_route_preflight import resolve_atomic_mssql_target
from dpone.runtime.storage_policy import RuntimeStoragePolicy, StoragePreflightService

__all__ = [
    "ADMISSION_OPTION",
    "LEASE_OPTION",
    "MssqlGenericTransactionState",
    "MssqlOperationLeaseHeartbeat",
    "QualityExecutionSnapshot",
    "RuntimeStoragePolicy",
    "StoragePreflightService",
    "_NativeRuntimeAssembly",
    "_digest",
    "live_target_coordinates",
    "resolve_atomic_mssql_target",
]
