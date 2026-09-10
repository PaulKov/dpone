"""Compatibility imports for the canonical verified runtime executor.

Execution, service-file ownership and diagnostics live under ``dpone.runtime``.
The public functions keep their signatures; the historical subprocess module
attribute remains available to existing test and library callers.
"""

from dpone.runtime.verified_pack_execution import (
    execute_verified_pack_command as execute_verified_pack_command,
)
from dpone.runtime.verified_pack_execution import (
    report_pack_os_error as report_pack_os_error,
)
from dpone.runtime.verified_pack_execution import (
    subprocess as subprocess,
)

__all__ = ["execute_verified_pack_command", "report_pack_os_error"]
