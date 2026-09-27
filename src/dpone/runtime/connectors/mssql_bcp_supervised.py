"""Narrow supervised BCP import and process-outcome classification."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.connectors.mssql_bcp_process import BcpOptions, BcpProcess


@dataclass(frozen=True)
class BcpSupervisedResult:
    """Closed process observation; only positive ACK and reap permit a stage barrier."""

    classification: str
    acknowledged: bool
    reaped: bool
    rows_copied: int | None

    def __post_init__(self) -> None:
        if (
            self.classification not in {"success", "failure", "timeout", "lost_ack", "cleanup_failed", "custody_lost"}
            or type(self.acknowledged) is not bool
            or type(self.reaped) is not bool
            or (self.rows_copied is not None and (type(self.rows_copied) is not int or self.rows_copied < 0))
            or (self.classification == "success") != self.acknowledged
            or (self.acknowledged and (not self.reaped or self.rows_copied is None))
        ):
            raise ValueError("mssql_native.invalid_writer_outcome")


def observe_bcp_process(handle: BcpProcess) -> BcpSupervisedResult:
    """Classify a launched child without promoting an ambiguous outcome to success."""
    result = None
    failure: BaseException | None = None
    try:
        result = handle.wait()
    except Exception as error:
        failure = error
    try:
        returncode = handle.process.poll()
        reaped = returncode is not None
    except BaseException:
        return BcpSupervisedResult("custody_lost", False, False, None)
    if handle._cleanup_failed:
        classification = "cleanup_failed"
    elif not reaped:
        classification = "custody_lost"
    elif isinstance(failure, subprocess.TimeoutExpired):
        classification = "timeout"
    elif failure is not None:
        classification = "failure" if returncode != 0 else "lost_ack"
    elif returncode != 0:
        classification = "failure"
    elif result is None or type(result.rows_copied) is not int:
        classification = "lost_ack"
    else:
        classification = "success"
    return BcpSupervisedResult(
        classification,
        classification == "success",
        reaped,
        result.rows_copied if classification == "success" and result is not None else None,
    )


class SupervisedBcpImport:
    """Connector facade for an opt-in BCP handle, separate from v1 row-count calls."""

    _runner_factory: Any
    _qualified_name: Any

    def bcp_import_process(
        self,
        schema: str,
        table: str,
        file_path: str,
        *,
        options: BcpOptions | None = None,
        database: str | None = None,
    ) -> BcpProcess:
        target = self._qualified_name(schema, table, database=database)
        return self._runner_factory(options).import_file_process(target, file_path)
