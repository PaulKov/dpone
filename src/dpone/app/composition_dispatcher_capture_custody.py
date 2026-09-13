"""Bind service capture storage to independent host and protected SQL originals.

Host callbacks never open SQL. SQL callbacks use the caller's existing ledger;
source/attempt authorization remains the capture store's responsibility. This
component neither enrolls a volume nor grants permission to execute an attempt.
"""

from __future__ import annotations

import math
import time
from pathlib import Path

from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    ClickHouseSupervisorEnrollment,
    read_service_enrollment,
)
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.composition_service_snapshot_files import ServiceSnapshotFiles
from dpone.adapters.composition_supervisor_probe_rpc import CaptureSupervisorFactsClient
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject
from dpone.contracts.strict_json import canonical_json_bytes


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_capture_custody")


class DispatcherCaptureCustody:
    """One explicitly pinned enrollment and bounded observation lifetime.

    The protected service factory provides the SQL-retained enrollment and one
    absolute request deadline. Each host check obtains two fresh authenticated
    observations under that deadline. A later status request needs its own
    observation lifetime; this object never renews an expired execution budget.
    """

    def __init__(self, *, enrollment: ClickHouseSupervisorEnrollment, socket_path: str | Path, deadline: float):
        _require(type(enrollment) is ClickHouseSupervisorEnrollment)
        enrollment.__post_init__()
        _require(type(deadline) in (float, int) and math.isfinite(deadline) and 0 < deadline - time.monotonic() <= 900)
        body = enrollment.body
        _require(body["schema"] == "dpone.composition-clickhouse-supervisor-enrollment.v2")
        try:
            custody = body["policy"]["capture_custody"]
            root = body["facts"]["linux"]["capture_custody"]["root_identity"]
            _require(type(root) is dict and set(root) == {"device", "inode", "uid", "gid", "mode"})
            _require(all(type(value) is int for value in root.values()))
            _require(root["device"] >= 0 and root["inode"] > 0)
            _require((root["uid"], root["gid"], root["mode"]) == (custody["uid"], custody["gid"], 0o700))
        except (KeyError, TypeError):
            raise CompositionAdmissionError("dispatcher_capture_custody") from None
        self._enrollment = enrollment
        self._deadline = float(deadline)
        self._client = CaptureSupervisorFactsClient(
            Path(socket_path), dispatcher_gid=custody["gid"], timeout_seconds=10.0
        )

    @property
    def deadline(self) -> float:
        """The immutable upper bound supplied by the request owner."""
        return self._deadline

    def require_host(self) -> None:
        """Compare exact canonical fresh facts without acquiring a SQL connection."""
        deadline = min(self.deadline, time.monotonic() + 10.0)
        expected = canonical_json_bytes(self._enrollment.body["facts"])
        for _ in range(2):
            _require(time.monotonic() < deadline)
            facts = self._client.capture(self._enrollment.enrollment_sha256, deadline=deadline)
            _require(time.monotonic() < deadline and canonical_json_bytes(facts) == expected)

    def require_enrollment_in(self, ledger: CompositionMssqlLedger, subject: SnapshotCaptureSubject) -> None:
        """Compare the exact SQL original on the store's already authorized ledger."""
        subject.__post_init__()
        cursor, schema, service = ledger.cursor, ledger.schema, ledger.expected_service_id
        transaction = ledger.require_transaction()
        original = read_service_enrollment(ledger, subject.target.service_id)
        _require(ledger.cursor is cursor and (ledger.schema, ledger.expected_service_id) == (schema, service))
        _require(ledger.require_transaction(transaction) == transaction)
        _require(original.document == self._enrollment.document)
        body = original.body
        _require(
            (body["service_id"], body["database_uuid"], body["target_enrollment_sha256"])
            == (subject.target.service_id, subject.target.database_id, subject.target.enrollment_sha256)
        )

    def open_files(self) -> ServiceSnapshotFiles:
        """Open only the enrolled fixed volume; the caller owns descriptor cleanup."""
        self.require_host()
        body = self._enrollment.body
        policy = body["policy"]["capture_custody"]
        root = body["facts"]["linux"]["capture_custody"]["root_identity"]
        return ServiceSnapshotFiles(
            Path(policy["destination"]),
            dispatcher_uid=policy["uid"],
            dispatcher_gid=policy["gid"],
            root_device=root["device"],
            root_inode=root["inode"],
            require_custody=self.require_host,
        )
