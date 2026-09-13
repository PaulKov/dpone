"""Verify real bootstrap authority before opening the already bound listener.

SQL sessions close before host observations. Every observation shares the startup
owner's fixed deadline; no attempt, activation permit or execution is created.
The caller must finally recheck its retained bootstrap original and stop signal.
"""

from __future__ import annotations

import math
import time
from pathlib import Path

from dpone.adapters.composition_dispatcher_artifact_isolation import require_dispatcher_artifact_isolation
from dpone.adapters.composition_dispatcher_process_observation import require_dispatcher_process
from dpone.adapters.composition_supervisor_probe_rpc import CaptureSupervisorFactsClient
from dpone.app.composition_dispatcher_bootstrap_enrollment import read_bootstrap_enrollment
from dpone.app.composition_dispatcher_context import StagedDispatcherContextLoader
from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_bootstrap_unverified")


def _time(deadline: float) -> None:
    _require(type(deadline) in (int, float) and math.isfinite(deadline) and 0 < deadline - time.monotonic() <= 900)


def require_dispatcher_bootstrap(
    config: DispatcherServiceConfig,
    loader: StagedDispatcherContextLoader,
    policy_path: Path,
    deadline: float,
) -> None:
    """Reopen SQL, host, process and artifact isolation twice without admission.

    The protected loader supplies every staged authority and target. Mount tables
    belong only to enrollment-selected containers, and their original digest pins
    remain independent of the bootstrap being checked. No observation renews time
    or substitutes a successful component result for the enclosing verification.
    """
    try:
        _time(deadline)
        policy = config.service_policy
        _require(policy is not None)
        assert policy is not None
        original = None
        for _ in range(2):
            _time(deadline)
            enrolled = read_bootstrap_enrollment(config, loader, deadline)
            _time(deadline)
            _require(original is None or original == enrolled.document)
            original = enrolled.document
            expected = canonical_json_bytes(enrolled.body["facts"])
            client = CaptureSupervisorFactsClient(
                config.host_probe_socket, dispatcher_gid=config.dispatcher_gid, timeout_seconds=10.0
            )
            _require(canonical_json_bytes(client.capture(enrolled.enrollment_sha256, deadline=deadline)) == expected)
            _time(deadline)
            pins = {
                identifier: enrolled.body["facts"]["linux"]["containers"][identifier]["mounts"]["mountinfo_sha256"]
                for identifier in (enrolled.role_id("dispatcher"), enrolled.role_id("clickhouse"))
            }
            tables = client.capture_mounts(
                enrolled.enrollment_sha256, expected_mountinfo_sha256=pins, deadline=deadline
            )
            _time(deadline)
            require_dispatcher_process(enrolled, policy, policy_path, deadline)
            _time(deadline)
            require_dispatcher_artifact_isolation(enrolled, policy, tables, deadline)
            _time(deadline)
            _require(canonical_json_bytes(client.capture(enrolled.enrollment_sha256, deadline=deadline)) == expected)
            _time(deadline)
    except Exception:
        raise CompositionAdmissionError("dispatcher_bootstrap_unverified") from None
