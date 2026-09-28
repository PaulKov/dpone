"""Fixed module entry point: bounded descriptor in, one observation frame out.

There is no SQL/callable/pickle dispatch, authority store, INSERT or publication
interface. Driver failures return a classified code, never their sensitive text.
"""

from __future__ import annotations

import sys
import time
from typing import Any

from dpone.adapters.target_acceptance.native import NativeSession, inspect_replicas, require_binding
from dpone.adapters.target_acceptance.queries import metric_plan, parse_metrics
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.target_acceptance import (
    MAX_FRAME_BYTES,
    TargetAcceptanceError,
    TargetAcceptanceRequest,
    unavailable_observation,
    validate_target_observation,
)


def execute(message: dict[str, Any]) -> dict[str, Any]:
    """Execute only an admission, metadata verification or generated metric plan."""
    mode, descriptor, deadline = message["mode"], message["descriptor"], message["deadline"]
    if mode == "lifecycle":
        while True:
            time.sleep(1)
    if mode not in {"admit", "verify", "collect"}:
        raise TargetAcceptanceError()
    raw = message["request"]
    request = None
    if mode != "admit":
        raw["columns"] = tuple(tuple(item) for item in raw["columns"])
        raw["null_columns"] = tuple(raw["null_columns"])
        raw["distinct_columns"] = tuple(raw["distinct_columns"])
        request = TargetAcceptanceRequest(**raw)
        metric_plan(request)
    inventory, before = inspect_replicas(
        descriptor, raw["cluster"], raw["database"], raw["table"], deadline, allow_absent=mode == "admit"
    )
    if mode == "admit":
        return {}
    assert request is not None
    require_binding(inventory, before, request.binding, request.columns)
    replica = sorted(inventory.replicas, key=lambda item: item.host)[0]
    if mode == "verify":
        return {}
    query, plan = metric_plan(request)
    session = NativeSession(descriptor, deadline, host=replica.host, port=replica.native_port)
    try:
        session.require_settings()
        try:
            rows, columns = session.read(query, typed=True)
        except TargetAcceptanceError:
            raise
        except Exception as error:
            _require_metric_unavailable(error)
            metrics = None
        else:
            metrics = parse_metrics(rows, columns, plan)
    finally:
        session.close()
    after_inventory, after = inspect_replicas(descriptor, request.cluster, request.database, request.table, deadline)
    require_binding(after_inventory, after, request.binding, request.columns)
    if after_inventory != inventory or after != before:
        raise TargetAcceptanceError("MISMATCH")
    observation = unavailable_observation(request, replica=replica.host, attempt_id=message["nonce"])
    if metrics is not None:
        observation.update(metrics)
        observation["warnings"] = []
    return validate_target_observation(request, observation, allow_unavailable=True)


def _require_metric_unavailable(error: Exception) -> None:
    """Only explicit unsupported metric errors permit the warn-only record.

    A socket timeout or server cancellation can arrive before the parent deadline.
    They remain incomplete attempts; malformed protocol and unknown exceptions
    are invalid observations. Error messages never cross the private boundary.
    """
    try:
        from clickhouse_driver.errors import ErrorCodes, NetworkError, ServerException, SocketTimeoutError
    except ImportError:
        raise TargetAcceptanceError() from None
    if isinstance(error, (SocketTimeoutError, NetworkError, TimeoutError)):
        raise TargetAcceptanceError("INCOMPLETE") from None
    if isinstance(error, ServerException):
        if error.code in {ErrorCodes.TIMEOUT_EXCEEDED, ErrorCodes.QUERY_WAS_CANCELLED}:
            raise TargetAcceptanceError("INCOMPLETE") from None
        if error.code in {ErrorCodes.UNKNOWN_FUNCTION, ErrorCodes.NOT_IMPLEMENTED, ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT}:
            return
    raise TargetAcceptanceError() from None


def main() -> None:
    """Read one bounded request; clean EOF is part of successful delivery."""
    message: dict[str, Any] = {}
    try:
        payload = sys.stdin.buffer.read(MAX_FRAME_BYTES + 1)
        if len(payload) > MAX_FRAME_BYTES:
            raise TargetAcceptanceError()
        message = strict_json_object(payload)
        result = execute(message)
        response = {"nonce": message["nonce"], "result": result, "error": None}
    except TargetAcceptanceError as error:
        response = {
            "nonce": message.get("nonce"),
            "result": None,
            "error": {"reason": error.code.rsplit("_", 1)[-1], "unavailable": error.probe_unavailable},
        }
    except Exception:
        response = {
            "nonce": message.get("nonce"),
            "result": None,
            "error": {"reason": "INCOMPLETE", "unavailable": False},
        }
    frame = canonical_json_bytes(response) + b"\n"
    if len(frame) > MAX_FRAME_BYTES:
        sys.exit(2)
    sys.stdout.buffer.write(frame)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
