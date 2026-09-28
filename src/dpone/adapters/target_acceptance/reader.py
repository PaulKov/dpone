"""Concrete bounded reader constructed only from inert native endpoint fields."""

from __future__ import annotations

import math
import os
import re
import secrets
import sys
import time
from dataclasses import asdict
from typing import Any

from dpone.adapters.target_acceptance.queries import metric_plan
from dpone.adapters.target_acceptance.supervisor import supervise
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.contracts.target_acceptance import (
    MAX_FRAME_BYTES,
    TargetAcceptanceError,
    TargetAcceptanceRequest,
    validate_target_observation,
)
from dpone.ports.target_acceptance import BoundedTargetAcceptanceReader

_DESCRIPTOR_FIELDS = (
    "host",
    "port",
    "driver",
    "database",
    "user",
    "password",
    "secure",
    "compression",
    "connect_timeout",
    "send_receive_timeout",
    "ca_cert",
    "settings",
)


class BoundedClickHouseTargetAcceptanceReader(BoundedTargetAcceptanceReader):
    """Construct without I/O; unsupported clients fail only at target admission.

    The process command is fixed. Credentials travel through private stdin and
    are neither rendered in exceptions nor placed in command arguments or files.
    Existing connector query settings cannot override the worker's strict limits.
    """

    def __init__(self, descriptor: dict[str, Any] | None) -> None:
        self._descriptor = descriptor

    @classmethod
    def from_connector(cls, connector: Any) -> BoundedClickHouseTargetAcceptanceReader:
        """Copy inert descriptor fields only; never access the connector client."""
        try:
            descriptor = {field: getattr(connector, field) for field in _DESCRIPTOR_FIELDS}
            canonical_json_bytes(descriptor)
        except (AttributeError, TypeError, ValueError):
            descriptor = None
        return cls(descriptor)

    def _require_descriptor(self) -> dict[str, Any]:
        descriptor = self._descriptor
        if os.name != "posix" or descriptor is None or descriptor.get("driver") != "native":
            raise TargetAcceptanceError("UNSUPPORTED")
        if set(descriptor) != set(_DESCRIPTOR_FIELDS):
            raise TargetAcceptanceError("UNSUPPORTED")
        if any(
            not isinstance(descriptor[field], str) or not descriptor[field] for field in ("host", "database", "user")
        ):
            raise TargetAcceptanceError("UNSUPPORTED")
        if type(descriptor["port"]) is not int or not 1 <= descriptor["port"] <= 65535:
            raise TargetAcceptanceError("UNSUPPORTED")
        for field in ("connect_timeout", "send_receive_timeout"):
            value = descriptor[field]
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise TargetAcceptanceError("UNSUPPORTED")
        return descriptor

    def require_ready(self, *, cluster: str, database: str, table: str) -> None:
        """Exercise forced kill/reap and real metadata/settings before extraction."""
        self._require_descriptor()
        try:
            self._run("lifecycle", {}, deadline=time.monotonic() + 0.2)
        except TargetAcceptanceError as error:
            if (
                not error.quiescent
                or not error.output_revoked
                or error.code != "DPONE_REPLAY_QUALITY_EVIDENCE_INCOMPLETE"
            ):
                raise TargetAcceptanceError("UNSUPPORTED", quiescent=error.quiescent) from None
        else:
            raise TargetAcceptanceError("UNSUPPORTED")
        self._run("admit", {"cluster": cluster, "database": database, "table": table}, deadline=time.monotonic() + 60)

    def validate_plan(self, request: TargetAcceptanceRequest) -> None:
        """Reject unknown or unsupported selected metrics before publication."""
        metric_plan(request)
        desired = request.binding.get("desired")
        if desired is not None and (
            not isinstance(desired, dict)
            or not isinstance(desired.get("engine_full"), str)
            or not re.match(r"^ReplicatedMergeTree\s*\(", desired["engine_full"])
        ):
            raise TargetAcceptanceError("UNSUPPORTED")
        message = {
            "mode": "collect",
            "request": asdict(request),
            "descriptor": self._require_descriptor(),
            "deadline": 1.7976931348623157e308,
            "nonce": "f" * 32,
        }
        if len(canonical_json_bytes(message)) > MAX_FRAME_BYTES:
            raise TargetAcceptanceError("UNSUPPORTED")

    def collect(self, request: TargetAcceptanceRequest, *, deadline: float) -> dict[str, Any]:
        deadline = min(deadline, time.monotonic() + 60)
        metric_plan(request)
        result = self._run("collect", asdict(request), deadline=deadline)
        observation = validate_target_observation(request, result, allow_unavailable=True)
        if time.monotonic() >= deadline:
            raise TargetAcceptanceError("INCOMPLETE")
        return observation

    def verify_generation(self, request: TargetAcceptanceRequest, *, deadline: float) -> None:
        metric_plan(request)
        self._run("verify", asdict(request), deadline=deadline)

    def _run(self, mode: str, request: dict[str, Any], *, deadline: float) -> dict[str, Any]:
        started = time.monotonic()
        if not math.isfinite(deadline):
            raise TargetAcceptanceError()
        nonce = secrets.token_hex(16)
        descriptor = self._require_descriptor()
        effective_deadline = min(deadline, started + 60)
        message = {
            "mode": mode,
            "request": request,
            "descriptor": descriptor,
            "deadline": effective_deadline,
            "nonce": nonce,
        }
        payload = canonical_json_bytes(message)
        if len(payload) > MAX_FRAME_BYTES:
            raise TargetAcceptanceError("UNSUPPORTED")
        response = supervise(
            [sys.executable, "-m", "dpone.adapters.target_acceptance.worker"], payload, deadline=effective_deadline
        )
        if set(response) != {"nonce", "result", "error"} or response["nonce"] != nonce:
            raise TargetAcceptanceError()
        if response["error"] is not None:
            error = response["error"]
            if (
                not isinstance(error, dict)
                or set(error) != {"reason", "unavailable"}
                or error["reason"] not in {"UNSUPPORTED", "MISMATCH", "INVALID", "INCOMPLETE"}
                or type(error["unavailable"]) is not bool
                or response["result"] is not None
            ):
                raise TargetAcceptanceError()
            raise TargetAcceptanceError(error["reason"], probe_unavailable=error["unavailable"])
        if not isinstance(response["result"], dict):
            raise TargetAcceptanceError()
        if time.monotonic() >= effective_deadline:
            raise TargetAcceptanceError("INCOMPLETE")
        return response["result"]
