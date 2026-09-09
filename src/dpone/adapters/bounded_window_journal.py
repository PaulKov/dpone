"""Fenced CAS adapters for immutable window invocation and progress journals.

Serialized schemas belong to dpone.contracts; this module owns storage keys,
lease checks, and revision handling through the injected WindowStore port.
"""

from __future__ import annotations

import json

from dpone.contracts.bounded_window import (
    WindowLease,
    WindowPlan,
    decode_window_plan,
    encode_window_plan,
    invocation_fingerprint,
    validate_window_record,
)
from dpone.contracts.process_errors import WindowContractError
from dpone.ports.bounded_window import WindowStore


class WindowJournal:
    """Versioned independent keys allow disjoint chunk workers to checkpoint."""

    def __init__(self, store: WindowStore, lease: WindowLease, run_id: str) -> None:
        self.store, self.lease, self.run_id = store, lease, run_id

    def read(self, name: str) -> dict[str, object] | None:
        """Fail closed on corrupt or unsupported metadata before external I/O."""
        record = self.store.load(f"{self.run_id}/{name}")
        if record is None:
            return None
        try:
            data = json.loads(record.payload)
        except (TypeError, ValueError) as error:
            raise WindowContractError("Malformed bounded-window journal JSON") from error
        return validate_window_record(name, data)

    def write(self, name: str, data: dict[str, object]) -> None:
        """Commit validated metadata with fencing and optimistic revision."""
        payload = validate_window_record(name, dict(data, version=1))
        key = f"{self.run_id}/{name}"
        record = self.store.load(key)
        self.store.save(
            key, None if record is None else record.revision, json.dumps(payload, sort_keys=True), self.lease
        )


class WindowInvocationRegistry:
    """One immutable plan per target/invocation, protected by store CAS and lease."""

    def __init__(self, store: WindowStore, lease: WindowLease, owner: str, request: str) -> None:
        self._store, self._lease, self._request = store, lease, request
        self._key = "rolling-invocation-v1/" + invocation_fingerprint(lease.target_id, owner)

    def load(self) -> WindowPlan | None:
        """Refuse changed request or corrupt descriptor before reopening a source."""
        self._store.assert_lease(self._lease)
        record = self._store.load(self._key)
        if record is None:
            return None
        try:
            value = json.loads(record.payload)
        except (TypeError, ValueError) as error:
            raise WindowContractError("rolling_window_registry_json_invalid") from error
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "request", "plan"}
            or type(value["version"]) is not int
            or value["version"] != 1
        ):
            raise WindowContractError("rolling_window_registry_version_invalid")
        if value["request"] != self._request:
            raise WindowContractError("rolling_window_invocation_request_changed: use a new invocation")
        plan = decode_window_plan(value["plan"])
        if plan.target_id != self._lease.target_id:
            raise WindowContractError("rolling_window_registry_target_mismatch")
        return plan

    def create(self, plan: WindowPlan) -> None:
        """CAS-create before executor admission; retry never overwrites a saved plan."""
        payload = {"version": 1, "request": self._request, "plan": encode_window_plan(plan)}
        self._store.save(self._key, None, json.dumps(payload, sort_keys=True), self._lease)
