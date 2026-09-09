"""Fail-closed parse-time side-effect probes for the Airflow benchmark."""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from importlib import import_module
from types import TracebackType
from typing import Any
from unittest.mock import patch

from tools.airflow_provider_parse_benchmark_support import (
    DAG_COUNT,
    WORKLOAD_COUNT,
    BenchmarkConfig,
    forbidden_parse_imports,
    is_canonical_sha256,
)

_REQUIRED_PROBES = {
    "network": (
        "socket.create_connection",
        "socket.socket.connect",
        "socket.socket.connect_ex",
        "socket.getaddrinfo",
    ),
    "database_session": (
        "sqlite3.connect",
        "sqlalchemy.Engine.connect",
        "airflow.settings.Session",
    ),
    "airflow_metadata": (
        "airflow.Variable.get",
        "airflow.Connection.get",
    ),
    "kubernetes_api": ("kubernetes.client.ApiClient.call_api",),
}


@dataclass(frozen=True)
class _ProbeCandidate:
    module: str
    owner: str | None
    attribute: str
    category: str
    group: str
    probe: str


_DATABASE_PROBE = ("database", "database_session")
_SECRET_PROBE = ("secret", "airflow_metadata")
_KUBERNETES_PROBE = ("kubernetes", "kubernetes_api")
_INSTALLED_PROBES = (
    _ProbeCandidate("sqlalchemy.engine", "Engine", "connect", *_DATABASE_PROBE, "sqlalchemy.Engine.connect"),
    _ProbeCandidate("airflow.settings", None, "Session", *_DATABASE_PROBE, "airflow.settings.Session"),
    _ProbeCandidate("airflow.models.variable", "Variable", "get", *_SECRET_PROBE, "airflow.Variable.get"),
    _ProbeCandidate("airflow.sdk", "Variable", "get", *_SECRET_PROBE, "airflow.Variable.get"),
    _ProbeCandidate("airflow.hooks.base", "BaseHook", "get_connection", *_SECRET_PROBE, "airflow.Connection.get"),
    _ProbeCandidate(
        "airflow.models.connection",
        "Connection",
        "get_connection_from_secrets",
        *_SECRET_PROBE,
        "airflow.Connection.get",
    ),
    _ProbeCandidate(
        "kubernetes.client", "ApiClient", "call_api", *_KUBERNETES_PROBE, "kubernetes.client.ApiClient.call_api"
    ),
)


class ParseSideEffectAttempt(RuntimeError):
    """A forbidden operation was blocked before execution."""


class ParseSideEffectTripwire:
    """Block named parse-time side effects and fail on incomplete coverage."""

    def __init__(self) -> None:
        self._stack: ExitStack | None = None
        self._probes: dict[str, dict[str, Any]] = {}
        self._armed_bindings: set[tuple[int, str, str]] = set()
        self._forbidden_imports: list[str] = []

    def __enter__(self) -> ParseSideEffectTripwire:
        self._stack = ExitStack()
        self._stack.__enter__()
        for owner, attribute, probe in (
            (socket, "create_connection", "socket.create_connection"),
            (socket.socket, "connect", "socket.socket.connect"),
            (socket.socket, "connect_ex", "socket.socket.connect_ex"),
            (socket, "getaddrinfo", "socket.getaddrinfo"),
        ):
            self.arm(
                owner,
                attribute,
                category="network",
                group="network",
                probe=probe,
            )
        self.arm(
            sqlite3,
            "connect",
            category="database",
            group="database_session",
            probe="sqlite3.connect",
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        assert self._stack is not None
        return bool(self._stack.__exit__(exc_type, exc, traceback))

    def arm(
        self,
        owner: object,
        attribute: str,
        *,
        category: str,
        probe: str,
        group: str | None = None,
    ) -> bool:
        """Replace one API with a redacted counter; never retain call arguments."""

        if self._stack is None:
            raise RuntimeError("tripwire must be entered before probes are armed")
        normalized_category = category.rstrip("s")
        state = self._probes.setdefault(
            probe,
            {
                "armed": False,
                "armed_bindings": 0,
                "calls": 0,
                "category": normalized_category,
                "group": group,
            },
        )
        if not hasattr(owner, attribute):
            return bool(state["armed"])
        binding = (id(owner), attribute, probe)
        if binding in self._armed_bindings:
            return True

        def blocked(*args: object, **kwargs: object) -> object:
            del args, kwargs
            state["calls"] += 1
            raise ParseSideEffectAttempt(f"{normalized_category} side effect blocked by {probe}")

        self._stack.enter_context(patch.object(owner, attribute, blocked))
        self._armed_bindings.add(binding)
        state["armed"] = True
        state["armed_bindings"] += 1
        return True

    def arm_installed_environment(self) -> None:
        """Import and arm version-tolerant probes before timed provider parsing."""

        for candidate in _INSTALLED_PROBES:
            try:
                module = import_module(candidate.module)
            except ImportError:
                self._record_missing(candidate)
                continue
            owner = getattr(module, candidate.owner, None) if candidate.owner else module
            if owner is None:
                self._record_missing(candidate)
                continue
            self.arm(
                owner,
                candidate.attribute,
                category=candidate.category,
                group=candidate.group,
                probe=candidate.probe,
            )

    def record_forbidden_imports(self, before: set[str], after: set[str]) -> None:
        """Record newly imported forbidden modules without importing them."""

        self._forbidden_imports = forbidden_parse_imports(before, after)

    def report(self) -> dict[str, Any]:
        """Return complete probe coverage and redacted call counters."""

        groups: dict[str, dict[str, Any]] = {}
        missing_groups: list[str] = []
        for group, required in _REQUIRED_PROBES.items():
            missing = [probe for probe in required if not self._probes.get(probe, {}).get("armed", False)]
            groups[group] = {
                "required_probes": list(required),
                "missing_probes": missing,
                "complete": not missing,
            }
            if missing:
                missing_groups.append(group)
        categories = ("network", "database", "secret", "kubernetes")
        calls = {
            category: sum(int(item["calls"]) for item in self._probes.values() if item["category"] == category)
            for category in categories
        }
        counters = {f"{category}_calls": count for category, count in calls.items()}
        complete = not missing_groups
        passed = complete and not any(calls.values()) and not self._forbidden_imports
        return {
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "coverage_complete": complete,
            "missing_required_groups": missing_groups,
            "required_groups": groups,
            "counters": counters,
            "probes": {name: self._probes[name] for name in sorted(self._probes)},
            "forbidden_imports": list(self._forbidden_imports),
            "argument_capture": "disabled",
        }

    def _record_missing(self, candidate: _ProbeCandidate) -> None:
        self._probes.setdefault(
            candidate.probe,
            {
                "armed": False,
                "armed_bindings": 0,
                "calls": 0,
                "category": candidate.category,
                "group": candidate.group,
            },
        )


def aggregate_side_effect_evidence(
    workers: Sequence[Mapping[str, Any]],
    expected_workers: int,
) -> dict[str, Any]:
    """Aggregate complete tripwire coverage without treating missing probes as zero."""

    keys = (
        "network_calls",
        "database_calls",
        "secret_calls",
        "kubernetes_calls",
    )
    counters = {
        key: sum(int(worker.get("side_effects", {}).get("counters", {}).get(key, 0)) for worker in workers)
        for key in keys
    }
    verified = sum(bool(worker.get("side_effects", {}).get("coverage_complete")) for worker in workers)
    forbidden = sorted(
        {str(module) for worker in workers for module in worker.get("side_effects", {}).get("forbidden_imports", ())}
    )
    passed = (
        len(workers) == expected_workers
        and verified == expected_workers
        and all(worker.get("side_effects", {}).get("passed") for worker in workers)
        and not any(counters.values())
        and not forbidden
    )
    groups = sorted(
        {str(group) for worker in workers for group in worker.get("side_effects", {}).get("required_groups", {})}
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "coverage_complete": verified == expected_workers,
        "expected_workers": expected_workers,
        "verified_workers": verified,
        "counters": counters,
        "required_group_names": groups,
        "forbidden_imports": forbidden,
        "argument_capture": "disabled",
    }


def process_integrity(
    workers: Sequence[Mapping[str, Any]],
    expected_workers: int,
) -> dict[str, Any]:
    """Require every bounded worker clock to precede material imports."""

    verified = sum(
        worker.get("measurement", {}).get("clock_started_before_forbidden_imports") is True
        and worker.get("measurement", {}).get("pre_clock_forbidden_imports") == []
        and worker.get("termination", {}).get("status") == "completed"
        and worker.get("termination", {}).get("terminate_sent") is False
        and worker.get("termination", {}).get("kill_sent") is False
        for worker in workers
    )
    passed = len(workers) == expected_workers == verified
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "expected_workers": expected_workers,
        "verified_workers": verified,
        "model": "sterile -I subprocess clocked before Airflow/provider imports",
    }


def load_integrity(
    config: BenchmarkConfig,
    workers: Sequence[Mapping[str, Any]],
    expected_workers: int,
    *,
    expected_index_sha256: object,
    expected_topology: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the exact fixture authority across every replay."""

    failed = [index for index, worker in enumerate(workers) if worker.get("status") != "PASS"]
    records: list[Mapping[str, Any]] = []
    missing: list[str] = []
    for index, worker in enumerate(workers):
        parses = worker.get("parses")
        for name, required in (
            ("first", True),
            ("warm", index < config.warm_samples),
        ):
            record = parses.get(name) if isinstance(parses, Mapping) else None
            if required and not isinstance(record, Mapping):
                missing.append(f"{index}:{name}")
            elif required:
                records.append(record)
    expected_parses = expected_workers + config.warm_samples
    fingerprints = {record.get("topology_fingerprint") for record in records}
    replay_match = len(records) == expected_parses and len(fingerprints) == 1
    topology_match = len(records) == expected_parses and all(
        _record_matches_topology(record, expected_topology) for record in records
    )
    index_match = (
        is_canonical_sha256(expected_index_sha256)
        and len(records) == expected_parses
        and all(record.get("index_sha256") == expected_index_sha256 for record in records)
    )
    passed = (
        len(workers) == expected_workers
        and not failed
        and not missing
        and replay_match
        and topology_match
        and index_match
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "expected_workers": expected_workers,
        "observed_workers": len(workers),
        "failed_workers": failed,
        "missing_parses": missing,
        "expected_dags_per_parse": DAG_COUNT,
        "expected_runtime_tasks_per_parse": WORKLOAD_COUNT,
        "verified_parses": len(records),
        "topology_replay_match": replay_match,
        "expected_topology_match": topology_match,
        "topology_fingerprint": (next(iter(fingerprints)) if len(fingerprints) == 1 else None),
        "expected_topology_fingerprint": expected_topology.get("topology_sha256"),
        "index_digest_replay_match": index_match,
        "index_sha256": (expected_index_sha256 if is_canonical_sha256(expected_index_sha256) else None),
    }


def _record_matches_topology(
    record: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> bool:
    return (
        record.get("passed") is True
        and record.get("dag_count") == expected.get("dag_count") == DAG_COUNT
        and record.get("runtime_task_count") == expected.get("runtime_task_count") == WORKLOAD_COUNT
        and record.get("workload_ids_match") is True
        and all(
            record.get(field) == expected.get(field)
            for field in (
                "dag_ids_sha256",
                "spec_fingerprints_sha256",
                "task_ids_sha256",
                "edges_sha256",
            )
        )
        and record.get("topology_fingerprint") == expected.get("topology_sha256")
    )
