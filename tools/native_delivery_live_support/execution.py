"""Narrow DDA-06 injection seam and measured source/visibility clock boundaries.

No composition, SQL, credential discovery, or service startup lives here. A
reviewed factory must execute the selected checkout's real runtime and BCP; its
fault controls wrap the existing source/transaction/state authorities.
"""

from __future__ import annotations

import importlib
import re
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from .artifacts import digest
from .profiles import Dataset

BASELINE_COMMIT = "d5ad9aaecc900c24df421b160ed36b4cfc726e45"
SHA = re.compile(r"[0-9a-f]{64}")


def git_identity(checkout: Path) -> dict[str, Any]:
    """Read exact tracked/untracked state without returning file names or remotes."""

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(checkout), *args], stderr=subprocess.DEVNULL, text=True
        ).strip()

    commit = git("rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid_subject_commit")
    return {"commit": commit, "dirty": bool(git("status", "--porcelain", "--untracked-files=normal"))}


def loaded_subject() -> Path:
    """Locate the dpone actually imported by this interpreter, never a SHA label."""
    import dpone

    path = Path(dpone.__file__).resolve()
    for parent in path.parents:
        if (parent / ".git").exists():
            return parent
    raise ValueError("subject_checkout_unavailable")


def unavailable(unit: str, reason: str, provenance: str = "native-delivery-live-v1") -> dict[str, Any]:
    return {"value": None, "unit": unit, "availability": "unavailable", "reason": reason, "provenance": provenance}


def measured(value: float | int, unit: str, provenance: str) -> dict[str, Any]:
    import math

    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("invalid_measurement")
    return {"value": value, "unit": unit, "availability": "measured", "reason": None, "provenance": provenance}


@dataclass
class DeliveryClock:
    """Factory invokes these hooks at source acquisition and confirmed visibility.

    committed_visible must follow commit acknowledgement or an exact receipt probe
    AND a successful independent target visibility probe, before evidence/state.
    All hooks run on the harness thread in one monotonic clock domain.
    """

    now: Callable[[], int] = time.monotonic_ns
    start: int | None = None
    visible: int | None = None

    def source_acquired(self) -> None:
        if self.start is not None:
            raise ValueError("duplicate_source_clock")
        self.start = self.now()

    def committed_visible(self) -> None:
        if self.start is None or self.visible is not None:
            raise ValueError("invalid_visibility_clock")
        self.visible = self.now()

    def finish(self) -> tuple[dict[str, Any], dict[str, Any]]:
        end = self.now()
        if self.start is None or self.visible is None or not self.start <= self.visible <= end:
            return unavailable("seconds", "missing_clock_boundary"), unavailable("seconds", "missing_clock_boundary")
        return (
            measured((self.visible - self.start) / 1e9, "seconds", "source_to_confirmed_visibility:monotonic_ns"),
            measured((end - self.start) / 1e9, "seconds", "source_to_evidence_checkpoint:monotonic_ns"),
        )


@dataclass(frozen=True)
class Snapshot:
    """Independent target/journal observations, outside the measured span.

    Rows contain only business columns; outside_rows covers every row outside the
    authored window (including its upper boundary). Metadata hashes must include
    canonical expected/observed framework columns. Receipt hashes bind the exact
    operation/window/owner/content identity, not merely receipt existence.
    Source-query and publication counters belong to this invocation and start
    at zero; fixture preparation and target observation queries are excluded.
    A successful fresh delivery performs one source query and one atomic target
    publication. commit_known also covers a confirmed no-publication rollback;
    pipeline_complete may advance only after successful evidence/checkpoint work.
    """

    rows: Iterable[Mapping[str, object]]
    outside_rows: Iterable[Mapping[str, object]] = ()
    metadata_expected: str | None = None
    metadata_observed: str | None = None
    receipt_expected: str | None = None
    receipt_observed: str | None = None
    source_queries: int = 0
    publications: int = 0
    stage_reads: int = 0
    commit_known: bool = True
    encoded_bytes: int | None = None
    sql_metrics: Mapping[str, int | float | None] = field(default_factory=dict)
    fault_events: tuple[str, ...] = ()
    receipt_probes: int = 0
    pipeline_complete: bool = False

    def __post_init__(self) -> None:
        for value in (self.source_queries, self.publications, self.stage_reads, self.receipt_probes):
            if type(value) is not int or value < 0:
                raise ValueError("invalid_observed_counter")
        if type(self.commit_known) is not bool or type(self.pipeline_complete) is not bool:
            raise ValueError("invalid_observed_state")
        allowed = {
            "before_commit",
            "after_eof",
            "lost_ack",
            "unknown_commit",
            "nonempty_switch_out",
            "layout_drift",
            "owner_drift",
            "between_switches",
        }
        if not isinstance(self.fault_events, tuple) or not set(self.fault_events) <= allowed:
            raise ValueError("invalid_fault_events")


class RouteSession(Protocol):
    """One owned fixture/invocation. All operations must use real observed state."""

    invocation_id: str

    def snapshot(self) -> Snapshot: ...
    def arm_fault(self, fault: str) -> None:
        """Inject after_eof, before_commit, lost_ack, unknown_commit or SWITCH drift."""
        ...

    def run(self) -> None:
        """Run the real runtime; source and confirmed-visibility hooks are mandatory."""
        ...

    def recover(self, *, source_allowed: bool) -> None:
        """Reconstruct from durable state. False injects a poison source opener."""
        ...

    def cleanup(self) -> None:
        """Verify owner and known outcome, then remove only this fixture's objects."""
        ...

    def close(self) -> None:
        """Close connections only; never resolve unknown commits or drop objects."""
        ...


class RouteFactory(Protocol):
    """DDA-06 supplies real composition; local tests supply declared hermetic doubles."""

    execution: Literal["live", "hermetic"]
    subject_checkout: Path

    def describe(self) -> dict[str, Any]:
        """Return versions, target_layout_sha256, resource_profile; no connections/SQL."""
        ...

    def open(self, dataset: Dataset, *, case: str, clock: DeliveryClock) -> RouteSession:
        """Provision synthetic owned fixtures before timing, with outside sentinels."""
        ...

    def attach(self, invocation_id: str) -> RouteSession:
        """Reopen the durable owned inventory; never provision/reset or guess objects."""
        ...


def environment_record(factory: RouteFactory) -> dict[str, Any]:
    """Only allow non-secret version tokens and numeric resource observations."""
    raw = factory.describe()
    if set(raw) != {"versions", "target_layout_sha256", "resource_profile"}:
        raise ValueError("invalid_environment_fields")
    versions = raw["versions"]
    if not isinstance(versions, dict) or not {"python", "dpone", "clickhouse", "mssql", "bcp"} <= versions.keys():
        raise ValueError("missing_environment_versions")
    for key, value in versions.items():
        if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", key) or not isinstance(value, str):
            raise ValueError("invalid_version")
        if not re.fullmatch(r"[a-zA-Z0-9 .()+,_-]{1,160}", value):
            raise ValueError("unsafe_version")
        if any(
            word in (key + value).lower()
            for word in ("password", "token", "secret", "select ", "insert ", "credential")
        ):
            raise ValueError("unsafe_version")
    if not isinstance(raw["target_layout_sha256"], str) or not SHA.fullmatch(raw["target_layout_sha256"]):
        raise ValueError("invalid_layout_identity")
    resources = raw["resource_profile"]
    allowed = {"cpu_count", "memory_bytes", "sql_memory_bytes", "sql_log_bytes", "disk_bytes"}
    if not isinstance(resources, dict) or not resources.keys() <= allowed:
        raise ValueError("invalid_resource_profile")
    for value in resources.values():
        measured(value, "count", "environment")
    return {**raw, "sha256": digest(raw)}


@dataclass(frozen=True)
class ExecutionAdapter:
    """Same protocol for actual baseline and candidate interpreters; no source relabeling."""

    factory: RouteFactory
    variant: str

    def subject(self) -> dict[str, Any]:
        checkout = loaded_subject()
        if self.factory.subject_checkout.resolve() != checkout:
            raise ValueError("loaded_subject_mismatch")
        identity = git_identity(checkout)
        if self.variant not in {"baseline", "candidate"}:
            raise ValueError("invalid_adapter")
        if self.variant == "baseline" and identity["commit"] != BASELINE_COMMIT:
            raise ValueError("baseline_commit_mismatch")
        if self.factory.execution not in {"live", "hermetic"}:
            raise ValueError("invalid_execution")
        return identity


def load_factory(reference: str, *, configuration: dict[str, Any], route: dict[str, str]) -> RouteFactory:
    """Call only after explicit approval. Configuration contains limits, never credentials."""
    module, separator, name = reference.partition(":")
    if not separator or not all(re.fullmatch(r"[A-Za-z_]\w*", part) for part in [*module.split("."), name]):
        raise ValueError("invalid_factory_reference")
    return getattr(importlib.import_module(module), name)(configuration=configuration, route=route)
