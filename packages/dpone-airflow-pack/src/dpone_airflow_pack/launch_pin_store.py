"""Create-only / CAS store protocol for launch-pin *pod refs*.

Production envelope authority is the Kubernetes pod annotation/env
(see ``launch_pin_pod`` / ``resolve_launch_pin``). Production *pointer*
authority is ``KubernetesConfigMapLaunchPinStore`` (resourceVersion CAS).
This module provides the protocol plus ``InMemoryLaunchPinStore`` for
hermetic tests via ``set_launch_pin_store``.

InMemory mirrors production **phase** boundaries (head admit → per-try →
head activate) with short critical sections so concurrent empty-head races
are not hidden by a single stronger-than-K8s atomic section.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any, Protocol

from dpone_airflow_pack.launch_pin_codes import (
    HEAD_PROVEN_CLOSED_STATUSES,
    LAUNCH_PIN_STATE_ACTIVE,
    LAUNCH_PIN_STATE_CANDIDATE,
    LAUNCH_PIN_STATE_CONSUMED,
    PIN_CONFLICT,
    PIN_RECOVERY_REQUIRED,
    PIN_STALE_WRITER,
    PIN_UNAVAILABLE,
)

_STORE_LOCK = threading.Lock()
_ACTIVE_STORE: LaunchPinStore | None = None


class LaunchPinStore(Protocol):
    """Concurrency-safe create-only store for launch pin pod-ref pointers."""

    def get(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        try_number: int | None = None,
    ) -> dict[str, Any] | None:
        """Return a pin for exact try, or the head try when try_number is None."""

    def create_once(self, pin: Mapping[str, Any]) -> dict[str, Any]:
        """Insert pin or return the retained winner under CAS rules."""


class InMemoryLaunchPinStore:
    """Process-local CAS store for unit tests only (via ``set_launch_pin_store``).

    Phases release the lock between head admit, per-try write, and head
    activate so two writers can interleave like the Kubernetes API.
    Optional ``phase_hook(phase)`` lets tests force deterministic interleaving.
    """

    def __init__(self, *, phase_hook: Any | None = None) -> None:
        self._lock = threading.Lock()
        self._subject_heads: dict[tuple[str, str, str, int], dict[str, Any]] = {}
        self._rows: dict[tuple[str, str, str, int, int], dict[str, Any]] = {}
        self._phase_hook = phase_hook
        self._resource_version = 0

    def get(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        try_number: int | None = None,
    ) -> dict[str, Any] | None:
        with self._lock:
            if try_number is not None:
                row = self._rows.get((dag_id, run_id, task_id, map_index, int(try_number)))
                return dict(row) if row is not None else None
            subject = (dag_id, run_id, task_id, map_index)
            head = self._subject_heads.get(subject)
            if head is None:
                return None
            row = self._rows.get((*subject, int(head["try_number"])))
            return dict(row) if row is not None else None

    def create_once(self, pin: Mapping[str, Any]) -> dict[str, Any]:
        key = _coordinate_key(pin)
        subject = key[:4]
        self._hook("before_head_admit")
        with self._lock:
            self._admit_head_candidate_locked(subject=subject, pin=pin)
        self._hook("after_head_admit")
        self._hook("before_per_try")
        with self._lock:
            existing = self._rows.get(key)
            if existing is not None:
                retained = _retain_or_conflict(existing=existing, candidate=pin)
                if str(existing.get("state") or "") == LAUNCH_PIN_STATE_ACTIVE:
                    self._require_dual_active_locked(subject=subject, pin=retained)
                    return dict(retained)
            else:
                stored = dict(pin)
                stored["state"] = LAUNCH_PIN_STATE_CANDIDATE
                stored["pointer_resource_version"] = self._next_rv_locked()
                self._rows[key] = stored
            self._require_head_reservation_locked(subject=subject, pin=pin)
            active = dict(self._rows[key])
            active["state"] = LAUNCH_PIN_STATE_ACTIVE
            active["pointer_resource_version"] = self._next_rv_locked()
            self._rows[key] = active
        self._hook("after_per_try_active")
        self._hook("before_head_activate")
        with self._lock:
            self._activate_head_locked(subject=subject, pin=pin)
            result = dict(self._rows[key])
            self._require_dual_active_locked(subject=subject, pin=result)
            return result

    def release_own_head_candidate(self, pin: Mapping[str, Any]) -> str:
        """Release own CANDIDATE head so a successor try can admit."""

        subject = (
            str(pin["dag_id"]),
            str(pin["run_id"]),
            str(pin["task_id"]),
            int(pin["map_index"]),
        )
        with self._lock:
            head = self._subject_heads.get(subject)
            if head is None:
                return "absent"
            if _head_matches(head, pin, states={LAUNCH_PIN_STATE_CONSUMED}):
                return "already_consumed"
            if not _head_matches(head, pin, states={LAUNCH_PIN_STATE_CANDIDATE}):
                raise RuntimeError(
                    f"{PIN_RECOVERY_REQUIRED}: cannot release non-own/ambiguous head after pre-ACTIVE "
                    f"failure (observed={head!r})"
                )
            self._subject_heads[subject] = _head_from_pin(pin, LAUNCH_PIN_STATE_CONSUMED)
            return "consumed"

    def delete_occurrence(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        pin_sha256: str,
        pointer_resource_version: str,
        pod_uid: str,
        try_number: int,
    ) -> dict[str, Any]:
        """Head ACTIVE→CONSUMED first, then per-try delete (mirrors production order)."""

        subject = (dag_id, run_id, task_id, map_index)
        key = (dag_id, run_id, task_id, map_index, int(try_number))
        with self._lock:
            head = self._subject_heads.get(subject)
            pin = {
                "try_number": int(try_number),
                "pod_uid": pod_uid,
                "pin_sha256": pin_sha256,
            }
            if head is None:
                head_status = "absent"
            elif not isinstance(head, Mapping):
                head_status = "skipped_mismatch"
            elif _head_matches(head, pin, states={LAUNCH_PIN_STATE_CONSUMED}):
                head_status = "already_consumed"
            elif _head_matches(head, pin, states={LAUNCH_PIN_STATE_ACTIVE}):
                self._subject_heads[subject] = _head_from_pin(
                    {**pin, "dag_id": dag_id, "run_id": run_id, "task_id": task_id, "map_index": map_index},
                    LAUNCH_PIN_STATE_CONSUMED,
                )
                head_status = "consumed"
            else:
                head_status = "skipped_mismatch"
            if str(head_status) not in HEAD_PROVEN_CLOSED_STATUSES:
                return {
                    "head_transition": head_status,
                    "per_try_delete": {"status": "skipped", "reason": "head_not_proven_closed"},
                }
            row = self._rows.get(key)
            if row is None:
                return {"head_transition": head_status, "per_try_delete": {"status": "absent"}}
            if str(row.get("pin_sha256") or "") != pin_sha256 or str(row.get("pod_uid") or "") != pod_uid:
                raise RuntimeError(f"{PIN_CONFLICT}: launch pin cleanup occurrence mismatch")
            row_rv = str(row.get("pointer_resource_version") or "")
            if row_rv and row_rv != str(pointer_resource_version):
                raise RuntimeError(f"{PIN_CONFLICT}: launch pin cleanup occurrence mismatch")
            del self._rows[key]
            return {
                "head_transition": head_status,
                "per_try_delete": {
                    "status": "deleted",
                    "pointer_resource_version": str(pointer_resource_version),
                },
            }

    def _next_rv_locked(self) -> str:
        self._resource_version += 1
        return str(self._resource_version)

    def _hook(self, phase: str) -> None:
        if self._phase_hook is not None:
            self._phase_hook(phase)

    def _admit_head_candidate_locked(self, *, subject: tuple[str, str, str, int], pin: Mapping[str, Any]) -> None:
        head = self._subject_heads.get(subject)
        try_number = int(pin["try_number"])
        if head is None:
            self._subject_heads[subject] = _head_from_pin(pin, LAUNCH_PIN_STATE_CANDIDATE)
            return
        if _head_matches(head, pin, states={LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}):
            return
        head_try = int(head["try_number"])
        if head_try > try_number:
            raise RuntimeError(
                f"{PIN_STALE_WRITER}: try_number={try_number} cannot overwrite existing try_number={head_try}"
            )
        if head_try == try_number:
            raise RuntimeError(
                f"{PIN_CONFLICT}: subject head reserved by another pod "
                f"uid={head.get('pod_uid')!r} digest={head.get('pin_sha256')!r}"
            )
        if str(head.get("state") or "") != LAUNCH_PIN_STATE_CONSUMED:
            prior = self._rows.get((*subject, head_try))
            if prior is None:
                raise RuntimeError(
                    f"{PIN_UNAVAILABLE}: launch pin head try_number={head_try} has no immutable per-try pointer"
                )
            block = _active_owner_blocks_successor(prior)
            if block:
                raise RuntimeError(block)
        self._subject_heads[subject] = _head_from_pin(pin, LAUNCH_PIN_STATE_CANDIDATE)

    def _activate_head_locked(self, *, subject: tuple[str, str, str, int], pin: Mapping[str, Any]) -> None:
        head = self._subject_heads.get(subject)
        if _head_matches(head, pin, states={LAUNCH_PIN_STATE_ACTIVE}):
            return
        if not _head_matches(head, pin, states={LAUNCH_PIN_STATE_CANDIDATE}):
            raise RuntimeError(
                f"{PIN_RECOVERY_REQUIRED}: launch pin head reservation lost before ACTIVE (observed={head!r})"
            )
        self._subject_heads[subject] = _head_from_pin(pin, LAUNCH_PIN_STATE_ACTIVE)

    def _require_head_reservation_locked(self, *, subject: tuple[str, str, str, int], pin: Mapping[str, Any]) -> None:
        head = self._subject_heads.get(subject)
        if not _head_matches(head, pin, states={LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}):
            raise RuntimeError(
                f"{PIN_CONFLICT}: launch pin head reservation mismatch after per-try create (observed={head!r})"
            )

    def _require_dual_active_locked(self, *, subject: tuple[str, str, str, int], pin: Mapping[str, Any]) -> None:
        if str(pin.get("state") or "") != LAUNCH_PIN_STATE_ACTIVE:
            raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: per-try pin is not ACTIVE after create_once")
        head = self._subject_heads.get(subject)
        if not _head_matches(head, pin, states={LAUNCH_PIN_STATE_ACTIVE}):
            raise RuntimeError(
                f"{PIN_RECOVERY_REQUIRED}: subject head is not ACTIVE(self) after create_once (observed={head!r})"
            )


def get_launch_pin_store() -> LaunchPinStore | None:
    """Return the injectable store, or ``None`` when unset."""

    with _STORE_LOCK:
        return _ACTIVE_STORE


def set_launch_pin_store(store: LaunchPinStore | None) -> None:
    """Install or clear the process-wide store (tests only)."""

    global _ACTIVE_STORE
    with _STORE_LOCK:
        _ACTIVE_STORE = store


def retain_or_conflict_pin(*, existing: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Return existing when uid+digest match; otherwise raise PIN_CONFLICT."""

    if str(existing.get("pod_uid")) == str(candidate.get("pod_uid")) and str(existing.get("pin_sha256")) == str(
        candidate.get("pin_sha256")
    ):
        return dict(existing)
    raise RuntimeError(
        f"{PIN_CONFLICT}: existing launch pin pod_uid={existing.get('pod_uid')!r} "
        f"pin_sha256={existing.get('pin_sha256')!r} conflicts with selected "
        f"pod_uid={candidate.get('pod_uid')!r} pin_sha256={candidate.get('pin_sha256')!r} "
        f"for try_number={candidate.get('try_number')}"
    )


_retain_or_conflict = retain_or_conflict_pin


def _coordinate_key(pin: Mapping[str, Any]) -> tuple[str, str, str, int, int]:
    return (
        str(pin["dag_id"]),
        str(pin["run_id"]),
        str(pin["task_id"]),
        int(pin["map_index"]),
        int(pin["try_number"]),
    )


def _head_from_pin(pin: Mapping[str, Any], state: str) -> dict[str, Any]:
    return {
        "try_number": int(pin["try_number"]),
        "pod_uid": str(pin["pod_uid"]),
        "pin_sha256": str(pin["pin_sha256"]),
        "state": state,
    }


def _head_matches(head: Mapping[str, Any] | None, pin: Mapping[str, Any], *, states: set[str]) -> bool:
    if head is None:
        return False
    return (
        int(head.get("try_number", -1) or -1) == int(pin["try_number"])
        and str(head.get("pod_uid") or "") == str(pin["pod_uid"])
        and str(head.get("pin_sha256") or "") == str(pin["pin_sha256"])
        and str(head.get("state") or "") in states
    )


def _active_owner_blocks_successor(existing: Mapping[str, Any]) -> str:
    state = str(existing.get("state") or "").strip() or LAUNCH_PIN_STATE_ACTIVE
    namespace = str(existing.get("pod_namespace") or "").strip()
    name = str(existing.get("pod_name") or "").strip()
    uid = str(existing.get("pod_uid") or "").strip()
    if not namespace or not name or not uid:
        return f"{PIN_RECOVERY_REQUIRED}: prior launch pin owner coordinates are incomplete"
    if state == LAUNCH_PIN_STATE_ACTIVE:
        return (
            f"{PIN_RECOVERY_REQUIRED}: prior ACTIVE launch pin try_number={existing.get('try_number')} "
            f"pod_uid={uid!r} blocks successor CAS; terminal/404 is insufficient without "
            "explicit reconciliation receipt"
        )
    if state != LAUNCH_PIN_STATE_CANDIDATE:
        return f"{PIN_RECOVERY_REQUIRED}: prior launch pin state {state!r} blocks successor CAS"
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader, pod_is_terminal_or_absent

    conn = existing.get("kubernetes_conn_id")
    kubernetes_conn_id = str(conn).strip() if isinstance(conn, str) and conn.strip() else None
    try:
        if pod_is_terminal_or_absent(
            namespace=namespace,
            name=name,
            expected_uid=uid,
            kubernetes_conn_id=kubernetes_conn_id,
            reader=get_launch_pin_pod_reader(),
        ):
            return ""
    except RuntimeError:
        return (
            f"{PIN_RECOVERY_REQUIRED}: prior launch pin owner pod status is unreconciled "
            f"(try_number={existing.get('try_number')})"
        )
    return (
        f"{PIN_STALE_WRITER}: CANDIDATE owner try_number={existing.get('try_number')} "
        f"pod_uid={uid!r} still exists; successor CAS requires prior terminal"
    )


__all__ = [
    "InMemoryLaunchPinStore",
    "LaunchPinStore",
    "get_launch_pin_store",
    "retain_or_conflict_pin",
    "set_launch_pin_store",
]
