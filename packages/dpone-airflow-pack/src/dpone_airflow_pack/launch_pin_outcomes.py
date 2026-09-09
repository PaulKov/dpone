"""Stage-aware launch-pin CAS outcomes (Authority C commit path).

Post-ACTIVE failures must never be classified as pre-authority. Same-pod
CAS winners are idempotent success; losers of other-pod conflicts may abandon
only their own unauthorized pod when ACTIVE is proved absent.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from dpone_airflow_pack.launch_pin_codes import (
    LAUNCH_PIN_STATE_ACTIVE,
    PIN_CONFLICT,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)

CasOutcomeKind = Literal[
    "PRE_ACTIVE_FAILURE",
    "ACTIVE_CONFIRMED",
    "ACTIVE_ACK_UNKNOWN",
    "IDEMPOTENT_SAME_POD_WINNER",
    "CONFLICT_OTHER_POD",
]


@dataclass(frozen=True)
class CasCommitOutcome:
    """Classified result of create-once / post-commit handling."""

    kind: CasOutcomeKind
    pin: dict[str, Any] | None = None
    detail: str = ""


def is_active_confirmed_pin(*, retained: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    """Return True when create_once returned the ACTIVE winner for this pod."""

    return (
        str(retained.get("state") or "") == LAUNCH_PIN_STATE_ACTIVE
        and str(retained.get("pod_uid") or "") == str(candidate.get("pod_uid") or "")
        and str(retained.get("pin_sha256") or "") == str(candidate.get("pin_sha256") or "")
    )


def classify_create_once_failure(
    exc: BaseException,
    *,
    candidate: Mapping[str, Any],
    store: Any,
) -> CasCommitOutcome:
    """Classify a create_once exception without assuming pre-ACTIVE safety."""

    detail = str(exc)
    # Explicit recovery / hard-block codes must never auto-abandon the selected pod.
    if detail.startswith(PIN_RECOVERY_REQUIRED):
        return CasCommitOutcome(kind="ACTIVE_ACK_UNKNOWN", detail=detail)
    try:
        observed = store.get(
            dag_id=str(candidate["dag_id"]),
            run_id=str(candidate["run_id"]),
            task_id=str(candidate["task_id"]),
            map_index=int(candidate["map_index"]),
            try_number=int(candidate["try_number"]),
        )
    except Exception as read_exc:  # noqa: BLE001 - read failure ⇒ ack unknown
        return CasCommitOutcome(
            kind="ACTIVE_ACK_UNKNOWN",
            detail=(
                f"{PIN_RECOVERY_REQUIRED}: launch pin create_once failed and exact-try "
                f"pointer read is unavailable ({read_exc}); original={detail}"
            ),
        )

    if observed is not None and is_active_confirmed_pin(retained=observed, candidate=candidate):
        return CasCommitOutcome(
            kind="IDEMPOTENT_SAME_POD_WINNER",
            pin=dict(observed),
            detail="same-pod ACTIVE pointer retained after create_once conflict",
        )

    if observed is not None and str(observed.get("state") or "") == LAUNCH_PIN_STATE_ACTIVE:
        if str(observed.get("pod_uid") or "") == str(candidate.get("pod_uid") or ""):
            return CasCommitOutcome(
                kind="IDEMPOTENT_SAME_POD_WINNER",
                pin=dict(observed),
                detail="same-pod ACTIVE pointer retained",
            )
        return CasCommitOutcome(
            kind="CONFLICT_OTHER_POD",
            pin=dict(observed),
            detail=detail if detail.startswith(PIN_CONFLICT) else f"{PIN_CONFLICT}: {detail}",
        )

    # ACTIVE absent (miss / non-ACTIVE) ⇒ safe to abandon this unauthorized pod.
    if observed is None or str(observed.get("state") or "") != LAUNCH_PIN_STATE_ACTIVE:
        return CasCommitOutcome(kind="PRE_ACTIVE_FAILURE", detail=detail)

    return CasCommitOutcome(
        kind="ACTIVE_ACK_UNKNOWN",
        detail=f"{PIN_RECOVERY_REQUIRED}: launch pin create_once outcome is ambiguous: {detail}",
    )


def raise_for_pre_active_abandon(*, abandoned: bool, detail: str) -> None:
    """Raise the stable pre-authority abandon / recovery error."""

    if abandoned:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin CAS failed before base authority; pod abandoned: {detail}")
    raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: launch pin CAS failed and pod abandon is unverified: {detail}")


__all__ = [
    "CasCommitOutcome",
    "CasOutcomeKind",
    "classify_create_once_failure",
    "is_active_confirmed_pin",
    "raise_for_pre_active_abandon",
]
