"""Subject-head ConfigMap CAS admit/activate (pre-activation admission).

Never ignore head ``409`` — always read back and compare the winner.
Head write timeout / 403 / lost write after per-try progress →
``PIN_RECOVERY_REQUIRED`` so the barrier keeps holding the base container.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    LAUNCH_PIN_STATE_ACTIVE,
    LAUNCH_PIN_STATE_CANDIDATE,
    LAUNCH_PIN_STATE_CONSUMED,
    PIN_CONFLICT,
    PIN_RECOVERY_REQUIRED,
    PIN_STALE_WRITER,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_k8s_head import (
    head_matches_pin,
    head_payload_from_pin,
    read_launch_pin_head,
)
from dpone_airflow_pack.launch_pin_k8s_validate import configmap_name_for_subject, head_configmap_body

ReadFn = Callable[[str], tuple[Any, str]]
CreateFn = Callable[[Mapping[str, Any]], Any]
ReplaceFn = Callable[..., Any]
PriorLoader = Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
SuccessorBlocker = Callable[[Mapping[str, Any]], str]


def cas_admit_head_candidate(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    create_fn: CreateFn,
    replace_fn: ReplaceFn,
    not_found_exc: type[BaseException],
    already_exists_exc: type[BaseException],
    load_prior: PriorLoader,
    successor_blocker: SuccessorBlocker,
) -> dict[str, Any]:
    """CAS subject head to ``CANDIDATE`` for this pin; only the winner proceeds."""

    head_name = configmap_name_for_subject(
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        try_number=None,
    )
    desired = head_payload_from_pin(pin, state=LAUNCH_PIN_STATE_CANDIDATE, namespace=namespace)
    existing = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if existing is None:
        return _create_head_candidate(
            api=api,
            namespace=namespace,
            head_name=head_name,
            desired=desired,
            pin=pin,
            read_fn=read_fn,
            create_fn=create_fn,
            not_found_exc=not_found_exc,
            already_exists_exc=already_exists_exc,
        )
    if head_matches_pin(
        existing,
        pin,
        states=frozenset({LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}),
    ):
        return existing
    _assert_may_replace_predecessor(
        existing=existing,
        pin=pin,
        load_prior=load_prior,
        successor_blocker=successor_blocker,
    )
    return _replace_head_with_readback(
        api=api,
        namespace=namespace,
        head_name=head_name,
        desired=desired,
        pin=pin,
        expected_states=frozenset({LAUNCH_PIN_STATE_CANDIDATE}),
        resource_version=str(existing["pointer_resource_version"]),
        read_fn=read_fn,
        replace_fn=replace_fn,
        not_found_exc=not_found_exc,
        recovery_on_lost=False,
    )


def cas_activate_head(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    not_found_exc: type[BaseException],
) -> dict[str, Any]:
    """CAS subject head from exact ``CANDIDATE(self)`` to exact ``ACTIVE(self)``."""

    head_name = configmap_name_for_subject(
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        try_number=None,
    )
    existing = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if head_matches_pin(existing, pin, states=frozenset({LAUNCH_PIN_STATE_ACTIVE})):
        assert existing is not None
        return existing
    if not head_matches_pin(existing, pin, states=frozenset({LAUNCH_PIN_STATE_CANDIDATE})):
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: launch pin head reservation lost before ACTIVE (observed={existing!r})"
        )
    assert existing is not None
    desired = head_payload_from_pin(pin, state=LAUNCH_PIN_STATE_ACTIVE, namespace=namespace)
    return _replace_head_with_readback(
        api=api,
        namespace=namespace,
        head_name=head_name,
        desired=desired,
        pin=pin,
        expected_states=frozenset({LAUNCH_PIN_STATE_ACTIVE}),
        resource_version=str(existing["pointer_resource_version"]),
        read_fn=read_fn,
        replace_fn=replace_fn,
        not_found_exc=not_found_exc,
        recovery_on_lost=True,
    )


def _create_head_candidate(
    *,
    api: Any,
    namespace: str,
    head_name: str,
    desired: Mapping[str, Any],
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    create_fn: CreateFn,
    not_found_exc: type[BaseException],
    already_exists_exc: type[BaseException],
) -> dict[str, Any]:
    body = head_configmap_body(name=head_name, namespace=namespace, head=desired)
    try:
        create_fn(body)
    except already_exists_exc as exc:
        return _winner_from_head_conflict(
            api=api,
            namespace=namespace,
            pin=pin,
            read_fn=read_fn,
            not_found_exc=not_found_exc,
            error=exc,
            accept_states=frozenset({LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}),
        )
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "status", None)
        observed = read_launch_pin_head(
            api=api,
            namespace=namespace,
            dag_id=str(pin["dag_id"]),
            run_id=str(pin["run_id"]),
            task_id=str(pin["task_id"]),
            map_index=int(pin["map_index"]),
            read_fn=read_fn,
            not_found_exc=not_found_exc,
        )
        if head_matches_pin(
            observed,
            pin,
            states=frozenset({LAUNCH_PIN_STATE_CANDIDATE, LAUNCH_PIN_STATE_ACTIVE}),
        ):
            assert observed is not None
            return observed
        if status in {403, 408, 500, 503} or isinstance(exc, TimeoutError):
            raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: launch pin head admit write failed: {exc}") from exc
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin head admit create failed: {exc}") from exc
    observed = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if not head_matches_pin(observed, pin, states=frozenset({LAUNCH_PIN_STATE_CANDIDATE})):
        raise RuntimeError(f"{PIN_CONFLICT}: launch pin head admit create lost race (observed={observed!r})")
    assert observed is not None
    return observed


def _replace_head_with_readback(
    *,
    api: Any,
    namespace: str,
    head_name: str,
    desired: Mapping[str, Any],
    pin: Mapping[str, Any],
    expected_states: frozenset[str],
    resource_version: str,
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    not_found_exc: type[BaseException],
    recovery_on_lost: bool,
) -> dict[str, Any]:
    body = head_configmap_body(name=head_name, namespace=namespace, head=desired)
    body["metadata"]["resourceVersion"] = resource_version
    try:
        replace_fn(name=head_name, namespace=namespace, body=body)
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "status", None)
        if status == 409:
            return _winner_from_head_conflict(
                api=api,
                namespace=namespace,
                pin=pin,
                read_fn=read_fn,
                not_found_exc=not_found_exc,
                error=exc,
                accept_states=expected_states,
            )
        if recovery_on_lost or status in {403, 408, 500, 503} or isinstance(exc, TimeoutError):
            raise RuntimeError(
                f"{PIN_RECOVERY_REQUIRED}: launch pin head write failed after per-try progress: {exc}"
            ) from exc
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin head replace failed: {exc}") from exc
    observed = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if not head_matches_pin(observed, pin, states=expected_states):
        code = PIN_RECOVERY_REQUIRED if recovery_on_lost else PIN_CONFLICT
        raise RuntimeError(f"{code}: launch pin head replace readback mismatch (observed={observed!r})")
    assert observed is not None
    return observed


def _winner_from_head_conflict(
    *,
    api: Any,
    namespace: str,
    pin: Mapping[str, Any],
    read_fn: ReadFn,
    not_found_exc: type[BaseException],
    error: BaseException,
    accept_states: frozenset[str],
) -> dict[str, Any]:
    observed = read_launch_pin_head(
        api=api,
        namespace=namespace,
        dag_id=str(pin["dag_id"]),
        run_id=str(pin["run_id"]),
        task_id=str(pin["task_id"]),
        map_index=int(pin["map_index"]),
        read_fn=read_fn,
        not_found_exc=not_found_exc,
    )
    if head_matches_pin(observed, pin, states=accept_states):
        assert observed is not None
        return observed
    raise RuntimeError(
        f"{PIN_CONFLICT}: launch pin head CAS lost to another writer (observed={observed!r}); original={error}"
    ) from error


def _assert_may_replace_predecessor(
    *,
    existing: Mapping[str, Any],
    pin: Mapping[str, Any],
    load_prior: PriorLoader,
    successor_blocker: SuccessorBlocker,
) -> None:
    head_try = int(existing["try_number"])
    candidate_try = int(pin["try_number"])
    if head_try > candidate_try:
        raise RuntimeError(
            f"{PIN_STALE_WRITER}: try_number={candidate_try} cannot overwrite existing try_number={head_try}"
        )
    if head_try == candidate_try:
        raise RuntimeError(
            f"{PIN_CONFLICT}: subject head reserved by another pod "
            f"uid={existing.get('pod_uid')!r} digest={existing.get('pin_sha256')!r}"
        )
    if str(existing.get("state") or "") == LAUNCH_PIN_STATE_CONSUMED:
        return
    prior = load_prior(existing)
    if prior is None:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin head try_number={head_try} has no immutable per-try pointer")
    block = successor_blocker(prior)
    if block:
        raise RuntimeError(block)


__all__ = ["cas_activate_head", "cas_admit_head_candidate"]
