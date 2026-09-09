"""Occurrence cleanup and pre-ACTIVE per-try failure handling for ConfigMap pins."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    HEAD_PROVEN_CLOSED_STATUSES,
    PIN_CONFLICT,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_k8s_head import mark_head_consumed_or_delete
from dpone_airflow_pack.launch_pin_k8s_validate import configmap_name_for_subject, require_valid_pin_from_configmap

ReadFn = Callable[[str], tuple[Any, str]]
ReplaceFn = Callable[..., Any]
ActivateDualFn = Callable[..., dict[str, Any]]
AssertMatchFn = Callable[..., None]
ReleaseHeadFn = Callable[[Mapping[str, Any]], str]


def _per_try_delete_when_head_closed(
    *,
    head_status: str,
    delete_per_try: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Skip per-try delete unless head closure is proven."""

    if str(head_status) not in HEAD_PROVEN_CLOSED_STATUSES:
        return {
            "head_transition": head_status,
            "per_try_delete": {"status": "skipped", "reason": "head_not_proven_closed"},
        }
    per_try = delete_per_try()
    return {"head_transition": head_status, "per_try_delete": per_try}


def delete_launch_pin_occurrence(
    *,
    namespace: str,
    api: Any,
    read_fn: ReadFn,
    replace_fn: ReplaceFn,
    not_found_exc: type[BaseException],
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    pin_sha256: str,
    pointer_resource_version: str,
    pod_uid: str,
    try_number: int,
) -> dict[str, Any]:
    """Head ACTIVE(self)→CONSUMED (+ readback) first, then exact per-try delete."""

    name = configmap_name_for_subject(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=map_index,
        try_number=int(try_number),
    )
    delete_fn = api.delete_namespaced_config_map
    try:
        body, resource_version = read_fn(name)
    except not_found_exc:
        pin = {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "map_index": map_index,
            "try_number": int(try_number),
            "pod_uid": pod_uid,
            "pin_sha256": pin_sha256,
        }
        head_status = mark_head_consumed_or_delete(
            api=api,
            namespace=namespace,
            pin=pin,
            read_fn=read_fn,
            replace_fn=replace_fn,
            delete_fn=delete_fn,
            not_found_exc=not_found_exc,
        )
        return _per_try_delete_when_head_closed(
            head_status=head_status,
            delete_per_try=lambda: {"status": "absent"},
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap cleanup read failed: {exc}") from exc
    if resource_version != pointer_resource_version:
        raise RuntimeError(f"{PIN_CONFLICT}: launch pin ConfigMap cleanup resourceVersion mismatch for {name}")
    pin = require_valid_pin_from_configmap(body, expected_name=name, expected_namespace=namespace)
    observed_try = pin.get("try_number")
    if (
        str(pin.get("pin_sha256") or "") != pin_sha256
        or str(pin.get("pod_uid") or "") != pod_uid
        or not isinstance(observed_try, int)
        or isinstance(observed_try, bool)
        or observed_try != int(try_number)
    ):
        raise RuntimeError(f"{PIN_CONFLICT}: launch pin ConfigMap cleanup occurrence mismatch for {name}")
    head_status = mark_head_consumed_or_delete(
        api=api,
        namespace=namespace,
        pin=pin,
        read_fn=read_fn,
        replace_fn=replace_fn,
        delete_fn=delete_fn,
        not_found_exc=not_found_exc,
    )

    def _delete_per_try() -> dict[str, Any]:
        try:
            delete_fn(
                name=name,
                namespace=namespace,
                body={"preconditions": {"resourceVersion": pointer_resource_version}},
            )
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status", None)
            if status == 404:
                return {
                    "status": "absent",
                    "pointer_resource_version": pointer_resource_version,
                }
            if status == 409:
                raise RuntimeError(
                    f"{PIN_CONFLICT}: launch pin ConfigMap cleanup resourceVersion conflict for {name}"
                ) from exc
            raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap cleanup delete failed: {exc}") from exc
        return {
            "status": "deleted",
            "pointer_resource_version": pointer_resource_version,
        }

    return _per_try_delete_when_head_closed(head_status=head_status, delete_per_try=_delete_per_try)


def after_per_try_create_failure(
    *,
    name: str,
    namespace: str,
    pin: Mapping[str, Any],
    candidate: Mapping[str, Any],
    error: Exception,
    read_fn: ReadFn,
    not_found_exc: type[BaseException],
    release_own_head: ReleaseHeadFn,
    assert_matches_candidate: AssertMatchFn,
    activate_dual: ActivateDualFn,
) -> dict[str, Any]:
    """Release own head when per-try is proved absent; else resume dual activation."""

    try:
        body, _rv = read_fn(name)
    except not_found_exc:
        status = release_own_head(pin)
        if str(status) not in HEAD_PROVEN_CLOSED_STATUSES:
            raise RuntimeError(
                f"{PIN_RECOVERY_REQUIRED}: head release did not prove closure after proved-absent per-try: {status!r}"
            )
        raise RuntimeError(f"{PIN_UNAVAILABLE}: launch pin ConfigMap create failed: {error}") from error
    except Exception as read_exc:  # noqa: BLE001
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: ambiguous per-try after head CANDIDATE admit: {read_exc}"
        ) from error
    try:
        observed = require_valid_pin_from_configmap(body, expected_name=name, expected_namespace=namespace)
        assert_matches_candidate(observed=observed, candidate=candidate)
    except RuntimeError as mismatch:
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: per-try present after failed create but does not match self: {mismatch}"
        ) from error
    return activate_dual(name=name, pin=observed, candidate=candidate)


__all__ = [
    "after_per_try_create_failure",
    "delete_launch_pin_occurrence",
]
