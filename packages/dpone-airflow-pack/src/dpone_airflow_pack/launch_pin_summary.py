"""Runtime summary ↔ locator occurrence coupling for launch pins."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_UNAVAILABLE

AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY = "dpone_runtime_launch_pin"

_LAUNCH_PIN_REF_FIELDS = (
    "try_number",
    "pod_uid",
    "pin_sha256",
    "envelope_sha256",
    "pointer_resource_version",
    "store_authority_digest",
)


def launch_pin_ref_error(value: object) -> str:
    """Return a diagnostic when ``launch_pin_ref`` is not a closed object."""

    if not isinstance(value, Mapping):
        return f"{PIN_INVALID}: runtime summary.launch_pin_ref must be an object"
    if not value:
        return f"{PIN_INVALID}: runtime summary.launch_pin_ref must be a non-empty closed object"
    for field in _LAUNCH_PIN_REF_FIELDS:
        raw = value.get(field)
        if field == "try_number":
            if not isinstance(raw, int) or isinstance(raw, bool) or int(raw) < 1:
                return f"{PIN_INVALID}: runtime summary.launch_pin_ref.try_number must be an integer >= 1"
            continue
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return f"{PIN_INVALID}: runtime summary.launch_pin_ref.{field} is required"
    return ""


def pull_locator_summary(
    *,
    ti: Any,
    upstream_task_id: str,
    runtime_summary: Mapping[str, Any] | None = None,
    required: bool = False,
) -> tuple[dict[str, Any] | None, str | None, str]:
    """Merge ``summary.launch_pin_ref`` with locator XCom.

    Returns ``(summary, error_status, detail)``. ``error_status`` is set on failure.
    """

    summary_ref = None
    if isinstance(runtime_summary, Mapping):
        raw_ref = runtime_summary.get("launch_pin_ref")
        if raw_ref is not None:
            ref_error = launch_pin_ref_error(raw_ref)
            if ref_error:
                return None, "INVALID", ref_error
            assert isinstance(raw_ref, Mapping)
            summary_ref = dict(raw_ref)
        elif required:
            return None, "MISSING_REQUIRED", "runtime summary.launch_pin_ref is required for occurrence coupling"
    locator = None
    if hasattr(ti, "xcom_pull"):
        try:
            raw = ti.xcom_pull(task_ids=upstream_task_id, key=AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY)
        except Exception as exc:  # noqa: BLE001
            return None, "UNAVAILABLE", f"{PIN_UNAVAILABLE}: launch pin locator summary XCom pull failed: {exc}"
        if raw is not None:
            if not isinstance(raw, Mapping):
                return None, "INVALID", f"{PIN_INVALID}: launch pin locator summary must be an object"
            locator = dict(raw)
            nested = locator.get("launch_pin_ref")
            if nested is not None:
                nested_error = launch_pin_ref_error(nested)
                if nested_error:
                    return (
                        None,
                        "INVALID",
                        nested_error.replace("runtime summary.launch_pin_ref", "locator.launch_pin_ref"),
                    )
    if summary_ref is not None and locator is not None:
        mismatch = ref_fields_mismatch(left=summary_ref, right=locator, left_name="summary.launch_pin_ref")
        if mismatch:
            return None, "INVALID", mismatch
    if summary_ref is not None:
        merged = dict(locator or {})
        merged.update(summary_ref)
        merged["launch_pin_ref"] = dict(summary_ref)
        if isinstance(locator, Mapping) and isinstance(locator.get("launch_pin_store"), Mapping):
            merged["launch_pin_store"] = dict(locator["launch_pin_store"])
        return merged, None, ""
    if locator is not None:
        if required:
            nested = locator.get("launch_pin_ref") if isinstance(locator.get("launch_pin_ref"), Mapping) else locator
            ref_error = launch_pin_ref_error(nested)
            if ref_error:
                return None, "MISSING_REQUIRED", ref_error
        return locator, None, ""
    return None, None, ""


def ref_fields_mismatch(*, left: Mapping[str, Any], right: Mapping[str, Any], left_name: str) -> str:
    for field in _LAUNCH_PIN_REF_FIELDS:
        left_value, right_value = left.get(field), right.get(field)
        if left_value is None or right_value is None:
            return f"{PIN_INVALID}: {left_name}.{field} and locator.{field} are both required for occurrence coupling"
        if str(left_value) != str(right_value):
            return f"{PIN_INVALID}: {left_name}.{field}={left_value!r} does not match locator {right_value!r}"
    return ""


def summary_pointer_mismatch(*, summary: Mapping[str, Any], pointer: Mapping[str, Any]) -> str:
    for field in ("try_number", "pod_uid", "pin_sha256", "envelope_sha256", "pointer_resource_version"):
        if str(summary.get(field) or "") != str(pointer.get(field) or ""):
            return (
                f"{PIN_INVALID}: launch pin locator summary.{field}={summary.get(field)!r} "
                f"does not match immutable pointer {pointer.get(field)!r}"
            )
    return ""


__all__ = [
    "AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY",
    "launch_pin_ref_error",
    "pull_locator_summary",
    "ref_fields_mismatch",
    "summary_pointer_mismatch",
]
