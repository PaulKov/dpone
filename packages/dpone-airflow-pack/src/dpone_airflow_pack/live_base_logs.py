"""Keep dpone KPO tasks streaming only the ``base`` container log."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_TRANSIENT_KUBERNETES_API_STATUSES = frozenset({429, 500, 502, 503, 504})


class _RetryableLiveLogTransportError(RuntimeError):
    """Private proof that a retryable error came from ``read_pod_logs``."""

    def __init__(self, status: int, pod_manager: Any) -> None:
        super().__init__("retryable Kubernetes live-log transport failure")
        self.status = status
        self.pod_manager = pod_manager


def ensure_live_base_container_logs(operator: Any) -> None:
    """Force KubernetesPodOperator to follow only the ``base`` container log.

    Contract:
    - Stream ``base`` (runtime-pack-exec / dbt execute-pack) into the Airflow UI.
    - Do not stream init-fetch (KPO never follows initContainers via get_logs).
    - Do not stream sidecars / ``container_logs=True`` (all app containers).

    Airflow 3 dag-processor skips re-import when the DAG file content hash is
    unchanged. A platform-only ``dpone-airflow-pack`` image roll that flips
    compose ``get_logs`` from False to True therefore leaves stale serialized
    task kwargs with ``get_logs=False``. Re-assert on the live operator instance
    (construction + execute) so the worker package wins over stale serialization.
    """

    operator.get_logs = True
    base_name = str(getattr(operator, "base_container_name", None) or "base")
    # Always pin to the single base container name — never True (all) or sidecars.
    operator.container_logs = base_name


def await_pod_completion_with_log_stream_fallback(
    operator: Any,
    *,
    pod: Any,
    await_with_live_logs: Callable[[], Any],
    api_exception_types: tuple[type[BaseException], ...],
) -> Any:
    """Keep a runtime alive when only Kubernetes live-log transport is transiently down.

    CNCF Kubernetes provider log streaming and pod-status polling use separate
    API calls. Provider 10.19 can exhaust its retries on a kubelet
    ``containerLogs`` retryable HTTP failure while the base container is still
    healthy, then fail the task and enter pod cleanup. Dpone first requests
    normal live logs; only a retryable Kubernetes HTTP status raised by the
    manager's actual ``read_pod_logs`` boundary degrades the current operator
    instance to the provider's ordinary base-container status poll. API errors
    from pod status, container discovery, callbacks, or cleanup never acquire
    that private provenance marker.

    Authentication, authorization, not-found and malformed exceptions remain
    fail-closed. A failure of the fallback poll also propagates unchanged.
    Exception text is deliberately neither inspected nor logged because an API
    response can contain cluster or workload details.
    """

    if not api_exception_types or not bool(getattr(operator, "get_logs", False)):
        return await_with_live_logs()

    restore_readers: list[Callable[[], None]] = []
    patched_manager_ids: set[int] = set()

    def patch_manager(pod_manager: Any) -> bool:
        manager_id = id(pod_manager)
        if manager_id in patched_manager_ids:
            return True
        restore = _install_live_log_transport_boundary(
            pod_manager,
            api_exception_types=api_exception_types,
        )
        if restore is None:
            return False
        patched_manager_ids.add(manager_id)
        restore_readers.append(restore)
        return True

    if not patch_manager(operator.pod_manager):
        return await_with_live_logs()

    restore_refresh = _install_manager_refresh_boundary(operator, patch_manager=patch_manager)

    try:
        try:
            return await_with_live_logs()
        except _RetryableLiveLogTransportError as exc:
            status = exc.status
            fallback_manager = exc.pod_manager
    finally:
        if restore_refresh is not None:
            restore_refresh()
        for restore_reader in reversed(restore_readers):
            restore_reader()

    operator.get_logs = False
    operator.log.warning(
        "DPONE_KPO_LOG_STREAM_DEGRADED status=%s action=await_container_completion",
        status,
    )
    return fallback_manager.await_container_completion(
        pod=pod,
        container_name=str(getattr(operator, "base_container_name", None) or "base"),
        polling_time=getattr(
            operator,
            "base_container_status_polling_interval",
            1,
        ),
    )


def _install_live_log_transport_boundary(
    pod_manager: Any,
    *,
    api_exception_types: tuple[type[BaseException], ...],
) -> Callable[[], None] | None:
    """Mark only typed retryable failures raised by this manager's log reader."""

    original_read_pod_logs = getattr(pod_manager, "read_pod_logs", None)
    if not callable(original_read_pod_logs):
        return None
    try:
        manager_vars = vars(pod_manager)
    except TypeError:
        return None
    had_instance_override = "read_pod_logs" in manager_vars
    original_instance_override = manager_vars.get("read_pod_logs")

    def read_pod_logs_with_transport_boundary(*args: Any, **kwargs: Any) -> Any:
        try:
            return original_read_pod_logs(*args, **kwargs)
        except Exception as exc:
            status = _bounded_http_status(getattr(exc, "status", None))
            if isinstance(exc, api_exception_types) and status in _TRANSIENT_KUBERNETES_API_STATUSES:
                raise _RetryableLiveLogTransportError(status, pod_manager) from None
            raise

    try:
        pod_manager.read_pod_logs = read_pod_logs_with_transport_boundary
    except (AttributeError, TypeError):
        return None

    def restore() -> None:
        if had_instance_override:
            pod_manager.read_pod_logs = original_instance_override
        else:
            del pod_manager.read_pod_logs

    return restore


def _install_manager_refresh_boundary(
    operator: Any,
    *,
    patch_manager: Callable[[Any], bool],
) -> Callable[[], None] | None:
    """Follow provider credential refreshes and bind each replacement manager."""

    original_refresh = getattr(operator, "_refresh_cached_properties", None)
    if not callable(original_refresh):
        return None
    try:
        operator_vars = vars(operator)
    except TypeError:
        return None
    had_instance_override = "_refresh_cached_properties" in operator_vars
    original_instance_override = operator_vars.get("_refresh_cached_properties")

    def refresh_with_log_boundary(*args: Any, **kwargs: Any) -> Any:
        result = original_refresh(*args, **kwargs)
        patch_manager(operator.pod_manager)
        return result

    try:
        operator._refresh_cached_properties = refresh_with_log_boundary
    except (AttributeError, TypeError):
        return None

    def restore() -> None:
        if had_instance_override:
            operator._refresh_cached_properties = original_instance_override
        else:
            del operator._refresh_cached_properties

    return restore


def _bounded_http_status(value: object) -> int | None:
    """Return one bounded HTTP status without accepting booleans."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        status = value
    elif isinstance(value, str) and len(value) == 3 and value.isascii() and value.isdigit():
        status = int(value)
    else:
        return None
    return status if 100 <= status <= 599 else None
