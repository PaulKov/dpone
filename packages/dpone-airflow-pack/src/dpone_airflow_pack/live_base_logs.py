"""Keep dpone KPO tasks streaming only the ``base`` container log."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone_airflow_pack.log_transport_manager import RetryableLiveLogTransportError


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

    boundary = getattr(operator, "_live_log_transport_boundary", None)
    if boundary is None:
        return await_with_live_logs()

    try:
        with boundary.classify(api_exception_types):
            return await_with_live_logs()
    except RetryableLiveLogTransportError as exc:
        status = exc.status
        fallback_manager = exc.pod_manager

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
