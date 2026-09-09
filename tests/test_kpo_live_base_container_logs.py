"""Regression: worker must stream only base; ignore stale/sidecar log settings."""

from __future__ import annotations

from importlib import metadata
from types import SimpleNamespace
from typing import Any

import pytest
from dpone_airflow_pack.live_base_logs import (
    await_pod_completion_with_log_stream_fallback,
    ensure_live_base_container_logs,
)


class _LogStreamError(RuntimeError):
    def __init__(self, status: object) -> None:
        super().__init__("untrusted kubernetes response")
        self.status = status


class _UnrelatedStatusError(RuntimeError):
    status = 500


class _RecordingLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str, *args: object) -> None:
        self.warnings.append(message % args)


class _PodManager:
    def __init__(
        self,
        *,
        polling_error: Exception | None = None,
        live_log_error: Exception | None = None,
        live_log_result: object | None = None,
    ) -> None:
        self.polling_error = polling_error
        self.live_log_error = live_log_error
        self.live_log_result = live_log_result
        self.calls: list[dict[str, object]] = []
        self.live_log_calls: list[dict[str, object]] = []

    def await_container_completion(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))
        if self.polling_error is not None:
            raise self.polling_error

    def read_pod_logs(self, **kwargs: object) -> object | None:
        self.live_log_calls.append(dict(kwargs))
        if self.live_log_error is not None:
            raise self.live_log_error
        return self.live_log_result


class _ProviderPodManager(_PodManager):
    def __init__(
        self,
        *,
        live_log_error: Exception | None = None,
        fetch_error: Exception | None = None,
    ) -> None:
        super().__init__(live_log_error=live_log_error)
        self.fetch_error = fetch_error
        self.fetch_calls: list[dict[str, object]] = []
        self.read_pod_calls: list[dict[str, object]] = []

    def fetch_requested_container_logs(self, **kwargs: object) -> None:
        self.fetch_calls.append(dict(kwargs))
        if self.fetch_error is not None:
            raise self.fetch_error
        self.read_pod_logs(pod=kwargs["pod"], container_name="base")

    def read_pod(self, **kwargs: object) -> object:
        self.read_pod_calls.append(dict(kwargs))
        return kwargs["pod"]


def _operator(*, get_logs: bool = True, manager: _PodManager | None = None) -> Any:
    return SimpleNamespace(
        get_logs=get_logs,
        base_container_name="base",
        base_container_status_polling_interval=2,
        pod_manager=manager or _PodManager(),
        log=_RecordingLog(),
    )


def test_ensure_live_base_container_logs_overrides_stale_false() -> None:
    operator = SimpleNamespace(
        get_logs=False,
        base_container_name="base",
        container_logs="base",
    )

    ensure_live_base_container_logs(operator)

    assert operator.get_logs is True
    assert operator.container_logs == "base"


def test_ensure_live_base_container_logs_drops_sidecar_list() -> None:
    operator = SimpleNamespace(
        get_logs=False,
        base_container_name="base",
        container_logs=["sidecar-only", "base"],
    )

    ensure_live_base_container_logs(operator)

    assert operator.get_logs is True
    assert operator.container_logs == "base"


def test_ensure_live_base_container_logs_rejects_follow_all_containers() -> None:
    operator = SimpleNamespace(
        get_logs=False,
        base_container_name="base",
        container_logs=True,
    )

    ensure_live_base_container_logs(operator)

    assert operator.get_logs is True
    assert operator.container_logs == "base"


def test_log_stream_success_keeps_live_logs_and_skips_fallback() -> None:
    expected = object()
    manager = _PodManager(live_log_result=expected)
    operator = _operator(manager=manager)

    result = await_pod_completion_with_log_stream_fallback(
        operator,
        pod=object(),
        await_with_live_logs=lambda: manager.read_pod_logs(container_name="base"),
        api_exception_types=(_LogStreamError,),
    )

    assert result is expected
    assert operator.get_logs is True
    assert operator.pod_manager.calls == []
    assert operator.log.warnings == []
    assert "read_pod_logs" not in vars(manager)


@pytest.mark.parametrize("status", [429, 500, "502", 503, 504])
def test_transient_log_api_failure_degrades_to_status_polling(
    status: int | str,
) -> None:
    error = _LogStreamError(status)
    manager = _PodManager(live_log_error=error)
    operator = _operator(manager=manager)
    pod = object()

    result = await_pod_completion_with_log_stream_fallback(
        operator,
        pod=pod,
        await_with_live_logs=lambda: manager.read_pod_logs(pod=pod, container_name="base"),
        api_exception_types=(_LogStreamError,),
    )

    assert result is None
    assert operator.get_logs is False
    assert operator.pod_manager.calls == [
        {
            "pod": pod,
            "container_name": "base",
            "polling_time": 2,
        }
    ]
    assert operator.log.warnings == [
        f"DPONE_KPO_LOG_STREAM_DEGRADED status={int(status)} action=await_container_completion"
    ]
    assert "read_pod_logs" not in vars(manager)


@pytest.mark.parametrize(
    "status",
    [None, True, 99, 400, 401, 403, 404, 409, 600, "not-a-status"],
)
def test_non_transient_log_failure_remains_fail_closed(status: object) -> None:
    error = _LogStreamError(status)
    manager = _PodManager(live_log_error=error)
    operator = _operator(manager=manager)

    with pytest.raises(_LogStreamError) as raised:
        await_pod_completion_with_log_stream_fallback(
            operator,
            pod=object(),
            await_with_live_logs=lambda: manager.read_pod_logs(container_name="base"),
            api_exception_types=(_LogStreamError,),
        )

    assert raised.value is error
    assert operator.get_logs is True
    assert operator.pod_manager.calls == []
    assert operator.log.warnings == []
    assert "read_pod_logs" not in vars(manager)


def test_unrelated_exception_with_transient_status_remains_fail_closed() -> None:
    error = _UnrelatedStatusError("callback failed")
    manager = _PodManager(live_log_error=error)
    operator = _operator(manager=manager)

    with pytest.raises(_UnrelatedStatusError) as raised:
        await_pod_completion_with_log_stream_fallback(
            operator,
            pod=object(),
            await_with_live_logs=lambda: manager.read_pod_logs(container_name="base"),
            api_exception_types=(_LogStreamError,),
        )

    assert raised.value is error
    assert operator.get_logs is True
    assert operator.pod_manager.calls == []
    assert operator.log.warnings == []
    assert "read_pod_logs" not in vars(manager)


def test_retryable_api_error_outside_log_read_remains_fail_closed() -> None:
    operator = _operator()
    error = _LogStreamError(500)

    with pytest.raises(_LogStreamError) as raised:
        await_pod_completion_with_log_stream_fallback(
            operator,
            pod=object(),
            await_with_live_logs=lambda: (_ for _ in ()).throw(error),
            api_exception_types=(_LogStreamError,),
        )

    assert raised.value is error
    assert operator.get_logs is True
    assert operator.pod_manager.calls == []
    assert operator.log.warnings == []


def test_status_polling_failure_is_not_hidden() -> None:
    polling_error = RuntimeError("status polling failed")
    manager = _PodManager(
        polling_error=polling_error,
        live_log_error=_LogStreamError(500),
    )
    operator = _operator(manager=manager)

    with pytest.raises(RuntimeError) as raised:
        await_pod_completion_with_log_stream_fallback(
            operator,
            pod=object(),
            await_with_live_logs=lambda: manager.read_pod_logs(container_name="base"),
            api_exception_types=(_LogStreamError,),
        )

    assert raised.value is polling_error
    assert operator.get_logs is False


def test_existing_status_polling_does_not_reclassify_api_failure() -> None:
    error = _LogStreamError(500)
    manager = _PodManager(live_log_error=error)
    operator = _operator(get_logs=False, manager=manager)

    with pytest.raises(_LogStreamError) as raised:
        await_pod_completion_with_log_stream_fallback(
            operator,
            pod=object(),
            await_with_live_logs=lambda: manager.read_pod_logs(container_name="base"),
            api_exception_types=(_LogStreamError,),
        )

    assert raised.value is error
    assert operator.pod_manager.calls == []
    assert operator.log.warnings == []


def test_pinned_kpo_composes_fallback_with_installed_cncf_provider() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod")

    from dpone_airflow_pack.operators import (
        PinnedXComSidecarKubernetesPodOperator,
    )
    from kubernetes.client.exceptions import ApiException

    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="log_stream_fallback_contract",
        name="log-stream-fallback-contract",
        image="example.invalid/dpone-runtime:test",
        get_logs=True,
        do_xcom_push=False,
    )
    manager = _ProviderPodManager(live_log_error=ApiException(status=500))
    operator.__dict__["pod_manager"] = manager
    pod = object()

    result = operator.await_pod_completion(pod)

    assert result is None
    assert operator.get_logs is False
    assert manager.calls == [
        {
            "pod": pod,
            "container_name": "base",
            "polling_time": getattr(operator, "base_container_status_polling_interval", 1),
        }
    ]
    assert len(manager.live_log_calls) == 1


def test_pinned_kpo_keeps_non_log_api_failure_fail_closed() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod")

    from dpone_airflow_pack.operators import (
        PinnedXComSidecarKubernetesPodOperator,
    )
    from kubernetes.client.exceptions import ApiException

    error = ApiException(status=500)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="non_log_api_failure_contract",
        name="non-log-api-failure-contract",
        image="example.invalid/dpone-runtime:test",
        get_logs=True,
        do_xcom_push=False,
    )
    manager = _ProviderPodManager(fetch_error=error)
    operator.__dict__["pod_manager"] = manager

    with pytest.raises(ApiException) as raised:
        operator.await_pod_completion(object())

    assert raised.value is error
    assert operator.get_logs is True
    assert manager.calls == []
    assert manager.live_log_calls == []


def test_provider_10_19_rebinds_log_boundary_after_credential_refresh() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod")
    if metadata.version("apache-airflow-providers-cncf-kubernetes") != "10.19.0":
        pytest.skip("exact deployed provider credential-refresh contract")

    from dpone_airflow_pack.operators import (
        PinnedXComSidecarKubernetesPodOperator,
    )
    from kubernetes.client.exceptions import ApiException

    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="credential_refresh_log_fallback_contract",
        name="credential-refresh-log-fallback-contract",
        image="example.invalid/dpone-runtime:test",
        get_logs=True,
        do_xcom_push=False,
    )
    first_manager = _ProviderPodManager(live_log_error=ApiException(status=401))
    replacement_manager = _ProviderPodManager(live_log_error=ApiException(status=500))
    operator.__dict__["pod_manager"] = first_manager
    pod = object()
    refresh_events: list[str] = []

    def refresh_cached_properties() -> None:
        refresh_events.append("refresh")
        operator.__dict__["pod_manager"] = replacement_manager

    operator._refresh_cached_properties = refresh_cached_properties

    result = operator.await_pod_completion(pod)

    assert result is None
    assert operator.get_logs is False
    assert refresh_events == ["refresh"]
    assert len(first_manager.fetch_calls) == 1
    assert len(first_manager.live_log_calls) == 1
    assert replacement_manager.read_pod_calls == [{"pod": pod}]
    assert len(replacement_manager.fetch_calls) == 1
    assert len(replacement_manager.live_log_calls) == 1
    assert replacement_manager.calls == [
        {
            "pod": pod,
            "container_name": "base",
            "polling_time": getattr(operator, "base_container_status_polling_interval", 1),
        }
    ]
    assert "read_pod_logs" not in vars(first_manager)
    assert "read_pod_logs" not in vars(replacement_manager)
    assert operator._refresh_cached_properties is refresh_cached_properties


def test_provider_10_19_fallback_preserves_xcom_final_wait_and_cleanup_sequence() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod")
    if metadata.version("apache-airflow-providers-cncf-kubernetes") != "10.19.0":
        pytest.skip("exact deployed provider lifecycle contract")

    from dpone_airflow_pack.operators import (
        PinnedXComSidecarKubernetesPodOperator,
    )
    from kubernetes.client.exceptions import ApiException

    events: list[str] = []
    pod = SimpleNamespace(metadata=SimpleNamespace(name="runtime", namespace="dpone"))
    expected_xcom = {"status": "success"}

    class LifecyclePodManager(_ProviderPodManager):
        def fetch_requested_container_logs(self, **kwargs: object) -> None:
            events.append("live_logs")
            super().fetch_requested_container_logs(**kwargs)

        def await_container_completion(self, **kwargs: object) -> None:
            events.append("fallback_base_completed")
            super().await_container_completion(**kwargs)

        def await_xcom_sidecar_container_start(self, **kwargs: object) -> None:
            events.append("xcom_sidecar_ready")

        def await_pod_completion(self, *args: object, **kwargs: object) -> object:
            events.append("final_pod_completed")
            return pod

    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="log_stream_lifecycle_contract",
        name="log-stream-lifecycle-contract",
        image="example.invalid/dpone-runtime:test",
        get_logs=True,
        do_xcom_push=True,
    )
    manager = LifecyclePodManager(live_log_error=ApiException(status=500))
    operator.__dict__["pod_manager"] = manager
    operator.pod_request_obj = pod
    operator.pod = pod
    operator.find_pod = lambda *args, **kwargs: pod
    operator.await_pod_start = lambda *args, **kwargs: None
    operator.await_init_containers_completion = lambda *args, **kwargs: None
    operator.is_istio_enabled = lambda *args, **kwargs: False

    def extract_xcom(*args: object, **kwargs: object) -> object:
        events.append("extract_xcom")
        return expected_xcom

    operator.extract_xcom = extract_xcom
    operator.post_complete_action = lambda *args, **kwargs: events.append("cleanup")
    task_instance = SimpleNamespace(xcom_push=lambda **kwargs: None)

    result = operator.execute_sync({"ti": task_instance})

    assert result is expected_xcom
    assert events == [
        "live_logs",
        "fallback_base_completed",
        "xcom_sidecar_ready",
        "extract_xcom",
        "final_pod_completed",
        "cleanup",
    ]
