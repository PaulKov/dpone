"""Log transport composition preserves provider dispatch without method mutation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest
from dpone_airflow_pack.log_transport_manager import (
    LiveLogTransportBoundary,
    LogTransportPodManagerMixin,
    RetryableLiveLogTransportError,
)


class _ApiError(RuntimeError):
    status = 500


class _Provider:
    def __init__(self, **kwargs: Any) -> None:
        self.options = kwargs

    def read_pod_logs(self, **kwargs: Any) -> None:
        assert "read_pod_logs" not in vars(self)
        raise _ApiError()

    def fetch_requested_container_logs(self) -> None:
        self.read_pod_logs(container_name="base")


class _Manager(LogTransportPodManagerMixin, _Provider):
    pass


def test_internal_provider_dispatch_classifies_only_inside_explicit_scope() -> None:
    boundary = LiveLogTransportBoundary()
    manager = _Manager(log_transport_boundary=boundary, kube_client="synthetic")
    assert manager.options == {"kube_client": "synthetic"}
    original_reader = manager.read_pod_logs
    with pytest.raises(_ApiError):
        manager.fetch_requested_container_logs()
    with boundary.classify((_ApiError,)):
        with pytest.raises(RetryableLiveLogTransportError) as raised:
            manager.fetch_requested_container_logs()
    assert raised.value.pod_manager is manager
    assert raised.value.status == 500
    assert manager.read_pod_logs == original_reader
    with pytest.raises(_ApiError):
        manager.fetch_requested_container_logs()


def test_shared_boundary_keeps_concurrent_contexts_isolated() -> None:
    boundary = LiveLogTransportBoundary()
    manager = _Manager(log_transport_boundary=boundary)
    barrier = Barrier(2)

    def classified() -> None:
        with boundary.classify((_ApiError,)):
            barrier.wait(timeout=5)
            with pytest.raises(RetryableLiveLogTransportError):
                manager.fetch_requested_container_logs()
            barrier.wait(timeout=5)

    def unclassified() -> None:
        barrier.wait(timeout=5)
        with pytest.raises(_ApiError):
            manager.fetch_requested_container_logs()
        barrier.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(classified), executor.submit(unclassified)]
        for future in futures:
            future.result(timeout=10)


def test_native_provider_refresh_reconstructs_owned_manager() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod")
    from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    operator = PinnedXComSidecarKubernetesPodOperator(task_id="log_composition", image="synthetic:test")
    if not hasattr(operator, "_refresh_cached_properties"):
        pytest.skip("provider has no credential-refresh method")
    first_client = object()
    operator.__dict__.update(client=first_client, hook=object())
    first_manager = operator.pod_manager
    assert isinstance(first_manager, PodManager)
    assert isinstance(first_manager, LogTransportPodManagerMixin)
    assert first_manager._client is first_client
    assert "read_pod_logs" not in vars(first_manager)
    refresh = operator._refresh_cached_properties

    operator._refresh_cached_properties()
    second_client = object()
    operator.__dict__.update(client=second_client, hook=object())
    second_manager = operator.pod_manager

    assert second_manager is not first_manager
    assert type(second_manager) is type(first_manager)
    assert second_manager._client is second_client
    assert second_manager.log_transport_boundary is first_manager.log_transport_boundary
    assert "read_pod_logs" not in vars(second_manager)
    assert "_refresh_cached_properties" not in vars(operator)
    assert operator._refresh_cached_properties == refresh
