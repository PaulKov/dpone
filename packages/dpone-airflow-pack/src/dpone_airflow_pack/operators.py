from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cached_property
from importlib.util import find_spec
from typing import Any

from dpone_airflow_pack.connection_projection import (
    AirflowBaseHookConnectionReader,
    ConnectionUriReader,
    ProjectedAirflowConnectionUriReader,
    RuntimeAirflowConnectionUriReader,
    UnsafeAirflowConnectionEnvBuilder,
    apply_database_overrides,
    apply_query_overrides,
)
from dpone_airflow_pack.connection_secret_identity import (
    AirflowConnectionSecretAttempt,
    AirflowTaskAttemptIdentityError,
    bind_attempt_secret_name,
    derive_airflow_connection_secret_attempt,
    require_airflow_task_attempt_identity,
)
from dpone_airflow_pack.connection_secret_lifecycle import AirflowConnectionSecretLifecycle
from dpone_airflow_pack.connection_secret_projector import (
    AirflowConnectionSecretConflictError,
    AirflowConnectionSecretDependencyError,
    AirflowConnectionSecretProjectionError,
    AirflowConnectionSecretProjector,
    KubernetesApiAirflowConnectionSecretProjector,
)
from dpone_airflow_pack.launch_pin_commit import (
    embed_launch_pin_ref_in_summary,
    inject_launch_pin_ref_into_defer_kwargs,
    rehydrate_committed_launch_pin_ref,
)
from dpone_airflow_pack.launch_pin_locator import (
    AIRFLOW_TASK_STATE_BACKEND,
    frozen_launch_pin_store_locator,
)
from dpone_airflow_pack.launch_pin_operator import (
    attach_operator_launch_pin_barrier,
    commit_operator_launch_pin,
    prepare_operator_launch_pin_pod,
)
from dpone_airflow_pack.launch_pin_task_state import configure_durable_kpo_kwargs
from dpone_airflow_pack.live_base_logs import (
    await_pod_completion_with_log_stream_fallback,
    ensure_live_base_container_logs,
)
from dpone_airflow_pack.log_transport_manager import LiveLogTransportBoundary, LogTransportPodManagerMixin
from dpone_airflow_pack.operator_runtime import (
    UNSAFE_AIRFLOW_CONNECTION_ENV_ANNOTATION,
    UNSAFE_AIRFLOW_CONNECTION_ENV_VALUE,
    _operator_deferrable,
    _operator_namespace,
    _projection_cleanup_policy,
    _projection_secret_name,
    build_airflow_connection_projected_secret,
    materialize_airflow_connection_secret_volume,
    materialize_runtime_connection_env,
    merge_env_vars,
    patch_pod_spec_env_vars,
    patch_pod_spec_secret_volume,
    stringify_env_vars,
)
from dpone_airflow_pack.outcome import (
    _operator_outcome_expectations,
    evaluate_operator_inline_outcome,
    pull_deferrable_xcom,
)
from dpone_airflow_pack.xcom_sidecar import (
    XComSidecarRuntimeConfig,
    pin_xcom_sidecar_image,
    require_xcom_sidecar_image,
    validate_strict_xcom_pod,
)


class KubernetesPodOperatorDependencyError(RuntimeError):
    """Fail closed when Airflow cannot materialize a Kubernetes pod task."""


def _airflow_core_available() -> bool:
    try:
        return find_spec("airflow.models") is not None
    except (ImportError, ValueError):
        return False


_KUBERNETES_API_EXCEPTION_TYPES: tuple[type[BaseException], ...] = ()
try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
    from kubernetes.client.exceptions import ApiException as _KubernetesApiException
except Exception:  # pragma: no cover - local tests may not have Airflow installed.
    _AIRFLOW_CORE_AVAILABLE = _airflow_core_available()

    class KubernetesPodOperator:  # type: ignore[no-redef]
        def __init__(self, *, durable: bool = True, **kwargs: Any) -> None:
            if _AIRFLOW_CORE_AVAILABLE:
                raise KubernetesPodOperatorDependencyError(
                    "DPONE_AIRFLOW_KUBERNETES_PROVIDER_UNAVAILABLE: Apache Airflow is installed, "
                    "but KubernetesPodOperator cannot be imported from "
                    "apache-airflow-providers-cncf-kubernetes"
                )
            self.kwargs = kwargs
            self.do_xcom_push = bool(kwargs.get("do_xcom_push", False))
            self.env_vars = kwargs.get("env_vars")
            self.full_pod_spec = kwargs.get("full_pod_spec")
            self.pod_template_dict = kwargs.get("pod_template_dict")
            self.durable = bool(durable)

        def build_pod_request_obj(self, context: Any | None = None) -> Any:
            pod = self.full_pod_spec if self.full_pod_spec is not None else self.kwargs.get("full_pod_spec")
            return patch_pod_spec_env_vars(pod, getattr(self, "env_vars", None))

        def execute(self, context: Any) -> Any:
            del context
            raise KubernetesPodOperatorDependencyError(
                "DPONE_AIRFLOW_KUBERNETES_OPERATOR_EXECUTION_UNAVAILABLE: "
                "dependency-light KubernetesPodOperator fallback cannot execute tasks"
            )

        def get_or_create_pod(self, pod_request_obj: Any, context: Any) -> Any:
            del context
            return pod_request_obj

        def trigger_reentry(self, context: Any, event: Any) -> Any:
            del context, event
            raise KubernetesPodOperatorDependencyError(
                "DPONE_AIRFLOW_KUBERNETES_OPERATOR_EXECUTION_UNAVAILABLE: "
                "dependency-light KubernetesPodOperator fallback cannot resume tasks"
            )

        def __rshift__(self, other: object) -> object:
            return other
else:
    _KUBERNETES_API_EXCEPTION_TYPES = (_KubernetesApiException,)

    class _LogTransportPodManager(LogTransportPodManagerMixin, PodManager):
        """Provider manager with an explicitly injected log transport boundary."""


class PinnedXComSidecarKubernetesPodOperator(KubernetesPodOperator):
    def __init__(
        self,
        *,
        xcom_sidecar: XComSidecarRuntimeConfig | None = None,
        inline_outcome_required_status: str | None = None,
        expected_runtime_evidence_sha256: str | None = None,
        pin_deployment_identity_for_separate_outcome_gate: bool = False,
        launch_pin_store: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        (
            self.expected_run_identity,
            self.expected_deployment_identity,
            self.expected_runtime_evidence_sha256,
        ) = _operator_outcome_expectations(
            kwargs.get("params"),
            explicit_evidence_digest=expected_runtime_evidence_sha256,
        )
        self.pin_deployment_identity_for_separate_outcome_gate = bool(pin_deployment_identity_for_separate_outcome_gate)
        self.launch_pin_store = dict(launch_pin_store) if isinstance(launch_pin_store, Mapping) else None
        if self.pin_deployment_identity_for_separate_outcome_gate:
            # Closed: author/pack cannot choose delete_succeeded_pod while a
            # separate outcome_gate must re-fetch Authority C from the live pod.
            kwargs["on_finish_action"] = "keep_pod"
            if frozen_launch_pin_store_locator(self.launch_pin_store).backend == AIRFLOW_TASK_STATE_BACKEND:
                kwargs = configure_durable_kpo_kwargs(
                    kpo_class=KubernetesPodOperator,
                    kwargs=kwargs,
                )
        super().__init__(**kwargs)
        if self.pin_deployment_identity_for_separate_outcome_gate:
            self.on_finish_action = "keep_pod"
            if hasattr(self, "kwargs") and isinstance(self.kwargs, dict):
                self.kwargs["on_finish_action"] = "keep_pod"
        self.xcom_sidecar = xcom_sidecar
        self.inline_outcome_required_status = inline_outcome_required_status
        ensure_live_base_container_logs(self)

    @cached_property
    def _live_log_transport_boundary(self) -> LiveLogTransportBoundary:
        return LiveLogTransportBoundary()

    @cached_property
    def pod_manager(self) -> Any:
        """Recreate the owned manager when provider refresh invalidates its cache."""
        if not _KUBERNETES_API_EXCEPTION_TYPES:
            raise KubernetesPodOperatorDependencyError(
                "DPONE_AIRFLOW_KUBERNETES_PROVIDER_UNAVAILABLE: log manager requires the Kubernetes provider"
            )
        return _LogTransportPodManager(
            kube_client=self.client,
            callbacks=self.callbacks,
            log_transport_boundary=self._live_log_transport_boundary,
        )

    def build_pod_request_obj(self, context: Any | None = None) -> Any:
        pod = super().build_pod_request_obj(context=context)
        pod = patch_pod_spec_env_vars(pod, getattr(self, "env_vars", None))
        pod = prepare_operator_launch_pin_pod(
            pod=pod,
            operator=self,
            context=context,
        )
        if getattr(self, "do_xcom_push", False):
            sidecar_image = require_xcom_sidecar_image(self.xcom_sidecar)
            pin_xcom_sidecar_image(pod, sidecar_image)
            strict_runtime_image = None if self.xcom_sidecar is None else self.xcom_sidecar.strict_runtime_image
            if strict_runtime_image is not None:
                validate_strict_xcom_pod(
                    pod,
                    sidecar_image=sidecar_image,
                    runtime_image_ref=strict_runtime_image,
                )
        pod = attach_operator_launch_pin_barrier(
            pod=pod,
            operator=self,
            context=context,
        )
        return pod

    def get_or_create_pod(self, pod_request_obj: Any, context: Any) -> Any:
        pod = super().get_or_create_pod(pod_request_obj, context)
        return commit_operator_launch_pin(
            pod=pod,
            operator=self,
            context=context,
        )

    def defer(self, *args: Any, **kwargs: Any) -> Any:
        # Providers 10.1 / 10.14 / 10.20 call defer(method_name="trigger_reentry")
        # without kwargs; Airflow merges our kwargs with event on resume.
        if "kwargs" in kwargs or not args:
            defer_kwargs = kwargs.get("kwargs")
            kwargs["kwargs"] = inject_launch_pin_ref_into_defer_kwargs(self, kwargs=defer_kwargs)
            return super().defer(*args, **kwargs)
        return super().defer(*args, **kwargs)

    def execute(self, context: Any) -> Any:
        # Beat stale Airflow-serialized get_logs=False from pre-fix DAG versions.
        ensure_live_base_container_logs(self)
        result = evaluate_operator_inline_outcome(self, super().execute(context))
        return embed_launch_pin_ref_in_summary(self, result, context=context)

    def await_pod_completion(self, pod: Any) -> Any:
        """Follow live logs, degrading only transient log API failures to polling."""

        parent_await = super().await_pod_completion
        return await_pod_completion_with_log_stream_fallback(
            self,
            pod=pod,
            await_with_live_logs=lambda: parent_await(pod),
            api_exception_types=_KUBERNETES_API_EXCEPTION_TYPES,
        )

    def trigger_reentry(self, context: Any, event: Any, **kwargs: Any) -> Any:
        ensure_live_base_container_logs(self)
        rehydrate_committed_launch_pin_ref(self, context=context, defer_kwargs=kwargs)
        result = super().trigger_reentry(context, event)
        if result is None and getattr(self, "do_xcom_push", False):
            result = pull_deferrable_xcom(context=context, task_id=str(self.task_id))
        result = evaluate_operator_inline_outcome(self, result)
        return embed_launch_pin_ref_in_summary(self, result, context=context)


class UnsafeAirflowConnectionEnvKubernetesPodOperator(PinnedXComSidecarKubernetesPodOperator):
    """Inject Airflow Connection URIs into pod env at execution time.

    This non-production adapter is useful while a platform moves from Airflow
    Connection projection to Secret/Vault projection. It never reads connection
    values during DAG parse.
    """

    def __init__(
        self,
        *,
        unsafe_airflow_connection_ids: Sequence[str],
        unsafe_connection_reader: ConnectionUriReader | None = None,
        unsafe_runtime_database_overrides: Mapping[str, str] | None = None,
        unsafe_runtime_query_overrides: Mapping[str, Mapping[str, str]] | None = None,
        unsafe_runtime_scheme_overrides: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.unsafe_airflow_connection_ids = tuple(unsafe_airflow_connection_ids)
        self.unsafe_runtime_database_overrides = dict(unsafe_runtime_database_overrides or {})
        self.unsafe_runtime_query_overrides = {
            connection_id: dict(values) for connection_id, values in dict(unsafe_runtime_query_overrides or {}).items()
        }
        self.unsafe_runtime_scheme_overrides = dict(unsafe_runtime_scheme_overrides or {})
        self._unsafe_connection_reader = unsafe_connection_reader

    def execute(self, context: Any) -> Any:
        builder = UnsafeAirflowConnectionEnvBuilder(reader=self._reader())
        direct_env = apply_query_overrides(
            apply_database_overrides(
                builder.build(self.unsafe_airflow_connection_ids),
                self.unsafe_runtime_database_overrides,
            ),
            self.unsafe_runtime_query_overrides,
        )
        self.env_vars = stringify_env_vars(
            merge_env_vars(getattr(self, "env_vars", None), direct_env),
        )
        materialize_runtime_connection_env(self)
        return super().execute(context)

    def _reader(self) -> ConnectionUriReader:
        reader = self._unsafe_connection_reader or AirflowBaseHookConnectionReader()
        if not self.unsafe_runtime_scheme_overrides:
            return reader
        return RuntimeAirflowConnectionUriReader(reader=reader, scheme_overrides=self.unsafe_runtime_scheme_overrides)


class AirflowConnectionSecretVolumeKubernetesPodOperator(PinnedXComSidecarKubernetesPodOperator):
    """Project Airflow Connection URIs into a Secret volume at task execution time.

    This compatibility bridge keeps Airflow as the operator-side resolver while
    keeping the runtime image Airflow-free. It never reads Airflow Connections
    during DAG parse and never places connection URIs in pod env vars or KPO
    arguments.
    """

    def __init__(
        self,
        *,
        airflow_connection_projection: Mapping[str, Any],
        airflow_connection_projection_reader: ConnectionUriReader | None = None,
        airflow_connection_secret_projector: AirflowConnectionSecretProjector | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.airflow_connection_projection = dict(airflow_connection_projection)
        self.airflow_connection_projected_secret: dict[str, str] | None = None
        self.airflow_connection_projected_secret_ref: dict[str, str] | None = None
        self._airflow_connection_projection_reader = airflow_connection_projection_reader
        self._airflow_connection_secret_projector = airflow_connection_secret_projector

    def execute(self, context: Any) -> Any:
        cleanup_policy = self._validate_secret_lifecycle_policy()
        attempt = self._attempt(context)
        attempt_projection = bind_attempt_secret_name(self.airflow_connection_projection, attempt)
        lifecycle = AirflowConnectionSecretLifecycle(
            secret_ref=attempt.secret_ref,
            attempt_ref=attempt.attempt_ref,
            cleanup_policy=cleanup_policy,
        )
        namespace = _operator_namespace(self)
        secret = build_airflow_connection_projected_secret(
            attempt_projection,
            reader=self._reader(),
            lifecycle=lifecycle,
        )
        projector = self._projector()
        projector.upsert(namespace=namespace, secret=secret)
        safe_ref = {
            "secret_ref": attempt.secret_ref,
            "attempt_ref": attempt.attempt_ref,
            "cleanup_policy": cleanup_policy,
        }
        self.airflow_connection_projected_secret_ref = safe_ref
        self.airflow_connection_projected_secret = dict(safe_ref)
        try:
            materialize_airflow_connection_secret_volume(self, attempt_projection, lifecycle=lifecycle)
            return super().execute(context)
        finally:
            if cleanup_policy == "after_execute":
                projector.delete(namespace=namespace, name=attempt.secret_name)

    def _attempt(self, context: object) -> AirflowConnectionSecretAttempt:
        identity = require_airflow_task_attempt_identity(context)
        return derive_airflow_connection_secret_attempt(
            _projection_secret_name(self.airflow_connection_projection),
            identity,
        )

    def _reader(self) -> ConnectionUriReader:
        projection = self.airflow_connection_projection
        base = self._airflow_connection_projection_reader or AirflowBaseHookConnectionReader()
        return ProjectedAirflowConnectionUriReader(
            reader=base,
            scheme_overrides=_string_mapping(projection.get("scheme_overrides")),
            database_overrides=_string_mapping(projection.get("database_overrides")),
            query_overrides=_nested_string_mapping(projection.get("query_overrides")),
        )

    def _projector(self) -> AirflowConnectionSecretProjector:
        return self._airflow_connection_secret_projector or KubernetesApiAirflowConnectionSecretProjector()

    def _validate_secret_lifecycle_policy(self) -> str:
        cleanup_policy = _projection_cleanup_policy(self.airflow_connection_projection)
        if _operator_deferrable(self) and cleanup_policy == "after_execute":
            raise ValueError(
                "airflow_connection Secret projection cannot use cleanup_policy: after_execute with deferrable "
                "KubernetesPodOperator; set cleanup_policy: retain and use platform-managed cleanup."
            )
        return cleanup_policy


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _nested_string_mapping(value: object) -> dict[str, dict[str, str]]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): _string_mapping(item) for key, item in value.items()}


__all__ = [
    "AirflowConnectionSecretConflictError",
    "AirflowConnectionSecretDependencyError",
    "AirflowConnectionSecretProjectionError",
    "AirflowConnectionSecretVolumeKubernetesPodOperator",
    "AirflowConnectionSecretProjector",
    "AirflowTaskAttemptIdentityError",
    "KubernetesApiAirflowConnectionSecretProjector",
    "PinnedXComSidecarKubernetesPodOperator",
    "ensure_live_base_container_logs",
    "UNSAFE_AIRFLOW_CONNECTION_ENV_ANNOTATION",
    "UNSAFE_AIRFLOW_CONNECTION_ENV_VALUE",
    "UnsafeAirflowConnectionEnvKubernetesPodOperator",
    "build_airflow_connection_projected_secret",
    "materialize_airflow_connection_secret_volume",
    "materialize_runtime_connection_env",
    "merge_env_vars",
    "patch_pod_spec_secret_volume",
    "patch_pod_spec_env_vars",
    "stringify_env_vars",
]
