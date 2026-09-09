"""A-priori locator for the production launch-pin authority.

The gate must not learn ``kubernetes_conn_id`` only from the pointer payload
(chicken/egg: reading the pointer already needs a client). Locator sources,
in order:

1. Explicit ``launch_pin_store`` pack / op_kwargs contract
2. Process env ``DPONE_LAUNCH_PIN_STORE_BACKEND`` /
   ``DPONE_LAUNCH_PIN_STORE_KUBERNETES_CONN_ID`` /
   ``DPONE_LAUNCH_PIN_STORE_NAMESPACE``
3. In-cluster service-account namespace (conn_id may stay null for in-cluster)

``required=false`` resolution must never open Kubernetes — callers short-circuit
before constructing a remote store.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_STORE_CONN_ENV = "DPONE_LAUNCH_PIN_STORE_KUBERNETES_CONN_ID"
_STORE_NAMESPACE_ENV = "DPONE_LAUNCH_PIN_STORE_NAMESPACE"
_STORE_BACKEND_ENV = "DPONE_LAUNCH_PIN_STORE_BACKEND"
_SA_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

KUBERNETES_CONFIGMAP_BACKEND = "kubernetes_configmap"
AIRFLOW_TASK_STATE_BACKEND = "airflow_task_state"
LAUNCH_PIN_STORE_BACKENDS = frozenset({KUBERNETES_CONFIGMAP_BACKEND, AIRFLOW_TASK_STATE_BACKEND})


@dataclass(frozen=True)
class LaunchPinStoreLocator:
    """Protected backend and coordinates for launch-pin authority I/O."""

    kubernetes_conn_id: str | None
    namespace: str | None
    backend: str = KUBERNETES_CONFIGMAP_BACKEND

    @property
    def allow_incluster(self) -> bool:
        return in_cluster_service_account_present()


def _validated_store_conn(value: object, *, field: str) -> str | None:
    """Return a closed conn id or raise ``PIN_INVALID`` on malformed values."""

    from dpone_airflow_pack.launch_pin_codes import PIN_INVALID

    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{PIN_INVALID}: launch_pin_store.{field} must be null or a non-empty string")
    stripped = value.strip()
    if not stripped:
        raise RuntimeError(f"{PIN_INVALID}: launch_pin_store.{field} must be null or a non-empty string")
    return stripped


def _validated_store_backend(value: object) -> str:
    """Return one explicit supported backend or raise ``PIN_INVALID``."""

    from dpone_airflow_pack.launch_pin_codes import PIN_INVALID

    if value is None:
        return KUBERNETES_CONFIGMAP_BACKEND
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(
            f"{PIN_INVALID}: launch_pin_store.backend must be one of {sorted(LAUNCH_PIN_STORE_BACKENDS)!r}"
        )
    backend = value.strip()
    if backend not in LAUNCH_PIN_STORE_BACKENDS:
        raise RuntimeError(
            f"{PIN_INVALID}: launch_pin_store.backend {backend!r} must be one of {sorted(LAUNCH_PIN_STORE_BACKENDS)!r}"
        )
    return backend


def launch_pin_store_locator_from_mapping(value: object) -> LaunchPinStoreLocator | None:
    """Parse a ``launch_pin_store`` mapping from pack / op_kwargs."""

    if not isinstance(value, Mapping):
        return None
    if "backend" in value and value.get("backend") is None:
        from dpone_airflow_pack.launch_pin_codes import PIN_INVALID

        raise RuntimeError(
            f"{PIN_INVALID}: launch_pin_store.backend must be one of {sorted(LAUNCH_PIN_STORE_BACKENDS)!r}"
        )
    backend = _validated_store_backend(value.get("backend"))
    conn_id = _validated_store_conn(value.get("kubernetes_conn_id"), field="kubernetes_conn_id")
    ns = _validated_store_conn(value.get("namespace"), field="namespace")
    if conn_id is None and ns is None and "backend" not in value:
        return None
    return LaunchPinStoreLocator(
        kubernetes_conn_id=conn_id,
        namespace=ns,
        backend=backend,
    )


def _kpo_conn_from_pack(pack: Mapping[str, Any]) -> str | None:
    kpo = pack.get("kpo_kwargs")
    if not isinstance(kpo, Mapping):
        return None
    conn = kpo.get("kubernetes_conn_id")
    return str(conn).strip() if isinstance(conn, str) and conn.strip() else None


def launch_pin_store_locator_from_pack(pack: Mapping[str, Any]) -> LaunchPinStoreLocator | None:
    """Resolve locator from pack contract (``launch_pin_store`` or KPO conn)."""

    kpo_conn = _kpo_conn_from_pack(pack)
    direct = launch_pin_store_locator_from_mapping(pack.get("launch_pin_store"))
    if direct is not None:
        return LaunchPinStoreLocator(
            kubernetes_conn_id=direct.kubernetes_conn_id or kpo_conn,
            namespace=direct.namespace,
            backend=direct.backend,
        )
    outcome = pack.get("outcome_gate")
    if isinstance(outcome, Mapping):
        nested = launch_pin_store_locator_from_mapping(outcome.get("launch_pin_store"))
        if nested is not None:
            return LaunchPinStoreLocator(
                kubernetes_conn_id=nested.kubernetes_conn_id or kpo_conn,
                namespace=nested.namespace,
                backend=nested.backend,
            )
    if kpo_conn is not None:
        return LaunchPinStoreLocator(kubernetes_conn_id=kpo_conn, namespace=None)
    return None


def resolve_launch_pin_store_locator(
    *,
    explicit: LaunchPinStoreLocator | Mapping[str, Any] | None = None,
) -> LaunchPinStoreLocator:
    """Merge explicit contract with process env defaults."""

    explicit_backend = isinstance(explicit, LaunchPinStoreLocator) or (
        isinstance(explicit, Mapping) and "backend" in explicit
    )
    if isinstance(explicit, LaunchPinStoreLocator):
        base = explicit
    else:
        base = launch_pin_store_locator_from_mapping(explicit) or LaunchPinStoreLocator(
            kubernetes_conn_id=None,
            namespace=None,
        )
    env_conn = str(os.environ.get(_STORE_CONN_ENV) or "").strip() or None
    env_ns = str(os.environ.get(_STORE_NAMESPACE_ENV) or "").strip() or None
    raw_env_backend = str(os.environ.get(_STORE_BACKEND_ENV) or "").strip() or None
    env_backend = _validated_store_backend(raw_env_backend)
    return LaunchPinStoreLocator(
        kubernetes_conn_id=base.kubernetes_conn_id or env_conn,
        namespace=base.namespace or env_ns,
        backend=base.backend if explicit_backend else env_backend,
    )


def materialize_launch_pin_store_locator(pack: Mapping[str, Any]) -> dict[str, str | None]:
    """Freeze pack+env locator once at pack build for runtime/barrier/gate/cleanup."""

    direct = pack.get("launch_pin_store")
    outcome = pack.get("outcome_gate")
    nested = outcome.get("launch_pin_store") if isinstance(outcome, Mapping) else None
    explicit = direct if isinstance(direct, Mapping) else nested
    locator = resolve_launch_pin_store_locator(explicit=explicit if isinstance(explicit, Mapping) else None)
    kpo_conn = _kpo_conn_from_pack(pack)
    if locator.kubernetes_conn_id is None and kpo_conn is not None:
        locator = LaunchPinStoreLocator(
            kubernetes_conn_id=kpo_conn,
            namespace=locator.namespace,
            backend=locator.backend,
        )
    return launch_pin_store_locator_to_mapping(locator)


def launch_pin_store_locator_to_mapping(locator: LaunchPinStoreLocator) -> dict[str, str | None]:
    """Serialize a locator to the immutable pack-time mapping contract."""

    return {
        "backend": locator.backend,
        "kubernetes_conn_id": locator.kubernetes_conn_id,
        "namespace": locator.namespace,
        "authority_digest": store_authority_digest(
            backend=locator.backend,
            kubernetes_conn_id=locator.kubernetes_conn_id,
            namespace=locator.namespace,
        ),
    }


def close_launch_pin_store_locator(
    *,
    launch_pin_store: Mapping[str, Any] | LaunchPinStoreLocator | None,
    kpo_kubernetes_conn_id: str | None = None,
    kpo_namespace: str | None = None,
) -> dict[str, str | None]:
    """Close store/KPO locator once at task-group construction.

    The returned mapping is immutable input for runtime, CAS barrier,
    ``outcome_gate``, and cleanup — never re-read from raw pack or env per worker.
    """

    locator = frozen_launch_pin_store_locator(launch_pin_store)
    kpo_ns = str(kpo_namespace).strip() if isinstance(kpo_namespace, str) and kpo_namespace.strip() else None
    if locator.namespace is None and kpo_ns is not None:
        locator = LaunchPinStoreLocator(
            kubernetes_conn_id=locator.kubernetes_conn_id,
            namespace=kpo_ns,
            backend=locator.backend,
        )
    closed = require_same_cluster_launch_pin_store(
        kpo_kubernetes_conn_id=kpo_kubernetes_conn_id,
        launch_pin_store=locator,
    )
    return launch_pin_store_locator_to_mapping(closed)


def frozen_launch_pin_store_locator(value: object) -> LaunchPinStoreLocator:
    """Use a pack-materialized locator without independent env re-resolution."""

    if isinstance(value, LaunchPinStoreLocator):
        return value
    parsed = launch_pin_store_locator_from_mapping(value)
    if parsed is not None:
        return parsed
    return LaunchPinStoreLocator(
        kubernetes_conn_id=None,
        namespace=None,
        backend=KUBERNETES_CONFIGMAP_BACKEND,
    )


def in_cluster_service_account_present() -> bool:
    """Return True when the process has an in-cluster service-account mount."""

    try:
        return os.path.isfile(_SA_TOKEN_PATH) and os.path.isfile(_SA_NAMESPACE_PATH)
    except OSError:
        return False


def remote_store_allowed(locator: LaunchPinStoreLocator) -> bool:
    """Whether production ConfigMap I/O may be attempted with this locator."""

    has_client_authority = bool(locator.kubernetes_conn_id or locator.allow_incluster)
    return locator.backend == KUBERNETES_CONFIGMAP_BACKEND and has_client_authority


def require_same_cluster_launch_pin_store(
    *,
    kpo_kubernetes_conn_id: str | None,
    launch_pin_store: Mapping[str, Any] | LaunchPinStoreLocator | None,
) -> LaunchPinStoreLocator:
    """Return a closed store/KPO locator; never fail-open on partial None conn.

    Rules:
    - store inherits KPO ``kubernetes_conn_id`` when store conn is absent
    - store conn + KPO in-cluster (``None``) is a mismatch (rejected)
    - both conn ids set must be equal
    - in-cluster (both conn absent) requires a concrete namespace
    - hermetic injected stores skip SA presence checks
    """

    from dpone_airflow_pack.launch_pin_codes import PIN_INVALID
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    locator = frozen_launch_pin_store_locator(launch_pin_store)
    store_conn = locator.kubernetes_conn_id
    store_ns = locator.namespace
    kpo_conn = (
        str(kpo_kubernetes_conn_id).strip()
        if isinstance(kpo_kubernetes_conn_id, str) and kpo_kubernetes_conn_id.strip()
        else None
    )
    if store_conn is None and kpo_conn is not None:
        store_conn = kpo_conn
    if store_conn is not None and kpo_conn is None:
        raise RuntimeError(
            f"{PIN_INVALID}: launch_pin_store.kubernetes_conn_id {store_conn!r} is incompatible "
            "with KPO in-cluster (kubernetes_conn_id=None); KPO and pin must share authority"
        )
    if store_conn is not None and kpo_conn is not None and store_conn != kpo_conn:
        raise RuntimeError(
            f"{PIN_INVALID}: launch_pin_store.kubernetes_conn_id {store_conn!r} must equal "
            f"KPO kubernetes_conn_id {kpo_conn!r} (in-cluster barrier only; "
            "remote control-cluster store blocked)"
        )
    if kpo_conn is not None and store_ns is None:
        raise RuntimeError(
            f"{PIN_INVALID}: remote KPO kubernetes_conn_id {kpo_conn!r} requires a concrete "
            "launch_pin_store.namespace (pack contract or KPO namespace); "
            "independent env/SA resolution per worker is blocked"
        )
    hermetic = get_launch_pin_store() is not None
    configmap_backend = locator.backend == KUBERNETES_CONFIGMAP_BACKEND
    if (
        configmap_backend
        and store_conn is None
        and kpo_conn is None
        and not hermetic
        and not in_cluster_service_account_present()
    ):
        raise RuntimeError(
            f"{PIN_INVALID}: launch pin store/KPO authority missing kubernetes_conn_id and process is not in-cluster"
        )
    if store_ns is None:
        env_ns = str(os.environ.get(_STORE_NAMESPACE_ENV) or "").strip() or None
        store_ns = env_ns or (_read_sa_namespace() if store_conn is None else None)
    if store_conn is None and not store_ns:
        if hermetic:
            store_ns = "default"
        else:
            raise RuntimeError(
                f"{PIN_INVALID}: in-cluster launch pin store requires a concrete namespace "
                f"(pack launch_pin_store.namespace / {_STORE_NAMESPACE_ENV} / SA namespace)"
            )
    return LaunchPinStoreLocator(
        kubernetes_conn_id=store_conn,
        namespace=store_ns,
        backend=locator.backend,
    )


def _read_sa_namespace() -> str | None:
    try:
        with open(_SA_NAMESPACE_PATH, encoding="utf-8") as handle:
            value = handle.read().strip()
    except OSError:
        return None
    return value or None


def store_authority_digest(
    *,
    kubernetes_conn_id: str | None,
    namespace: str | None,
    backend: str = KUBERNETES_CONFIGMAP_BACKEND,
) -> str:
    """Stable digest of frozen store coordinates for cleanup handles."""

    import hashlib

    normalized_backend = _validated_store_backend(backend)
    if normalized_backend == KUBERNETES_CONFIGMAP_BACKEND:
        # Preserve all existing ConfigMap authority digests byte-for-byte.
        payload = f"{kubernetes_conn_id or ''}\0{namespace or ''}".encode()
    else:
        payload = (f"{normalized_backend}\0{kubernetes_conn_id or ''}\0{namespace or ''}").encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def attempt_pinned_store_locator_from_evidence(
    summary: Mapping[str, Any] | None,
) -> LaunchPinStoreLocator | None:
    """Extract attempt-pinned store coords from gate locator evidence (routing hint only)."""

    from dpone_airflow_pack.launch_pin_codes import PIN_INVALID

    if not isinstance(summary, Mapping):
        return None
    nested = summary.get("launch_pin_store")
    if isinstance(nested, Mapping):
        locator = launch_pin_store_locator_from_mapping(nested)
        if locator is None:
            return None
        expected_digest = nested.get("authority_digest")
        if expected_digest is not None and not isinstance(expected_digest, str):
            raise RuntimeError(f"{PIN_INVALID}: launch_pin_store.authority_digest must be a non-empty string")
        if isinstance(expected_digest, str) and expected_digest.strip():
            observed = store_authority_digest(
                backend=locator.backend,
                kubernetes_conn_id=locator.kubernetes_conn_id,
                namespace=locator.namespace,
            )
            if expected_digest.strip() != observed:
                raise RuntimeError(
                    f"{PIN_INVALID}: launch_pin_store.authority_digest does not match frozen store coordinates"
                )
        return locator
    conn_id = _validated_store_conn(summary.get("kubernetes_conn_id"), field="kubernetes_conn_id")
    store_ns = _validated_store_conn(summary.get("store_namespace"), field="store_namespace")
    if conn_id is None and store_ns is None:
        return None
    backend = _validated_store_backend(summary.get("store_backend"))
    return LaunchPinStoreLocator(
        kubernetes_conn_id=conn_id,
        namespace=store_ns,
        backend=backend,
    )


__all__ = [
    "AIRFLOW_TASK_STATE_BACKEND",
    "KUBERNETES_CONFIGMAP_BACKEND",
    "LAUNCH_PIN_STORE_BACKENDS",
    "LaunchPinStoreLocator",
    "attempt_pinned_store_locator_from_evidence",
    "close_launch_pin_store_locator",
    "frozen_launch_pin_store_locator",
    "in_cluster_service_account_present",
    "launch_pin_store_locator_from_mapping",
    "launch_pin_store_locator_from_pack",
    "launch_pin_store_locator_to_mapping",
    "materialize_launch_pin_store_locator",
    "remote_store_allowed",
    "require_same_cluster_launch_pin_store",
    "resolve_launch_pin_store_locator",
    "store_authority_digest",
]
