"""Helper routines for compact pack release materialization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.airflow_compact_pack_release_models import CompactPackReleaseError

_AWS_ENV_TEMPLATES = {
    "AWS_ACCESS_KEY_ID": "{{ conn.s3_dpone_artifacts_reader.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.s3_dpone_artifacts_reader.password }}",
    "AWS_ENDPOINT_URL": "{{ conn.s3_dpone_artifacts_reader.extra_dejson.endpoint_url }}",
    "AWS_DEFAULT_REGION": "{{ conn.s3_dpone_artifacts_reader.extra_dejson.region_name }}",
}


def closed_connection_projection(projection: object) -> dict[str, Any]:
    """Normalize to the authoritative closed init-fetch bridge projection."""

    from dpone_airflow_pack.init_fetch_connection_bridge import (
        require_closed_init_fetch_connection_bridge,
    )
    from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

    try:
        return dict(require_closed_init_fetch_connection_bridge(projection))
    except InitFetchProviderError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_PROJECTION_INVALID",
            str(exc),
        ) from exc


def rewrite_strict_init_fetch_pack(pack: Mapping[str, Any], *, xcom_sidecar_image: str) -> dict[str, Any]:
    """Rewrite one compact reconcile pack into the closed strict init_fetch shape."""

    from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
    from dpone_airflow_pack.pack_identity import compute_pack_fingerprint
    from dpone_airflow_pack.xcom_sidecar import require_strict_xcom_sidecar_image

    if not str(xcom_sidecar_image or "").strip():
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_XCOM_SIDECAR_REQUIRED",
            "digest-pinned xcom sidecar image is required",
        )
    rewritten = dict(pack)
    airflow = dict(rewritten.get("airflow") or {})
    execution = dict(airflow.get("execution") or {})
    closed_execution = {key: execution[key] for key in ("inlets", "outlets") if key in execution}
    airflow["execution"] = closed_execution
    rewritten["airflow"] = airflow
    projection = dict(rewritten.get("connection_projection") or {})
    projection.setdefault("payload_format", "airflow_connection_uri")
    projection.setdefault("secret_values", False)
    rewritten["connection_projection"] = closed_connection_projection(projection)
    rewritten["xcom"] = {"sidecar_image": str(xcom_sidecar_image).strip()}
    try:
        rewritten["xcom"] = {"sidecar_image": require_strict_xcom_sidecar_image(rewritten)}
    except InitFetchProviderError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_XCOM_SIDECAR_INVALID",
            str(exc),
        ) from exc
    provider_execution = dict(rewritten.get("provider_execution") or {})
    kpo_kwargs = dict(provider_execution.get("kpo_kwargs") or {})
    env_vars = dict(kpo_kwargs.get("env_vars") or {})
    env_vars.update(_AWS_ENV_TEMPLATES)
    kpo_kwargs["env_vars"] = env_vars
    provider_execution["kpo_kwargs"] = kpo_kwargs
    rewritten["provider_execution"] = provider_execution
    rewritten.pop("pack_fingerprint", None)
    rewritten["pack_fingerprint"] = compute_pack_fingerprint(rewritten)
    return rewritten


def rewrite_strict_init_fetch_dag_spec(
    spec: Mapping[str, Any],
    *,
    workload_ids: Sequence[str],
) -> dict[str, Any]:
    """Keep certified scheduler overrides and refresh the strict fingerprint."""

    from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint

    rewritten = dict(spec)
    overrides = _certified_strict_dag_operator_overrides(rewritten.get("operator_overrides"))
    if overrides:
        rewritten["operator_overrides"] = overrides
    else:
        rewritten.pop("operator_overrides", None)
    nodes = rewritten.get("nodes")
    if isinstance(nodes, list):
        selected = {str(item) for item in workload_ids}
        rewritten["nodes"] = [dict(node) if isinstance(node, dict) else node for node in nodes]
        for node in rewritten["nodes"]:
            if not isinstance(node, dict):
                continue
            workload_id = str(node.get("workload_id") or "")
            if workload_id in selected:
                node["pack_ref"] = f"cached://workloads/{workload_id}"
    rewritten.pop("spec_fingerprint", None)
    rewritten["spec_fingerprint"] = compute_dag_spec_fingerprint(rewritten)
    return rewritten


def _certified_strict_dag_operator_overrides(value: object) -> dict[str, Any]:
    """Project author overrides onto the provider's closed scheduler surface."""

    from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
    from dpone_airflow_pack.init_fetch_pod_guard import (
        STRICT_OPERATOR_OVERRIDE_FIELDS,
        validate_strict_operator_overrides,
    )

    raw = value if isinstance(value, Mapping) else {}
    selected = {str(key): item for key, item in raw.items() if str(key) in STRICT_OPERATOR_OVERRIDE_FIELDS}
    try:
        return validate_strict_operator_overrides(selected)
    except InitFetchProviderError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_OPERATOR_OVERRIDES_INVALID",
            str(exc),
        ) from exc


__all__ = [
    "closed_connection_projection",
    "rewrite_strict_init_fetch_dag_spec",
    "rewrite_strict_init_fetch_pack",
]


def require_legacy_pack_authority(pack: Mapping[str, Any]) -> None:
    """Keep native workspace authority out of descriptor-less compact releases."""
    from dpone.contracts.legacy_release_dbt_authority import require_legacy_dbt_authority

    try:
        require_legacy_dbt_authority(pack)
    except ValueError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_NATIVE_AUTHORITY_REQUIRED",
            "native dbt inputs require a complete workspace descriptor; rebuild with workspace compile",
        ) from exc
