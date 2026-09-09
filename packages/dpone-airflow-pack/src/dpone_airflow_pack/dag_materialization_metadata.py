"""Attach trusted dpone metadata to a materialized Airflow DAG."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint
from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext
from dpone_airflow_pack.run_identity import (
    AIRFLOW_ACTIVATION_CONTEXT_KEY,
    AIRFLOW_ACTIVATION_TAG_PREFIX,
    AIRFLOW_DEPLOYMENT_TAG_PREFIX,
    AIRFLOW_RELEASE_TAG_PREFIX,
)

_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_SHA256_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TRUSTED_IDENTITY_TAG_PREFIXES = (
    AIRFLOW_ACTIVATION_TAG_PREFIX,
    AIRFLOW_RELEASE_TAG_PREFIX,
    AIRFLOW_DEPLOYMENT_TAG_PREFIX,
)


def attach_run_identity_context(
    dag: Any,
    context: Mapping[str, Any] | None,
) -> None:
    if context is not None:
        setattr(dag, "_dpone_run_identity_context", dict(context))


def dag_tags(
    spec: Mapping[str, Any],
    run_identity_context: Mapping[str, Any] | None,
) -> list[str]:
    """Merge user tags with trusted exact release/deployment/activation markers."""

    tags = [str(tag) for tag in spec.get("tags") or () if not str(tag).startswith(_TRUSTED_IDENTITY_TAG_PREFIXES)]
    if run_identity_context is not None:
        release_tag = _digest_tag(
            AIRFLOW_RELEASE_TAG_PREFIX,
            run_identity_context.get("release_id"),
        )
        if release_tag is not None:
            tags.append(release_tag)
        deployment_tag = _digest_tag(
            AIRFLOW_DEPLOYMENT_TAG_PREFIX,
            run_identity_context.get("deployment_id"),
        )
        if deployment_tag is not None:
            tags.append(deployment_tag)
        activation_id = run_identity_context.get(AIRFLOW_ACTIVATION_CONTEXT_KEY)
        if isinstance(activation_id, str) and _UUID_V4.fullmatch(activation_id):
            tags.append(AIRFLOW_ACTIVATION_TAG_PREFIX + activation_id)
    return list(dict.fromkeys(tags))


def attach_delivery_context(
    dag: Any,
    context: InitFetchDeliveryContext | None,
) -> None:
    if context is not None:
        setattr(dag, "_dpone_init_fetch_context", context)


def spec_fingerprint(spec: Mapping[str, Any]) -> str:
    value = spec.get("spec_fingerprint")
    if isinstance(value, str) and value.startswith("sha256:"):
        return value
    return compute_dag_spec_fingerprint(spec)


def existing_spec_fingerprint(dag: Any) -> str | None:
    value = getattr(dag, "_dpone_spec_fingerprint", None)
    return value if isinstance(value, str) and value.startswith("sha256:") else None


def attach_spec_fingerprint(dag: Any, spec: Mapping[str, Any]) -> None:
    try:
        setattr(dag, "_dpone_spec_fingerprint", spec_fingerprint(spec))
    except Exception:  # noqa: BLE001 - optional metadata must not replace load error.
        return


def attach_partition_metadata(dag: Any, spec: Mapping[str, Any]) -> None:
    plan = spec.get("partition_plan")
    if not isinstance(plan, Mapping):
        return
    from dpone_airflow_pack.asset_partitions import partition_materialization_mode

    setattr(dag, "_dpone_partition_plan", dict(plan))
    setattr(dag, "_dpone_partition_mode", partition_materialization_mode(plan))


def _digest_tag(prefix: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None
    digest = value.removeprefix("sha256:")
    if not _SHA256_DIGEST.fullmatch(digest):
        return None
    return prefix + digest


__all__ = [
    "attach_delivery_context",
    "attach_partition_metadata",
    "attach_run_identity_context",
    "attach_spec_fingerprint",
    "dag_tags",
    "existing_spec_fingerprint",
    "spec_fingerprint",
]
