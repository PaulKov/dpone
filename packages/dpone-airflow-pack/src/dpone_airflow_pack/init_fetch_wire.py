"""Strict parser for ``dpone.airflow-deployment-index.v2`` / ``.v3`` init-fetch data."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.init_fetch_contract import (
    AIRFLOW_INDEX_SCHEMA_V2,
    AIRFLOW_INDEX_SCHEMA_V3,
    ExactArtifact,
    InitFetchDeliveryContext,
    InitFetchProviderError,
)
from dpone_airflow_pack.init_fetch_delivery import (
    parse_init_fetch_delivery,
)
from dpone_airflow_pack.init_fetch_inventory import (
    parse_runtime_payloads,
    parse_workload_packs,
)
from dpone_airflow_pack.init_fetch_validation import (
    ENVIRONMENT_RE,
    airflow_bundle_ref,
    cache_parts,
    cache_ref,
    digest,
    exact_mapping,
    field_invalid,
    positive_integer,
    require_cache_prefix,
    runtime_image,
)
from dpone_airflow_pack.mssql_outlet_projection_contract import (
    parse_mssql_asset_outlet_projection,
)

_INDEX_KEYS_V2 = frozenset(
    {
        "schema",
        "release_id",
        "deployment_id",
        "trust_tier",
        "dag_specs",
        "workload_packs",
        "binding_set_ref",
        "connection_registry_ref",
        "credential_runtime_ref",
        "binding_set",
        "connection_registry",
        "credential_runtime",
        "runtime_image_ref",
        "runtime_image_digest",
        "runtime_image_dbt_ref",
        "runtime_image_dbt_digest",
        "airflow_bundle_ref",
        "runtime_artifact_delivery",
        "release",
        "deployment",
        "runtime_payloads",
        "semantic_refresh_dag_projections",
        "dev_evidence_delivery",
    }
)
_INDEX_KEYS_V3 = _INDEX_KEYS_V2 | frozenset({"mssql_asset_outlet_projection"})
_OPTIONAL_INDEX_KEYS_V2 = frozenset(
    {
        "runtime_payloads",
        "dev_evidence_delivery",
        "runtime_image_dbt_ref",
        "runtime_image_dbt_digest",
        "semantic_refresh_dag_projections",
    }
)
_OPTIONAL_INDEX_KEYS_V3 = _OPTIONAL_INDEX_KEYS_V2
_CONTEXT_DIGEST_DIR_RE = re.compile(r"^sha256-[0-9a-f]{64}$")
_SUPPORTED_INDEX_SCHEMAS = frozenset({AIRFLOW_INDEX_SCHEMA_V2, AIRFLOW_INDEX_SCHEMA_V3})


def init_fetch_context_from_payload(
    payload: Mapping[str, Any],
    *,
    path: Path | None = None,
) -> InitFetchDeliveryContext:
    """Validate the strict v2/v3 wire projection without importing core dpone."""

    schema = payload.get("schema")
    if schema not in _SUPPORTED_INDEX_SCHEMAS:
        raise InitFetchProviderError(
            "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID",
            f"Expected schema {AIRFLOW_INDEX_SCHEMA_V2} or {AIRFLOW_INDEX_SCHEMA_V3}",
            path=_path_text(path),
        )
    is_v3 = schema == AIRFLOW_INDEX_SCHEMA_V3
    exact_mapping(
        payload,
        "airflow deployment index v3" if is_v3 else "airflow deployment index v2",
        _INDEX_KEYS_V3 if is_v3 else _INDEX_KEYS_V2,
        optional=_OPTIONAL_INDEX_KEYS_V3 if is_v3 else _OPTIONAL_INDEX_KEYS_V2,
        path=path,
    )
    delivery = parse_init_fetch_delivery(payload, path=path)
    release_id = digest(payload.get("release_id"), "release_id", path)
    deployment_id = digest(payload.get("deployment_id"), "deployment_id", path)
    for field in ("binding_set_ref", "connection_registry_ref", "credential_runtime_ref"):
        digest(payload.get(field), field, path)
    airflow_bundle_ref(payload.get("airflow_bundle_ref"), path)
    image_digest = digest(
        payload.get("runtime_image_digest"),
        "runtime_image_digest",
        path,
    )
    image_ref = runtime_image(payload.get("runtime_image_ref"), image_digest, path)
    dbt_digest_raw = payload.get("runtime_image_dbt_digest")
    dbt_ref_raw = payload.get("runtime_image_dbt_ref")
    if dbt_digest_raw is None and dbt_ref_raw is None:
        dbt_digest = None
        dbt_ref = None
    elif dbt_digest_raw is None or dbt_ref_raw is None:
        raise field_invalid(
            "runtime_image_dbt_ref and runtime_image_dbt_digest must both be set or both be absent",
            path,
        )
    else:
        dbt_digest = digest(dbt_digest_raw, "runtime_image_dbt_digest", path)
        dbt_ref = runtime_image(dbt_ref_raw, dbt_digest, path)
    release = _artifact(payload.get("release"), "release", path)
    deployment = _artifact(payload.get("deployment"), "deployment", path)
    binding_set, connection_registry, credential_runtime = _runtime_connection_artifacts(payload, path=path)
    environment = _artifact_environment(
        release=release,
        deployment=deployment,
        release_id=release_id,
        deployment_id=deployment_id,
        path=path,
    )
    runtime_payloads = parse_runtime_payloads(
        payload.get("runtime_payloads", []),
        release_id=release_id,
        path=path,
    )
    workloads = parse_workload_packs(
        payload.get("workload_packs"),
        release_id=release_id,
        runtime_payload_ids=frozenset(item.id for item in runtime_payloads),
        path=path,
    )
    mssql_projection = parse_mssql_asset_outlet_projection(
        payload.get("mssql_asset_outlet_projection"),
        path=path,
        expected_environment=environment,
        expected_binding_set_ref=str(payload.get("binding_set_ref") or ""),
        expected_connection_registry_ref=str(payload.get("connection_registry_ref") or ""),
        require_present=is_v3,
    )
    context = InitFetchDeliveryContext(
        environment=environment,
        trust_tier=delivery.trust_tier,
        release_id=release_id,
        deployment_id=deployment_id,
        runtime_image_ref=image_ref,
        runtime_image_digest=image_digest,
        artifact_registry_ref=delivery.artifact_registry_ref,
        registry_configuration=delivery.registry_configuration,
        trust_policy=delivery.trust_policy,
        identity=delivery.identity,
        release=release,
        deployment=deployment,
        binding_set=binding_set,
        connection_registry=connection_registry,
        credential_runtime=credential_runtime,
        workload_packs=workloads,
        runtime_payloads=runtime_payloads,
        verify=delivery.verify,
        dev_evidence_delivery=delivery.dev_evidence_delivery,
        runtime_image_dbt_ref=dbt_ref,
        runtime_image_dbt_digest=dbt_digest,
        mssql_asset_uri_by_ref=mssql_projection,
    )
    for workload in workloads:
        context.encode_plan(
            workload_id=workload.id,
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )
    return context


def _artifact(value: object, field: str, path: Path | None) -> ExactArtifact:
    item = exact_mapping(
        value,
        field,
        frozenset({"artifact_ref", "sha256", "bytes"}),
        path=path,
    )
    return ExactArtifact(
        artifact_ref=cache_ref(item["artifact_ref"], f"{field}.artifact_ref", path),
        sha256=digest(item["sha256"], f"{field}.sha256", path),
        bytes=positive_integer(item["bytes"], f"{field}.bytes", path),
    )


def _runtime_connection_artifacts(
    payload: Mapping[str, Any],
    *,
    path: Path | None,
) -> tuple[ExactArtifact, ExactArtifact, ExactArtifact]:
    fields = (
        ("binding_set", "binding-set.json"),
        ("connection_registry", "connection-registry.json"),
        ("credential_runtime", "credential-runtime.json"),
    )
    parsed = tuple(_artifact(payload.get(field), field, path) for field, _ in fields)
    if len(parsed) != 3:
        raise field_invalid(
            "runtime connection artifact shape must contain exactly three entries",
            path,
        )
    parents: set[tuple[str, ...]] = set()
    for artifact, (field, filename) in zip(parsed, fields, strict=True):
        parts = cache_parts(artifact.artifact_ref, f"{field}.artifact_ref", path)
        if (
            len(parts) != 3
            or parts[0] != "runtime-connection-contexts"
            or not _CONTEXT_DIGEST_DIR_RE.fullmatch(parts[1])
            or parts[2] != filename
        ):
            raise field_invalid(
                f"{field}.artifact_ref is not a strict runtime connection artifact",
                path,
            )
        parents.add(parts[:-1])
    if len(parents) != 1:
        raise field_invalid(
            "runtime connection artifacts must share one immutable context",
            path,
        )
    return parsed[0], parsed[1], parsed[2]


def _artifact_environment(
    *,
    release: ExactArtifact,
    deployment: ExactArtifact,
    release_id: str,
    deployment_id: str,
    path: Path | None,
) -> str:
    require_cache_prefix(
        release.artifact_ref,
        ("releases", release_id.replace(":", "-")),
        "release",
        path,
    )
    parts = cache_parts(deployment.artifact_ref, "deployment.artifact_ref", path)
    expected_dir = deployment_id.replace(":", "-")
    if len(parts) < 4 or parts[0] != "deployments" or parts[2] != expected_dir:
        raise field_invalid(
            "deployment artifact_ref is outside its pinned identity",
            path,
        )
    environment = parts[1]
    if not ENVIRONMENT_RE.fullmatch(environment):
        raise field_invalid("deployment environment is invalid", path)
    return environment


def _path_text(path: Path | None) -> str | None:
    return path.as_posix() if path is not None else None


__all__ = ["init_fetch_context_from_payload"]
