"""Build-plane readmission of native and composed workspace releases."""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dpone.contracts.development_delivery_authority import DevelopmentAuthorityReceipt
from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.release_set_validation import validate_release_set
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.airflow_compact_pack_release_models import CompactPackReleaseError, CompactPackReleaseReport
from dpone.readiness.airflow_local_release import (
    ImmutableLocalReleaseDurabilityError,
    materialize_immutable_local_release,
)


def read_workspace_release_descriptor(root: Path) -> dict[str, Any]:
    """Bound descriptor acquisition before parsing or selecting a source verifier."""
    try:
        return strict_json_object(read_confined_file(root, "release-set.json", max_bytes=8 * 1024 * 1024))
    except (ValueError, OSError, TypeError, RecursionError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_WORKSPACE_INVALID",
            "workspace descriptor is invalid or exceeds its metadata bound",
        ) from exc


def materialize_composed_release(
    root: Path,
    cache: Path,
    *,
    xcom_sidecar_image: str,
    dag_ids=None,
    development_authority: DevelopmentAuthorityReceipt | None = None,
) -> CompactPackReleaseReport:
    from dpone.app.release_composition import build_release_composition_service

    try:
        release = read_workspace_release_descriptor(root)
        if validate_release_set(release).failure is not None:
            raise ValueError("composition metadata is invalid")
        artifacts = release["artifacts"]
        expected = {row["id"] for row in artifacts["dag_specs"]}
        if dag_ids is not None and set(dag_ids) != expected:
            raise ValueError("composition installation requires all constituent DAGs")
        for descriptor in artifacts["workload_packs"]:
            pack = strict_json_object(read_confined_file(root, descriptor["path"], max_bytes=descriptor["bytes"]))
            if pack.get("xcom", {}).get("sidecar_image") != xcom_sidecar_image:
                raise ValueError("composition installation cannot rewrite pinned sidecar transport")
        report = build_release_composition_service(development_authority=development_authority).install(
            root, cache_root=cache
        )
    except (ValueError, OSError, RuntimeError, TypeError, KeyError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPOSITION_INVALID",
            "composition source admission failed; regenerate from complete pinned constituents",
        ) from exc
    if not report.passed:
        raise CompactPackReleaseError(
            "DPONE_COMPOSITION_DURABILITY_UNCERTAIN",
            f"release {report.release_id} is visible but durability is uncertain; retry identical inputs",
        )
    return CompactPackReleaseReport(
        release_id=report.release_id or "",
        release_dir=str(report.output_dir),
        dag_ids=tuple(sorted(expected)),
        workload_ids=tuple(row["id"] for row in artifacts["workload_packs"]),
        pack_fingerprints={row["id"]: row["pack_fingerprint"] for row in artifacts["workload_packs"]},
        connection_projection_mode="runtime_connection_context",
        xcom_sidecar_image=xcom_sidecar_image,
    )


def materialize_native_workspace_release(
    root: Path,
    cache: Path,
    *,
    xcom_sidecar_image: str,
    dag_ids: Sequence[str] | None,
    development_authority: DevelopmentAuthorityReceipt | None,
) -> CompactPackReleaseReport:
    """Materialize one authenticated native workspace into the immutable cache."""
    from dpone.app.dbt_promotion_composition import build_dbt_compact_workspace_release_builder
    from dpone.readiness.airflow_release_schema_validation import validate_release_set_schema

    descriptor = read_workspace_release_descriptor(root)
    development = descriptor.get("schema") == "dpone.dbt-release-set.development.v1"
    if development and (
        development_authority is None
        or development_authority.release_projection() != descriptor.get("development_authority")
    ):
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_WORKSPACE_INVALID",
            "development materialization requires externally verified authority",
        )
    try:
        files = build_dbt_compact_workspace_release_builder(development=development).build(
            root, xcom_sidecar_image=xcom_sidecar_image, dag_ids=dag_ids
        )
        release = json.loads(files["release-set.json"])
        validate_release_set_schema(release, path=root / "release-set.json")
    except (ValueError, OSError, TypeError, KeyError, RecursionError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_WORKSPACE_INVALID",
            "native workspace release is invalid, incomplete or incompatible; regenerate the complete workspace with a compatible producer",
        ) from exc
    release_dir = cache / "releases" / release["release_id"].replace(":", "-", 1)
    try:
        materialize_immutable_local_release(release_dir, files)
    except ImmutableLocalReleaseDurabilityError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_DURABILITY_UNCERTAIN",
            "complete release is visible but durable publication is unproven; retry identical inputs after storage recovery",
        ) from exc
    except OSError as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_WRITE_FAILED",
            "release publication failed; inspect storage and retry identical inputs",
        ) from exc
    return CompactPackReleaseReport(
        release_id=release["release_id"],
        release_dir=release_dir.as_posix(),
        dag_ids=tuple(item["id"] for item in release["artifacts"]["dag_specs"]),
        workload_ids=tuple(item["id"] for item in release["artifacts"]["workload_packs"]),
        pack_fingerprints={item["id"]: item["pack_fingerprint"] for item in release["artifacts"]["workload_packs"]},
        connection_projection_mode="runtime_connection_context",
        xcom_sidecar_image=xcom_sidecar_image.strip(),
    )


__all__ = ["materialize_composed_release", "materialize_native_workspace_release", "read_workspace_release_descriptor"]
