"""Build-plane readmission of an existing composition into the immutable cache."""

from pathlib import Path

from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.release_set_validation import validate_release_set
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.airflow_compact_pack_release_models import CompactPackReleaseError, CompactPackReleaseReport


def materialize_composed_release(
    root: Path, cache: Path, *, xcom_sidecar_image: str, dag_ids=None
) -> CompactPackReleaseReport:
    from dpone.app.release_composition import build_release_composition_service

    try:
        release = strict_json_object(read_confined_file(root, "release-set.json", max_bytes=8 * 1024 * 1024))
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
        report = build_release_composition_service().install(root, cache_root=cache)
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
