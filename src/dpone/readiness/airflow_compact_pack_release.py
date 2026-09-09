"""Promote compact reconcile packs into an immutable release-set.

Compact packs alone do not deliver verified ``RuntimeConnectionContext``.
Legacy reconcile roots use the closed Airflow Connection bridge and release v1.
Canonical workspace roots preserve complete dbt wire-v2 authority and native
RuntimeConnectionContext delivery. Both use immutable verified publication.

When any rewritten pack declares ``runtime_payload_ids`` (dbt self-service),
the matching files under ``pack_root/runtime/`` are required and copied into
the release as ``artifacts.runtime_payloads``. Missing payloads fail closed
before publish — never emit dangling workload refs into the Airflow index.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.gitops.schema_release_set_promotion import (
    COMPACT_PROMOTION_PROFILE,
    COMPACT_PROMOTION_SCHEMA,
)
from dpone.readiness.airflow_compact_pack_release_helpers import (
    closed_connection_projection,
    rewrite_strict_init_fetch_dag_spec,
    rewrite_strict_init_fetch_pack,
)
from dpone.readiness.airflow_compact_pack_release_models import (
    CompactPackReleaseError,
    CompactPackReleaseReport,
)
from dpone.readiness.airflow_compact_pack_runtime_payloads import (
    _materialize_runtime_payloads,
    _runtime_payload_ids_from_pack,
)
from dpone.readiness.airflow_deployment_projection import compute_release_id
from dpone.readiness.airflow_local_release import (
    ImmutableLocalReleaseDurabilityError,
    ImmutableLocalReleaseError,
    materialize_immutable_local_release,
)
from dpone.readiness.airflow_runtime_payload_refs import (
    RuntimePayloadRefError,
    require_runtime_payload_refs,
)


def materialize_compact_pack_release(
    *,
    pack_root: Path,
    cache_root: Path,
    xcom_sidecar_image: str,
    dag_ids: Sequence[str] | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> CompactPackReleaseReport:
    """Rewrite compact packs and write one immutable release-set."""

    try:
        return _materialize(
            pack_root=pack_root,
            cache_root=cache_root,
            xcom_sidecar_image=xcom_sidecar_image,
            dag_ids=dag_ids,
            provenance=provenance,
        )
    except CompactPackReleaseError as exc:
        return CompactPackReleaseReport(
            release_id="",
            release_dir="",
            dag_ids=(),
            workload_ids=(),
            pack_fingerprints={},
            connection_projection_mode="",
            xcom_sidecar_image=xcom_sidecar_image,
            blockers=(f"{exc.code}: {exc}",),
        )
    except ImmutableLocalReleaseError as exc:
        return CompactPackReleaseReport(
            release_id="",
            release_dir="",
            dag_ids=(),
            workload_ids=(),
            pack_fingerprints={},
            connection_projection_mode="",
            xcom_sidecar_image=xcom_sidecar_image,
            blockers=(f"DPONE_COMPACT_PACK_RELEASE_IMMUTABLE_CONFLICT: {exc}",),
        )


def _materialize(
    *,
    pack_root: Path,
    cache_root: Path,
    xcom_sidecar_image: str,
    dag_ids: Sequence[str] | None,
    provenance: Mapping[str, Any] | None,
) -> CompactPackReleaseReport:
    root = pack_root.absolute()
    try:
        cache = cache_root.resolve()
    except (OSError, RuntimeError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_CACHE_INVALID", "cache root cannot be resolved safely"
        ) from exc
    if (root / "release-set.json").exists() or (root / "release-set.json").is_symlink():
        return _materialize_workspace(root, cache, xcom_sidecar_image=xcom_sidecar_image, dag_ids=dag_ids)
    dag_dir = root / "_dags"
    if not dag_dir.is_dir():
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_DAG_SPECS_MISSING",
            f"dag-spec directory is missing: {dag_dir.as_posix()}",
        )
    selected = {str(item) for item in dag_ids} if dag_ids else None
    dag_paths = sorted(dag_dir.glob("*.dag-spec.json"))
    if selected is not None:
        dag_paths = [path for path in dag_paths if path.name.removesuffix(".dag-spec.json") in selected]
        missing = sorted(selected - {path.name.removesuffix(".dag-spec.json") for path in dag_paths})
        if missing:
            raise CompactPackReleaseError(
                "DPONE_COMPACT_PACK_RELEASE_DAG_SPEC_MISSING",
                "requested dag-specs are missing: " + ", ".join(missing),
            )
    if not dag_paths:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_EMPTY",
            "no dag-specs selected for compact pack release materialization",
        )

    written_dags: dict[str, bytes] = {}
    written_packs: dict[str, bytes] = {}
    pack_fingerprints: dict[str, str] = {}
    dag_artifacts: list[dict[str, Any]] = []
    pack_artifacts: list[dict[str, Any]] = []
    projection_modes: set[str] = set()
    ordered_workload_ids: list[str] = []

    for dag_path in dag_paths:
        dag_id = dag_path.name.removesuffix(".dag-spec.json")
        spec = _load_json_object(dag_path)
        workload_ids = _workload_ids_from_dag_spec(spec)
        if not workload_ids:
            raise CompactPackReleaseError(
                "DPONE_COMPACT_PACK_RELEASE_WORKLOADS_MISSING",
                f"dag-spec {dag_id!r} has no workload_id nodes",
            )
        rewritten_spec = rewrite_strict_init_fetch_dag_spec(spec, workload_ids=workload_ids)
        dag_bytes = _json_bytes(rewritten_spec)
        written_dags[dag_id] = dag_bytes
        dag_artifacts.append(
            {
                "id": dag_id,
                "path": f"dags/{dag_id}.dag-spec.json",
                "sha256": _sha256_bytes(dag_bytes),
                "bytes": len(dag_bytes),
            }
        )
        for workload_id in workload_ids:
            if workload_id in written_packs:
                continue
            pack_path = root / workload_id / "airflow-pack.json"
            if not pack_path.is_file():
                raise CompactPackReleaseError(
                    "DPONE_COMPACT_PACK_RELEASE_PACK_MISSING",
                    f"missing compact pack for workload {workload_id!r}",
                )
            pack = rewrite_strict_init_fetch_pack(
                _load_json_object(pack_path),
                xcom_sidecar_image=xcom_sidecar_image,
            )
            pack_bytes = _json_bytes(pack)
            written_packs[workload_id] = pack_bytes
            fingerprint = str(pack["pack_fingerprint"])
            pack_fingerprints[workload_id] = fingerprint
            projection_modes.add(str(pack["connection_projection"]["mode"]))
            ordered_workload_ids.append(workload_id)
            pack_descriptor: dict[str, Any] = {
                "id": workload_id,
                "path": f"packs/{workload_id}.airflow-pack.json",
                "sha256": _sha256_bytes(pack_bytes),
                "bytes": len(pack_bytes),
                "pack_fingerprint": fingerprint,
            }
            payload_ids = _runtime_payload_ids_from_pack(
                pack,
                error_factory=CompactPackReleaseError,
            )
            if payload_ids:
                pack_descriptor["runtime_payload_ids"] = list(payload_ids)
            pack_artifacts.append(pack_descriptor)

    if projection_modes != {"kubernetes_secret_volume"}:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_PROJECTION_INVALID",
            f"all packs must use kubernetes_secret_volume (got {sorted(projection_modes)!r})",
        )

    runtime_files, runtime_artifacts = _materialize_runtime_payloads(
        pack_root=root,
        pack_artifacts=pack_artifacts,
        error_factory=CompactPackReleaseError,
    )
    try:
        require_runtime_payload_refs(
            workload_packs=pack_artifacts,
            runtime_payloads=runtime_artifacts,
            code="DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_MISSING",
        )
    except RuntimePayloadRefError as exc:
        raise CompactPackReleaseError(exc.code, str(exc)) from exc

    # Provenance must stay free of absolute paths / runner-local noise: the
    # registry stores release-set.json under the release_id key, so volatile
    # bytes at a reused release_id cause DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT.
    stable_provenance = {
        "source": "dpone.readiness.airflow_compact_pack_release",
        "built_by": "dpone gitops airflow release-materialize",
        "promotion_profile": COMPACT_PROMOTION_PROFILE,
        "dag_ids": [path.name.removesuffix(".dag-spec.json") for path in dag_paths],
        "workload_ids": list(ordered_workload_ids),
        "strict_init_fetch_rewrite": True,
    }
    for key, value in dict(provenance or {}).items():
        if key in {"repo_root", "pack_root", "cache_root", "wrapper"}:
            continue
        if isinstance(value, str) and (value.startswith("/") or "://" in value[:12]):
            continue
        stable_provenance[key] = value

    artifacts: dict[str, Any] = {
        "dag_specs": dag_artifacts,
        "workload_packs": pack_artifacts,
        "canonical_schemas": [],
    }
    if runtime_artifacts:
        artifacts["runtime_payloads"] = runtime_artifacts
        stable_provenance["runtime_payload_count"] = len(runtime_artifacts)

    release: dict[str, Any] = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": artifacts,
        # The release variant is identity-bearing so its CAS key cannot collide
        # with a legacy release that carries the same artifact inventory.
        "promotion": {
            "schema": COMPACT_PROMOTION_SCHEMA,
            "profile": COMPACT_PROMOTION_PROFILE,
        },
        "provenance": stable_provenance,
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache / "releases" / _digest_dir(release_id)
    files = {
        **{f"dags/{dag_id}.dag-spec.json": payload for dag_id, payload in written_dags.items()},
        **{f"packs/{workload_id}.airflow-pack.json": payload for workload_id, payload in written_packs.items()},
        **runtime_files,
        "release-set.json": _json_bytes(release),
    }
    materialize_immutable_local_release(release_dir, files)
    verified = _load_json_object(release_dir / "release-set.json")
    if compute_release_id(verified) != release_id:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_FINGERPRINT_DRIFT",
            "release-set fingerprint drifted after immutable write",
        )
    return CompactPackReleaseReport(
        release_id=release_id,
        release_dir=release_dir.as_posix(),
        dag_ids=tuple(path.name.removesuffix(".dag-spec.json") for path in dag_paths),
        workload_ids=tuple(ordered_workload_ids),
        pack_fingerprints=pack_fingerprints,
        connection_projection_mode="kubernetes_secret_volume",
        xcom_sidecar_image=str(xcom_sidecar_image).strip(),
    )


def _materialize_workspace(
    root: Path, cache: Path, *, xcom_sidecar_image: str, dag_ids: Sequence[str] | None
) -> CompactPackReleaseReport:
    from dpone.app.dbt_promotion_composition import build_dbt_compact_workspace_release_builder
    from dpone.readiness.airflow_release_schema_validation import validate_release_set_schema

    if cache.is_relative_to(root.resolve()):
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_CACHE_INVALID", "cache root must be outside the immutable source tree"
        )
    try:
        files = build_dbt_compact_workspace_release_builder().build(
            root, xcom_sidecar_image=xcom_sidecar_image, dag_ids=dag_ids
        )
        release = json.loads(files["release-set.json"])
        validate_release_set_schema(release, path=root / "release-set.json")
    except (ValueError, OSError, TypeError, KeyError, RecursionError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_WORKSPACE_INVALID",
            "native workspace release is invalid, incomplete or incompatible; regenerate the complete workspace with a compatible producer",
        ) from exc
    release_id = release["release_id"]
    release_dir = cache / "releases" / _digest_dir(release_id)
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
        release_id=release_id,
        release_dir=release_dir.as_posix(),
        dag_ids=tuple(item["id"] for item in release["artifacts"]["dag_specs"]),
        workload_ids=tuple(item["id"] for item in release["artifacts"]["workload_packs"]),
        pack_fingerprints={item["id"]: item["pack_fingerprint"] for item in release["artifacts"]["workload_packs"]},
        connection_projection_mode="runtime_connection_context",
        xcom_sidecar_image=xcom_sidecar_image.strip(),
    )


def _workload_ids_from_dag_spec(spec: Mapping[str, Any]) -> tuple[str, ...]:
    nodes = spec.get("nodes")
    if not isinstance(nodes, list):
        return ()
    ordered: list[str] = []
    seen: set[str] = set()
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        workload_id = str(node.get("workload_id") or "").strip()
        if not workload_id or workload_id in seen:
            continue
        seen.add(workload_id)
        ordered.append(workload_id)
    return tuple(ordered)


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_JSON_INVALID",
            f"invalid JSON object at {path.as_posix()}",
        ) from exc
    if not isinstance(payload, dict):
        raise CompactPackReleaseError(
            "DPONE_COMPACT_PACK_RELEASE_JSON_INVALID",
            f"expected JSON object at {path.as_posix()}",
        )
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _digest_dir(release_id: str) -> str:
    return release_id.replace(":", "-", 1)


__all__ = [
    "CompactPackReleaseError",
    "CompactPackReleaseReport",
    "closed_connection_projection",
    "materialize_compact_pack_release",
    "rewrite_strict_init_fetch_dag_spec",
    "rewrite_strict_init_fetch_pack",
]
