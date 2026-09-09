"""Load Airflow packs with parse-time provenance evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_activation_contract import cache_read_lease
from dpone_airflow_pack.cache_artifact_contract import read_confined_cache_file
from dpone_airflow_pack.cache_status import (
    DEFAULT_MAX_CACHE_JSON_BYTES,
    DEFAULT_MAX_CACHED_PACK_BYTES,
    _read_airflow_pack_cache_status_unleased,
    airflow_pack_cache_dir,
    frozen_missing_cache_status,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.pack_identity import (
    PackIdentityError,
    parse_pack_json,
    verify_pack_fingerprint,
)
from dpone_airflow_pack.pack_validation import validate_airflow_pack_payload
from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError

CACHED_REF_PREFIX = "cached://"
DEFAULT_MAX_AIRFLOW_PACK_BYTES = DEFAULT_MAX_CACHED_PACK_BYTES


def load_dpone_airflow_pack_with_provenance(
    pack_ref: str | Path,
    *,
    cache_dir: str | Path | None = None,
    expected_sha256: str | None = None,
    max_bytes: int = DEFAULT_MAX_AIRFLOW_PACK_BYTES,
    max_index_bytes: int = DEFAULT_MAX_CACHE_JSON_BYTES,
    confined_root: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load a local or cached pack and return immutable parse-time provenance."""

    ref = str(pack_ref)
    if ref.startswith(CACHED_REF_PREFIX):
        return _load_cached_pack(
            ref,
            cache_dir=cache_dir,
            expected_sha256=expected_sha256,
            max_bytes=max_bytes,
            max_index_bytes=max_index_bytes,
        )
    return _load_local_pack(
        Path(pack_ref),
        expected_sha256=expected_sha256,
        max_bytes=max_bytes,
        confined_root=confined_root,
    )


def _load_cached_pack(
    ref: str,
    *,
    cache_dir: str | Path | None,
    expected_sha256: str | None,
    max_bytes: int,
    max_index_bytes: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    workload_id = _cached_workload_id(ref)
    if not workload_id:
        raise DponeAirflowContractError(
            "dpone cached Airflow pack reference has no workload id",
            blockers=(
                {
                    "code": "airflow_pack_cache_ref_invalid",
                    "path": ref,
                    "message": "Expected cached://<id> or cached://workloads/<id>",
                },
            ),
        )
    root = airflow_pack_cache_dir(cache_dir)
    with cache_read_lease(root) as lease:
        cache_status = (
            _read_airflow_pack_cache_status_unleased(
                root,
                workload_ids=(workload_id,),
                max_index_bytes=max_index_bytes,
                max_pack_bytes=max_bytes,
            )
            if lease.root_available
            else frozen_missing_cache_status(root, workload_ids=(workload_id,))
        )
        workload_status = cache_status.get("workloads", {}).get(workload_id, {})
        blockers = tuple(cache_status.get("blockers") or workload_status.get("blockers") or ())
        if blockers:
            raise DponeAirflowContractError(
                f"dpone cached Airflow pack is not ready: {ref}",
                blockers=tuple(dict(item) for item in blockers),
            )
        pack_path = Path(str(workload_status["path"]))
        pack, actual_sha256, verified_pack_fingerprint = _load_pack_bytes(
            pack_path,
            expected_sha256=expected_sha256 or _expected_cache_sha256(workload_status),
            max_bytes=max_bytes,
        )
    provenance = {
        "kind": "dpone.airflow_pack_provenance",
        "schema_version": "1",
        "source": "cached",
        "workload_id": workload_id,
        "ref": ref,
        "path": str(pack_path),
        "generation": cache_status.get("current_generation"),
        "pack_sha256": actual_sha256,
        "verified_pack_fingerprint": verified_pack_fingerprint,
        "index_sha256": cache_status.get("index_sha256"),
        "cache_status": cache_status,
    }
    return pack, provenance


def _load_local_pack(
    path: Path,
    *,
    expected_sha256: str | None,
    max_bytes: int,
    confined_root: Path | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    pack, actual_sha256, verified_pack_fingerprint = _load_pack_bytes(
        path,
        expected_sha256=expected_sha256,
        max_bytes=max_bytes,
        confined_root=confined_root,
    )
    provenance = {
        "kind": "dpone.airflow_pack_provenance",
        "schema_version": "1",
        "source": "local_path",
        "path": str(path),
        "pack_sha256": actual_sha256,
        "verified_pack_fingerprint": verified_pack_fingerprint,
    }
    return pack, provenance


def _cached_workload_id(ref: str) -> str:
    """Normalize ``cached://`` refs used by compact dag-specs and v2 indexes."""

    raw = ref.removeprefix(CACHED_REF_PREFIX).split("?", 1)[0].strip("/")
    if raw.startswith("workloads/"):
        return raw.removeprefix("workloads/").strip("/")
    if raw.startswith("deployments/"):
        segments = raw.removeprefix("deployments/").split("/")
        if len(segments) == 3 and segments[1] == "workloads":
            return segments[2].strip("/")
    return raw


def _load_pack_bytes(
    path: Path,
    *,
    expected_sha256: str | None,
    max_bytes: int,
    confined_root: Path | None = None,
) -> tuple[dict[str, Any], str, str | None]:
    if max_bytes <= 0:
        raise _pack_error(
            "airflow_pack_size_limit_invalid",
            path,
            "Pack size limit must be positive",
        )
    if confined_root is not None:
        try:
            raw = read_confined_cache_file(
                path,
                cache_root=confined_root,
                max_bytes=max_bytes,
            )
        except AirflowDeploymentIndexError as exc:
            raise _pack_error(exc.code, path, str(exc)) from exc
    else:
        try:
            with path.open("rb") as handle:
                raw = handle.read(max_bytes + 1)
        except FileNotFoundError as exc:
            raise _pack_error("airflow_pack_missing", path, "Pack file does not exist") from exc
        except OSError as exc:
            raise _pack_error("airflow_pack_read_failed", path, "Pack file could not be read") from exc
    if len(raw) > max_bytes:
        raise _pack_error("airflow_pack_too_large", path, "Pack file exceeds configured size limit")
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and _digest_value(expected_sha256) != actual_sha256:
        raise _pack_error("airflow_pack_hash_mismatch", path, "Pack checksum differs from expected digest")
    try:
        raw_pack = parse_pack_json(raw)
    except PackIdentityError as exc:
        raise _pack_error(
            "airflow_pack_json_invalid",
            path,
            "Pack must be one duplicate-free UTF-8 JSON object",
        ) from exc
    verified_pack_fingerprint: str | None = None
    if raw_pack.get("pack_identity") is not None:
        try:
            verified_pack_fingerprint = verify_pack_fingerprint(raw_pack)
        except PackIdentityError as exc:
            raise _pack_error(
                "airflow_pack_identity_invalid",
                path,
                "Pack identity does not match the exact loaded bytes",
            ) from exc
    return (
        validate_airflow_pack_payload(raw_pack, location=str(path)),
        actual_sha256,
        verified_pack_fingerprint,
    )


def _expected_cache_sha256(workload_status: dict[str, Any]) -> str | None:
    expected = workload_status.get("expected_sha256")
    return str(expected) if expected else None


def _digest_value(value: str) -> str:
    return value.removeprefix("sha256:")


def _pack_error(code: str, path: Path, message: str) -> DponeAirflowContractError:
    return DponeAirflowContractError(
        f"dpone Airflow pack cannot be loaded: {path}",
        blockers=({"code": code, "path": str(path), "message": message},),
    )
