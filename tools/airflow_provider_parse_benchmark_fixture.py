"""Deterministic strict-v2 fixture generation and index inspection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint
from tools.airflow_provider_parse_benchmark_support import (
    DAG_COUNT,
    DEPLOYMENT_ID,
    RELEASE_ID,
    RUNTIME_SUFFIX,
    STRICT_INDEX_SCHEMA,
    BenchmarkIntegrityError,
    canonical_json_sha256,
    expected_layout,
    topology_summary,
)

_ENVIRONMENT = "benchmark"
_RUNTIME_IMAGE_DIGEST = "sha256:" + "c" * 64
_RUNTIME_IMAGE_REF = f"registry.example/dpone/runtime@{_RUNTIME_IMAGE_DIGEST}"
_VALID_TRUST_TIERS = frozenset({"production", "non_production"})
_CONNECTION_FILES = {
    "binding_set": "binding-set.json",
    "connection_registry": "connection-registry.json",
    "credential_runtime": "credential-runtime.json",
}


def build_fixture(root: Path) -> tuple[Path, dict[str, Any]]:
    """Write one deterministic strict-v2 cache and derive its metadata."""

    cache_root = root / ".dpone-cache"
    with cache_write_lease(cache_root):
        pass
    release_key, deployment_key = _digest_key(RELEASE_ID), _digest_key(DEPLOYMENT_ID)
    release_dir = cache_root / "releases" / release_key
    deployment_dir = cache_root / "deployments" / _ENVIRONMENT / deployment_key
    for path in (release_dir / "dags", release_dir / "packs", deployment_dir):
        path.mkdir(parents=True)
    dag_specs, packs = _write_release_artifacts(release_dir, release_key)
    connection = _write_connection_artifacts(cache_root)
    delivery = runtime_artifact_delivery()
    runtime = {
        "binding_set_ref": connection["binding_set"]["sha256"],
        "connection_registry_ref": connection["connection_registry"]["sha256"],
        "credential_runtime_ref": connection["credential_runtime"]["sha256"],
        **connection,
        "runtime_image_ref": _RUNTIME_IMAGE_REF,
        "runtime_image_digest": _RUNTIME_IMAGE_DIGEST,
        "airflow_bundle_ref": "git:parse-benchmark",
        "runtime_artifact_delivery": delivery,
    }
    release = _exact_artifact(
        release_dir / "release-set.json",
        {"schema": "dpone.release-set.v1", "release_id": RELEASE_ID},
        f"cache://releases/{release_key}/release-set.json",
    )
    deployment_payload = {
        "schema": "dpone.deployment-set.v2",
        "deployment_id": DEPLOYMENT_ID,
        "deployment_type": "environment",
        "runnable": True,
        "environment": _ENVIRONMENT,
        "trust_tier": "non_production",
        "release_ref": RELEASE_ID,
        **runtime,
        "workloads": [
            {"id": item["id"], "sha256": item["sha256"], "pack_fingerprint": item["pack_fingerprint"]} for item in packs
        ],
    }
    deployment = _exact_artifact(
        deployment_dir / "deployment.json",
        deployment_payload,
        f"cache://deployments/{_ENVIRONMENT}/{deployment_key}/deployment.json",
    )
    index_path = deployment_dir / "airflow-index.json"
    index_sha256, _ = _json_file(
        index_path,
        {
            "schema": STRICT_INDEX_SCHEMA,
            "trust_tier": "non_production",
            "release_id": RELEASE_ID,
            "deployment_id": DEPLOYMENT_ID,
            "dag_specs": dag_specs,
            "workload_packs": packs,
            **runtime,
            "release": release,
            "deployment": deployment,
        },
    )
    metadata = inspect_fixture_index(index_path, expected_index_sha256=index_sha256)
    fixture_sha256, total_bytes, file_count = _tree_digest(cache_root)
    ready = all(
        (
            metadata["strict_v2_validated"],
            metadata["index_digest_match"],
            metadata["cache_only"],
            metadata["runtime_connection_artifacts"]["passed"],
            metadata["expected_topology"]["dag_count"] == DAG_COUNT,
        )
    )
    dimensions = metadata["workloads"] // metadata["dags"] if metadata["dags"] else None
    return index_path, {
        "status": "READY" if ready else "INVALID",
        **metadata,
        "workloads_per_dag": dimensions,
        "total_bytes": total_bytes,
        "file_count": file_count,
        "sha256": fixture_sha256,
    }


def _write_release_artifacts(
    release_dir: Path,
    release_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint

    dag_specs: list[dict[str, Any]] = []
    packs: list[dict[str, Any]] = []
    for dag_id, workload_ids in expected_layout().items():
        nodes: list[dict[str, str]] = []
        for workload_id in workload_ids:
            pack = _strict_workload_pack(workload_id)
            artifact = _artifact(
                workload_id,
                release_dir / "packs" / f"{workload_id}.airflow-pack.json",
                f"releases/{release_key}/packs",
                pack,
            )
            artifact["pack_fingerprint"] = pack["pack_fingerprint"]
            packs.append(artifact)
            nodes.append(
                {
                    "node_id": workload_id,
                    "workload_id": workload_id,
                    "pack_ref": f"cached://deployments/{DEPLOYMENT_ID}/workloads/{workload_id}",
                }
            )
        spec: dict[str, Any] = {
            "kind": "gitops.airflow_dag_spec",
            "schema_version": "1",
            "producer": "airflow-provider-parse-benchmark",
            "dag_id": dag_id,
            "schedule": None,
            "start_date": "2026-07-18",
            "catchup": False,
            "tags": ["dpone", "parse-benchmark"],
            "nodes": nodes,
            "edges": [],
            "topological_order": list(workload_ids),
        }
        spec["spec_fingerprint"] = compute_dag_spec_fingerprint(spec)
        dag_specs.append(
            _artifact(
                dag_id,
                release_dir / "dags" / f"{dag_id}.dag-spec.json",
                f"releases/{release_key}/dags",
                spec,
            )
        )
    return dag_specs, packs


def _strict_workload_pack(workload_id: str) -> dict[str, Any]:
    task_id, name = f"{workload_id}{RUNTIME_SUFFIX}", f"dpone-{workload_id.replace('_', '-')}"
    payload: dict[str, Any] = {
        "id": workload_id,
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": workload_id},
        "airflow": {"execution": {}},
        "connection_projection": {},
        "kpo_kwargs": {"task_id": task_id, "name": name},
        "provider_execution": {
            "schema": "dpone.airflow-provider-execution.v1",
            "kpo_kwargs": {
                "task_id": task_id,
                "name": name,
                "labels": {"dpone.dev/workload-id": workload_id},
                "env_vars": {},
            },
            "pod_spec": {"spec": {"containers": [{"name": "base"}]}},
        },
        "xcom": {"sidecar_image": "registry.example/airflow/xcom@sha256:" + "d" * 64},
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def inspect_fixture_index(index_path: Path, *, expected_index_sha256: str) -> dict[str, Any]:
    """Derive metadata from the exact written index and validate strict v2."""

    raw = index_path.read_bytes()
    payload = _json_object(raw)
    delivery = payload.get("runtime_artifact_delivery")
    delivery = delivery if isinstance(delivery, Mapping) else {}
    schema = _optional_text(payload.get("schema"))
    delivery_mode = _optional_text(delivery.get("mode"))
    trust_tier = _optional_text(payload.get("trust_tier"))
    delivery_trust_tier = _optional_text(delivery.get("trust_tier"))
    valid, error_code = _validate_strict_v2(
        index_path,
        schema,
        delivery_mode,
        trust_tier,
        delivery_trust_tier,
    )
    connection = _verify_connection_artifacts(payload, index_path.parents[3])
    if not connection["passed"]:
        valid, error_code = False, "DPONE_BENCHMARK_RUNTIME_CONNECTION_ARTIFACT_INVALID"
    topology = _expected_topology_summary(index_path)
    dag_specs, packs = payload.get("dag_specs"), payload.get("workload_packs")
    observed_sha256 = _sha256(raw)
    return {
        "cache_only": _cache_only(payload),
        "dags": len(dag_specs) if isinstance(dag_specs, list) else 0,
        "workloads": len(packs) if isinstance(packs, list) else 0,
        "release_id": _optional_text(payload.get("release_id")),
        "deployment_id": _optional_text(payload.get("deployment_id")),
        "index_bytes": len(raw),
        "index_schema": schema,
        "index_sha256": observed_sha256,
        "expected_index_sha256": expected_index_sha256,
        "index_digest_match": observed_sha256 == expected_index_sha256,
        "delivery_mode": delivery_mode,
        "trust_tier": trust_tier,
        "delivery_trust_tier": delivery_trust_tier,
        "strict_v2_validated": valid,
        "validation_error_code": error_code,
        "runtime_connection_artifacts": connection,
        "expected_topology": topology,
    }


def load_expected_topology(index_path: Path) -> dict[str, dict[str, Any]]:
    """Load exact DAG/spec/task/edge authority from checksum-bound bytes."""

    from tools.airflow_provider_parse_benchmark_worker import load_expected_topology as load

    return load(index_path)


def _expected_topology_summary(index_path: Path) -> dict[str, Any]:
    try:
        topology = load_expected_topology(index_path)
    except BenchmarkIntegrityError:
        return topology_summary(())
    return topology_summary([{"dag_id": dag_id, **record} for dag_id, record in topology.items()])


def verify_index_digest(index_path: Path, expected_sha256: str) -> str:
    """Fail when the index no longer matches its verified descriptor."""

    observed = _sha256(index_path.read_bytes())
    if observed != expected_sha256:
        raise BenchmarkIntegrityError("benchmark index digest changed after fixture validation")
    return observed


def runtime_artifact_delivery() -> dict[str, Any]:
    """Return the strict non-production init-fetch descriptor."""

    registry = "dpone-parse-benchmark-artifacts"
    identity = {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime", "namespace": "airflow"}
    config_name, config_sha256 = "dpone-artifact-registry", "sha256:" + "4" * 64
    registry_config = {
        "kind": "kubernetes_config_map",
        "name": config_name,
        "key": "registry.json",
        "sha256": config_sha256,
    }
    return {
        "mode": "init_fetch",
        "trust_tier": "non_production",
        "artifact_registry_ref": registry,
        "identity": identity,
        "registry_config_ref": registry_config,
        "source": {"artifact_registry_ref": registry},
        "verify": {"checksums": "required", "attestations": "optional"},
    }


def _write_connection_artifacts(cache_root: Path) -> dict[str, dict[str, Any]]:
    collections = {"binding_set": "bindings", "connection_registry": "connections", "credential_runtime": None}
    payloads = {
        name: {
            "schema": f"dpone.{name.replace('_', '-')}.v1",
            "environment": _ENVIRONMENT,
            **({collection: {}} if collection else {}),
        }
        for name, collection in collections.items()
    }
    encoded = {name: _json_bytes(payload) for name, payload in payloads.items()}
    identity = canonical_json_sha256(
        {name: {"sha256": _sha256(raw), "bytes": len(raw)} for name, raw in encoded.items()}
    )
    context_key = _digest_key(identity)
    context_dir = cache_root / "runtime-connection-contexts" / context_key
    context_dir.mkdir(parents=True)
    descriptors: dict[str, dict[str, Any]] = {}
    for name, raw in encoded.items():
        filename = _CONNECTION_FILES[name]
        (context_dir / filename).write_bytes(raw)
        descriptors[name] = {
            "artifact_ref": f"cache://runtime-connection-contexts/{context_key}/{filename}",
            "sha256": _sha256(raw),
            "bytes": len(raw),
        }
    return descriptors


def _verify_connection_artifacts(payload: Mapping[str, Any], cache_root: Path) -> dict[str, Any]:
    verified: list[dict[str, Any]] = []
    for field, filename in _CONNECTION_FILES.items():
        descriptor = payload.get(field)
        try:
            raw = _verified_artifact_bytes(descriptor, cache_root)
        except BenchmarkIntegrityError:
            continue
        artifact_ref = str(descriptor["artifact_ref"])
        if Path(artifact_ref.removeprefix("cache://")).name == filename:
            verified.append(
                {
                    "field": field,
                    "artifact_ref": artifact_ref,
                    "sha256": descriptor["sha256"],
                    "bytes": len(raw),
                }
            )
    passed = len(verified) == len(_CONNECTION_FILES)
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "expected": len(_CONNECTION_FILES),
        "verified": len(verified),
        "descriptors_sha256": canonical_json_sha256(verified),
    }


def _verified_artifact_bytes(descriptor: object, cache_root: Path) -> bytes:
    if not isinstance(descriptor, Mapping):
        raise BenchmarkIntegrityError("benchmark artifact descriptor is malformed")
    artifact_ref, expected_sha256, expected_bytes = (
        descriptor.get("artifact_ref"),
        descriptor.get("sha256"),
        descriptor.get("bytes"),
    )
    if not (
        isinstance(artifact_ref, str)
        and artifact_ref.startswith("cache://")
        and isinstance(expected_sha256, str)
        and isinstance(expected_bytes, int)
        and not isinstance(expected_bytes, bool)
        and expected_bytes > 0
    ):
        raise BenchmarkIntegrityError("benchmark artifact descriptor is malformed")
    relative = Path(artifact_ref.removeprefix("cache://"))
    if relative.is_absolute() or ".." in relative.parts:
        raise BenchmarkIntegrityError("benchmark artifact descriptor escapes the cache")
    path = (cache_root / relative).resolve(strict=False)
    try:
        path.relative_to(cache_root.resolve(strict=True))
        raw = path.read_bytes()
    except (OSError, ValueError) as exc:
        raise BenchmarkIntegrityError("benchmark artifact is unavailable") from exc
    if len(raw) != expected_bytes or _sha256(raw) != expected_sha256:
        raise BenchmarkIntegrityError("benchmark artifact digest or size differs")
    return raw


def _validate_strict_v2(
    index_path: Path,
    schema: str | None,
    delivery_mode: str | None,
    trust_tier: str | None,
    delivery_trust_tier: str | None,
) -> tuple[bool, str | None]:
    candidate = (schema, delivery_mode, delivery_trust_tier) == (STRICT_INDEX_SCHEMA, "init_fetch", trust_tier)
    candidate = candidate and trust_tier in _VALID_TRUST_TIERS
    if not candidate:
        return False, "DPONE_BENCHMARK_STRICT_V2_REQUIRED"
    try:
        from dpone_airflow_pack.deployment_index import load_airflow_deployment_index

        descriptor = load_airflow_deployment_index(index_path)
    except Exception as exc:  # noqa: BLE001 - evidence persists only a stable code.
        code = getattr(exc, "code", None)
        return False, str(code) if isinstance(code, str) else "DPONE_BENCHMARK_STRICT_V2_INVALID"
    valid = descriptor.schema == STRICT_INDEX_SCHEMA and all(
        (descriptor.delivery_context is not None, descriptor.runtime_artifact_delivery is not None)
    )
    return (True, None) if valid else (False, "DPONE_BENCHMARK_STRICT_V2_INVALID")


def _cache_only(payload: Mapping[str, Any]) -> bool:
    collections = [payload.get(name) for name in ("dag_specs", "workload_packs") if isinstance(payload.get(name), list)]
    refs = [
        artifact.get("artifact_ref")
        for artifacts in collections
        for artifact in artifacts
        if isinstance(artifact, Mapping)
    ]
    refs.extend(
        artifact.get("artifact_ref")
        for name in ("release", "deployment", *_CONNECTION_FILES)
        if isinstance((artifact := payload.get(name)), Mapping)
    )
    return bool(refs) and all(isinstance(value, str) and value.startswith("cache://") for value in refs)


def _artifact(artifact_id: str, path: Path, parent_ref: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {"id": artifact_id, **_exact_artifact(path, payload, f"cache://{parent_ref}/{path.name}")}


def _exact_artifact(path: Path, payload: Mapping[str, Any], artifact_ref: str) -> dict[str, Any]:
    digest, size = _json_file(path, payload)
    return {"artifact_ref": artifact_ref, "sha256": digest, "bytes": size}


def _json_file(path: Path, payload: Mapping[str, Any]) -> tuple[str, int]:
    raw = _json_bytes(payload)
    path.write_bytes(raw)
    return _sha256(raw), len(raw)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _tree_digest(root: Path) -> tuple[str, int, int]:
    digest, total = hashlib.sha256(), 0
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files:
        raw, relative = path.read_bytes(), path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big") + relative + len(raw).to_bytes(8, "big") + raw)
        total += len(raw)
    return "sha256:" + digest.hexdigest(), total, len(files)


def _sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _digest_key(value: str) -> str:
    return value.replace(":", "-")


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) else None
