from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.storage import ObjectStorageObject, ObjectStorageUri

PACK_INDEX_SCHEMA = "gitops.airflow_pack_index"
RELEASE_MARKER_SCHEMA = "gitops.airflow_pack_release_marker"


class ArtifactObjectClient(Protocol):
    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject: ...

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None: ...

    def delete_prefix(self, prefix: ObjectStorageUri) -> int: ...

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]: ...


class ObjectArtifactStore:
    """Small artifact-store facade over the connector-neutral object storage port."""

    def __init__(self, client: ArtifactObjectClient) -> None:
        self._client = client

    def put_json(self, payload: str | bytes, destination: str | ObjectStorageUri) -> ObjectStorageObject:
        import tempfile

        uri = _uri(destination)
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        with tempfile.TemporaryDirectory(prefix="dpone-artifact-json-") as tmp:
            path = Path(tmp) / "payload.json"
            path.write_bytes(raw)
            return self._client.put_file(path, uri, content_type="application/json")

    def put_file(self, path: str | Path, destination: str | ObjectStorageUri) -> ObjectStorageObject:
        return self._client.put_file(path, _uri(destination), content_type="application/json")

    def get_file(self, source: str | ObjectStorageUri, local_path: str | Path) -> None:
        self._client.get_file(_uri(source), local_path)

    def list_objects(self, prefix: str | ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        return tuple(self._client.list_objects(_uri(prefix).prefix()))

    def delete_prefix(self, prefix: str | ObjectStorageUri) -> int:
        return self._client.delete_prefix(_uri(prefix).prefix())


@dataclass(frozen=True, slots=True)
class AirflowPackIndexBuildResult:
    index: dict[str, Any]
    blockers: tuple[str, ...] = ()


class AirflowPackIndexBuilder:
    def __init__(self, *, max_pack_bytes: int = 10 * 1024**2, max_index_bytes: int = 25 * 1024**2) -> None:
        self._max_pack_bytes = max_pack_bytes
        self._max_index_bytes = max_index_bytes

    def build(
        self,
        *,
        packs: dict[str, Path],
        uri_prefix: ObjectStorageUri,
        git_sha: str,
        generated_at: datetime | None = None,
        dag_specs: dict[str, Path] | None = None,
    ) -> AirflowPackIndexBuildResult:
        artifacts: dict[str, dict[str, object]] = {}
        blockers: list[str] = []
        for workload_id, path in sorted(packs.items()):
            raw = path.read_bytes()
            if len(raw) > self._max_pack_bytes:
                blockers.append(f"airflow_pack_size_limit_exceeded:{workload_id}")
            artifacts[workload_id] = {
                "path": f"airflow/{workload_id}/airflow-pack.json",
                "uri": str(uri_prefix.prefix().child(workload_id, "airflow-pack.json")),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size_bytes": len(raw),
            }
        dag_spec_artifacts: dict[str, dict[str, object]] = {}
        for dag_id, path in sorted((dag_specs or {}).items()):
            raw = path.read_bytes()
            if len(raw) > self._max_pack_bytes:
                blockers.append(f"airflow_dag_spec_size_limit_exceeded:{dag_id}")
            dag_spec_artifacts[dag_id] = {
                "path": f"airflow/_dags/{dag_id}.dag-spec.json",
                "uri": str(uri_prefix.prefix().child("_dags", f"{dag_id}.dag-spec.json")),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size_bytes": len(raw),
            }
        index = {
            "kind": PACK_INDEX_SCHEMA,
            "schema_version": "1",
            "git_sha": git_sha,
            "generated_at": (generated_at or datetime.now(tz=timezone.utc)).isoformat(),  # noqa: UP017
            "artifacts": artifacts,
            "dag_specs": dag_spec_artifacts,
        }
        if len(json.dumps(index, sort_keys=True).encode()) > self._max_index_bytes:
            blockers.append("airflow_pack_index_size_limit_exceeded")
        return AirflowPackIndexBuildResult(index=index, blockers=tuple(dict.fromkeys(blockers)))


class AirflowPackPublisher:
    """Publish compact Airflow packs to an immutable object-storage release prefix."""

    def __init__(
        self,
        *,
        store: ObjectArtifactStore,
        index_builder: AirflowPackIndexBuilder | None = None,
    ) -> None:
        self._store = store
        self._index_builder = index_builder or AirflowPackIndexBuilder()

    def publish(
        self,
        *,
        packs: dict[str, Path],
        uri_prefix: str,
        latest_index_uri: str,
        git_sha: str,
        env: str | None = None,
        dag_specs: dict[str, Path] | None = None,
    ) -> dict[str, object]:
        storage_uri = _storage_uri()
        release_prefix = _uri(storage_uri.parse(uri_prefix.format(git_sha=git_sha, env=env or "")).prefix())
        specs = dag_specs or {}
        result = self._index_builder.build(
            packs=packs,
            uri_prefix=release_prefix,
            git_sha=git_sha,
            dag_specs=specs,
        )
        if result.blockers:
            return _publish_report(git_sha=git_sha, uri_prefix=str(release_prefix), blockers=result.blockers)
        for workload_id, path in sorted(packs.items()):
            self._store.put_file(path, release_prefix.child(workload_id, "airflow-pack.json"))
        for dag_id, path in sorted(specs.items()):
            self._store.put_file(path, release_prefix.child("_dags", f"{dag_id}.dag-spec.json"))
        index_json = _json(result.index)
        self._store.put_json(index_json, release_prefix.child("pack-index.json"))
        self._store.put_json(
            _json(_release_marker(git_sha, status="successful")),
            release_prefix.child("release-marker.json"),
        )
        self._store.put_json(index_json, latest_index_uri.format(git_sha=git_sha, env=env or ""))
        return _publish_report(
            git_sha=git_sha,
            uri_prefix=str(release_prefix),
            index=result.index,
            pack_count=len(packs),
            dag_spec_count=len(specs),
        )


def build_object_artifact_store(options: dict[str, Any], *, uri: str) -> ObjectArtifactStore:
    local_root_dir = options.get("local_root_dir")
    if local_root_dir:
        local_client = getattr(import_module("dpone.storage.local"), "LocalObjectStorageClient")
        return ObjectArtifactStore(local_client(str(local_root_dir)))
    connection_id = options.get("connection_id")
    if not connection_id:
        raise SystemExit("--connection-id or --local-root-dir is required")
    resolver_module = import_module("dpone.runtime.object_storage_connection_resolver")
    models_module = import_module("dpone.runtime.object_storage_access_models")
    client = resolver_module.ObjectStorageConnectionResolver().build_client(
        ref=models_module.ObjectStorageConnectionRef(
            connection_type=str(options.get("connection_type") or "env"),
            connection_id=str(connection_id),
        ),
        uri=_storage_uri().parse(uri),
    )
    return ObjectArtifactStore(client)


def _storage_uri() -> type[ObjectStorageUri]:
    return getattr(import_module("dpone.storage"), "ObjectStorageUri")


def _uri(value: str | ObjectStorageUri) -> ObjectStorageUri:
    storage_uri = _storage_uri()
    return value if isinstance(value, storage_uri) else storage_uri.parse(str(value))


def _release_marker(git_sha: str, *, status: str) -> dict[str, object]:
    return {
        "kind": RELEASE_MARKER_SCHEMA,
        "schema_version": "1",
        "git_sha": git_sha,
        "status": status,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),  # noqa: UP017
    }


def _publish_report(
    *,
    git_sha: str,
    uri_prefix: str,
    index: dict[str, Any] | None = None,
    pack_count: int = 0,
    dag_spec_count: int = 0,
    blockers: tuple[str, ...] = (),
) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_pack_publish",
        "schema_version": "1",
        "git_sha": git_sha,
        "uri_prefix": uri_prefix,
        "pack_count": pack_count,
        "dag_spec_count": dag_spec_count,
        "index": index or {},
        "blockers": list(blockers),
        "passed": not blockers,
    }


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


__all__ = [
    "AirflowPackIndexBuilder",
    "AirflowPackPublisher",
    "ObjectArtifactStore",
    "build_object_artifact_store",
]
