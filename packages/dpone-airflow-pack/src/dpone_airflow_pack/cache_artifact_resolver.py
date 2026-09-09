"""Resolver for ``cached://`` references declared by deployment indexes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dpone_airflow_pack.deployment_index_contract import (
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_INDEX_BYTES,
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    AirflowIndexArtifact,
    _is_canonical_sha256_digest,
    load_airflow_deployment_index,
    verify_airflow_index_artifact,
)

_CACHE_REF_PIN_KEYS = frozenset({"release", "deployment"})


@dataclass(frozen=True)
class CacheResolution:
    """Resolved cache artifact for a pinned dpone deployment context."""

    kind: str
    logical_id: str
    release_id: str
    deployment_id: str
    artifact_ref: str
    resolved_path: Path
    cache_root: Path
    sha256: str
    bytes: int
    provenance: str = "local_cache"

    def to_jsonable(self) -> dict[str, str | int]:
        return {
            "kind": self.kind,
            "logical_id": self.logical_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "artifact_ref": self.artifact_ref,
            "resolved_path": self.resolved_path.as_posix(),
            "sha256": self.sha256,
            "bytes": self.bytes,
            "provenance": self.provenance,
        }


class CacheResolver:
    """Parse-safe resolver for ``cached://`` references from one deployment index."""

    def __init__(self, index: AirflowDeploymentIndex) -> None:
        self._index = index

    @classmethod
    def from_index(
        cls,
        index_path: str | Path,
        *,
        cache_root: str | Path | None = None,
        max_index_bytes: int = DEFAULT_MAX_INDEX_BYTES,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
    ) -> CacheResolver:
        """Load one deployment index and return a resolver bound to it."""

        return cls(
            load_airflow_deployment_index(
                index_path,
                cache_root=cache_root,
                max_index_bytes=max_index_bytes,
                max_artifact_bytes=max_artifact_bytes,
            )
        )

    @property
    def release_id(self) -> str:
        return self._index.release_id

    @property
    def deployment_id(self) -> str:
        return self._index.deployment_id

    def resolve(self, cached_ref: str) -> CacheResolution:
        """Resolve one ``cached://dags`` or ``cached://workloads`` reference."""

        kind, logical_id, pins = _parse_cached_ref(cached_ref)
        self._validate_pins(pins, cached_ref)
        artifact = self._lookup(kind, logical_id, cached_ref)
        artifact_bytes = verify_airflow_index_artifact(
            artifact,
            max_artifact_bytes=self._index.max_artifact_bytes,
        )
        return CacheResolution(
            kind=kind,
            logical_id=logical_id,
            release_id=self._index.release_id,
            deployment_id=self._index.deployment_id,
            artifact_ref=artifact.artifact_ref,
            resolved_path=artifact.path,
            cache_root=self._index.cache_root,
            sha256=artifact.sha256,
            bytes=artifact_bytes,
        )

    def _validate_pins(self, pins: Mapping[str, str], cached_ref: str) -> None:
        expected = {"release": self._index.release_id, "deployment": self._index.deployment_id}
        for key, value in expected.items():
            pinned = pins.get(key)
            if pinned is not None and pinned != value:
                raise AirflowDeploymentIndexError(
                    "DPONE_CACHE_REF_PIN_MISMATCH",
                    f"cached ref {key} pin does not match deployment index",
                    path=cached_ref,
                )

    def _lookup(self, kind: str, logical_id: str, cached_ref: str) -> AirflowIndexArtifact:
        artifacts = self._index.workload_packs if kind == "workload" else self._index.dag_specs
        for artifact in artifacts:
            if artifact.id == logical_id:
                return artifact
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_NOT_FOUND",
            f"cached {kind} ref is not listed in deployment index",
            path=cached_ref,
        )


def _parse_cached_ref(cached_ref: str) -> tuple[str, str, Mapping[str, str]]:
    parsed = urlparse(cached_ref)
    if parsed.scheme != "cached":
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_INVALID",
            "cached references must use the cached:// scheme",
            path=cached_ref,
        )
    pins = _parse_cached_ref_pins(parsed.query, cached_ref)
    if parsed.netloc in {"dags", "workloads"}:
        logical_id = _single_logical_id(parsed.path.lstrip("/"), cached_ref)
        kind = "dag" if parsed.netloc == "dags" else "workload"
        return kind, logical_id, pins
    if parsed.netloc == "deployments":
        segments = parsed.path.lstrip("/").split("/")
        if len(segments) != 3 or segments[1] not in {"dags", "workloads"}:
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_INVALID",
                "deployment-scoped cached references must use "
                "cached://deployments/<deployment_id>/{dags|workloads}/<id>",
                path=cached_ref,
            )
        deployment_id = _single_logical_id(segments[0], cached_ref)
        if not _is_canonical_sha256_digest(deployment_id):
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_PIN_INVALID",
                "deployment-scoped cached reference deployment id must be a sha256 digest",
                path=cached_ref,
            )
        logical_id = _single_logical_id(segments[2], cached_ref)
        query_deployment = pins.get("deployment")
        if query_deployment is not None and query_deployment != deployment_id:
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_PIN_MISMATCH",
                "cached ref deployment query pin does not match deployment-scoped path",
                path=cached_ref,
            )
        pins = {**pins, "deployment": deployment_id}
        kind = "dag" if segments[1] == "dags" else "workload"
        return kind, logical_id, pins
    raise AirflowDeploymentIndexError(
        "DPONE_CACHE_REF_INVALID",
        "cached references must use cached://dags/<id>, cached://workloads/<id>, "
        "or cached://deployments/<deployment_id>/{dags|workloads}/<id>",
        path=cached_ref,
    )


def _single_logical_id(value: str, cached_ref: str) -> str:
    logical_id = value.strip()
    if not logical_id or "/" in logical_id or logical_id in {".", ".."}:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_INVALID",
            "cached reference must contain one logical id segment",
            path=cached_ref,
        )
    return logical_id


def _parse_cached_ref_pins(query: str, cached_ref: str) -> Mapping[str, str]:
    parsed_pins = parse_qs(query, keep_blank_values=True)
    duplicate_pins = sorted(key for key, values in parsed_pins.items() if len(values) > 1)
    if duplicate_pins:
        duplicate = ", ".join(duplicate_pins)
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_PIN_INVALID",
            f"cached reference query pins must be unique; got duplicates: {duplicate}",
            path=cached_ref,
        )
    pins = {key: values[-1] for key, values in parsed_pins.items() if values}
    unknown_pins = sorted(set(pins) - _CACHE_REF_PIN_KEYS)
    if unknown_pins:
        unknown = ", ".join(unknown_pins)
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_PIN_INVALID",
            f"cached reference supports only release and deployment pins; got: {unknown}",
            path=cached_ref,
        )
    malformed_pins = sorted(key for key, value in pins.items() if not _is_canonical_sha256_digest(value))
    if malformed_pins:
        malformed = ", ".join(malformed_pins)
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_PIN_INVALID",
            f"cached reference release/deployment pins must be sha256 digests; got malformed: {malformed}",
            path=cached_ref,
        )
    return pins


__all__ = ["CacheResolution", "CacheResolver"]
