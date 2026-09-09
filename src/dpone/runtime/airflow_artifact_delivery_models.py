"""Value objects for pinned Airflow artifact publication and materialization."""

from __future__ import annotations

import re
import stat
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

DEFAULT_MAX_OBJECT_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_TOTAL_BYTES = 512 * 1024 * 1024
PUBLICATION_MODES = frozenset({"compatible", "exact"})
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SECRET_SHAPED_NAME = re.compile(
    r"^(?:"
    r"sk_(?:live|test|prod)_[A-Za-z0-9_-]{8,}"
    r"|sk-(?:proj|svcacct)-[A-Za-z0-9_-]{8,}"
    r"|gh[pousr]_[A-Za-z0-9]{20,}"
    r"|github_pat_[A-Za-z0-9_]{20,}"
    r"|xox[baprs]-[A-Za-z0-9-]{16,}"
    r")$",
    re.IGNORECASE,
)


class AirflowArtifactDeliveryError(RuntimeError):
    """Stable delivery failure that is safe to map into ``dpone.error.v1``."""

    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class ArtifactFile:
    key: PurePosixPath
    local_path: Path
    size_bytes: int
    sha256: str
    completion_marker: bool = False
    source_root: Path | None = None


@dataclass(frozen=True, slots=True)
class ArtifactInventory:
    release: tuple[ArtifactFile, ...]
    deployment: tuple[ArtifactFile, ...]

    @property
    def objects(self) -> tuple[ArtifactFile, ...]:
        return self.release + self.deployment

    @property
    def total_bytes(self) -> int:
        return sum(item.size_bytes for item in self.objects)


@dataclass(frozen=True, slots=True)
class _BaseRequest:
    cache_root: Path
    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    max_object_bytes: int = DEFAULT_MAX_OBJECT_BYTES
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES

    def __post_init__(self) -> None:
        object.__setattr__(self, "cache_root", _normalized_cache_root(self.cache_root))
        if not is_canonical_sha256_digest(self.release_id):
            raise AirflowArtifactDeliveryError(
                "DPONE_RELEASE_ID_INVALID", "release_id must be a canonical sha256 digest"
            )
        if not is_canonical_sha256_digest(self.deployment_id):
            raise AirflowArtifactDeliveryError(
                "DPONE_DEPLOYMENT_ID_INVALID",
                "deployment_id must be a canonical sha256 digest",
            )
        if not is_safe_artifact_delivery_name(self.environment):
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "environment must be a safe logical name",
            )
        if not is_safe_artifact_delivery_name(self.artifact_registry_ref):
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "artifact_registry_ref must be a non-mutable logical reference",
            )
        if self.max_object_bytes <= 0 or self.max_total_bytes <= 0:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "artifact size limits must be positive",
            )
        if self.max_object_bytes > self.max_total_bytes:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "max_object_bytes must not exceed max_total_bytes",
            )

    @property
    def release_dir_name(self) -> str:
        return _digest_dir(self.release_id)

    @property
    def deployment_dir_name(self) -> str:
        return _digest_dir(self.deployment_id)


@dataclass(frozen=True, slots=True)
class PublishRequest(_BaseRequest):
    """Exact local projection and immutable remote publication policy."""

    attestation_bundle_path: Path | None = None
    registry_scope_id: str | None = None
    publication_mode: str = "compatible"

    def __post_init__(self) -> None:
        _BaseRequest.__post_init__(self)
        if self.attestation_bundle_path is not None:
            object.__setattr__(
                self,
                "attestation_bundle_path",
                Path(self.attestation_bundle_path).absolute(),
            )
        if self.registry_scope_id is not None and not is_canonical_sha256_digest(self.registry_scope_id):
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
                "registry_scope_id must be a canonical sha256 digest",
            )
        if self.publication_mode not in PUBLICATION_MODES:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "publication_mode must be compatible or exact",
            )


@dataclass(frozen=True, slots=True)
class PublishedArtifactDescriptor:
    """Exact identity of one remotely verified publication object."""

    object_key: str
    sha256: str
    bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PublicationCommitment:
    """Root artifacts independently verified through the registry read port."""

    release: PublishedArtifactDescriptor
    deployment: PublishedArtifactDescriptor
    airflow_index: PublishedArtifactDescriptor
    registry_scope_id: str
    projection_verified: bool = True
    verification_mode: str = "remote_readback_sha256"
    schema: str = "dpone.airflow-publication-commitment.v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "verification_mode": self.verification_mode,
            "registry_scope_id": self.registry_scope_id,
            "projection_verified": self.projection_verified,
            "release": self.release.to_dict(),
            "deployment": self.deployment.to_dict(),
            "airflow_index": self.airflow_index.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class MaterializeRequest(_BaseRequest):
    """Exact remote projection and bounded local materialization policy."""


@dataclass(frozen=True, slots=True)
class PublishReport:
    status: str
    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    created_objects: int
    existing_equal_objects: int
    published_release: bool
    published_deployment: bool
    verified_objects: int = 0
    publication_commitment: PublicationCommitment | None = None
    publication_mode: str = "compatible"

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema": "dpone.airflow-artifact-publish.v1",
            "status": self.status,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "artifact_registry_ref": self.artifact_registry_ref,
            "created_objects": self.created_objects,
            "existing_equal_objects": self.existing_equal_objects,
            "published_release": self.published_release,
            "published_deployment": self.published_deployment,
            "errors": [],
        }
        if self.publication_mode == "compatible":
            return payload
        if self.publication_commitment is None:
            raise AirflowArtifactDeliveryError(
                "DPONE_EXACT_PUBLICATION_COMMITMENT_MISSING",
                "exact publication completed without a remote read-back commitment",
            )
        return {
            **payload,
            "schema": "dpone.airflow-artifact-publish.v2",
            "verified_objects": self.verified_objects,
            "publication_commitment": self.publication_commitment.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class MaterializeReport:
    status: str
    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    downloaded_objects: int
    downloaded_bytes: int
    local_release_state: str
    local_deployment_state: str
    projection_verified: bool
    activated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"schema": "dpone.airflow-cache-materialize.v1", **asdict(self), "errors": []}


def _digest_dir(value: str) -> str:
    return value.replace(":", "-", 1)


def _normalized_cache_root(value: Path) -> Path:
    lexical = Path(value).absolute()
    try:
        mode = lexical.lstat().st_mode
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "artifact delivery cache root could not be inspected safely",
        ) from exc
    else:
        if stat.S_ISLNK(mode):
            raise AirflowArtifactDeliveryError(
                "DPONE_CACHE_PATH_ESCAPE",
                "artifact delivery cache root must not be a symlink",
            )
    try:
        return lexical.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "artifact delivery cache root could not be normalized safely",
        ) from exc


def is_safe_artifact_delivery_name(value: str) -> bool:
    return (
        bool(_SAFE_NAME.fullmatch(value))
        and value.lower() not in {"current", "latest"}
        and not _SECRET_SHAPED_NAME.fullmatch(value)
    )


def canonical_digest_or_none(value: str) -> str | None:
    return value if is_canonical_sha256_digest(value) else None


__all__ = [
    "ArtifactFile",
    "ArtifactInventory",
    "AirflowArtifactDeliveryError",
    "canonical_digest_or_none",
    "DEFAULT_MAX_OBJECT_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "is_safe_artifact_delivery_name",
    "MaterializeReport",
    "MaterializeRequest",
    "PublicationCommitment",
    "PublishReport",
    "PublishRequest",
    "PublishedArtifactDescriptor",
]
