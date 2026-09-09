"""Content-addressed dbt project bundle contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from dpone.contracts.dbt_contract_validation import (
    contract_error,
    require_digest,
    require_non_negative,
    require_positive,
    require_relative,
    sha256_bytes,
)

DBT_PROJECT_BUNDLE_SCHEMA = "dpone.dbt-project-bundle.v1"
_BUNDLE_ERROR = "DPONE_DBT_BUNDLE_INVALID"


@dataclass(frozen=True, slots=True)
class DbtProjectBundleLimits:
    max_files: int = 20_000
    max_file_bytes: int = 64 * 1024 * 1024
    max_archive_bytes: int = 256 * 1024 * 1024
    max_extracted_bytes: int = 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in ("max_files", "max_file_bytes", "max_archive_bytes", "max_extracted_bytes"):
            require_positive(getattr(self, name), name, _BUNDLE_ERROR)


DEFAULT_DBT_PROJECT_BUNDLE_LIMITS = DbtProjectBundleLimits()


@dataclass(frozen=True, slots=True)
class DbtProjectFile:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        require_relative(self.path, "project file path", _BUNDLE_ERROR)
        require_digest(self.sha256, "project file sha256", _BUNDLE_ERROR)
        require_non_negative(self.bytes, "project file bytes", _BUNDLE_ERROR)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DbtProjectBundle:
    archive_sha256: str
    archive_bytes: int
    extracted_bytes: int
    files: tuple[DbtProjectFile, ...]
    schema: str = DBT_PROJECT_BUNDLE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_PROJECT_BUNDLE_SCHEMA:
            raise contract_error(_BUNDLE_ERROR, "dbt project bundle schema is invalid")
        require_digest(self.archive_sha256, "archive sha256", _BUNDLE_ERROR)
        require_positive(self.archive_bytes, "archive bytes", _BUNDLE_ERROR)
        require_non_negative(self.extracted_bytes, "extracted bytes", _BUNDLE_ERROR)
        files = tuple(self.files)
        paths = tuple(item.path for item in files if isinstance(item, DbtProjectFile))
        if len(paths) != len(files) or not files or paths != tuple(sorted(paths)) or len(set(paths)) != len(paths):
            raise contract_error(_BUNDLE_ERROR, "dbt project bundle inventory must be non-empty and sorted")
        if sum(item.bytes for item in files) != self.extracted_bytes:
            raise contract_error(_BUNDLE_ERROR, "dbt project bundle extracted size differs from inventory")
        object.__setattr__(self, "files", files)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "archive": {"format": "tar+gzip", "sha256": self.archive_sha256, "bytes": self.archive_bytes},
            "extracted_bytes": self.extracted_bytes,
            "files": [item.to_dict() for item in self.files],
        }


@dataclass(frozen=True, slots=True)
class DbtProjectBundleArtifact:
    bundle: DbtProjectBundle
    archive: bytes = field(repr=False)

    def __post_init__(self) -> None:
        archive = bytes(self.archive)
        if len(archive) != self.bundle.archive_bytes or sha256_bytes(archive) != self.bundle.archive_sha256:
            raise contract_error(_BUNDLE_ERROR, "dbt project archive differs from its descriptor")
        object.__setattr__(self, "archive", archive)


__all__ = [
    "DBT_PROJECT_BUNDLE_SCHEMA",
    "DEFAULT_DBT_PROJECT_BUNDLE_LIMITS",
    "DbtProjectBundle",
    "DbtProjectBundleArtifact",
    "DbtProjectBundleLimits",
    "DbtProjectFile",
]
