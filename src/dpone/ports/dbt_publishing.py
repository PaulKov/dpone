"""Narrow infrastructure ports used by the dbt runtime service."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.dbt_publishing import (
    DbtCredentialVersion,
    DbtExecutionEvidence,
    DbtProfileSpec,
    DbtPublishingError,
    DbtPublishIssue,
    DbtSqlServerRuntimePolicy,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtCompileReport


@dataclass(frozen=True, slots=True)
class DbtCommandResult:
    """Secret-free bounded output and the unmodified process exit code."""

    exit_code: int
    stdout: str = field(default="", repr=False)
    stderr: str = field(default="", repr=False)
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise _execution_error("dbt exit code must be an integer")
        if not isinstance(self.stdout, str) or not isinstance(self.stderr, str):
            raise _execution_error("dbt output must be text")
        if not isinstance(self.stdout_truncated, bool) or not isinstance(self.stderr_truncated, bool):
            raise _execution_error("dbt output truncation flags must be boolean")


@dataclass(frozen=True, slots=True)
class RenderedDbtProfile:
    """Private profile bytes plus safe evidence and output-redaction material."""

    content: bytes = field(repr=False)
    credential_versions: tuple[DbtCredentialVersion, ...]
    logical_target_sha256: str
    redaction_values: tuple[str, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        content = bytes(self.content)
        versions = tuple(self.credential_versions)
        redactions = tuple(self.redaction_values)
        if not content or any(not isinstance(item, DbtCredentialVersion) for item in versions):
            raise _profile_error("rendered dbt profile is invalid")
        if (
            not isinstance(self.logical_target_sha256, str)
            or not self.logical_target_sha256.startswith("sha256:")
            or len(self.logical_target_sha256) != 71
        ):
            raise _profile_error("rendered dbt target identity is invalid")
        if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 4096 for item in redactions):
            raise _profile_error("profile redaction values are invalid")
        object.__setattr__(self, "content", content)
        object.__setattr__(self, "credential_versions", versions)
        object.__setattr__(
            self,
            "redaction_values",
            tuple(sorted(set(redactions), key=len, reverse=True)),
        )


@dataclass(frozen=True, slots=True)
class DbtInstalledToolchain:
    """Observed runtime versions, returned without importing dbt in domain code."""

    dbt_core_version: str
    adapter_name: str
    adapter_version: str


@dataclass(frozen=True, slots=True)
class DbtExecutionOutcome:
    exit_code: int
    evidence: DbtExecutionEvidence

    @property
    def passed(self) -> bool:
        return self.evidence.status == "passed"


class DbtCliExecutionEvidence(Protocol):
    """Public evidence projection consumed by the dbt CLI adapter."""

    def to_dict(self) -> dict[str, object]: ...


class DbtCliExecutionOutcome(Protocol):
    """Narrow runtime result consumed by the shell-free dbt CLI adapter."""

    exit_code: int
    evidence: DbtCliExecutionEvidence


class DbtPublishCompiler(Protocol):
    """Build-plane compiler capability consumed by the dbt CLI adapter."""

    def build(
        self,
        manifest_path: str | Path,
        *,
        profiles_path: str | Path | None = None,
        require_contracts: bool = True,
        allow_empty: bool = False,
        model_selector: str | None = None,
    ) -> DbtCompileReport: ...


class DbtPublishArtifactWriter(Protocol):
    """Immutable-publication capability consumed by the dbt CLI adapter."""

    def write(
        self,
        report: DbtCompileReport,
        output_dir: str | Path,
        *,
        project_root: str | Path | None = None,
        environment: str = "dev",
    ) -> DbtCompileReport: ...


class DbtReleaseMaterializationResult(Protocol):
    """Identity returned after immutable release materialization."""

    release_id: str


class DbtReleaseMaterializer(Protocol):
    """Immutable release-cache capability consumed by the dbt CLI adapter."""

    def materialize(
        self,
        *,
        compiled_root: Path,
        cache_root: Path,
    ) -> DbtReleaseMaterializationResult: ...


class DbtPublishComposition(Protocol):
    """Composition boundary required by dbt authoring commands."""

    def build_dbt_publish_compiler(
        self,
        *,
        root: Path,
        require_certified_routes: bool,
    ) -> DbtPublishCompiler: ...

    def build_dbt_artifact_writer(
        self,
        *,
        dbt_profiles_dir: Path | None,
    ) -> DbtPublishArtifactWriter: ...

    def build_dbt_release_materializer(self) -> DbtReleaseMaterializer: ...


class DbtProjectPolicyValidator(Protocol):
    """Validate one authoring project without performing dbt subprocess work."""

    def validate_manifest(
        self,
        manifest_path: str | Path,
    ) -> tuple[DbtPublishIssue, ...]: ...

    def validate_root(
        self,
        project_root: Path,
    ) -> tuple[DbtPublishIssue, ...]: ...


class DbtToolchainInspector(Protocol):
    """Inspect installed distributions before credentials or database I/O."""

    def inspect(self, adapter_name: str) -> DbtInstalledToolchain: ...


class DbtProfileRenderer(Protocol):
    """Resolve runtime credentials and render one in-memory dbt profile."""

    def render(
        self,
        profile: DbtProfileSpec,
        adapter_runtime: DbtSqlServerRuntimePolicy,
    ) -> RenderedDbtProfile: ...


class DbtProfileStore(Protocol):
    """Materialize profile bytes for only the lifetime of one invocation."""

    def materialize(self, content: bytes) -> AbstractContextManager[Path]: ...


class DbtCommandRunner(Protocol):
    """Execute one already-tokenized dbt command without a shell."""

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult: ...


class DbtRunResultsReader(Protocol):
    """Read one bounded, no-follow dbt run-results artifact."""

    def read(self, path: Path, *, root: Path, max_bytes: int) -> Mapping[str, Any]: ...


class DbtRunResultsSchemaValidator(Protocol):
    """Validate one parsed artifact against the pinned official schema."""

    def validate(
        self,
        payload: Mapping[str, Any],
        *,
        version: int,
    ) -> tuple[object, ...]: ...


class DbtManifestSchemaDiagnostic(Protocol):
    """One bounded manifest diagnostic classified by severity."""

    severity: str


class DbtManifestSchemaValidator(Protocol):
    """Validate one parsed manifest against the pinned official schema."""

    def validate(
        self,
        payload: Mapping[str, Any],
        *,
        version: int,
    ) -> tuple[DbtManifestSchemaDiagnostic, ...]: ...


class DbtExecutionEvidenceWriter(Protocol):
    """Persist strict secret-free execution evidence."""

    def write(self, evidence: DbtExecutionEvidence) -> Path: ...


def _execution_error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_EXECUTION_FAILED", message)


def _profile_error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_PROFILE_INVALID", message)


__all__ = [
    "DbtCommandRunner",
    "DbtCommandResult",
    "DbtExecutionEvidenceWriter",
    "DbtExecutionOutcome",
    "DbtInstalledToolchain",
    "DbtManifestSchemaDiagnostic",
    "DbtManifestSchemaValidator",
    "DbtCliExecutionOutcome",
    "DbtPublishComposition",
    "DbtProfileRenderer",
    "DbtProfileStore",
    "DbtProjectPolicyValidator",
    "DbtRunResultsReader",
    "DbtRunResultsSchemaValidator",
    "DbtToolchainInspector",
    "RenderedDbtProfile",
]
