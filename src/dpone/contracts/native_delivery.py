"""Native original acquisition and generation handles without dispatch authority."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import ClassVar
from uuid import UUID

from dpone.contracts.dbt_project_bundle import DbtProjectBundle
from dpone.contracts.dbt_source_inventory_binding import DbtReleaseSources
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_identity import OriginalRef


class NativeGenerationContractError(ValueError):
    """A generation admission value is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class GenerationReservation:
    """A retained generation and revision under an existing physical guard."""

    generation_id: UUID
    guard_epoch: int
    revision: int
    reservation: OriginalRef

    def __post_init__(self) -> None:
        if type(self.generation_id) is not UUID or type(self.reservation) is not OriginalRef:
            raise NativeGenerationContractError("reservation requires exact generation and original identity")
        self.reservation.__post_init__()
        for value in (self.guard_epoch, self.revision):
            if type(value) is not int or not 1 <= value <= 9223372036854775807:
                raise NativeGenerationContractError("reservation epoch/revision must be positive SQL bigint values")


@dataclass(frozen=True, slots=True)
class NativeOriginalsRefV1:
    """Exact original references and a local projection location, never authority.

    Projection paths stay local execution inputs. They are not serialized into
    a subject, policy fingerprint or durable source/delivery identity.
    """

    projection_root: Path
    release: OriginalRef
    deployment: OriginalRef
    pack: OriginalRef
    schema: ClassVar[str] = "dpone.native-originals-ref.v1"

    def __post_init__(self) -> None:
        _absolute_local_directory(self.projection_root)
        for reference in (self.release, self.deployment, self.pack):
            if type(reference) is not OriginalRef:
                raise ValueError("native original input requires exact references")
            reference.__post_init__()


@dataclass(frozen=True, slots=True)
class NativePolicyDocumentRef:
    """The complete policy member in a verified project bundle, without selfhash."""

    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        OriginalRef(self.path, self.sha256)
        if type(self.bytes) is not int or self.bytes <= 0:
            raise ValueError("policy document size must be an exact positive integer")

    def to_dict(self) -> dict[str, str | int]:
        """Return the closed intent member; no enclosing reference is included."""
        self.__post_init__()
        return {"path": self.path, "sha256": self.sha256, "bytes": self.bytes}


@dataclass(frozen=True, slots=True)
class ResolvedNativeOriginals:
    """Verified acquisition result; shape alone is not a live execution grant.

    The verifier must establish complete source/project/policy membership and
    active workspace authority. Its owned extraction directory must remain alive
    while this local value is consumed; no pathname reconstructs that ownership.
    """

    authority: DbtWorkspaceRuntimeAuthority
    sources: DbtReleaseSources
    project_bundle: DbtProjectBundle
    policy_document: bytes
    policy_sha256: str
    project_directory: Path

    def __post_init__(self) -> None:
        if type(self.authority) is not DbtWorkspaceRuntimeAuthority or type(self.sources) is not DbtReleaseSources:
            raise ValueError("native resolution requires existing workspace and source contracts")
        if self.authority.release_id != self.sources.release_id:
            raise ValueError("native sources differ from workspace release authority")
        if type(self.project_bundle) is not DbtProjectBundle:
            raise ValueError("native resolution requires an exact project bundle")
        self.project_bundle.__post_init__()
        if type(self.policy_document) is not bytes or not self.policy_document:
            raise ValueError("native resolution requires full nonempty policy bytes")
        if self.policy_sha256 != "sha256:" + sha256(self.policy_document).hexdigest():
            raise ValueError("native policy digest differs from the complete bytes")
        _absolute_local_directory(self.project_directory)


def _absolute_local_directory(value: Path) -> None:
    if not isinstance(value, Path) or not value.is_absolute() or ".." in value.parts:
        raise ValueError("native projection/project directory must be an absolute path without traversal")
