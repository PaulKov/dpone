"""Authenticated invocation environment records; decoding does not qualify a runtime.

An authorized bootstrap produces these records only after policy membership,
actual runtime inventory and qualification evidence have been verified. Root
identities describe directories owned and held by that bootstrap, never authority
obtained from an arbitrary path in a document.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from dpone.contracts.dbt_toolchain import DbtToolchainContract
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError


def _references(*values: OriginalRef) -> None:
    for value in values:
        if type(value) is not OriginalRef:
            raise NativeSourceCustodyError("environment requires exact original references")
        value.__post_init__()


def _label(value: str) -> None:
    if type(value) is not str or not value or value != value.strip() or any(ord(c) < 32 for c in value):
        raise NativeSourceCustodyError("environment identity must be a nonempty canonical label")


@dataclass(frozen=True, slots=True)
class TrustedDbtToolchain:
    """Unmodified dbt contract plus independently authenticated runtime inventory."""

    schema: Literal["dpone.trusted-dbt-toolchain.v1"]
    dbt_contract: DbtToolchainContract
    runtime_identity: OriginalRef

    def __post_init__(self) -> None:
        if self.schema != "dpone.trusted-dbt-toolchain.v1" or type(self.schema) is not str:
            raise NativeSourceCustodyError("unsupported trusted toolchain schema")
        if type(self.dbt_contract) is not DbtToolchainContract:
            raise NativeSourceCustodyError("trusted toolchain requires the existing dbt contract")
        for value in self.dbt_contract.to_dict().values():
            _label(value)
        _references(self.runtime_identity)


@dataclass(frozen=True, slots=True)
class TrustedDbtQualification:
    """Generation-local attestation referencing retained upstream qualification.

    Evidence membership is structural here. A nonempty list, by itself, is never
    proof that a provider, driver, image or execution route was qualified.
    """

    schema: Literal["dpone.trusted-dbt-qualification.v1"]
    policy_id: str
    profile: OriginalRef
    toolchain: OriginalRef
    command_plan: OriginalRef
    evidence: tuple[OriginalRef, ...]

    def __post_init__(self) -> None:
        if self.schema != "dpone.trusted-dbt-qualification.v1" or type(self.schema) is not str:
            raise NativeSourceCustodyError("unsupported trusted qualification schema")
        _label(self.policy_id)
        if type(self.evidence) is not tuple or not self.evidence:
            raise NativeSourceCustodyError("qualification requires retained evidence references")
        _references(self.profile, self.toolchain, self.command_plan, *self.evidence)
        if len(set(self.evidence)) != len(self.evidence):
            raise NativeSourceCustodyError("qualification evidence references must be unique")


@dataclass(frozen=True, slots=True)
class TrustedDbtOwnedRoot:
    """Actual bootstrap-owned directory identity, with no serialized path grant."""

    schema: Literal["dpone.trusted-dbt-owned-root.v1"]
    role: Literal["PROJECT", "OUTPUT", "PROFILE"]
    executor_invocation_id: UUID
    root_id: str
    device: int
    inode: int
    content_inventory: OriginalRef | None

    def __post_init__(self) -> None:
        if self.schema != "dpone.trusted-dbt-owned-root.v1" or type(self.schema) is not str:
            raise NativeSourceCustodyError("unsupported trusted owned-root schema")
        if type(self.role) is not str or self.role not in {"PROJECT", "OUTPUT", "PROFILE"}:
            raise NativeSourceCustodyError("unsupported trusted owned-root role")
        if type(self.executor_invocation_id) is not UUID:
            raise NativeSourceCustodyError("owned root requires an exact invocation UUID")
        _label(self.root_id)
        if any(type(value) is not int or value < 0 for value in (self.device, self.inode)):
            raise NativeSourceCustodyError("owned root identity requires exact nonnegative integers")
        if self.role == "PROJECT":
            if self.content_inventory is None:
                raise NativeSourceCustodyError("project root requires authenticated content inventory")
            _references(self.content_inventory)
        elif self.content_inventory is not None:
            raise NativeSourceCustodyError("writable roots cannot claim immutable project content")
