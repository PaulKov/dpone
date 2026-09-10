"""Bounded documentary plan values, without artifact or physical observation.

Names and digests describe expected objects. Only later protected capabilities
can reopen originals, resolve actual participants and establish source bounds.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, fields
from typing import Any

from dpone.contracts.composition_physical_identity import composition_physical_guard_id
from dpone.contracts.nonproduction_scope import (
    MAX_DOCUMENT_BYTES,
    NonproductionAuthorityError,
    digest,
    exact_fields,
    sequence,
    text,
    uuid_text,
)


@dataclass(frozen=True, slots=True)
class NonproductionPlanOriginal:
    """An expected bounded metadata original, never independently read here."""

    path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        text(self.path)
        digest(self.sha256)
        if any(
            part in {"", ".", ".."} or re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None for part in self.path.split("/")
        ):
            raise NonproductionAuthorityError("plan_originals")
        if type(self.size_bytes) is not int or not 1 <= self.size_bytes <= MAX_DOCUMENT_BYTES:
            raise NonproductionAuthorityError("plan_originals")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> NonproductionPlanOriginal:
        return cls(**exact_fields(value, {field.name for field in fields(cls)}))


@dataclass(frozen=True, slots=True)
class NonproductionPlanObject:
    """Exact lexical object and expected subject; no new physical formula."""

    object_id: str
    connector: str
    service_id: str
    physical_subject_sha256: str
    subject_sha256: str
    object_kind: str
    qualified_name: tuple[str, ...]

    def __post_init__(self) -> None:
        text(self.object_id)
        uuid_text(self.service_id)
        digest(self.physical_subject_sha256)
        digest(self.subject_sha256)
        if text(self.connector) not in {"postgres", "mssql", "clickhouse"}:
            raise NonproductionAuthorityError("plan_objects")
        kinds = {"table", "enum", "sequence"} if self.connector == "postgres" else {"table"}
        if text(self.object_kind) not in kinds:
            raise NonproductionAuthorityError("plan_objects")
        if type(self.qualified_name) is not tuple or len(self.qualified_name) != (
            2 if self.connector == "clickhouse" else 3
        ):
            raise NonproductionAuthorityError("plan_objects")
        for part in self.qualified_name:
            if (
                re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text(part, maximum=63 if self.connector == "postgres" else 128))
                is None
            ):
                raise NonproductionAuthorityError("plan_objects")

    @property
    def domain(self) -> tuple[str, str, str]:
        self.__post_init__()
        return self.connector, self.service_id, self.physical_subject_sha256

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (*self.domain, self.subject_sha256)

    @property
    def guard_id(self) -> str:
        self.__post_init__()
        return composition_physical_guard_id(
            connector=self.connector, service_id=self.service_id, physical_subject_sha256=self.physical_subject_sha256
        )

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return {**asdict(self), "qualified_name": list(self.qualified_name)}

    @classmethod
    def from_dict(cls, value: object) -> NonproductionPlanObject:
        body = exact_fields(value, {field.name for field in fields(cls)})
        return cls(**dict(body, qualified_name=tuple(sequence(body["qualified_name"], maximum=3))))


@dataclass(frozen=True, order=True, slots=True)
class NonproductionPlanEffect:
    """One directional declared effect; purpose never changes access."""

    object_id: str
    access: str
    purpose: str

    def __post_init__(self) -> None:
        text(self.object_id)
        if text(self.access) not in {"read", "write"} or text(self.purpose) not in {
            "fixture",
            "source",
            "target",
            "helper",
            "staging",
            "state",
        }:
            raise NonproductionAuthorityError("plan_item")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> NonproductionPlanEffect:
        return cls(**exact_fields(value, {field.name for field in fields(cls)}))


@dataclass(frozen=True, slots=True)
class NonproductionSourceBound:
    """Original calculation obligations, without claimed computed maxima."""

    source_object_id: str
    accounting_profile: str
    input_original: NonproductionPlanOriginal
    projection_original: NonproductionPlanOriginal
    derivation_original: NonproductionPlanOriginal

    def __post_init__(self) -> None:
        text(self.source_object_id)
        if text(self.accounting_profile) not in {"mssql_bcp_native_file", "postgres_copy_payload"}:
            raise NonproductionAuthorityError("plan_profile")
        for original in (self.input_original, self.projection_original, self.derivation_original):
            if type(original) is not NonproductionPlanOriginal:
                raise NonproductionAuthorityError("plan_originals")
            original.__post_init__()

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> NonproductionSourceBound:
        body = exact_fields(value, {field.name for field in fields(cls)})
        return cls(
            body["source_object_id"],
            body["accounting_profile"],
            NonproductionPlanOriginal.from_dict(body["input_original"]),
            NonproductionPlanOriginal.from_dict(body["projection_original"]),
            NonproductionPlanOriginal.from_dict(body["derivation_original"]),
        )
