"""Closed common scope for the isolated synthetic authority family.

Scope describes every read/write effect across the complete parent. Digests and
labels do not prove physical enrollment, isolation, route support or permission;
protected admission must independently reconstruct and compare this exact scope.
No field here changes physical guard identity or existing production contracts.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

PURPOSE = "synthetic_composition_validation"
TRUST_TIER = "non_production"
MAX_DOCUMENT_BYTES = 1024 * 1024


class NonproductionAuthorityError(ValueError):
    """Stable, value-free error; never carry credential or verifier output text."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"DPONE_NONPRODUCTION_AUTHORITY_INVALID: {reason}")


def exact_fields(value: object, expected: set[str]) -> dict[str, Any]:
    """Require an ordinary closed object, with no coercion of keys or values."""
    if type(value) is not dict or set(value) != expected:
        raise NonproductionAuthorityError("fields")
    return value


def canonical_document(value: dict[str, Any]) -> bytes:
    """Use the existing finite UTF-8 canonicalizer with a fixed transport bound."""
    try:
        raw = canonical_json_bytes(value)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise NonproductionAuthorityError("document") from None
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise NonproductionAuthorityError("document_budget")
    return raw


def document_sha256(value: dict[str, Any]) -> str:
    """Hash exact canonical bytes, with no legacy path/string normalization."""
    return "sha256:" + hashlib.sha256(canonical_document(value)).hexdigest()


def parse_document(raw: bytes, schema: str) -> dict[str, Any]:
    """Reject duplicate/unknown schema, noncanonical bytes and oversized input."""
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_DOCUMENT_BYTES:
        raise NonproductionAuthorityError("document_budget")
    try:
        body = strict_json_object(raw)
        if body.get("schema") != schema or canonical_document(body) != raw:
            raise NonproductionAuthorityError("document_identity")
        return body
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise NonproductionAuthorityError("document") from None


def digest(value: object) -> str:
    if type(value) is not str or not is_canonical_sha256_digest(value):
        raise NonproductionAuthorityError("digest")
    return value


def text(value: object, *, maximum: int = 512) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(c) < 32 for c in value)
    ):
        raise NonproductionAuthorityError("text")
    return value


def uuid_text(value: object, *, version4: bool = False) -> str:
    try:
        if (
            type(value) is not str
            or str(UUID(value)) != value
            or UUID(value).int == 0
            or (version4 and UUID(value).version != 4)
        ):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise NonproductionAuthorityError("uuid") from None
    return value


def repository(value: object) -> str:
    """Require one credential-free HTTPS repository spelling, with no URL options."""
    value = text(value)
    try:
        parsed = urlsplit(value)
        parts = parsed.path.split("/")[1:]
        if (
            not value.startswith("https://")
            or parsed.scheme != "https"
            or parsed.netloc != parsed.hostname
            or not parsed.hostname
            or len(parsed.hostname) > 253
            or any(
                re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None
                for label in parsed.hostname.split(".")
            )
            or parsed.query
            or parsed.fragment
            or "?" in value
            or "#" in value
            or len(parts) < 2
            or any(part in {"", ".", ".."} or re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None for part in parts)
        ):
            raise ValueError
    except ValueError:
        raise NonproductionAuthorityError("source_repository") from None
    return value


def sequence(value: object, *, maximum: int, allow_empty: bool = False) -> list[Any]:
    if type(value) is not list or not (0 if allow_empty else 1) <= len(value) <= maximum:
        raise NonproductionAuthorityError("array")
    return value


def ordered(values: tuple[Any, ...], *, maximum: int, allow_empty: bool = False) -> None:
    if type(values) is not tuple or not (0 if allow_empty else 1) <= len(values) <= maximum:
        raise NonproductionAuthorityError("closure")
    if values != tuple(sorted(set(values))):
        raise NonproductionAuthorityError("closure")


@dataclass(frozen=True, order=True, slots=True)
class NonproductionRouteDimensions:
    """Exact six-dimensional transfer intent; this is not route qualification."""

    source: str
    sink: str
    strategy: str
    transport: str
    schema_evolution: str
    airflow_runtime_mode: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", text(value, maximum=64)) is None:
                raise NonproductionAuthorityError("route_dimension")
        if (self.source, self.sink) not in {
            ("mssql", "clickhouse"),
            ("postgres", "mssql"),
        } or self.strategy != "full_refresh":
            raise NonproductionAuthorityError("route_scope")

    @classmethod
    def from_dict(cls, value: object) -> NonproductionRouteDimensions:
        return cls(
            **exact_fields(
                value, {"source", "sink", "strategy", "transport", "schema_evolution", "airflow_runtime_mode"}
            )
        )


@dataclass(frozen=True, order=True, slots=True)
class NonproductionParticipant:
    """Stable physical subject and sorted effect digests; aliases are not IDs.

    Read and write scopes include helpers, staging and state effects. A physical
    relation may occur in both sets. Enrollment must independently prove newly
    isolated synthetic participants, including single-node ClickHouse topology.
    """

    connector: str
    service_id: str
    physical_subject_sha256: str
    read_relations: tuple[str, ...]
    write_relations: tuple[str, ...]

    def __post_init__(self) -> None:
        if text(self.connector) not in {"postgres", "mssql", "clickhouse"}:
            raise NonproductionAuthorityError("participant_connector")
        uuid_text(self.service_id)
        digest(self.physical_subject_sha256)
        for values in (self.read_relations, self.write_relations):
            if type(values) is not tuple:
                raise NonproductionAuthorityError("relation_scope")
            for value in values:
                digest(value)
            ordered(values, maximum=8192, allow_empty=True)
        if not self.read_relations and not self.write_relations:
            raise NonproductionAuthorityError("relation_scope")

    @classmethod
    def from_dict(cls, value: object) -> NonproductionParticipant:
        body = exact_fields(
            value, {"connector", "service_id", "physical_subject_sha256", "read_relations", "write_relations"}
        )
        return cls(
            **dict(
                body,
                **{
                    key: tuple(sequence(body[key], maximum=8192, allow_empty=True))
                    for key in ("read_relations", "write_relations")
                },
            )
        )


def require_participants(values: tuple[NonproductionParticipant, ...]) -> None:
    """Require the full cross-service union without duplicate physical subjects."""
    if type(values) is not tuple or any(type(row) is not NonproductionParticipant for row in values):
        raise NonproductionAuthorityError("participants")
    for row in values:
        row.__post_init__()
    ordered(values, maximum=256)
    identities = {(row.connector, row.service_id, row.physical_subject_sha256) for row in values}
    if len(identities) != len(values) or {row.connector for row in values} != {"postgres", "mssql", "clickhouse"}:
        raise NonproductionAuthorityError("participant_closure")
    if sum(len(row.read_relations) + len(row.write_relations) for row in values) > 8192:
        raise NonproductionAuthorityError("relation_budget")


@dataclass(frozen=True, slots=True)
class NonproductionScope:
    """One immutable complete campaign subject embedded in both grant phases.

    Both transfer families are mandatory even for a qualification grant. An
    individual qualification observation may select one route from this scope.
    Native dbt effects belong to participants, not a fabricated transfer route.
    Original pack verification must reconstruct every route/read/write mapping;
    this DTO cannot prove that claims match actual source or runtime effects.
    """

    purpose: str
    trust_tier: str
    source_repository: str
    source_commit: str
    fixture_sha256: str
    generator_sha256: str
    compilation_intent_sha256: str
    toolchain_sha256: str
    image_sha256: str
    campaign_id: str
    environment_id: str
    policy_sha256: str
    enrollment_sha256: str
    routes: tuple[NonproductionRouteDimensions, ...]
    participants: tuple[NonproductionParticipant, ...]

    def __post_init__(self) -> None:
        if (self.purpose, self.trust_tier) != (PURPOSE, TRUST_TIER):
            raise NonproductionAuthorityError("purpose")
        repository(self.source_repository)
        if re.fullmatch(r"[0-9a-f]{40}", text(self.source_commit, maximum=40)) is None:
            raise NonproductionAuthorityError("source_commit")
        for value in (
            self.fixture_sha256,
            self.generator_sha256,
            self.compilation_intent_sha256,
            self.toolchain_sha256,
            self.image_sha256,
            self.policy_sha256,
            self.enrollment_sha256,
        ):
            digest(value)
        uuid_text(self.campaign_id, version4=True)
        uuid_text(self.environment_id)
        if type(self.routes) is not tuple or any(type(row) is not NonproductionRouteDimensions for row in self.routes):
            raise NonproductionAuthorityError("routes")
        for route in self.routes:
            route.__post_init__()
        ordered(self.routes, maximum=64)
        if {(route.source, route.sink) for route in self.routes} != {("mssql", "clickhouse"), ("postgres", "mssql")}:
            raise NonproductionAuthorityError("route_closure")
        require_participants(self.participants)

    @classmethod
    def from_dict(cls, value: object) -> NonproductionScope:
        body = exact_fields(
            value,
            {
                "purpose",
                "trust_tier",
                "source_repository",
                "source_commit",
                "fixture_sha256",
                "generator_sha256",
                "compilation_intent_sha256",
                "toolchain_sha256",
                "image_sha256",
                "campaign_id",
                "environment_id",
                "policy_sha256",
                "enrollment_sha256",
                "routes",
                "participants",
            },
        )
        return cls(
            **dict(
                body,
                routes=tuple(
                    NonproductionRouteDimensions.from_dict(row) for row in sequence(body["routes"], maximum=64)
                ),
                participants=tuple(
                    NonproductionParticipant.from_dict(row) for row in sequence(body["participants"], maximum=256)
                ),
            )
        )

    def to_dict(self) -> dict[str, Any]:
        """Detached JSON-compatible values; common scope has no separate wire family."""
        self.__post_init__()
        return strict_json_object(canonical_document(asdict(self)))

    @property
    def scope_sha256(self) -> str:
        return document_sha256(self.to_dict())
