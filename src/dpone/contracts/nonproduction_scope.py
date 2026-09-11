"""Closed common scope for the isolated synthetic authority family.

Scope describes every read/write effect across the complete parent. Digests and
labels do not prove physical enrollment, isolation, route support or permission;
protected admission must independently reconstruct and compare this exact scope.
No field here changes physical guard identity or existing production contracts.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

from dpone.contracts.nonproduction_document import (
    MAX_DOCUMENT_BYTES as MAX_DOCUMENT_BYTES,
)
from dpone.contracts.nonproduction_document import (
    NonproductionAuthorityError as NonproductionAuthorityError,
)
from dpone.contracts.nonproduction_document import (
    canonical_document as canonical_document,
)
from dpone.contracts.nonproduction_document import (
    digest as digest,
)
from dpone.contracts.nonproduction_document import (
    document_sha256 as document_sha256,
)
from dpone.contracts.nonproduction_document import (
    exact_fields as exact_fields,
)
from dpone.contracts.nonproduction_document import (
    ordered as ordered,
)
from dpone.contracts.nonproduction_document import (
    parse_document as parse_document,
)
from dpone.contracts.nonproduction_document import (
    repository as repository,
)
from dpone.contracts.nonproduction_document import (
    sequence as sequence,
)
from dpone.contracts.nonproduction_document import (
    text as text,
)
from dpone.contracts.nonproduction_document import (
    uuid_text as uuid_text,
)
from dpone.contracts.strict_json import strict_json_object

PURPOSE = "synthetic_composition_validation"
TRUST_TIER = "non_production"


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
