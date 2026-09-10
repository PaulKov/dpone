"""Closed finite fixture recipes and complete documentary object originals."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject, NonproductionPlanOriginal
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    ordered,
    parse_document,
    sequence,
    text,
)

_SCHEMA = "dpone.nonproduction-fixture-plan.v1"
BCP_PROFILE = "mssql_clickhouse_bcp_wide_v1"
PG_PROFILE = "postgres_mssql_wide_v1"
PG_SEQUENCES = ("c_bigserial_sequence", "c_identity_sequence", "c_serial_sequence", "c_smallserial_sequence")


@dataclass(frozen=True, slots=True)
class NonproductionFixture:
    """One reviewed recipe with explicit parameters and object bindings."""

    fixture_id: str
    profile: str
    recipe_original: NonproductionPlanOriginal
    parameters: tuple[tuple[str, int | bool], ...]
    bindings: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        text(self.fixture_id)
        if text(self.profile) not in {BCP_PROFILE, PG_PROFILE}:
            raise NonproductionAuthorityError("plan_profile")
        if type(self.recipe_original) is not NonproductionPlanOriginal:
            raise NonproductionAuthorityError("plan_originals")
        self.recipe_original.__post_init__()
        expected_path = (
            "tools/mssql_clickhouse_bcp_native_fixtures.py"
            if self.profile == BCP_PROFILE
            else "tests/integration/postgres/postgres_mssql_wide_fixtures.py"
        )
        if self.recipe_original.path != expected_path:
            raise NonproductionAuthorityError("plan_profile")
        for entries in (self.parameters, self.bindings):
            if type(entries) is not tuple or any(type(pair) is not tuple or len(pair) != 2 for pair in entries):
                raise NonproductionAuthorityError("plan_bindings")
            for key, _ in entries:
                text(key)
            ordered(tuple(key for key, _ in entries), maximum=7)
        parameter = "row_count" if self.profile == BCP_PROFILE else "watermark_key_3"
        if tuple(key for key, _ in self.parameters) != (parameter,):
            raise NonproductionAuthorityError("plan_profile")
        value = self.parameters[0][1]
        if self.profile == BCP_PROFILE:
            if type(value) is not int or not 0 <= value <= 100000:
                raise NonproductionAuthorityError("plan_profile")
        elif type(value) is not bool:
            raise NonproductionAuthorityError("plan_profile")
        required = {"source", "target"} if self.profile == BCP_PROFILE else {"source", "target", "enum", *PG_SEQUENCES}
        if {key for key, _ in self.bindings} != required:
            raise NonproductionAuthorityError("plan_bindings")
        for _, object_id in self.bindings:
            text(object_id)
        if len({value for _, value in self.bindings}) != len(self.bindings):
            raise NonproductionAuthorityError("plan_bindings")

    def require_objects(self, objects: dict[str, NonproductionPlanObject]) -> None:
        """Compare exact connector/kind bindings and the PG helper domain."""
        self.__post_init__()
        bindings = dict(self.bindings)
        if not set(bindings.values()) <= objects.keys():
            raise NonproductionAuthorityError("plan_reference")
        source, target = objects[bindings["source"]], objects[bindings["target"]]
        expected = ("mssql", "clickhouse") if self.profile == BCP_PROFILE else ("postgres", "mssql")
        if (source.connector, target.connector) != expected or (source.object_kind, target.object_kind) != (
            "table",
            "table",
        ):
            raise NonproductionAuthorityError("plan_bindings")
        for key in {"enum", *PG_SEQUENCES} if self.profile == PG_PROFILE else set():
            helper = objects[bindings[key]]
            if (
                helper.domain != source.domain
                or helper.qualified_name[:2] != source.qualified_name[:2]
                or helper.object_kind != ("enum" if key == "enum" else "sequence")
            ):
                raise NonproductionAuthorityError("plan_bindings")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "fixture_id": self.fixture_id,
            "profile": self.profile,
            "recipe_original": self.recipe_original.to_dict(),
            "parameters": dict(self.parameters),
            "bindings": dict(self.bindings),
        }

    @classmethod
    def from_dict(cls, value: object) -> NonproductionFixture:
        body = exact_fields(value, {field.name for field in fields(cls)})
        if type(body["parameters"]) is not dict or type(body["bindings"]) is not dict:
            raise NonproductionAuthorityError("plan_bindings")
        for key in (*body["parameters"], *body["bindings"]):
            text(key)
        return cls(
            body["fixture_id"],
            body["profile"],
            NonproductionPlanOriginal.from_dict(body["recipe_original"]),
            tuple(sorted(body["parameters"].items())),
            tuple(sorted(body["bindings"].items())),
        )


@dataclass(frozen=True, slots=True)
class NonproductionFixturePlan:
    """The entire expected fixture/object set, bound to one complete scope."""

    scope_sha256: str
    objects: tuple[NonproductionPlanObject, ...]
    fixtures: tuple[NonproductionFixture, ...]

    def __post_init__(self) -> None:
        digest(self.scope_sha256)
        if (
            type(self.objects) is not tuple
            or not 1 <= len(self.objects) <= 8192
            or any(type(row) is not NonproductionPlanObject for row in self.objects)
        ):
            raise NonproductionAuthorityError("plan_objects")
        for row in self.objects:
            row.__post_init__()
        ordered(tuple(row.object_id for row in self.objects), maximum=8192)
        if (
            len({row.identity for row in self.objects}) != len(self.objects)
            or len({(row.domain, row.object_kind, row.qualified_name) for row in self.objects}) != len(self.objects)
            or len({row.domain for row in self.objects}) > 256
        ):
            raise NonproductionAuthorityError("plan_objects")
        if (
            type(self.fixtures) is not tuple
            or not 1 <= len(self.fixtures) <= 64
            or any(type(row) is not NonproductionFixture for row in self.fixtures)
        ):
            raise NonproductionAuthorityError("plan_profile")
        objects = {row.object_id: row for row in self.objects}
        for fixture in self.fixtures:
            fixture.require_objects(objects)
        ordered(tuple(row.fixture_id for row in self.fixtures), maximum=64)
        sources = [objects[dict(row.bindings)["source"]].identity for row in self.fixtures]
        if len(set(sources)) != len(sources):
            raise NonproductionAuthorityError("plan_bindings")
        canonical_document(self._document())

    def _document(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "scope_sha256": self.scope_sha256,
            "objects": [row.to_dict() for row in self.objects],
            "fixtures": [row.to_dict() for row in self.fixtures],
        }

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return self._document()

    def to_bytes(self) -> bytes:
        return canonical_document(self.to_dict())

    @property
    def fixture_plan_sha256(self) -> str:
        return document_sha256(self.to_dict())

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> NonproductionFixturePlan:
        digest(expected_sha256)
        body = exact_fields(parse_document(raw, _SCHEMA), {"schema", "scope_sha256", "objects", "fixtures"})
        result = cls(
            body["scope_sha256"],
            tuple(NonproductionPlanObject.from_dict(row) for row in sequence(body["objects"], maximum=8192)),
            tuple(NonproductionFixture.from_dict(row) for row in sequence(body["fixtures"], maximum=64)),
        )
        if result.fixture_plan_sha256 != expected_sha256:
            raise NonproductionAuthorityError("plan_originals")
        return result
