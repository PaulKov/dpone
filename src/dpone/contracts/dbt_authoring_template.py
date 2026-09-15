"""Explicit immutable coordinates for the installed native dbt starter.

These values only describe an authoring project. They neither resolve credentials
nor authorize database creation or native execution. Mapping conversion rejects
extra fields and does not infer coordinates from a profile name or output path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import contract_error, require_strict_mapping
from dpone.contracts.dbt_invocation import DbtInvocationTarget
from dpone.contracts.dbt_publish_schema_contract_common import IDENTIFIER

_ERROR = "DPONE_DBT_PROFILES_INVALID"
_IDENTIFIER_PATTERN = re.compile(str(IDENTIFIER["pattern"]))
_raw_identifier_limit = IDENTIFIER["maxLength"]
assert isinstance(_raw_identifier_limit, int)
_IDENTIFIER_LIMIT = _raw_identifier_limit
_RELATION_KEYS = frozenset({"database", "schema", "name"})
_TEMPLATE_KEYS = frozenset({"project_name", "invocation_target", "source_relation"})


@dataclass(frozen=True, slots=True)
class DbtSourceRelation:
    """Three explicit source identifiers using the policy schema vocabulary."""

    database: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        for field in ("database", "schema", "name"):
            _require_identifier(getattr(self, field), f"source_relation.{field}")

    @classmethod
    def from_mapping(cls, value: object) -> DbtSourceRelation:
        raw = require_strict_mapping(value, "source_relation", _RELATION_KEYS, _ERROR)
        return cls(database=raw["database"], schema=raw["schema"], name=raw["name"])

    def to_dict(self) -> dict[str, str]:
        """Return a fresh mapping, never a mutable view of stored inputs."""
        return {"database": self.database, "schema": self.schema, "name": self.name}


@dataclass(frozen=True, slots=True)
class DbtAuthoringTemplate:
    """Validated project name, invocation target and source relation.

    The existing invocation-target validator and its error code remain authoritative.
    Source identifiers use the narrower existing policy identifier vocabulary.
    """

    project_name: str
    invocation_target: DbtInvocationTarget
    source_relation: DbtSourceRelation

    def __post_init__(self) -> None:
        _require_identifier(self.project_name, "project_name")
        if type(self.invocation_target) is not DbtInvocationTarget:
            raise contract_error(_ERROR, "invocation_target must be a validated DbtInvocationTarget")
        if type(self.source_relation) is not DbtSourceRelation:
            raise contract_error(_ERROR, "source_relation must be a validated DbtSourceRelation")

    @classmethod
    def from_mapping(cls, value: object) -> DbtAuthoringTemplate:
        raw = require_strict_mapping(value, "authoring_template", _TEMPLATE_KEYS, _ERROR)
        return cls(
            project_name=raw["project_name"],
            invocation_target=DbtInvocationTarget.from_mapping(raw["invocation_target"]),
            source_relation=DbtSourceRelation.from_mapping(raw["source_relation"]),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed authoring_template mapping without inferred fields."""
        return {
            "project_name": self.project_name,
            "invocation_target": self.invocation_target.to_dict(),
            "source_relation": self.source_relation.to_dict(),
        }


def _require_identifier(value: object, field: str) -> None:
    if not isinstance(value, str) or len(value) > _IDENTIFIER_LIMIT or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise contract_error(_ERROR, f"{field} must be a policy identifier of at most {_IDENTIFIER_LIMIT} characters")


__all__ = ["DbtAuthoringTemplate", "DbtSourceRelation"]
