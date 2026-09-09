"""Canonical semantic-refresh model-definition proof."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_types import ClosureStatus

MODEL_DEFINITION_PROOF_SCHEMA = "dpone.semantic-refresh-model-definition-proof.v1"
_DIGEST_FIELD = "model_definition_proof_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "status",
        "model_unique_id",
        "manifest_sha256",
        "raw_code_sha256",
        "compiled_sql_sha256",
        "macro_closure_sha256",
        "toolchain_sha256",
        "resolved_relation_dependency_digest",
        "resolved_module_dependency_digest",
        "catalog_observation_digest",
        "parser_runtime_policy_digest",
        "target_independence_policy_digest",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshModelDefinitionProof(SemanticRefreshDocumentCodec):
    """Immutable raw/compiled/macro closure proof for one dbt model."""

    status: ClosureStatus
    model_unique_id: str
    manifest_sha256: str
    raw_code_sha256: str
    compiled_sql_sha256: str
    macro_closure_sha256: str
    toolchain_sha256: str
    resolved_relation_dependency_digest: str
    resolved_module_dependency_digest: str
    catalog_observation_digest: str
    parser_runtime_policy_digest: str
    target_independence_policy_digest: str
    model_definition_proof_sha256: str
    schema: str = MODEL_DEFINITION_PROOF_SCHEMA

    schema_id: ClassVar[str] = MODEL_DEFINITION_PROOF_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.status, ClosureStatus):
            raise ValueError("model definition proof status is unsupported")
        require_text(self.model_unique_id, "model_unique_id")
        for field_name in (
            "manifest_sha256",
            "raw_code_sha256",
            "compiled_sql_sha256",
            "macro_closure_sha256",
            "toolchain_sha256",
            "resolved_relation_dependency_digest",
            "resolved_module_dependency_digest",
            "catalog_observation_digest",
            "parser_runtime_policy_digest",
            "target_independence_policy_digest",
        ):
            require_digest(getattr(self, field_name), field_name)
        validate_digest(self._unsigned(), self.model_definition_proof_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        status: ClosureStatus,
        model_unique_id: str,
        manifest_sha256: str,
        raw_code_sha256: str,
        compiled_sql_sha256: str,
        macro_closure_sha256: str,
        toolchain_sha256: str,
        resolved_relation_dependency_digest: str,
        resolved_module_dependency_digest: str,
        catalog_observation_digest: str,
        parser_runtime_policy_digest: str,
        target_independence_policy_digest: str,
    ) -> SemanticRefreshModelDefinitionProof:
        """Build a proof with its canonical content digest."""

        unsigned = _unsigned_mapping(
            status,
            model_unique_id,
            manifest_sha256,
            raw_code_sha256,
            compiled_sql_sha256,
            macro_closure_sha256,
            toolchain_sha256,
            resolved_relation_dependency_digest,
            resolved_module_dependency_digest,
            catalog_observation_digest,
            parser_runtime_policy_digest,
            target_independence_policy_digest,
        )
        return cls(
            status,
            model_unique_id,
            manifest_sha256,
            raw_code_sha256,
            compiled_sql_sha256,
            macro_closure_sha256,
            toolchain_sha256,
            resolved_relation_dependency_digest,
            resolved_module_dependency_digest,
            catalog_observation_digest,
            parser_runtime_policy_digest,
            target_independence_policy_digest,
            semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshModelDefinitionProof:
        """Parse a strict proof and recompute its digest."""

        raw = require_closed_mapping(value, "model_definition_proof", required=_FIELDS)
        return cls(
            status=require_enum(raw.get("status"), "status", ClosureStatus),
            model_unique_id=require_text(raw.get("model_unique_id"), "model_unique_id"),
            manifest_sha256=require_digest(raw.get("manifest_sha256"), "manifest_sha256"),
            raw_code_sha256=require_digest(raw.get("raw_code_sha256"), "raw_code_sha256"),
            compiled_sql_sha256=require_digest(raw.get("compiled_sql_sha256"), "compiled_sql_sha256"),
            macro_closure_sha256=require_digest(raw.get("macro_closure_sha256"), "macro_closure_sha256"),
            toolchain_sha256=require_digest(raw.get("toolchain_sha256"), "toolchain_sha256"),
            resolved_relation_dependency_digest=require_digest(
                raw.get("resolved_relation_dependency_digest"), "resolved_relation_dependency_digest"
            ),
            resolved_module_dependency_digest=require_digest(
                raw.get("resolved_module_dependency_digest"), "resolved_module_dependency_digest"
            ),
            catalog_observation_digest=require_digest(
                raw.get("catalog_observation_digest"), "catalog_observation_digest"
            ),
            parser_runtime_policy_digest=require_digest(
                raw.get("parser_runtime_policy_digest"), "parser_runtime_policy_digest"
            ),
            target_independence_policy_digest=require_digest(
                raw.get("target_independence_policy_digest"), "target_independence_policy_digest"
            ),
            model_definition_proof_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.status,
            self.model_unique_id,
            self.manifest_sha256,
            self.raw_code_sha256,
            self.compiled_sql_sha256,
            self.macro_closure_sha256,
            self.toolchain_sha256,
            self.resolved_relation_dependency_digest,
            self.resolved_module_dependency_digest,
            self.catalog_observation_digest,
            self.parser_runtime_policy_digest,
            self.target_independence_policy_digest,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.model_definition_proof_sha256}


def _unsigned_mapping(
    status: ClosureStatus,
    model_unique_id: str,
    manifest_sha256: str,
    raw_code_sha256: str,
    compiled_sql_sha256: str,
    macro_closure_sha256: str,
    toolchain_sha256: str,
    resolved_relation_dependency_digest: str,
    resolved_module_dependency_digest: str,
    catalog_observation_digest: str,
    parser_runtime_policy_digest: str,
    target_independence_policy_digest: str,
) -> dict[str, object]:
    return {
        "compiled_sql_sha256": compiled_sql_sha256,
        "catalog_observation_digest": catalog_observation_digest,
        "macro_closure_sha256": macro_closure_sha256,
        "manifest_sha256": manifest_sha256,
        "model_unique_id": model_unique_id,
        "raw_code_sha256": raw_code_sha256,
        "resolved_module_dependency_digest": resolved_module_dependency_digest,
        "resolved_relation_dependency_digest": resolved_relation_dependency_digest,
        "schema": MODEL_DEFINITION_PROOF_SCHEMA,
        "status": status.value,
        "parser_runtime_policy_digest": parser_runtime_policy_digest,
        "target_independence_policy_digest": target_independence_policy_digest,
        "toolchain_sha256": toolchain_sha256,
    }


__all__ = ["MODEL_DEFINITION_PROOF_SCHEMA", "SemanticRefreshModelDefinitionProof"]
