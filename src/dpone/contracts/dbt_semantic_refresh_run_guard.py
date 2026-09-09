"""Canonical run guard closure derived from deployment plan topology."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import (
    canonical_fingerprint,
    contract_error,
    require_digest,
    require_strict_mapping,
    require_strings,
    require_text,
)

RUN_GUARD_CLOSURE_SCHEMA = "dpone.dbt-semantic-refresh-run-guard-closure.v1"
_FIELDS = frozenset(
    {
        "resource_guard_ids",
        "run_guard_closure_sha256",
        "schema",
        "workflow_guard_resource_id",
    }
)
_ERROR = "DPONE_DBT_V2_RUN_GUARD_INVALID"


@dataclass(frozen=True, slots=True)
class SemanticRefreshRunGuardClosure:
    """Exact workflow and non-journal resource guards authorized by a plan."""

    workflow_guard_resource_id: str
    resource_guard_ids: tuple[str, ...]
    run_guard_closure_sha256: str
    schema: str = RUN_GUARD_CLOSURE_SCHEMA

    def __post_init__(self) -> None:
        require_text(self.workflow_guard_resource_id, "workflow_guard_resource_id", _ERROR)
        if self.resource_guard_ids != tuple(sorted(set(self.resource_guard_ids))) or not self.resource_guard_ids:
            raise contract_error(_ERROR, "resource_guard_ids must be a non-empty canonical closure")
        for item in self.resource_guard_ids:
            require_text(item, "resource_guard_ids", _ERROR)
        expected = canonical_fingerprint(self._unsigned())
        if require_digest(self.run_guard_closure_sha256, "run_guard_closure_sha256", _ERROR) != expected:
            raise contract_error(_ERROR, "run guard closure digest differs from canonical content")

    @classmethod
    def build(
        cls,
        *,
        deployment_id: str,
        workflow_name: str,
        resource_guard_ids: tuple[str, ...],
    ) -> SemanticRefreshRunGuardClosure:
        """Derive guard identities from protected deployment/workflow resources."""

        deployment = require_digest(deployment_id, "deployment_id", _ERROR)
        workflow = require_text(workflow_name, "workflow_name", _ERROR)
        resources = tuple(sorted(set(resource_guard_ids)))
        if not resources or len(resources) != len(resource_guard_ids):
            raise contract_error(_ERROR, "resource_guard_ids must be non-empty and unique")
        workflow_resource = "workflow://" + canonical_fingerprint(
            {
                "deployment_id": deployment,
                "schema": "dpone.dbt-semantic-refresh-workflow-guard-resource.v1",
                "workflow_name": workflow,
            }
        )
        unsigned = {
            "resource_guard_ids": list(resources),
            "schema": RUN_GUARD_CLOSURE_SCHEMA,
            "workflow_guard_resource_id": workflow_resource,
        }
        return cls(workflow_resource, resources, canonical_fingerprint(unsigned))

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshRunGuardClosure:
        """Parse a closed guard closure and recompute its identity."""

        raw = require_strict_mapping(value, "run_guard_closure", _FIELDS, _ERROR)
        if raw.get("schema") != RUN_GUARD_CLOSURE_SCHEMA:
            raise contract_error(_ERROR, "run_guard_closure schema is unsupported")
        resources = require_strings(raw.get("resource_guard_ids"), "resource_guard_ids", _ERROR)
        return cls(
            workflow_guard_resource_id=require_text(
                raw.get("workflow_guard_resource_id"), "workflow_guard_resource_id", _ERROR
            ),
            resource_guard_ids=resources,
            run_guard_closure_sha256=require_digest(
                raw.get("run_guard_closure_sha256"), "run_guard_closure_sha256", _ERROR
            ),
        )

    def _unsigned(self) -> dict[str, object]:
        return {
            "resource_guard_ids": list(self.resource_guard_ids),
            "schema": self.schema,
            "workflow_guard_resource_id": self.workflow_guard_resource_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._unsigned(), "run_guard_closure_sha256": self.run_guard_closure_sha256}


__all__ = ["RUN_GUARD_CLOSURE_SCHEMA", "SemanticRefreshRunGuardClosure"]
