"""GitOps adapter for Airflow cache-status evidence validation."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation import GitOpsSchemaValidator


class GitOpsAirflowCacheStatusSchemaValidator:
    """Adapt the shared GitOps registry to the cache publication port."""

    def __init__(self, validator: GitOpsSchemaValidator | None = None) -> None:
        self._validator = validator or GitOpsSchemaValidator()

    def supports(self, schema: str) -> bool:
        return get_gitops_schema_contract(schema) is not None

    def has_violations(self, payload: Mapping[str, object], *, expected_schema: str) -> bool:
        return bool(self._validator.validate(payload, expected_kind=expected_schema))


__all__ = ["GitOpsAirflowCacheStatusSchemaValidator"]
