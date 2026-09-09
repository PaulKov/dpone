"""Exact dbt project-config overlay owned by the semantic-refresh platform."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256


def semantic_refresh_project_overlay(
    selected_fqns: tuple[tuple[str, ...], ...],
) -> dict[str, Any]:
    """Build exact model-path config that injects the protected lifecycle."""

    if (
        not selected_fqns
        or any(len(fqn) < 2 or any(not isinstance(part, str) or not part for part in fqn) for fqn in selected_fqns)
        or len(selected_fqns) != len(set(selected_fqns))
    ):
        raise ValueError("selected_fqns must be a unique non-empty tuple of exact dbt FQNs")
    models: dict[str, Any] = {}
    for fqn in sorted(selected_fqns):
        node = models
        for ordinal, part in enumerate(fqn):
            existing = node.setdefault(part, {})
            if not isinstance(existing, dict):
                raise ValueError("selected_fqns contain an overlapping model path")
            node = existing
            if ordinal < len(fqn) - 1 and any(key.startswith("+") for key in node):
                raise ValueError("selected_fqns contain an overlapping model path")
        if node:
            raise ValueError("selected_fqns contain an overlapping model path")
        node.update(
            {
                "+contract": {"enforced": True},
                "+incremental_strategy": "dpone_scope_merge",
                "+materialized": "incremental",
                "+on_schema_change": "fail",
            }
        )
    payload = {
        "models": models,
        "schema": "dpone.dbt-semantic-refresh-project-overlay.v1",
    }
    return {**payload, "project_overlay_sha256": semantic_refresh_sha256(payload)}


def dbt_project_models_config(overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Project the verified overlay to the only fragment dbt may consume."""

    expected = semantic_refresh_project_overlay(_fqns(overlay.get("models")))
    if dict(overlay) != expected:
        raise ValueError("semantic-refresh project overlay is not canonical")
    return {"models": expected["models"]}


def _fqns(models: object) -> tuple[tuple[str, ...], ...]:
    if not isinstance(models, Mapping):
        raise ValueError("semantic-refresh project overlay models are missing")
    result: list[tuple[str, ...]] = []

    def visit(node: Mapping[str, Any], prefix: tuple[str, ...]) -> None:
        config_keys = {key for key in node if str(key).startswith("+")}
        if config_keys:
            result.append(prefix)
            return
        for key, value in node.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                raise ValueError("semantic-refresh project overlay is malformed")
            visit(value, (*prefix, key))

    visit(models, ())
    return tuple(result)


__all__ = ["dbt_project_models_config", "semantic_refresh_project_overlay"]
