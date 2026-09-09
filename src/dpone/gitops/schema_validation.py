from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from dpone.contracts.gitops_schema_semantics import validate_gitops_semantics
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation_scalar import validate_scalar_constraints


class GitOpsSchemaValidator:
    """Validates public GitOps payload shape without a runtime jsonschema dependency."""

    def validate(self, payload: object, *, expected_kind: str) -> tuple[GitOpsIssue, ...]:
        contract = get_gitops_schema_contract(expected_kind)
        if contract is None:
            return (
                GitOpsIssue(
                    code="schema_unknown_kind",
                    message=f"No GitOps schema contract is registered for {expected_kind}",
                    path=str(expected_kind),
                    source="gitops.schema",
                ),
            )
        if not isinstance(payload, Mapping):
            return (
                GitOpsIssue(
                    code="schema_type_mismatch",
                    message="Payload must be a JSON object",
                    path="$",
                    source=contract.kind,
                ),
            )

        issues: list[GitOpsIssue] = []
        identity_field = "kind" if "kind" in payload else "schema"
        actual_kind = payload.get(identity_field)
        if actual_kind != contract.kind:
            issues.append(
                GitOpsIssue(
                    code="schema_kind_mismatch",
                    message=f"Expected {identity_field} {contract.kind}, got {actual_kind!r}",
                    path=identity_field,
                    source=contract.kind,
                )
            )

        issues.extend(
            _validate_node(payload, contract.schema, path="", source=contract.kind, root_schema=contract.schema)
        )
        if not issues:
            issues.extend(
                GitOpsIssue(code=code, message=message, path=path, source=contract.kind)
                for code, message, path in validate_gitops_semantics(payload, kind=contract.kind)
            )
        return tuple(issues)


def _validate_node(
    value: object,
    schema: Mapping[str, Any],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    schema = _resolve_ref(schema, root_schema)
    one_of = schema.get("oneOf")
    if isinstance(one_of, list):
        return _validate_one_of(value, one_of, path=path, source=source, root_schema=root_schema)
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        return _validate_any_of(value, any_of, path=path, source=source, root_schema=root_schema)

    issues, stop_validation = validate_scalar_constraints(value, schema, path=path, source=source)
    if stop_validation:
        return issues

    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        issues.extend(_validate_all_of(value, all_of, path=path, source=source, root_schema=root_schema))

    if not isinstance(value, Mapping):
        if isinstance(value, list):
            issues.extend(_validate_array_items(value, schema, path=path, source=source, root_schema=root_schema))
        return issues

    required = schema.get("required", ())
    if isinstance(required, list | tuple):
        for field in required:
            if isinstance(field, str) and field not in value:
                issues.append(
                    GitOpsIssue(
                        code="schema_required_field_missing",
                        message=f"Required field is missing: {field}",
                        path=_join_path(path, field),
                        source=source,
                    )
                )

    properties = schema.get("properties", {})
    if isinstance(properties, Mapping):
        for field, field_schema in properties.items():
            if field not in value or not isinstance(field, str) or not isinstance(field_schema, Mapping):
                continue
            issues.extend(
                _validate_node(
                    value[field],
                    field_schema,
                    path=_join_path(path, field),
                    source=source,
                    root_schema=root_schema,
                )
            )

    property_names = schema.get("propertyNames")
    if isinstance(property_names, Mapping):
        for field in value:
            issues.extend(
                _validate_node(
                    field,
                    property_names,
                    path=_join_path(path, "<key>"),
                    source=source,
                    root_schema=root_schema,
                )
            )

    min_properties = schema.get("minProperties")
    if isinstance(min_properties, int) and len(value) < min_properties:
        issues.append(
            GitOpsIssue(
                code="schema_min_properties_violation",
                message=f"Field {_display_path(path)} must contain at least {min_properties} properties",
                path=_display_path(path),
                source=source,
            )
        )

    max_properties = schema.get("maxProperties")
    if isinstance(max_properties, int) and len(value) > max_properties:
        issues.append(
            GitOpsIssue(
                code="schema_max_properties_violation",
                message=f"Field {_display_path(path)} must contain at most {max_properties} properties",
                path=_display_path(path),
                source=source,
            )
        )

    additional_properties = schema.get("additionalProperties")
    if additional_properties is False:
        known_properties = set(properties) if isinstance(properties, Mapping) else set()
        for field in value:
            if field not in known_properties:
                issues.append(
                    GitOpsIssue(
                        code="schema_additional_property_forbidden",
                        message=f"Field {_join_path(path, str(field))} is not allowed",
                        path=_join_path(path, str(field)),
                        source=source,
                    )
                )
    if isinstance(additional_properties, Mapping):
        known_properties = set(properties) if isinstance(properties, Mapping) else set()
        for field, field_value in value.items():
            if field in known_properties:
                continue
            issues.extend(
                _validate_node(
                    field_value,
                    additional_properties,
                    path=_join_path(path, str(field)),
                    source=source,
                    root_schema=root_schema,
                )
            )
    return issues


def _canonical_item_key(value: object) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except TypeError:
        return repr(value)


def _validate_array_items(
    value: list[object],
    schema: Mapping[str, Any],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    issues: list[GitOpsIssue] = []
    min_items = schema.get("minItems")
    if isinstance(min_items, int) and not isinstance(min_items, bool) and len(value) < min_items:
        issues.append(
            GitOpsIssue(
                code="schema_min_items_violation",
                message=f"Field {_display_path(path)} must contain at least {min_items} items",
                path=_display_path(path),
                source=source,
            )
        )

    max_items = schema.get("maxItems")
    if isinstance(max_items, int) and not isinstance(max_items, bool) and len(value) > max_items:
        issues.append(
            GitOpsIssue(
                code="schema_max_items_violation",
                message=f"Field {_display_path(path)} must contain at most {max_items} items",
                path=_display_path(path),
                source=source,
            )
        )

    unique_items = schema.get("uniqueItems")
    if unique_items is True and len({_canonical_item_key(item) for item in value}) != len(value):
        issues.append(
            GitOpsIssue(
                code="schema_unique_items_violation",
                message=f"Field {_display_path(path)} must contain unique items",
                path=_display_path(path),
                source=source,
            )
        )

    items_schema = schema.get("items")
    if not isinstance(items_schema, Mapping):
        return issues
    for index, item in enumerate(value):
        issues.extend(
            _validate_node(
                item,
                items_schema,
                path=f"{_display_path(path)}[{index}]" if path else f"[{index}]",
                source=source,
                root_schema=root_schema,
            )
        )
    return issues


def _validate_one_of(
    value: object,
    candidates: list[object],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    matching_candidate = _candidate_by_resolver(value, candidates, root_schema)
    if matching_candidate is not None:
        return _validate_node(value, matching_candidate, path=path, source=source, root_schema=root_schema)

    candidate_results = [
        _validate_node(value, candidate, path=path, source=source, root_schema=root_schema)
        for candidate in candidates
        if isinstance(candidate, Mapping)
    ]
    valid_candidates = [result for result in candidate_results if not result]
    if len(valid_candidates) == 1:
        return []
    return [
        GitOpsIssue(
            code="schema_one_of_mismatch",
            message=f"Field {_display_path(path)} must match exactly one allowed schema",
            path=_display_path(path),
            source=source,
        )
    ]


def _validate_any_of(
    value: object,
    candidates: list[object],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    candidate_results = [
        _validate_node(value, candidate, path=path, source=source, root_schema=root_schema)
        for candidate in candidates
        if isinstance(candidate, Mapping)
    ]
    if any(not result for result in candidate_results):
        return []
    return [
        GitOpsIssue(
            code="schema_any_of_mismatch",
            message=f"Field {_display_path(path)} must match at least one allowed schema",
            path=_display_path(path),
            source=source,
        )
    ]


def _validate_all_of(
    value: object,
    candidates: list[object],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    issues: list[GitOpsIssue] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        issues.extend(_validate_all_of_candidate(value, candidate, path=path, source=source, root_schema=root_schema))
    return issues


def _validate_all_of_candidate(
    value: object,
    candidate: Mapping[str, Any],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> list[GitOpsIssue]:
    conditional = candidate.get("if")
    if isinstance(conditional, Mapping):
        if _schema_matches(value, conditional, path=path, source=source, root_schema=root_schema):
            then_schema = candidate.get("then")
            if isinstance(then_schema, Mapping):
                return _validate_node(value, then_schema, path=path, source=source, root_schema=root_schema)
            return []
        else_schema = candidate.get("else")
        if isinstance(else_schema, Mapping):
            return _validate_node(value, else_schema, path=path, source=source, root_schema=root_schema)
        return []
    return _validate_node(value, candidate, path=path, source=source, root_schema=root_schema)


def _schema_matches(
    value: object,
    schema: Mapping[str, Any],
    *,
    path: str,
    source: str,
    root_schema: Mapping[str, Any],
) -> bool:
    return not _validate_node(value, schema, path=path, source=source, root_schema=root_schema)


def _candidate_by_resolver(
    value: object,
    candidates: list[object],
    root_schema: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    resolver = value.get("resolver")
    if not isinstance(resolver, str):
        return None
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        resolved = _resolve_ref(candidate, root_schema)
        properties = resolved.get("properties")
        if not isinstance(properties, Mapping):
            continue
        resolver_schema = properties.get("resolver")
        if isinstance(resolver_schema, Mapping) and resolver_schema.get("const") == resolver:
            return resolved
    return None


def _resolve_ref(schema: Mapping[str, Any], root_schema: Mapping[str, Any]) -> Mapping[str, Any]:
    ref = schema.get("$ref")
    if not isinstance(ref, str):
        return schema
    prefix = "#/$defs/"
    if not ref.startswith(prefix):
        return schema
    defs = root_schema.get("$defs")
    if not isinstance(defs, Mapping):
        return schema
    resolved = defs.get(ref.removeprefix(prefix))
    if isinstance(resolved, Mapping):
        return resolved
    return schema


def _join_path(parent: str, field: str) -> str:
    if not parent:
        return field
    return f"{parent}.{field}"


def _display_path(path: str) -> str:
    return path or "$"


__all__ = ["GitOpsSchemaValidator"]
