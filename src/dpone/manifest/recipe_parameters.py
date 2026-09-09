"""Restricted scalar parameter contract for declarative recipes."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping, Sequence
from typing import Any

MAX_PARAMETERS = 64
MAX_STRING_LENGTH = 1024
MAX_PROCESSES = 100
MAX_EXPANSION_DEPTH = 32
MAX_EXPANSION_NODES = 10_000
_ROOT_KEYS = {"$schema", "type", "additionalProperties", "properties", "required"}
_PROPERTY_KEYS = {
    "type",
    "default",
    "enum",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "x-dpone-format",
}
_SCALAR_TYPES = {"string", "integer", "number", "boolean"}
_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$-]{0,127}\Z")
_CONNECTION_REF_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z")
_SENSITIVE_NAME_PARTS = ("password", "passwd", "token", "secret", "private_key", "credential", "vault_path")
_SENSITIVE_VALUE_PREFIXES = ("vault://", "secret://", "env://", "${", "-----begin private key")
_EXECUTABLE_KEYS = {
    "python",
    "module",
    "entrypoint",
    "callable",
    "shell",
    "command",
    "jinja",
    "template",
    "vault_path",
    "password",
    "token",
    "secret",
}


class RecipeContractError(ValueError):
    """Typed pure-policy failure adapted at recipe application boundaries."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class RecipeParameterError(RecipeContractError):
    """Typed parameter-validation failure."""


def effective_recipe_parameters(
    parameter_schema: object,
    *,
    profile_values: object,
    locked_parameters: object,
    answers: object,
    override_allowlist: object,
    require_complete: bool = True,
) -> dict[str, Any]:
    """Validate precedence defaults -> profile -> answers and return scalars."""

    schema = validate_recipe_parameter_schema(parameter_schema)
    properties = schema["properties"]
    profile = _string_mapping(profile_values, label="profile values")
    answer_values = _string_mapping(answers, label="recipe parameters")
    locked = _string_set(locked_parameters, label="locked_parameters")
    allowlist = _string_set(override_allowlist, label="override_allowlist")
    names = set(properties)

    if not set(profile) <= names or not locked <= names or not allowlist <= names:
        raise _schema_error("Profile, lock, and override names must be declared parameters.")
    if locked & allowlist:
        raise _schema_error("Locked parameters cannot be present in override_allowlist.")
    _validate_answers(answer_values, declared=names, allowlist=allowlist, locked=locked)

    effective: dict[str, Any] = {
        name: raw["default"] for name, raw in properties.items() if isinstance(raw, Mapping) and "default" in raw
    }
    effective.update(profile)
    effective.update(answer_values)
    required = schema.get("required", [])
    missing = sorted(name for name in required if name not in effective)
    if missing and require_complete:
        raise RecipeParameterError(
            "DPONE_RECIPE_PARAMETER_REQUIRED",
            f"Required recipe parameter is missing: {missing[0]}",
        )
    for name, value in effective.items():
        _validate_value(name, value, properties[name])
    return effective


def validate_recipe_parameter_schema(raw: object) -> dict[str, Any]:
    """Return a normalized restricted parameter schema or fail closed."""

    if not isinstance(raw, Mapping) or not set(raw) <= _ROOT_KEYS:
        raise _schema_error("parameter_schema contains unsupported root fields.")
    schema = dict(raw)
    if (
        schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
    ):
        raise _schema_error("parameter_schema must be a closed draft 2020-12 object.")
    properties = schema.get("properties")
    if not isinstance(properties, Mapping) or len(properties) > MAX_PARAMETERS:
        raise _schema_error(f"parameter_schema supports at most {MAX_PARAMETERS} properties.")
    normalized: dict[str, dict[str, Any]] = {}
    for name, value in properties.items():
        if not isinstance(name, str) or _IDENTIFIER_RE.fullmatch(name) is None or not isinstance(value, Mapping):
            raise _schema_error("Parameter names and definitions are invalid.")
        definition = dict(value)
        if not set(definition) <= _PROPERTY_KEYS:
            raise _schema_error(f"Parameter {name} contains unsupported schema fields.")
        _validate_type_contract(name, definition)
        normalized[name] = definition
    required = schema.get("required", [])
    if not isinstance(required, list) or any(not isinstance(name, str) for name in required):
        raise _schema_error("required must be a list of parameter names.")
    if len(set(required)) != len(required) or not set(required) <= set(normalized):
        raise _schema_error("required contains duplicate or undeclared parameters.")
    schema["properties"] = normalized
    schema["required"] = required
    return schema


def expand_recipe_components(
    components: Sequence[Mapping[str, Any]],
    *,
    parameters: Mapping[str, Any],
    context: Mapping[str, str],
) -> tuple[Mapping[str, Any], ...]:
    """Expand bounded whole-value placeholders into canonical process mappings."""

    expanded: list[Mapping[str, Any]] = []
    budget = [0]
    for component in components:
        for process in component["processes"]:
            value = _expand_value(process, parameters=parameters, context=context, depth=0, budget=budget)
            if not isinstance(value, Mapping):
                raise RecipeContractError("DPONE_RECIPE_COMPONENT_INVALID", "Expanded process must be an object.")
            expanded.append(value)
            if len(expanded) > MAX_PROCESSES:
                raise RecipeContractError(
                    "DPONE_RECIPE_EXPANSION_LIMIT_EXCEEDED",
                    "Expanded recipe exceeds its process limit.",
                )
    return tuple(expanded)


def validate_recipe_profile_contract(recipe: Mapping[str, Any], profile: Mapping[str, Any]) -> None:
    """Validate one partial profile through the authoritative parameter rules."""

    effective_recipe_parameters(
        recipe.get("parameter_schema"),
        profile_values=profile.get("values"),
        locked_parameters=profile.get("locked_parameters"),
        answers={},
        override_allowlist=recipe.get("override_allowlist"),
        require_complete=False,
    )


def validate_recipe_component_contracts(
    components: Sequence[Mapping[str, Any]], *, parameter_names: Sequence[str]
) -> None:
    """Validate placeholders and expansion bounds without requiring user answers."""

    expand_recipe_components(
        components,
        parameters={name: None for name in parameter_names},
        context={"pipeline_id": "catalog-validation", "domain": "catalog-validation"},
    )


def reject_executable_recipe_content(value: Any) -> None:
    """Reject executable, templated, credential-bearing component content."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in _EXECUTABLE_KEYS:
                raise RecipeContractError(
                    "DPONE_RECIPE_COMPONENT_INVALID",
                    "Component contains an executable or credential-bearing field.",
                )
            reject_executable_recipe_content(item)
    elif isinstance(value, list):
        for item in value:
            reject_executable_recipe_content(item)
    elif isinstance(value, str) and any(token in value for token in ("{{", "{%", "${{", "python:", "shell:")):
        raise RecipeContractError(
            "DPONE_RECIPE_COMPONENT_INVALID",
            "Component contains executable template syntax.",
        )


def _expand_value(
    value: Any,
    *,
    parameters: Mapping[str, Any],
    context: Mapping[str, str],
    depth: int,
    budget: list[int],
) -> Any:
    budget[0] += 1
    if depth > MAX_EXPANSION_DEPTH or budget[0] > MAX_EXPANSION_NODES:
        raise RecipeContractError(
            "DPONE_RECIPE_EXPANSION_LIMIT_EXCEEDED",
            "Recipe expansion exceeds its depth or node limit.",
        )
    if isinstance(value, Mapping):
        placeholder_keys = set(value) & {"$param", "$context"}
        if placeholder_keys:
            if len(value) != 1 or len(placeholder_keys) != 1:
                raise RecipeContractError(
                    "DPONE_RECIPE_COMPONENT_INVALID",
                    "A placeholder must be the complete value mapping.",
                )
            key = next(iter(placeholder_keys))
            name = value[key]
            source = parameters if key == "$param" else context
            if not isinstance(name, str) or name not in source:
                raise RecipeContractError(
                    "DPONE_RECIPE_COMPONENT_INVALID",
                    "Component placeholder references an unknown safe value.",
                )
            return copy.deepcopy(source[name])
        return {
            str(key): _expand_value(item, parameters=parameters, context=context, depth=depth + 1, budget=budget)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _expand_value(item, parameters=parameters, context=context, depth=depth + 1, budget=budget)
            for item in value
        ]
    return copy.deepcopy(value)


def _validate_type_contract(name: str, definition: Mapping[str, Any]) -> None:
    raw_type = definition.get("type")
    if isinstance(raw_type, str):
        types = {raw_type}
    elif (
        isinstance(raw_type, list)
        and len(raw_type) == 2
        and "null" in raw_type
        and len(set(raw_type)) == 2
        and all(isinstance(item, str) for item in raw_type)
    ):
        types = set(raw_type)
    else:
        raise _schema_error(f"Parameter {name} must declare one scalar type, optionally plus null.")
    if not (types - {"null"}) <= _SCALAR_TYPES or not (types - {"null"}):
        raise _schema_error(f"Parameter {name} uses an unsupported type.")
    value_format = definition.get("x-dpone-format")
    if value_format not in {None, "identifier", "connection_ref"}:
        raise _schema_error(f"Parameter {name} uses an unsupported dpone format.")
    if value_format is not None and "string" not in types:
        raise _schema_error(f"Parameter {name} format requires type string.")
    for value in _declared_values(definition):
        _validate_value(name, value, definition)


def _declared_values(definition: Mapping[str, Any]) -> Sequence[Any]:
    values: list[Any] = []
    if "default" in definition:
        values.append(definition["default"])
    enum = definition.get("enum")
    if enum is not None:
        if not isinstance(enum, list) or not enum or len(enum) > 64:
            raise _schema_error("Parameter enum must be a non-empty bounded list.")
        values.extend(enum)
    return values


def _validate_answers(
    values: Mapping[str, Any],
    *,
    declared: set[str],
    allowlist: set[str],
    locked: set[str],
) -> None:
    for name, value in values.items():
        lowered = name.lower()
        if any(part in lowered for part in _SENSITIVE_NAME_PARTS) or _looks_sensitive_value(value):
            raise RecipeParameterError(
                "DPONE_RECIPE_ANSWERS_UNSAFE",
                "Recipe answers contain a credential-like key or value; values were redacted.",
            )
        if name not in declared or name not in allowlist or name in locked:
            raise RecipeParameterError(
                "DPONE_RECIPE_OVERRIDE_FORBIDDEN",
                f"Recipe parameter cannot be overridden: {name}",
            )


def _validate_value(name: str, value: Any, definition: Mapping[str, Any]) -> None:
    types = definition["type"] if isinstance(definition["type"], list) else [definition["type"]]
    if value is None:
        if "null" not in types:
            raise _schema_error(f"Parameter {name} does not allow null.")
        return
    expected = next(item for item in types if item != "null")
    valid = {
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
    }[expected]
    if not valid:
        raise _schema_error(f"Parameter {name} has the wrong scalar type.")
    if isinstance(value, str):
        min_length = definition.get("minLength", 0)
        max_length = definition.get("maxLength", MAX_STRING_LENGTH)
        if not isinstance(min_length, int) or not isinstance(max_length, int) or not 0 <= min_length <= max_length:
            raise _schema_error(f"Parameter {name} string limits are invalid.")
        if len(value) < min_length or len(value) > min(max_length, MAX_STRING_LENGTH):
            raise _schema_error(f"Parameter {name} violates string length bounds.")
        value_format = definition.get("x-dpone-format")
        if value_format == "identifier" and _IDENTIFIER_RE.fullmatch(value) is None:
            raise _schema_error(f"Parameter {name} is not a valid identifier.")
        if value_format == "connection_ref" and _CONNECTION_REF_RE.fullmatch(value) is None:
            raise _schema_error(f"Parameter {name} is not a valid connection_ref.")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = definition.get("minimum")
        maximum = definition.get("maximum")
        if minimum is not None and (not isinstance(minimum, (int, float)) or value < minimum):
            raise _schema_error(f"Parameter {name} violates its minimum.")
        if maximum is not None and (not isinstance(maximum, (int, float)) or value > maximum):
            raise _schema_error(f"Parameter {name} violates its maximum.")
    enum = definition.get("enum")
    if enum is not None and value not in enum:
        raise _schema_error(f"Parameter {name} is outside its enum.")


def _string_mapping(raw: object, *, label: str) -> dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping) or len(raw) > MAX_PARAMETERS or any(not isinstance(key, str) for key in raw):
        raise _schema_error(f"{label} must be a bounded scalar mapping.")
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in raw.values()):
        raise _schema_error(f"{label} values must be scalar.")
    return dict(raw)


def _string_set(raw: object, *, label: str) -> set[str]:
    if raw is None:
        return set()
    if not isinstance(raw, list) or len(raw) > MAX_PARAMETERS or any(not isinstance(item, str) for item in raw):
        raise _schema_error(f"{label} must be a bounded string list.")
    if len(set(raw)) != len(raw):
        raise _schema_error(f"{label} values must be unique.")
    return set(raw)


def _looks_sensitive_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower().startswith(_SENSITIVE_VALUE_PREFIXES)


def _schema_error(message: str) -> RecipeParameterError:
    return RecipeParameterError("DPONE_RECIPE_PARAMETER_SCHEMA_INVALID", message)


__all__ = [
    "MAX_PROCESSES",
    "MAX_PARAMETERS",
    "RecipeContractError",
    "RecipeParameterError",
    "effective_recipe_parameters",
    "expand_recipe_components",
    "reject_executable_recipe_content",
    "validate_recipe_component_contracts",
    "validate_recipe_parameter_schema",
    "validate_recipe_profile_contract",
]
