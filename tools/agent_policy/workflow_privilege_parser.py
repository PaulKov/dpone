"""Strict, side-effect-free parsers for PR3B policy and workflow inputs."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from tools.agent_policy import workflow_privilege_contracts as contracts
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, ScanLimits, SnapshotFile
from yaml.events import AliasEvent, DocumentStartEvent, NodeEvent
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")
_FLOAT = re.compile(r"-?(?:0|[1-9][0-9]*)\.[0-9]+(?:[eE][+-]?[0-9]+)?\Z")
_EXPONENTIAL = re.compile(r"-?(?:0|[1-9][0-9]*)(?:[eE][+-]?[0-9]+)\Z")
_MAX_INTEGER_DIGITS = 640


class YamlInputError(ValueError):
    """Reject non-bounded or ambiguous YAML input."""

    def __init__(self, message: str, *, limit_dimension: str | None = None) -> None:
        super().__init__(message)
        self.limit_dimension = limit_dimension


class PolicyValidationError(ValueError): ...  # strict policy input


class WorkflowValidationError(ValueError): ...  # strict workflow input


@dataclass(frozen=True, slots=True)
class ParsedPolicy:
    schema_version: int
    limits: ScanLimits
    value: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PermissionDeclaration:
    values: Mapping[str, str]
    is_explicit: bool

    @property
    def non_none(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((key, value) for key, value in self.values.items() if value != "none"))


@dataclass(frozen=True, slots=True)
class ParsedWorkflow:
    path: str
    name: str
    permissions: PermissionDeclaration
    value: Mapping[str, Any]

    def to_mapping(self) -> dict[str, Any]:
        return {"path": self.path, **dict(self.value)}


def normalized_needs(job: Mapping[str, Any]) -> tuple[str, ...]:
    """Return a deterministic, duplicate-free job predecessor set."""
    value = job.get("needs", ())
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, (list, tuple)) or not all(isinstance(item, str) for item in value):
        raise ValueError("job needs must contain only job identifiers")
    return tuple(sorted(set(value), key=str.encode))


def _valid_permissions(value: object) -> bool:
    mapping = isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items())
    return mapping or isinstance(value, str) and value in {"read-all", "write-all"}


_FILTERS = ("branches", "branches-ignore", "paths", "paths-ignore")
_SELECTOR_LIMITS = {"types": (64, 64), "workflows": (256, 256), **dict.fromkeys(_FILTERS, (1024, 256))}


def _string_selector(key: str, value: object) -> bool:
    values = [value] if isinstance(value, str) else value if isinstance(value, list) else []
    pattern = r"[a-z][a-z0-9_-]{0,63}\Z" if key == "types" else None
    return (
        bool(values)
        and len(values) <= _SELECTOR_LIMITS[key][1]
        and all(
            contracts.valid_public_text(item, _SELECTOR_LIMITS[key][0])
            and (pattern is None or re.fullmatch(pattern, item))
            for item in values
        )
    )


_SECURITY_EVENT_FILTERS = {
    **dict.fromkeys(("pull_request", "pull_request_target"), frozenset({"types", *_FILTERS})),
    "workflow_run": frozenset({"workflows", "types", "branches", "branches-ignore"}),
    "workflow_call": frozenset({"inputs", "outputs", "secrets"}),
}
_CALL_JOB_KEYS = frozenset("name uses with secrets needs if concurrency permissions".split())


def _valid_security_trigger(event: str, configuration: object) -> bool:
    if not isinstance(configuration, dict):
        return configuration is None and event != "workflow_run"
    if event == "workflow_call":
        return not set(configuration) - _SECURITY_EVENT_FILTERS[event]
    return (
        not set(configuration) - _SECURITY_EVENT_FILTERS[event]
        and all(_string_selector(key, item) for key, item in configuration.items())
        and (event != "workflow_run" or "workflows" in configuration)
    )


def _valid_output_fields(name: object, jobs: Mapping[str, Any]) -> bool:
    fields: list[tuple[object, int]] = [(name, 256), *((job_id, 256) for job_id in jobs)]
    for job in jobs.values():
        runner, environment = job.get("runs-on", job.get("runs_on")), job.get("environment")
        labels = [runner] if isinstance(runner, str) else runner if isinstance(runner, list) else []
        if isinstance(environment, Mapping):
            if (
                set(environment) - {"name", "url"}
                or not isinstance(environment.get("name"), str)
                or ("url" in environment and not contracts.valid_public_text(environment["url"]))
            ):
                return False
            environment = environment["name"]
        elif environment is not None and not isinstance(environment, str):
            return False
        if len(labels) > 16 and all(isinstance(item, str) for item in labels):
            return False
        fields.extend((item, 256) for item in normalized_needs(job))
        fields.extend((item, 128) for item in labels if isinstance(item, str))
        fields.extend(((environment, 256),) if isinstance(environment, str) else ())
    return all(contracts.valid_public_text(value, limit) for value, limit in fields)


def workflow_call_validator(callee: Mapping[str, Any]) -> Callable[[Mapping[str, Any]], bool]:
    """Compile one closed reusable-workflow declaration for bounded caller checks."""
    events = callee.get("on", {})
    raw = events.get("workflow_call") if isinstance(events, Mapping) and "workflow_call" in events else _MISSING
    call = {} if raw is None else raw
    inputs = call.get("inputs", {}) if isinstance(call, Mapping) else None
    secrets = call.get("secrets", {}) if isinstance(call, Mapping) else None
    if not isinstance(inputs, Mapping) or not isinstance(secrets, Mapping):
        return lambda _job: False
    if not all(_valid_call_input(value) for value in inputs.values()) or not all(
        isinstance(value, Mapping)
        and not set(value) - {"description", "required"}
        and ("description" not in value or isinstance(value["description"], str))
        and ("required" not in value or type(value["required"]) is bool)
        for value in secrets.values()
    ):
        return lambda _job: False
    required_inputs = frozenset(key for key, value in inputs.items() if value.get("required") is True)
    required_secrets = frozenset(key for key, value in secrets.items() if value.get("required") is True)

    def validates(job: Mapping[str, Any]) -> bool:
        supplied, sent_secrets = job.get("with", {}), job.get("secrets", {})
        return (
            isinstance(supplied, Mapping)
            and isinstance(sent_secrets, Mapping)
            and not set(job) - _CALL_JOB_KEYS
            and _valid_call_shape(job)
            and sum(key in required_inputs for key in supplied) == len(required_inputs)
            and sum(key in required_secrets for key in sent_secrets) == len(required_secrets)
            and all(key in inputs and _valid_call_input(inputs[key], value) for key, value in supplied.items())
            and all(key in secrets for key in sent_secrets)
        )

    return validates


def valid_workflow_call(caller_job: Mapping[str, Any], callee: Mapping[str, Any]) -> bool:
    return workflow_call_validator(callee)(caller_job)


_MISSING = object()


def _valid_call_shape(job: Mapping[str, Any]) -> bool:
    values = job.get("name", ""), job.get("if", ""), job.get("concurrency", ""), job.get("secrets", {})
    name, condition, concurrency, secrets = values
    return (
        isinstance(name, str)
        and isinstance(condition, (bool, str))
        and isinstance(concurrency, str)
        and isinstance(secrets, Mapping)
        and all(isinstance(key, str) and isinstance(value, str) for key, value in secrets.items())
    )


def _valid_call_input(value: object, supplied: object = _MISSING) -> bool:
    if not isinstance(value, Mapping) or set(value) - {"default", "description", "required", "type"}:
        return False
    kind = value.get("type")
    expected = {"boolean": bool, "number": (int, float), "string": str}.get(kind)
    return (
        expected is not None
        and ("required" not in value or type(value["required"]) is bool)
        and ("description" not in value or isinstance(value["description"], str))
        and (
            "default" not in value
            or (isinstance(value["default"], expected) and not (kind == "number" and type(value["default"]) is bool))
        )
        and not (isinstance(value.get("default"), str) and "${{" in value["default"])
        and (
            supplied is _MISSING
            or isinstance(supplied, expected)
            and not (kind == "number" and type(supplied) is bool)
            and not (isinstance(supplied, str) and "${{" in supplied)
        )
    )


def load_yaml12_mapping(content: bytes, *, max_bytes: int, max_depth: int, max_nodes: int) -> dict[str, Any]:
    """Load one duplicate-safe YAML mapping using only JSON scalar types."""
    if len(content) > max_bytes:
        raise YamlInputError("YAML byte limit exceeded", limit_dimension="workflow_bytes")
    if content.startswith(b"\xef\xbb\xbf"):
        raise YamlInputError("UTF-8 BOM is not permitted")
    if b"\x00" in content:
        raise YamlInputError("NUL is not permitted")
    try:
        text = content.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise YamlInputError("input is not strict UTF-8") from exc

    try:
        events = tuple(yaml.parse(text, Loader=yaml.BaseLoader))
    except (RecursionError, yaml.YAMLError) as exc:
        raise YamlInputError("input is not valid YAML") from exc
    if sum(isinstance(event, DocumentStartEvent) for event in events) != 1:
        raise YamlInputError("exactly one YAML document is required")
    if any(
        isinstance(event, AliasEvent)
        or (isinstance(event, NodeEvent) and (event.anchor is not None or event.tag is not None))
        for event in events
    ):
        raise YamlInputError("anchors, aliases, and explicit tags are forbidden")
    try:
        nodes = tuple(yaml.compose_all(text, Loader=yaml.BaseLoader))
    except (RecursionError, yaml.YAMLError) as exc:
        raise YamlInputError("input is not valid YAML") from exc
    if len(nodes) != 1 or not isinstance(root := nodes[0], MappingNode):
        raise YamlInputError("document root must be a mapping")
    count, depth = _measure(root)
    if depth > max_depth:
        raise YamlInputError("YAML depth limit exceeded", limit_dimension="yaml_depth")
    if count > max_nodes:
        raise YamlInputError("YAML node limit exceeded", limit_dimension="yaml_nodes")
    value = _convert(root)
    if not isinstance(value, dict):  # defensive: root type is checked above
        raise YamlInputError("document root must be a mapping")
    return value


def _measure(node: Node, depth: int = 1) -> tuple[int, int]:
    children = (
        [item for pair in node.value for item in pair]
        if isinstance(node, MappingNode)
        else list(node.value)
        if isinstance(node, SequenceNode)
        else []
    )
    total, maximum = 1, depth
    for child in children:
        count, child_depth = _measure(child, depth + 1 if isinstance(child, (MappingNode, SequenceNode)) else depth)
        total += count
        maximum = max(maximum, child_depth)
    return total, maximum


def _convert(node: Node) -> Any:
    if isinstance(node, ScalarNode):
        return _scalar(node)
    if isinstance(node, SequenceNode):
        return [_convert(child) for child in node.value]
    if not isinstance(node, MappingNode):
        raise YamlInputError("unsupported YAML node")
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        if not isinstance(key_node, ScalarNode):
            raise YamlInputError("mapping keys must be scalar strings")
        key = _scalar(key_node)
        if not isinstance(key, str) or (key_node.style is None and key == "<<"):
            raise YamlInputError("mapping keys must be unambiguous strings")
        if key in result:
            raise YamlInputError(f"duplicate mapping key: {key}")
        result[key] = _convert(value_node)
    return result


def _scalar(node: ScalarNode) -> Any:
    value = node.value
    if node.style is not None:
        return value
    folded = value.casefold()
    if folded in {".nan", ".inf", "+.inf", "-.inf"}:
        raise YamlInputError("non-finite numbers are forbidden")
    if folded in {"", "null", "~"}:
        return None
    if folded == "true":
        return True
    if folded == "false":
        return False
    if _INTEGER.fullmatch(value):
        if len(value.removeprefix("-")) > _MAX_INTEGER_DIGITS:
            raise YamlInputError("integer scalar exceeds the closed conversion limit")
        try:
            return int(value)
        except ValueError as exc:
            raise YamlInputError("integer scalar exceeds the closed conversion limit") from exc
    if _FLOAT.fullmatch(value) or _EXPONENTIAL.fullmatch(value):
        number = float(value)
        if not math.isfinite(number):
            raise YamlInputError("non-finite numbers are forbidden")
        return number
    return value


def parse_policy(source: SnapshotFile, *, schema: Mapping[str, Any]) -> ParsedPolicy:
    """Strictly parse and validate one exact, versioned privilege policy."""
    try:
        Draft202012Validator.check_schema(schema)
        value = load_yaml12_mapping(
            source.content,
            max_bytes=V1_LIMITS.policy_bytes,
            max_depth=V1_LIMITS.yaml_depth,
            max_nodes=V1_LIMITS.yaml_nodes,
        )
        errors = tuple(Draft202012Validator(schema).iter_errors(value))
    except (PolicyValidationError, YamlInputError):
        raise
    except Exception as exc:
        raise PolicyValidationError("privilege policy could not be validated") from exc
    if errors:
        raise PolicyValidationError("privilege policy does not match the closed v1 schema")
    expected_policy = schema.get("const")
    if not isinstance(expected_policy, Mapping) or contracts.canonical_json_bytes(
        value
    ) != contracts.canonical_json_bytes(expected_policy):
        raise PolicyValidationError("privilege policy values or JSON scalar types differ from v1")
    try:
        limits = ScanLimits(**value["limits"])
    except (KeyError, TypeError) as exc:
        raise PolicyValidationError("privilege policy limits are not closed") from exc
    schema_version = value.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version < 1:
        raise PolicyValidationError("privilege policy schema version is invalid")
    return ParsedPolicy(schema_version=schema_version, limits=limits, value=value)


def parse_workflow(source: SnapshotFile, *, limits: ScanLimits) -> ParsedWorkflow:
    """Parse one workflow and require explicit least-privilege authority."""
    value = load_yaml12_mapping(
        source.content, max_bytes=limits.workflow_bytes, max_depth=limits.yaml_depth, max_nodes=limits.yaml_nodes
    )
    permissions_value = value.get("permissions")
    if "permissions" not in value or not _valid_permissions(permissions_value):
        raise WorkflowValidationError(f"workflow {source.path} requires explicit permissions")
    permissions = permissions_value if isinstance(permissions_value, dict) else {}
    raw_events = value.get("on")
    if isinstance(raw_events, (str, list)):
        selected = [raw_events] if isinstance(raw_events, str) else raw_events
        events = dict.fromkeys(selected) if all(isinstance(event, str) for event in selected) else None
    else:
        events = raw_events
    if not isinstance(events, dict):
        raise WorkflowValidationError(f"workflow {source.path} triggers must be closed")
    value["on"] = events
    if any(
        not _valid_security_trigger(event, events[event]) for event in events.keys() & _SECURITY_EVENT_FILTERS.keys()
    ):
        raise WorkflowValidationError(f"workflow {source.path} triggers must be closed")
    jobs = value.get("jobs")
    if not isinstance(jobs, dict):
        raise WorkflowValidationError(f"workflow {source.path} jobs must be a mapping")
    for job in jobs.values():
        if not isinstance(job, dict):
            raise WorkflowValidationError(f"workflow {source.path} job must be a mapping")
        try:
            normalized_needs(job)
        except ValueError as exc:
            raise WorkflowValidationError(f"workflow {source.path} job needs are invalid") from exc
        if "permissions" in job and not _valid_permissions(job["permissions"]):
            raise WorkflowValidationError(f"workflow {source.path} job permissions are invalid")
        if isinstance(secrets := job.get("secrets"), str):
            job["secrets"] = secrets.upper()
    if not _valid_output_fields(name := value.get("name"), jobs):
        raise WorkflowValidationError(f"workflow {source.path} name is required")
    return ParsedWorkflow(source.path, name, PermissionDeclaration(dict(permissions), True), value)


__all__ = """ParsedPolicy ParsedWorkflow PermissionDeclaration PolicyValidationError WorkflowValidationError YamlInputError
load_yaml12_mapping normalized_needs parse_policy parse_workflow valid_workflow_call workflow_call_validator""".split()
