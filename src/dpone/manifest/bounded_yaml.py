"""Small, deterministic YAML reader for untrusted build-plane artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml
from yaml.events import CollectionEndEvent, CollectionStartEvent, ScalarEvent
from yaml.nodes import MappingNode
from yaml.tokens import AliasToken, AnchorToken


class BoundedYamlError(ValueError):
    """A YAML document violated a deterministic safety bound."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class BoundedYamlLimits:
    """Hard limits applied before a YAML object reaches domain validation."""

    max_bytes: int = 1024 * 1024
    max_tokens: int = 20_000
    max_depth: int = 32
    max_nodes: int = 10_000


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: _UniqueKeySafeLoader, node: MappingNode, deep: bool = False) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise BoundedYamlError("mapping_key_invalid", "YAML mapping keys must be scalar values.") from exc
        if duplicate:
            raise BoundedYamlError("duplicate_key", "YAML mapping keys must be unique.")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_bounded_yaml(
    content: bytes,
    *,
    limits: BoundedYamlLimits = BoundedYamlLimits(),
) -> Any:
    """Decode one YAML document after byte, token, depth, and node checks."""

    if len(content) > limits.max_bytes:
        raise BoundedYamlError("file_too_large", "YAML input exceeds its byte limit.")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BoundedYamlError("invalid_utf8", "YAML input must be UTF-8.") from exc
    try:
        _check_budget(text, limits=limits)
        return yaml.load(text, Loader=_UniqueKeySafeLoader)
    except BoundedYamlError:
        raise
    except (yaml.YAMLError, RecursionError) as exc:
        raise BoundedYamlError("invalid_yaml", "YAML input is malformed or exceeds parser limits.") from exc


def _check_budget(text: str, *, limits: BoundedYamlLimits) -> None:
    for count, token in enumerate(yaml.scan(text, Loader=yaml.SafeLoader), start=1):
        if isinstance(token, (AliasToken, AnchorToken)):
            raise BoundedYamlError("alias_forbidden", "YAML anchors and aliases are forbidden.")
        if count > limits.max_tokens:
            raise BoundedYamlError("token_limit", "YAML input exceeds its token limit.")

    depth = 0
    nodes = 0
    for event in yaml.parse(text, Loader=yaml.SafeLoader):
        if isinstance(event, CollectionStartEvent):
            depth += 1
            nodes += 1
            if depth > limits.max_depth:
                raise BoundedYamlError("depth_limit", "YAML input exceeds its nesting limit.")
        elif isinstance(event, CollectionEndEvent):
            depth -= 1
        elif isinstance(event, ScalarEvent):
            nodes += 1
        if nodes > limits.max_nodes:
            raise BoundedYamlError("node_limit", "YAML input exceeds its node limit.")


__all__ = ["BoundedYamlError", "BoundedYamlLimits", "load_bounded_yaml"]
