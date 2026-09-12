"""Workflow permission declarations with explicit structural ownership."""

from pathlib import PurePosixPath

import yaml
from tools.agent_policy.public_clean_receipts import GateError

PERMISSIONS = frozenset(
    {
        "actions",
        "attestations",
        "checks",
        "contents",
        "deployments",
        "discussions",
        "id-token",
        "issues",
        "models",
        "packages",
        "pages",
        "pull-requests",
        "repository-projects",
        "security-events",
        "statuses",
    }
)


def permission_context(text: str, label: str, start: int, end: int) -> tuple[int, int]:
    """Require an actual workflow root/jobs permissions map, not arbitrary metadata."""
    path = PurePosixPath(label)
    if path.parts[:2] != (".github", "workflows") or len(path.parts) != 3 or path.suffix not in {".yaml", ".yml"}:
        raise GateError("SYNTAX_PERMISSION_LOCATION")
    depth = 0
    for index, event in enumerate(yaml.parse(text, Loader=yaml.SafeLoader)):
        if isinstance(event, (yaml.events.MappingStartEvent, yaml.events.SequenceStartEvent)):
            depth += 1
        if isinstance(event, (yaml.events.MappingEndEvent, yaml.events.SequenceEndEvent)):
            depth -= 1
        if index >= 10000 or depth > 64:
            raise GateError("SYNTAX_PARSE_LIMIT")
        if isinstance(event, yaml.events.AliasEvent) or getattr(event, "anchor", None):
            raise GateError("SYNTAX_YAML_ALIAS")
    root = yaml.compose(text, Loader=yaml.SafeLoader)

    def mapping(node):
        if not isinstance(node, yaml.MappingNode) or node.tag != "tag:yaml.org,2002:map":
            raise GateError("SYNTAX_YAML_MAPPING")
        result = {}
        for key, value in node.value:
            if not isinstance(key, yaml.ScalarNode) or key.value in result or key.value == "<<":
                raise GateError("SYNTAX_YAML_DUPLICATE")
            result[key.value] = (key, value)
        return result

    def check(node):
        if isinstance(node, yaml.MappingNode):
            for _, child in mapping(node).values():
                check(child)
        elif isinstance(node, yaml.SequenceNode):
            for child in node.value:
                check(child)
        elif not isinstance(node, yaml.ScalarNode) or node.tag not in {
            "tag:yaml.org,2002:str",
            "tag:yaml.org,2002:int",
            "tag:yaml.org,2002:float",
            "tag:yaml.org,2002:bool",
            "tag:yaml.org,2002:null",
        }:
            raise GateError("SYNTAX_YAML_TAG")

    check(root)
    top = mapping(root)
    if "jobs" not in top or "on" not in top:
        raise GateError("SYNTAX_WORKFLOW_STRUCTURE")
    owners = [top]
    owners.extend(mapping(job) for _, job in mapping(top["jobs"][1]).values())
    matches = []
    for owner in owners:
        if "permissions" not in owner:
            continue
        for name, (key, value) in mapping(owner["permissions"][1]).items():
            if name not in PERMISSIONS or not isinstance(value, yaml.ScalarNode):
                raise GateError("SYNTAX_PERMISSION_ENUM")
            allowed = {"none", "write"} if name == "id-token" else {"none", "read", "write"}
            if value.tag != "tag:yaml.org,2002:str" or value.value not in allowed or value.style is not None:
                raise GateError("SYNTAX_PERMISSION_ENUM")
            if key.style is None and key.start_mark.index <= start < key.end_mark.index and end == value.end_mark.index:
                matches.append((key.start_mark.index, value.end_mark.index))
    if len(matches) != 1:
        raise GateError("SYNTAX_PERMISSION_UNPROVEN")
    return matches[0]
