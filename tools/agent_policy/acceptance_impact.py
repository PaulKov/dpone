"""Conservative semantic policy for data-only GitOps schedule edits.

Only a small, explicit subset can earn schedule-only acceptance. The policy
never imports a changed module, instantiates YAML tags, or resolves includes.
Everything outside this proof selects broader acceptance.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode
from yaml.tokens import AliasToken, AnchorToken

CLASSIFIER_VERSION = "dpone.acceptance-impact.v1"
MAX_DOCUMENT_BYTES = 262_144
MAX_DOCUMENT_TOKENS = 10_000
_DAG_FIELDS = frozenset(
    {
        "description",
        "schedule",
        "start_date",
        "timezone",
        "catchup",
        "max_active_runs",
        "tags",
        "default_args",
        "operator_overrides",
        "workloads",
        "wiring",
    }
)
_SCALAR_TAGS = frozenset({"str", "int", "bool", "null"})
_CRON_PRESETS = frozenset({"@hourly", "@daily", "@weekly", "@monthly", "@yearly"})


def _decode(node: yaml.Node, depth: int = 0) -> Any:
    if depth > 32:
        raise ValueError("document nesting exceeds policy limit")
    if isinstance(node, ScalarNode):
        kind = node.tag.removeprefix("tag:yaml.org,2002:")
        if kind not in _SCALAR_TAGS:
            raise ValueError("unsupported scalar tag")
        if kind == "bool" and node.value.lower() not in {"true", "false", "yes", "no", "on", "off"}:
            raise ValueError("invalid boolean scalar")
        if kind == "int" and not re.fullmatch(r"[-+]?[0-9]+", node.value):
            raise ValueError("unsupported integer scalar")
        if kind == "null" and node.value.lower() not in {"", "~", "null"}:
            raise ValueError("invalid null scalar")
        # Retain scalar type and lexical form: bool/int or changed expressions
        # must never compare equal through Python's permissive scalar equality.
        return (kind, node.value)
    if isinstance(node, SequenceNode):
        if node.tag != "tag:yaml.org,2002:seq":
            raise ValueError("unsupported sequence tag")
        return [_decode(item, depth + 1) for item in node.value]
    if not isinstance(node, MappingNode) or node.tag != "tag:yaml.org,2002:map":
        raise ValueError("unsupported mapping tag")
    result = {}
    for key, value in node.value:
        if not isinstance(key, ScalarNode) or key.tag != "tag:yaml.org,2002:str" or key.value in result:
            raise ValueError("non-string or duplicate mapping key")
        result[key.value] = _decode(value, depth + 1)
    return result


def _document(content: bytes) -> dict[str, Any]:
    if len(content) > MAX_DOCUMENT_BYTES:
        raise ValueError("document exceeds policy byte limit")
    for count, token in enumerate(yaml.scan(content), start=1):
        if count > MAX_DOCUMENT_TOKENS or isinstance(token, (AliasToken, AnchorToken)):
            raise ValueError("aliases, anchors, or excessive document tokens")
    node = yaml.compose(content, Loader=yaml.SafeLoader)
    if node is None:
        raise ValueError("empty document")
    payload = _decode(node)
    if not isinstance(payload, dict) or set(payload) != {"domain", "workloads", "dags"}:
        raise ValueError("unsupported domain document shape")
    if not _string(payload["domain"]) or not isinstance(payload["workloads"], dict) or not payload["workloads"]:
        raise ValueError("unsupported domain/workload declaration")
    for workload in payload["workloads"].values():
        if not isinstance(workload, dict) or set(workload) != {"manifest"} or not _string(workload["manifest"]):
            raise ValueError("unsupported workload declaration")
    dags = payload["dags"]
    if not isinstance(dags, dict) or not dags:
        raise ValueError("missing DAG declarations")
    for dag in dags.values():
        if not isinstance(dag, dict) or set(dag) - _DAG_FIELDS or "schedule" not in dag:
            raise ValueError("unsupported DAG fields")
        _validate_dag(dag, payload["workloads"])
    return payload


def _validate_dag(dag: dict[str, Any], workloads: dict[str, Any]) -> None:
    """Restrict eligibility to the simple declarative DAG authoring subset."""
    if not _string(dag.get("start_date")):
        raise ValueError("missing ISO start date")
    datetime.fromisoformat(dag["start_date"][1])
    members = dag.get("workloads")
    if not isinstance(members, list) or not members or any(not _string(v) or v[1] not in workloads for v in members):
        raise ValueError("invalid workload membership")
    for field in ("description", "timezone"):
        if field in dag and not _string(dag[field]):
            raise ValueError("invalid string field")
    if "catchup" in dag and (not isinstance(dag["catchup"], tuple) or dag["catchup"][0] != "bool"):
        raise ValueError("invalid catchup type")
    if "max_active_runs" in dag:
        value = dag["max_active_runs"]
        if not isinstance(value, tuple) or value[0] != "int" or not re.fullmatch(r"[1-9][0-9]*", value[1]):
            raise ValueError("invalid concurrency bound")
    if "tags" in dag and (not isinstance(dag["tags"], list) or any(not _string(v) for v in dag["tags"])):
        raise ValueError("invalid tags")
    for field in ("default_args", "operator_overrides", "wiring"):
        if field in dag and not isinstance(dag[field], dict):
            raise ValueError("invalid mapping field")
    wiring = dag.get("wiring", {})
    if set(wiring) - {"mode", "max_parallel_workloads"}:
        raise ValueError("unsupported wiring")
    if "mode" in wiring and wiring["mode"] not in (("str", "waves"), ("str", "explicit"), ("str", "assets")):
        raise ValueError("unsupported wiring mode")
    if "max_parallel_workloads" in wiring:
        value = wiring["max_parallel_workloads"]
        if not isinstance(value, tuple) or value[0] != "int" or not re.fullmatch(r"[1-9][0-9]*", value[1]):
            raise ValueError("invalid wiring concurrency bound")


def _string(value: Any) -> bool:
    return isinstance(value, tuple) and len(value) == 2 and value[0] == "str" and bool(value[1])


def _schedule(value: Any) -> str:
    if not _string(value):
        raise ValueError("schedule must be a supported string")
    text = value[1].strip()
    if text in _CRON_PRESETS:
        return text
    fields = text.split()
    bounds = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
    if len(fields) != 5:
        raise ValueError("schedule is outside supported cron subset")
    for field, (lower, upper) in zip(fields, bounds, strict=True):
        if field != "*" and (not re.fullmatch(r"[0-9]{1,2}", field) or not lower <= int(field) <= upper):
            raise ValueError("schedule is outside supported cron subset")
    return " ".join(fields)


def schedule_only(path: str, before: bytes, after: bytes) -> tuple[bool, str]:
    """Prove at least one schedule changed and every other typed field is equal."""
    candidate = PurePosixPath(path)
    if not (
        path.startswith("examples/")
        and candidate.parent.name == "domains"
        and candidate.parent.parent.name == "gitops"
        and candidate.suffix in {".yaml", ".yml"}
    ):
        return False, "Path is outside the declarative schedule allowlist."
    try:
        old, new = _document(before), _document(after)
        if old["dags"].keys() != new["dags"].keys():
            return False, "DAG identities changed."
        changed = False
        for name in old["dags"]:
            previous = _schedule(old["dags"][name].pop("schedule"))
            current = _schedule(new["dags"][name].pop("schedule"))
            changed |= previous != current
        if old != new:
            return False, "Non-schedule content changed."
        if not changed:
            return False, "No semantic schedule change was proven."
    except (ValueError, yaml.YAMLError, UnicodeError, RecursionError):
        return False, "Malformed or unsupported declarative input."
    return True, "Only supported cron schedule fields changed; all other typed fields are equal."
