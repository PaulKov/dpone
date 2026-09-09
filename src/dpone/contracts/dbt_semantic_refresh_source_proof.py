"""Bounded raw Jinja and macro-closure proof for semantic refresh V2."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from jinja2 import Environment, TemplateSyntaxError, meta, nodes

from dpone.contracts.dbt_semantic_refresh_common import (
    ProofStatus,
    SemanticRefreshProofIssue,
    nonconformant,
    proof_status,
    unverified,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256

SOURCE_DEFINITION_PROOF_SCHEMA = "dpone.semantic-refresh-source-definition-proof.v1"

_JINJA = re.compile(r"{{.*?}}|{%.*?%}", re.DOTALL)
_VAR_CALL = re.compile(r"\bvar\s*\(\s*(['\"])([^'\"]+)\1", re.IGNORECASE)
_VAR_REFERENCE = re.compile(r"\bvar\b", re.IGNORECASE)
_DIRECT_JINJA_CALLS = frozenset({"config", "ref", "source", "var"})
_MAXIMUM_MACRO_CLOSURE_NODES = 4096
_MAXIMUM_MACRO_CLOSURE_EDGES = 16384
_FORBIDDEN_JINJA: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("this", re.compile(r"\bthis\b", re.IGNORECASE)),
    ("is_incremental", re.compile(r"\bis_incremental\b", re.IGNORECASE)),
    ("run_query", re.compile(r"\brun_query\b", re.IGNORECASE)),
    ("statement", re.compile(r"\bstatement\b", re.IGNORECASE)),
    ("adapter", re.compile(r"\badapter\b", re.IGNORECASE)),
    (
        "relation_introspection",
        re.compile(
            r"\b(?:load_relation|get_relation|get_columns_in_relation|get_missing_columns|expand_target_column_types)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "caller_environment",
        re.compile(
            r"\b(?:builtins|context|dbt|env_var|flags|execute|invocation_id|invocation_args_dict|target)\b",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class SourceDefinitionProof:
    """Digested raw model and complete author-owned macro source closure."""

    status: ProofStatus
    source_digests: tuple[tuple[str, str], ...]
    proof_sha256: str
    issues: tuple[SemanticRefreshProofIssue, ...]
    schema: str = SOURCE_DEFINITION_PROOF_SCHEMA


def prove_raw_jinja_closure(
    *,
    model_raw_sql: str,
    macro_sources: Mapping[str, str],
    required_macro_ids: tuple[str, ...],
    allowed_vars: tuple[str, ...],
    maximum_source_bytes: int,
) -> SourceDefinitionProof:
    """Reject target/runtime-sensitive author Jinja before dbt compilation."""

    _positive(maximum_source_bytes, "maximum_source_bytes")
    required = _tokens(required_macro_ids, "required_macro_ids", allow_empty=True)
    allowed = _tokens(allowed_vars, "allowed_vars", allow_empty=True)
    issues: list[SemanticRefreshProofIssue] = []
    if not isinstance(model_raw_sql, str) or not model_raw_sql.strip():
        issues.append(
            unverified(
                "DPONE_DBT_V2_SOURCE_UNVERIFIED",
                "model_raw_sql",
                "raw model source is unavailable",
            )
        )
        model_raw_sql = ""
    if not isinstance(macro_sources, Mapping) or any(
        not isinstance(key, str) or not key or not isinstance(value, str) or not value.strip()
        for key, value in macro_sources.items()
    ):
        issues.append(
            unverified(
                "DPONE_DBT_V2_SOURCE_UNVERIFIED",
                "macro_sources",
                "macro source closure is unavailable or malformed",
            )
        )
        macro_sources = {}
    supplied_ids = set(macro_sources)
    missing = set(required) - supplied_ids
    extra = supplied_ids - set(required)
    if missing:
        issues.append(
            unverified(
                "DPONE_DBT_V2_MACRO_CLOSURE_UNVERIFIED",
                "macro_sources",
                "one or more required macro sources are unavailable",
            )
        )
    if extra:
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_MACRO_CLOSURE_UNCLASSIFIED",
                "macro_sources",
                "macro sources outside the resolved closure are not admitted",
            )
        )
    sources = {"model": model_raw_sql, **dict(macro_sources)}
    macro_globals = _macro_globals(required)
    total_bytes = sum(len(source.encode("utf-8")) for source in sources.values())
    if total_bytes > maximum_source_bytes:
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_SOURCE_BUDGET_EXCEEDED",
                "maximum_source_bytes",
                "raw model and macro closure exceed the explicit source budget",
            )
        )
    for source_id, source in sorted(sources.items()):
        issues.extend(
            _source_issues(
                source_id,
                source,
                frozenset(allowed),
                macro_globals,
            )
        )
    digests = tuple(
        (source_id, "sha256:" + hashlib.sha256(source.encode("utf-8")).hexdigest())
        for source_id, source in sorted(sources.items())
        if source
    )
    ordered = tuple(sorted(set(issues), key=lambda issue: (issue.unique_id or "", issue.field, issue.code)))
    payload = {
        "schema": SOURCE_DEFINITION_PROOF_SCHEMA,
        "required_macro_ids": list(required),
        "allowed_vars": list(allowed),
        "maximum_source_bytes": maximum_source_bytes,
        "source_digests": [list(item) for item in digests],
        "issues": [issue.to_jsonable() for issue in ordered],
    }
    return SourceDefinitionProof(
        proof_status(ordered),
        digests,
        semantic_refresh_sha256(payload),
        ordered,
    )


def resolve_manifest_macro_source_closure(
    *,
    root_macro_ids: tuple[str, ...],
    macros: Mapping[str, object],
) -> dict[str, str]:
    """Resolve the exact bounded, acyclic transitive manifest macro closure."""

    roots = _tokens(root_macro_ids, "root_macro_ids", allow_empty=True)
    states: dict[str, int] = {}
    sources: dict[str, str] = {}
    pending_sources: dict[str, str] = {}
    edge_count = 0
    for root in roots:
        stack = [(root, False)]
        while stack:
            macro_id, exiting = stack.pop()
            state = states.get(macro_id, 0)
            if exiting:
                states[macro_id] = 2
                sources[macro_id] = pending_sources.pop(macro_id)
                continue
            if state == 1:
                raise ValueError("manifest macro dependency closure contains a cycle")
            if state == 2:
                continue
            if len(states) >= _MAXIMUM_MACRO_CLOSURE_NODES:
                raise ValueError("manifest macro dependency closure exceeds the node budget")
            raw_macro = macros.get(macro_id)
            if not isinstance(raw_macro, Mapping):
                raise ValueError("manifest macro dependency closure is missing a required macro")
            source = raw_macro.get("macro_sql")
            if not isinstance(source, str) or not source.strip():
                raise ValueError("manifest macro dependency closure is missing required source")
            depends_on = raw_macro.get("depends_on", {})
            if not isinstance(depends_on, Mapping):
                raise ValueError("manifest macro dependency metadata is malformed")
            raw_dependencies = depends_on.get("macros", ())
            if not isinstance(raw_dependencies, list | tuple) or any(
                not isinstance(item, str) or not item for item in raw_dependencies
            ):
                raise ValueError("manifest macro dependency identifiers are malformed")
            dependencies = _tokens(tuple(raw_dependencies), "macro dependencies", allow_empty=True)
            edge_count += len(dependencies)
            if edge_count > _MAXIMUM_MACRO_CLOSURE_EDGES:
                raise ValueError("manifest macro dependency closure exceeds the edge budget")
            states[macro_id] = 1
            pending_sources[macro_id] = source
            stack.append((macro_id, True))
            stack.extend((dependency, False) for dependency in reversed(dependencies))
    return dict(sorted(sources.items()))


def _source_issues(
    source_id: str,
    source: str,
    allowed_vars: frozenset[str],
    macro_globals: Mapping[str, frozenset[str]],
) -> list[SemanticRefreshProofIssue]:
    issues: list[SemanticRefreshProofIssue] = []
    jinja = "\n".join(match.group(0) for match in _JINJA.finditer(source))
    for field, pattern in _FORBIDDEN_JINJA:
        if pattern.search(jinja):
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_DYNAMIC_MODEL_SOURCE_UNSUPPORTED",
                    field,
                    "raw Jinja or macro source depends on runtime target state or can issue side effects",
                    unique_id=source_id,
                )
            )
    var_calls = tuple(match.group(2) for match in _VAR_CALL.finditer(jinja))
    var_reference_count = len(_VAR_REFERENCE.findall(jinja))
    if var_reference_count != len(var_calls) or any(variable not in allowed_vars for variable in var_calls):
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_CALLER_VAR_UNSUPPORTED",
                "var",
                "only exact platform-owned scheduler variables are admitted",
                unique_id=source_id,
            )
        )
    issues.extend(_parsed_jinja_issues(source_id, source, macro_globals))
    return issues


def _parsed_jinja_issues(
    source_id: str,
    source: str,
    macro_globals: Mapping[str, frozenset[str]],
) -> list[SemanticRefreshProofIssue]:
    try:
        environment = Environment(extensions=("jinja2.ext.do",))
        environment.globals.clear()
        parsed = environment.parse(source)
    except TemplateSyntaxError:
        return [
            unverified(
                "DPONE_DBT_V2_SOURCE_UNVERIFIED",
                "jinja_syntax",
                "raw Jinja cannot be parsed for exact capability closure",
                unique_id=source_id,
            )
        ]
    issues: list[SemanticRefreshProofIssue] = []
    macro_name_counts: dict[str, int] = {}
    for names in macro_globals.values():
        for name in names:
            macro_name_counts[name] = macro_name_counts.get(name, 0) + 1
    macro_names = frozenset(name for name, count in macro_name_counts.items() if count == 1)
    local_macro_names = frozenset(item.name for item in parsed.find_all(nodes.Macro))
    direct_calls = _DIRECT_JINJA_CALLS | macro_names | local_macro_names
    allowed_globals = direct_calls | frozenset(macro_globals)
    unknown = sorted(meta.find_undeclared_variables(parsed) - allowed_globals)
    if (
        unknown
        or bool(local_macro_names & (_DIRECT_JINJA_CALLS | frozenset(macro_globals)))
        or any(parsed.find_all((nodes.Import, nodes.FromImport, nodes.Include, nodes.Extends)))
        or not _has_only_admitted_call_shapes(parsed, direct_calls, macro_globals)
    ):
        issues.append(_dynamic_capability_issue(source_id, "jinja_global"))
    return issues


def _has_only_admitted_call_shapes(
    parsed: nodes.Template,
    direct_calls: frozenset[str],
    macro_globals: Mapping[str, frozenset[str]],
) -> bool:
    if any(parsed.find_all((nodes.Filter, nodes.Test))):
        return False
    admitted_names: set[int] = set()
    admitted_attributes: set[int] = set()
    for call in parsed.find_all(nodes.Call):
        callee = call.node
        if isinstance(callee, nodes.Name) and callee.name in direct_calls:
            admitted_names.add(id(callee))
            continue
        if (
            isinstance(callee, nodes.Getattr)
            and isinstance(callee.node, nodes.Name)
            and callee.node.name in macro_globals
            and callee.attr in macro_globals[callee.node.name]
        ):
            admitted_names.add(id(callee.node))
            admitted_attributes.add(id(callee))
            continue
        return False
    if any(True for _item in parsed.find_all(nodes.Getitem)):
        return False
    if any(id(item) not in admitted_attributes for item in parsed.find_all(nodes.Getattr)):
        return False
    guarded_globals = direct_calls | frozenset(macro_globals)
    return all(
        item.name not in guarded_globals or id(item) in admitted_names
        for item in parsed.find_all(nodes.Name)
        if item.ctx == "load"
    )


def _dynamic_capability_issue(source_id: str, field: str) -> SemanticRefreshProofIssue:
    return nonconformant(
        "DPONE_DBT_JINJA_GLOBAL",
        field,
        "raw Jinja contains an unclassified runtime capability or dynamic template dependency",
        unique_id=source_id,
    )


def _macro_globals(required_macro_ids: tuple[str, ...]) -> dict[str, frozenset[str]]:
    values: dict[str, set[str]] = {}
    for unique_id in required_macro_ids:
        parts = unique_id.split(".")
        if len(parts) != 3 or parts[0] != "macro" or not all(parts[1:]):
            continue
        values.setdefault(parts[1], set()).add(parts[2])
    return {package: frozenset(names) for package, names in values.items()}


def _tokens(values: object, field: str, *, allow_empty: bool) -> tuple[str, ...]:
    if (
        not isinstance(values, tuple)
        or (not allow_empty and not values)
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{field} must be a unique tuple of non-empty strings")
    return tuple(sorted(values))


def _positive(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


__all__ = [
    "SOURCE_DEFINITION_PROOF_SCHEMA",
    "SourceDefinitionProof",
    "prove_raw_jinja_closure",
    "resolve_manifest_macro_source_closure",
]
