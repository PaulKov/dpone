"""Closed three-valued evaluator for the approved GitHub expression subset."""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from typing import Any, TypeAlias, cast

from tools.agent_policy.workflow_privilege_contracts import (
    V1_LIMITS,
    EdgeKind,
    JobResult,
    Root,
    Route,
    ScanLimits,
    TruthValue,
)
from tools.agent_policy.workflow_privilege_graph import job_dependencies, job_dependency_order

_TOKEN = re.compile(
    r"\s*(?:(\|\||&&|==|!=|[!(),])|([A-Za-z_][A-Za-z0-9_.-]*)|"
    r"('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")|([0-9]{1,640}(?![0-9])))"
)
_UNKNOWN = object()
_PR_REF_PREFIX = "refs/pull/"
_EXACT_PR_REF = re.compile(r"^refs/pull/[1-9][0-9]*/merge$", re.IGNORECASE)
_TOKEN_KINDS = ("operator", "identifier", "string", "number")
_QUOTED_TEXT = re.compile(r"'(?:''|[^'])*'")
_SECRET_ROOT = re.compile(r"(?:^\s*|[^A-Za-z0-9_.\s-]\s*)secrets(?![A-Za-z0-9_-])", re.I)
_TOKEN_ACCESS = re.compile(
    r"""\s*(?:\.\s*GITHUB_TOKEN(?![A-Za-z0-9_-])|\[\s*(?P<q>['"])GITHUB_TOKEN(?P=q)\s*\])""", re.I
)


def _expression_bodies(value: str) -> Iterator[str]:
    cursor = 0
    while (start := value.find("${{", cursor)) >= 0:
        cursor, quoted = start + 3, False
        while cursor < len(value) - 1:
            if value[cursor] == "'":
                if quoted and value.startswith("''", cursor):
                    cursor += 1
                else:
                    quoted = not quoted
            elif not quoted and value.startswith("}}", cursor):
                yield value[start + 3 : cursor]
                cursor += 2
                break
            cursor += 1
        else:
            return


def contains_secret_reference(value: object) -> bool:
    """Detect non-token secret authority with bounded alias-aware traversal."""
    stack, seen = [value], set()
    while stack:
        item = stack.pop()
        if isinstance(item, str) and any(
            _TOKEN_ACCESS.match(body, secret.end()) is None
            for body in _expression_bodies(item)
            for code in (_QUOTED_TEXT.sub(lambda part: " " * len(part.group()), body),)
            for secret in _SECRET_ROOT.finditer(code)
        ):
            return True
        if isinstance(item, (Mapping, list, tuple)) and id(item) not in seen:
            seen.add(id(item))
            stack.extend(item.values() if isinstance(item, Mapping) else item)
    return False


class ExpressionLimitError(ValueError):
    """Raised when expression bytes or lexical tokens exceed v1 limits."""

    def __init__(self, dimension: str) -> None:
        super().__init__(f"expression exceeds {dimension}")
        self.dimension = dimension


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Closed values available to expression evaluation for one route."""

    values: Mapping[str, object]
    needs: Mapping[str, JobResult]


@dataclass(frozen=True, slots=True)
class ExpressionResult:
    """Tri-state proof plus explicit status-function override evidence."""

    value: TruthValue
    status_override: bool = False


@dataclass(frozen=True, slots=True)
class _Value:
    value: object
    status_override: bool = False


_PULL_REQUEST_REF = object()
_Workflows: TypeAlias = Mapping[str, Mapping[str, Any]]
_ProofKey: TypeAlias = tuple[int, str, str, str]
_TRUE, _FALSE, _UNKNOWN_RESULT = TruthValue.TRUE, TruthValue.FALSE, TruthValue.UNKNOWN


def tri_not(value: TruthValue) -> TruthValue:
    return _UNKNOWN_RESULT if value is _UNKNOWN_RESULT else _FALSE if value is _TRUE else _TRUE


def tri_and(left: TruthValue, right: TruthValue) -> TruthValue:
    return _FALSE if _FALSE in {left, right} else _TRUE if left is right is _TRUE else _UNKNOWN_RESULT


def tri_or(left: TruthValue, right: TruthValue) -> TruthValue:
    return _TRUE if _TRUE in {left, right} else _FALSE if left is right is _FALSE else _UNKNOWN_RESULT


def _event_context(event_name: object, action: object, ref: object, merged: object) -> EvaluationContext:
    return EvaluationContext(
        values={
            "github.event_name": event_name,
            "github.event.action": action,
            "github.ref": ref,
            "github.event.pull_request.merged": merged,
        },
        needs={},
    )


def direct_pr_context(variant: str, *, target_branch_ref: str | None, event: str = "pull_request") -> EvaluationContext:
    """Create one correlated direct pull-request route context."""

    closed = variant.startswith("CLOSED_")
    action = "closed" if closed else variant.removeprefix("ACTIVITY:")
    ref = (target_branch_ref or _UNKNOWN) if variant == "CLOSED_MERGED" else _PULL_REQUEST_REF
    return _event_context(event, action, ref, variant == "CLOSED_MERGED" if closed else False)


def transition_context(
    context: EvaluationContext,
    edge_kind: EdgeKind | str,
    *,
    workflow_run_activity: str | None = None,
) -> EvaluationContext:
    """Apply the exact event-context reset at a graph transition."""

    return (
        _event_context("workflow_run", workflow_run_activity or _UNKNOWN, _UNKNOWN, _UNKNOWN)
        if edge_kind == EdgeKind.WORKFLOW_RUN
        else context
    )


def with_needs(context: EvaluationContext, needs: Mapping[str, JobResult]) -> EvaluationContext:
    """Return a context with one exact job-result projection."""

    return replace(context, needs={name.casefold(): result for name, result in needs.items()})


def evaluate_condition(expression: str, context: EvaluationContext, *, limits: ScanLimits) -> ExpressionResult:
    """Evaluate the closed subset; unsupported syntax is conservatively UNKNOWN."""

    if len(expression.encode("utf-8")) > limits.expression_bytes:
        raise ExpressionLimitError("expression_bytes")
    try:
        source, names, token_count = _compile_expression(expression)
        if token_count > limits.expression_tokens:
            raise ExpressionLimitError("expression_tokens")
        node = ast.parse(source, mode="eval").body
        result = _AstEvaluator(context, names)(node)
    except ExpressionLimitError:
        raise
    except (ValueError, SyntaxError, RecursionError):
        return ExpressionResult(TruthValue.UNKNOWN)
    return ExpressionResult(_truth(result.value), result.status_override)


class RouteConditionProof:
    """Prove route and predecessor guards with one bounded memoized context."""

    def __init__(self, roots: tuple[Root, ...], workflows: _Workflows) -> None:
        self._roots, self._workflows = roots, workflows
        self._cache: dict[_ProofKey, dict[str, tuple[TruthValue, JobResult]]] = {}

    def __call__(self, route: Route) -> TruthValue:
        root = self._roots[route.root_index]
        context = direct_pr_context(
            route.event_variant.split(">", 1)[0],
            target_branch_ref=self._target_branch_ref(root),
            event=root.event,
        )
        reachability = TruthValue.TRUE
        phase = "DIRECT"
        for edge in route.edge_chain:
            if edge.kind == EdgeKind.LOCAL_WORKFLOW_CALL and edge.source_job is not None:
                caller, _ = self._job_state(context, route, phase, edge.source_workflow, edge.source_job)
                reachability = tri_and(reachability, caller)
            if edge.kind == EdgeKind.WORKFLOW_RUN:
                activity = route.event_variant.split(">WORKFLOW_RUN:", 1)[1]
                context = transition_context(context, edge.kind, workflow_run_activity=activity)
                phase = f"WORKFLOW_RUN:{activity}"
        endpoint, _ = self._job_state(context, route, phase, route.workflow, route.job_id)
        return tri_and(reachability, endpoint)

    def _job_state(
        self,
        context: EvaluationContext,
        route: Route,
        phase: str,
        workflow: str,
        job_id: str,
    ) -> tuple[TruthValue, JobResult]:
        states_by_job = self._cache.setdefault((route.root_index, route.event_variant, phase, workflow), {})
        if job_id in states_by_job:
            return states_by_job[job_id]
        try:
            order = job_dependency_order(self._workflows, workflow)
        except ValueError:
            states_by_job[job_id] = TruthValue.UNKNOWN, JobResult.UNKNOWN
            return states_by_job[job_id]
        jobs = cast(Mapping[str, Mapping[str, Any]], self._workflows[workflow]["jobs"])
        for current in order:
            if current in states_by_job:
                continue
            states = {
                dependency: states_by_job[dependency]
                for dependency in job_dependencies(self._workflows, workflow, current)
            }
            gate = TruthValue.TRUE
            for reachability, _ in states.values():
                gate = tri_and(gate, reachability)
            needs = {name: result for name, (_, result) in states.items()}
            condition = self._job_condition(with_needs(context, needs), jobs[current])
            reachability = condition.value if condition.status_override else tri_and(gate, condition.value)
            result = JobResult.SKIPPED if reachability is TruthValue.FALSE else JobResult.UNKNOWN
            states_by_job[current] = reachability, result
        return states_by_job[job_id]

    def _job_condition(self, context: EvaluationContext, job: Mapping[str, Any]) -> ExpressionResult:
        condition = job.get("if")
        if condition is None or type(condition) is bool:
            return ExpressionResult(TruthValue.FALSE if condition is False else TruthValue.TRUE)
        if not isinstance(condition, str):
            return ExpressionResult(TruthValue.UNKNOWN)
        expression = condition.strip()
        if expression.startswith("${{") and expression.endswith("}}"):
            expression = expression[3:-2].strip()
        return evaluate_condition(expression, context, limits=V1_LIMITS)

    def _target_branch_ref(self, root: Root) -> str | None:
        events = self._workflows[root.workflow].get("on", {})
        configuration = events.get(root.event) if isinstance(events, Mapping) else None
        branches = configuration.get("branches") if isinstance(configuration, Mapping) else None
        branches = [branches] if isinstance(branches, str) else branches
        if not isinstance(branches, list) or len(branches) != 1 or not isinstance(branches[0], str):
            return None
        branch = branches[0]
        return None if not branch or any(character in branch for character in "*?[]!") else f"refs/heads/{branch}"


def _compile_expression(expression: str) -> tuple[str, dict[str, str], int]:
    parts: list[str] = []
    names: dict[str, str] = {}
    position = 0
    while position < len(expression):
        match = _TOKEN.match(expression, position)
        if match is None:
            if expression[position:].strip():
                raise ValueError("unsupported expression token")
            break
        groups = match.groups()
        index = next(index for index, value in enumerate(groups) if value is not None)
        kind, token = _TOKEN_KINDS[index], groups[index]
        key = f"_v{len(parts)}"
        if kind == "operator":
            parts.append({"||": "or", "&&": "and", "!": "~"}.get(token, token))
        elif kind == "identifier" and token.casefold() in {"true", "false"}:
            parts.append(token.title())
        elif kind == "identifier":
            names[key] = token.casefold()
            parts.append(key)
        elif kind == "string":
            parts.append(repr(_decode_string(token)))
        else:
            parts.append(token)
        position = match.end()
    return " ".join(parts), names, len(parts)


class _AstEvaluator:
    def __init__(self, context: EvaluationContext, names: Mapping[str, str]) -> None:
        self._context, self._names = context, names

    def __call__(self, node: ast.expr) -> _Value:
        if isinstance(node, ast.Constant) and type(node.value) in {bool, int, str}:
            return _Value(node.value)
        if isinstance(node, ast.Name):
            return _Value(self._name(node.id))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Invert):
            value = self(node.operand)
            return _Value(tri_not(_truth(value.value)), value.status_override)
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            values = tuple(self(value) for value in node.values)
            operation = tri_and if isinstance(node.op, ast.And) else tri_or
            result = _truth(values[0].value)
            for value in values[1:]:
                result = operation(result, _truth(value.value))
            return _Value(result, any(value.status_override for value in values))
        if isinstance(node, ast.Compare) and len(node.ops) == len(node.comparators) == 1:
            if not isinstance(node.ops[0], (ast.Eq, ast.NotEq)):
                raise ValueError("unsupported comparison")
            left, right = self(node.left), self(node.comparators[0])
            result = _equals(left.value, right.value)
            if isinstance(node.ops[0], ast.NotEq):
                result = tri_not(result)
            return _Value(result, left.status_override or right.status_override)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and not node.keywords:
            return self._call(self._names.get(node.func.id, ""), [self(value) for value in node.args])
        raise ValueError("unsupported expression syntax")

    def _name(self, key: str) -> object:
        name = self._names.get(key, "")
        if name.startswith("needs.") and name.endswith(".result"):
            job_id = name.removeprefix("needs.").removesuffix(".result")
            result = self._context.needs.get(job_id, JobResult.UNKNOWN)
            return result.value if result is not JobResult.UNKNOWN else _UNKNOWN
        return self._context.values.get(name, _UNKNOWN)

    def _call(self, name: str, arguments: list[_Value]) -> _Value:
        override = any(argument.status_override for argument in arguments)
        if name == "startswith" and len(arguments) == 2:
            left, prefix = (argument.value for argument in arguments)
            if left is _PULL_REQUEST_REF and isinstance(prefix, str):
                return _Value(_symbolic_starts_with(prefix), override)
            if isinstance(left, str) and isinstance(prefix, str):
                matches = left.casefold().startswith(prefix.casefold())
                return _Value(TruthValue.TRUE if matches else TruthValue.FALSE, override)
            return _Value(TruthValue.UNKNOWN, override)
        if arguments:
            return _Value(TruthValue.UNKNOWN, override)
        if name == "always":
            return _Value(TruthValue.TRUE, True)
        if name in {"success", "failure", "cancelled"}:
            return _Value(_status(name, self._context.needs), True)
        return _Value(TruthValue.UNKNOWN, override)


def _decode_string(token: str) -> str:
    return re.sub(r"\\(['\"\\])", r"\1", token[1:-1])


def _truth(value: object) -> TruthValue:
    if isinstance(value, TruthValue):
        return value
    return TruthValue.TRUE if value is True else TruthValue.FALSE if value is False else TruthValue.UNKNOWN


def _equals(left: object, right: object) -> TruthValue:
    if left is _PULL_REQUEST_REF or right is _PULL_REQUEST_REF:
        if left is right:
            return TruthValue.TRUE
        literal = right if left is _PULL_REQUEST_REF else left
        return (
            TruthValue.UNKNOWN if not isinstance(literal, str) or _EXACT_PR_REF.fullmatch(literal) else TruthValue.FALSE
        )
    if left is _UNKNOWN or right is _UNKNOWN or type(left) is not type(right):
        return TruthValue.UNKNOWN
    if isinstance(left, str) and isinstance(right, str):
        equal = left.casefold() == right.casefold()
    elif type(left) in {int, float, bool} or left is None:
        equal = left == right
    else:
        return TruthValue.UNKNOWN
    return TruthValue.TRUE if equal else TruthValue.FALSE


def _symbolic_starts_with(prefix: str) -> TruthValue:
    folded = prefix.casefold()
    if _PR_REF_PREFIX.startswith(folded):
        return TruthValue.TRUE
    return TruthValue.UNKNOWN if folded.startswith(_PR_REF_PREFIX) else TruthValue.FALSE


def _status(name: str, needs: Mapping[str, JobResult]) -> TruthValue:
    values = tuple(needs.values())
    if not values or JobResult.UNKNOWN in values:
        return TruthValue.UNKNOWN
    expected = {"failure": JobResult.FAILURE, "cancelled": JobResult.CANCELLED}.get(name)
    if expected is not None:
        fallback = TruthValue.UNKNOWN if JobResult.SKIPPED in values else TruthValue.FALSE
        return TruthValue.TRUE if expected in values else fallback
    return TruthValue.TRUE if all(value is JobResult.SUCCESS for value in values) else TruthValue.FALSE


__all__ = """EvaluationContext ExpressionLimitError ExpressionResult RouteConditionProof contains_secret_reference
direct_pr_context evaluate_condition transition_context tri_and tri_not tri_or with_needs""".split()
