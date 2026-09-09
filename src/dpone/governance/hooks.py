"""Typed dbt-style hook graph for source preparation and load governance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from dpone._compat import UTC
from dpone.governance.hook_sql_files import resolve_hook_sql

HOOK_PHASES = ("pre_hook", "post_hook")
HOOK_KINDS = frozenset(
    {
        "source_refresh",
        "source_materialization",
        "source_maintenance",
        "target_preparation",
        "target_maintenance",
        "data_quality_probe",
        "notification",
        "custom",
    }
)


class HookValidationError(ValueError):
    """Raised when hook graph configuration is unsafe or ambiguous."""


class HookProvider(Protocol):
    def execute(self, action: HookDefinition, context: HookExecutionContext) -> Mapping[str, Any] | None: ...


class LoadStepAuditStorage(Protocol):
    def record_step(self, record: LoadStepAuditRecord) -> None: ...


@dataclass(frozen=True, slots=True)
class HookExecutionMode:
    cli: str = "inline"
    airflow: str = "inline"

    @classmethod
    def from_config(cls, raw: object) -> HookExecutionMode:
        values = raw if isinstance(raw, Mapping) else {}
        return cls(
            cli=_choice(values.get("cli"), default="inline", choices={"inline"}),
            airflow=_choice(values.get("airflow"), default="inline", choices={"inline", "separate_task"}),
        )


@dataclass(frozen=True, slots=True)
class HookLineage:
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()

    @classmethod
    def from_config(cls, raw: object) -> HookLineage:
        values = raw if isinstance(raw, Mapping) else {}
        return cls(inputs=_strings(values.get("inputs")), outputs=_strings(values.get("outputs")))


@dataclass(frozen=True, slots=True)
class HookDefinition:
    id: str
    type: str
    sql: str | None = None
    sql_file: str | None = None
    sql_hash: str | None = None
    kind: str = "custom"
    connector: str = "source"
    autocommit: bool = False
    mutates_source: bool = False
    retry_policy: str = "none"
    depends_on: tuple[str, ...] = ()
    lineage: HookLineage = field(default_factory=HookLineage)
    execution: HookExecutionMode = field(default_factory=HookExecutionMode)
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_config(
        cls,
        raw: Mapping[str, Any],
        *,
        manifest_dir: str | None = None,
        repo_root: str | None = None,
    ) -> HookDefinition:
        hook_id = _required_text(raw.get("id"), "hook.id")
        hook_type = _required_text(raw.get("type"), f"hook {hook_id}.type")
        kind = str(raw.get("kind") or "custom").strip() or "custom"
        if kind not in HOOK_KINDS:
            raise HookValidationError(f"hook {hook_id} kind must be one of: {', '.join(sorted(HOOK_KINDS))}")
        try:
            resolved_sql = resolve_hook_sql(
                inline_sql=raw.get("sql"),
                sql_file=raw.get("sql_file"),
                manifest_dir=manifest_dir,
                repo_root=repo_root,
            )
        except (OSError, ValueError) as exc:
            raise HookValidationError(f"hook {hook_id} {exc}") from exc
        mutates_source = bool(raw.get("mutates_source", False))
        if hook_type == "sql" and _looks_mutating(resolved_sql.sql) and not mutates_source:
            raise HookValidationError(f"hook {hook_id} executes mutating SQL; set mutates_source: true")
        retry_policy = str(
            raw.get("retry_policy") or ("none" if mutates_source or kind == "source_refresh" else "default")
        )
        return cls(
            id=hook_id,
            type=hook_type,
            sql=resolved_sql.sql,
            sql_file=resolved_sql.sql_file,
            sql_hash=resolved_sql.sql_hash,
            kind=kind,
            connector=str(raw.get("connector") or "source").strip() or "source",
            autocommit=bool(raw.get("autocommit", False)),
            mutates_source=mutates_source,
            retry_policy=retry_policy,
            depends_on=_strings(raw.get("depends_on")),
            lineage=HookLineage.from_config(raw.get("lineage")),
            execution=HookExecutionMode.from_config(raw.get("execution")),
            raw=dict(raw),
        )


@dataclass(frozen=True, slots=True)
class HookGraph:
    pre_hook: tuple[HookDefinition, ...] = ()
    post_hook: tuple[HookDefinition, ...] = ()

    @classmethod
    def from_config(
        cls,
        raw: object,
        *,
        manifest_dir: str | None = None,
        repo_root: str | None = None,
    ) -> HookGraph:
        if raw in (None, False):
            return cls()
        if not isinstance(raw, Mapping):
            raise HookValidationError("hooks config must be an object")
        unsupported = sorted(key for key in raw if str(key) in {"pre_run", "post_run"})
        if unsupported:
            raise HookValidationError("use dbt-style pre_hook/post_hook instead of pre_run/post_run")
        phases = {
            phase: _actions(raw.get(phase), phase=phase, manifest_dir=manifest_dir, repo_root=repo_root)
            for phase in HOOK_PHASES
        }
        for phase, actions in phases.items():
            _validate_phase_graph(phase, actions)
        return cls(pre_hook=phases["pre_hook"], post_hook=phases["post_hook"])

    def phase(self, phase: str) -> tuple[HookDefinition, ...]:
        if phase == "pre_hook":
            return self.pre_hook
        if phase == "post_hook":
            return self.post_hook
        raise HookValidationError(f"unsupported hook phase: {phase}")

    def ordered_phase(self, phase: str) -> tuple[HookDefinition, ...]:
        actions = self.phase(phase)
        by_id = {action.id: action for action in actions}
        visited: set[str] = set()
        order: list[HookDefinition] = []

        def visit(action: HookDefinition) -> None:
            if action.id in visited:
                return
            for dependency in action.depends_on:
                visit(by_id[dependency])
            visited.add(action.id)
            order.append(action)

        for action in actions:
            visit(action)
        return tuple(order)


@dataclass(frozen=True, slots=True)
class HookExecutionContext:
    run_id: str
    load_id: str
    process_name: str | None = None
    phase: str = "pre_hook"


@dataclass(frozen=True, slots=True)
class LoadStepAuditRecord:
    run_id: str
    load_id: str
    step_id: str
    phase: str
    kind: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    error_message: str | None = None
    details: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HookStepEvidence:
    id: str
    phase: str
    kind: str
    type: str
    status: str
    result: Mapping[str, Any] = field(default_factory=dict)
    lineage: HookLineage = field(default_factory=HookLineage)
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class HookPhaseEvidence:
    phase: str
    steps: tuple[HookStepEvidence, ...]

    @property
    def passed(self) -> bool:
        return all(step.status == "succeeded" for step in self.steps)


@dataclass
class InMemoryLoadStepAuditStorage:
    records: list[LoadStepAuditRecord] = field(default_factory=list)

    def record_step(self, record: LoadStepAuditRecord) -> None:
        self.records.append(record)


class HookGraphRunner:
    def __init__(
        self,
        *,
        providers: Mapping[str, HookProvider],
        audit_storage: LoadStepAuditStorage | None = None,
    ) -> None:
        self._providers = dict(providers)
        self._audit_storage = audit_storage

    def run_phase(self, graph: HookGraph, phase: str, context: HookExecutionContext) -> HookPhaseEvidence:
        steps: list[HookStepEvidence] = []
        for action in graph.ordered_phase(phase):
            step_context = HookExecutionContext(
                run_id=context.run_id,
                load_id=context.load_id,
                process_name=context.process_name,
                phase=phase,
            )
            started_at = _utc_now()
            self._record(action, step_context, "running", started_at=started_at)
            try:
                provider = self._providers[action.type]
                result = dict(provider.execute(action, step_context) or {})
            except Exception as exc:
                self._record(action, step_context, "failed", started_at=started_at, error_message=str(exc))
                steps.append(_evidence(action, step_context, "failed", error_message=str(exc)))
                raise
            self._record(action, step_context, "succeeded", started_at=started_at)
            steps.append(_evidence(action, step_context, "succeeded", result=result))
        return HookPhaseEvidence(phase=phase, steps=tuple(steps))

    def _record(
        self,
        action: HookDefinition,
        context: HookExecutionContext,
        status: str,
        *,
        started_at: datetime,
        error_message: str | None = None,
    ) -> None:
        if self._audit_storage is None:
            return
        self._audit_storage.record_step(
            LoadStepAuditRecord(
                run_id=context.run_id,
                load_id=context.load_id,
                step_id=action.id,
                phase=context.phase,
                kind=action.kind,
                status=status,
                started_at=started_at,
                finished_at=_utc_now() if status != "running" else None,
                error_message=error_message,
            )
        )


def _actions(
    raw: object,
    *,
    phase: str,
    manifest_dir: str | None = None,
    repo_root: str | None = None,
) -> tuple[HookDefinition, ...]:
    if raw in (None, False):
        return ()
    if not isinstance(raw, list):
        raise HookValidationError(f"{phase} must be a list of hook actions")
    return tuple(
        HookDefinition.from_config(item, manifest_dir=manifest_dir, repo_root=repo_root)
        for item in raw
        if isinstance(item, Mapping)
    )


def _validate_phase_graph(phase: str, actions: Sequence[HookDefinition]) -> None:
    ids = [action.id for action in actions]
    duplicate_ids = sorted({hook_id for hook_id in ids if ids.count(hook_id) > 1})
    if duplicate_ids:
        raise HookValidationError(f"{phase} has duplicate hook ids: {', '.join(duplicate_ids)}")
    by_id = {action.id: action for action in actions}
    for action in actions:
        for dependency in action.depends_on:
            if dependency not in by_id:
                raise HookValidationError(f"{phase} hook {action.id} has missing dependency: {dependency}")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(action: HookDefinition) -> None:
        if action.id in visited:
            return
        if action.id in visiting:
            raise HookValidationError(f"{phase} hook graph contains a cycle at {action.id}")
        visiting.add(action.id)
        for dependency in action.depends_on:
            visit(by_id[dependency])
        visiting.remove(action.id)
        visited.add(action.id)

    for action in actions:
        visit(action)


def _evidence(
    action: HookDefinition,
    context: HookExecutionContext,
    status: str,
    *,
    result: Mapping[str, Any] | None = None,
    error_message: str | None = None,
) -> HookStepEvidence:
    return HookStepEvidence(
        id=action.id,
        phase=context.phase,
        kind=action.kind,
        type=action.type,
        status=status,
        result=dict(result or {}),
        lineage=action.lineage,
        error_message=error_message,
    )


def _strings(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    if not isinstance(raw, Sequence):
        return ()
    return tuple(str(item).strip() for item in raw if str(item).strip())


def _choice(raw: object, *, default: str, choices: set[str]) -> str:
    value = str(raw or default).strip().lower()
    if value not in choices:
        raise HookValidationError(f"expected one of {', '.join(sorted(choices))}; got {value!r}")
    return value


def _required_text(raw: object, label: str) -> str:
    value = _optional_text(raw)
    if not value:
        raise HookValidationError(f"{label} is required")
    return value


def _optional_text(raw: object) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _looks_mutating(sql: str | None) -> bool:
    if not sql:
        return False
    first = sql.strip().split(None, 1)[0].lower() if sql.strip() else ""
    return first in {"exec", "execute", "insert", "update", "delete", "merge", "create", "alter", "drop", "truncate"}


def _utc_now() -> datetime:
    return datetime.now(UTC)
