"""Application facade for deterministic project workload selection."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
    sha256_confined_file,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url, manual_fix
from dpone.readiness.project_selection_loader import (
    NamedSelection,
    ProjectSelectionLoader,
    ProjectSelectionOutcome,
    SelectionEngine,
    SelectionError,
    SelectionRequest,
    SelectionState,
    parse_selection_expression,
    parse_selection_state,
    state_from_graph,
)

_MAX_NAMED_SELECTORS = 100
_MAX_EXPRESSIONS_PER_SELECTOR = 100
_MAX_SELECTOR_BYTES = 1024 * 1024
_MAX_STATE_BYTES = 4 * 1024 * 1024


class ProjectSelectionService:
    """Compose project loading, named/state inputs, pure selection, and drift checks."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        loader: ProjectSelectionLoader | None = None,
        engine: SelectionEngine | None = None,
    ) -> None:
        self._root = Path(root).resolve(strict=True)
        self._loader = loader or ProjectSelectionLoader(root=self._root)
        self._engine = engine or SelectionEngine()

    def select(
        self,
        *,
        target: str | Path,
        select: tuple[str, ...] = (),
        exclude: tuple[str, ...] = (),
        state_path: str | Path | None = None,
        selectors_path: str = "selectors.yaml",
        max_selected: int = 1000,
    ) -> ProjectSelectionOutcome:
        loaded = self._loader.load(target)
        requires_named = any(
            parse_selection_expression(expression).method == "selector" for expression in (*select, *exclude)
        )
        named, selectors_digest = load_named_selections(
            self._root,
            selectors_path,
            required=requires_named,
        )
        requires_state = _requires_state(select, exclude, named)
        baseline = None
        state_label = None
        state_digest = None
        if state_path is not None or requires_state:
            requested_state = state_path or ".dpone-cache/current/selection-state.json"
            baseline, state_label, state_digest = load_selection_state(self._root, requested_state)
        report = self._engine.select(
            loaded.graph,
            SelectionRequest(
                select=select,
                exclude=exclude,
                named=named,
                state=baseline,
                max_selected=max_selected,
            ),
        )
        consumed = dict(loaded.consumed_files)
        if selectors_digest is not None:
            consumed[selectors_path] = selectors_digest
        if state_label is not None and state_digest is not None:
            consumed[state_label] = state_digest
        self.verify_consumed_files(consumed)
        return ProjectSelectionOutcome(
            graph=loaded.graph,
            report=report,
            state=state_from_graph(loaded.graph),
            checked_sources=loaded.checked_sources,
            consumed_files=dict(sorted(consumed.items())),
            dags=loaded.dags,
        )

    def verify_consumed_files(self, consumed_files: dict[str, str]) -> None:
        for path, expected in consumed_files.items():
            try:
                actual = sha256_confined_file(
                    self._root,
                    path,
                    follow_in_root_symlinks=path.startswith(".dpone-cache/"),
                )
            except ConfinedFileError as exc:
                raise SelectionError(
                    "DPONE_SELECTION_STATE_CHANGED", "Selection input changed during planning."
                ) from exc
            if actual != expected:
                raise SelectionError("DPONE_SELECTION_STATE_CHANGED", "Selection input changed during planning.")


def _requires_state(
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    named: Mapping[str, object],
) -> bool:
    seen: set[str] = set()

    def visit(expression: str) -> bool:
        parsed = parse_selection_expression(expression)
        if parsed.method == "state":
            return True
        if parsed.method != "selector" or parsed.value in seen:
            return False
        seen.add(parsed.value)
        definition = named.get(parsed.value)
        if definition is None:
            return False
        nested = (*getattr(definition, "select", ()), *getattr(definition, "exclude", ()))
        return any(visit(item) for item in nested)

    return any(visit(item) for item in (*selected, *excluded))


def load_named_selections(
    root: Path,
    relative_path: str,
    *,
    required: bool,
) -> tuple[dict[str, NamedSelection], str | None]:
    """Load one bounded named-selector catalog from the project root."""

    candidate = root / relative_path
    if not candidate.exists() and not candidate.is_symlink():
        if required:
            raise SelectionError("DPONE_SELECTION_NAMED_NOT_FOUND", "Named selector file was not found.")
        return {}, None
    try:
        content = read_confined_file(root, relative_path, max_bytes=_MAX_SELECTOR_BYTES)
        payload = load_bounded_yaml(content, limits=BoundedYamlLimits(max_bytes=_MAX_SELECTOR_BYTES))
        digest = sha256_confined_file(root, relative_path)
    except (ConfinedFileError, BoundedYamlError) as exc:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Named selector file is unsafe or invalid.") from exc
    if not isinstance(payload, Mapping) or payload.get("schema") != "dpone.selectors.v1":
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Named selector schema must be dpone.selectors.v1.")
    raw_selectors = payload.get("selectors")
    if not isinstance(raw_selectors, Mapping) or len(raw_selectors) > _MAX_NAMED_SELECTORS:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Named selector mapping is invalid or too large.")
    selectors = {
        _selector_name(raw_name): _named_selection(raw_name, raw_definition)
        for raw_name, raw_definition in raw_selectors.items()
    }
    return selectors, digest


def _named_selection(raw_name: Any, raw_definition: Any) -> NamedSelection:
    name = _selector_name(raw_name)
    if not isinstance(raw_definition, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Named selector {name} must be an object.")
    if set(raw_definition) - {"description", "select", "exclude"}:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Named selector {name} has unknown fields.")
    return NamedSelection(
        name=name,
        select=_expressions(raw_definition.get("select"), name=name, required=True),
        exclude=_expressions(raw_definition.get("exclude", ()), name=name, required=False),
        description=_optional_text(raw_definition.get("description")),
    )


def _selector_name(value: Any) -> str:
    try:
        return parse_selection_expression(f"selector:{str(value).strip()}").value
    except SelectionError as exc:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Named selector name is invalid.") from exc


def _expressions(value: Any, *, name: str, required: bool) -> tuple[str, ...]:
    if not isinstance(value, list | tuple) or (required and not value):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Named selector {name} needs a select list.")
    if len(value) > _MAX_EXPRESSIONS_PER_SELECTOR:
        raise SelectionError("DPONE_SELECTION_LIMIT_EXCEEDED", f"Named selector {name} exceeds expression budget.")
    return tuple(parse_selection_expression(item).raw for item in value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > 500:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Named selector description is invalid.")
    return value.strip()


def load_selection_state(root: Path, path: str | Path) -> tuple[SelectionState, str, str]:
    """Load one bounded, confined selection-state baseline."""

    try:
        relative = project_relative_path(root, Path(path))
        follow_symlinks = relative.startswith(".dpone-cache/")
        content = read_confined_file(
            root,
            relative,
            max_bytes=_MAX_STATE_BYTES,
            follow_in_root_symlinks=follow_symlinks,
        )
        state = parse_selection_state(load_bounded_yaml(content, limits=BoundedYamlLimits(max_bytes=_MAX_STATE_BYTES)))
        digest = sha256_confined_file(root, relative, follow_in_root_symlinks=follow_symlinks)
    except SelectionError:
        raise
    except (ConfinedFileError, BoundedYamlError) as exc:
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state could not be read safely.") from exc
    return state, relative, digest


def selection_error_result(error: SelectionError, *, stage: str) -> SelfServiceResult:
    fixes = []
    if error.code == "DPONE_SELECTION_STATE_REQUIRED":
        fixes.append(manual_fix("provide_selection_state", command="dpone check . --state <selection-state.json>"))
    elif error.code in {"DPONE_SELECTION_EXPRESSION_INVALID", "DPONE_SELECTION_NAMED_NOT_FOUND"}:
        fixes.append(manual_fix("review_selector_syntax", command="dpone check --help"))
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                error.code,
                str(error),
                stage=stage,
                docs_url=error_docs_url(error.code),
                fixes=fixes,
                extra=error.context,
            ),
        ),
        exit_code=selection_error_exit_code(error.code),
    )


def selection_error_exit_code(code: str) -> int:
    if code in {
        "DPONE_SELECTION_EXPRESSION_INVALID",
        "DPONE_SELECTION_NAMED_NOT_FOUND",
        "DPONE_SELECTION_NAMED_CYCLE",
        "DPONE_SELECTION_STATE_REQUIRED",
        "DPONE_SELECTION_STATE_INVALID",
    }:
        return 2
    if code in {
        "DPONE_DISCOVERY_PATH_INVALID",
        "DPONE_LAYOUT_ROOT_INVALID",
        "DPONE_SELECTION_LIMIT_EXCEEDED",
        "DPONE_SELECTION_RUN_REQUIRES_SAFE_SAMPLE",
    }:
        return 4
    return 1


__all__ = [
    "ProjectSelectionOutcome",
    "ProjectSelectionService",
    "SelectionError",
    "selection_error_exit_code",
    "selection_error_result",
]
