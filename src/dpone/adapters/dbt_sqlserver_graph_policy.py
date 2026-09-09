"""Build-plane adapter for the dbt SQL Server selected-graph policy."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.adapters.dbt_semantic_refresh_sql_proof import (
    prove_target_independent_compiled_sql,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.dbt_selection import (
    manifest_preview_selected_graph as manifest_preview_selected_graph,
)
from dpone.contracts.dbt_selection import (
    require_supported_graph as require_supported_graph,
)
from dpone.contracts.dbt_selection import (
    semantic_refresh_preview_selected_graph as semantic_refresh_preview_selected_graph,
)
from dpone.contracts.dbt_semantic_refresh_selection import prove_mutation_closure
from dpone.contracts.dbt_semantic_refresh_source_proof import (
    prove_raw_jinja_closure,
    resolve_manifest_macro_source_closure,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
    evaluate_dbt_sqlserver_selected_graph,
)
from dpone.contracts.dbt_workflow_graph_policy import (
    evaluate_dbt_workflow_graph_ownership,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

MAX_DBT_SELECTION_OUTPUT_BYTES = 16 * 1024 * 1024


class DbtSqlserverPreviewGraphPolicyValidator:
    """Evaluate the manifest-preview closure used by authoring checks."""

    def __init__(self, *, manifest_reader: Callable[[Path], Mapping[str, Any]] | None = None) -> None:
        self._manifest_reader = manifest_reader if manifest_reader is not None else _read_manifest

    def validate(
        self,
        manifest_path: str | Path,
        selected_unique_ids_by_workflow: Mapping[str, tuple[str, ...]],
    ) -> tuple[DbtPublishIssue, ...]:
        path = Path(manifest_path)
        try:
            manifest = self._manifest_reader(path)
            selected_graphs = {
                workflow: manifest_preview_selected_graph(manifest, selected_unique_ids)
                for workflow, selected_unique_ids in sorted(selected_unique_ids_by_workflow.items())
            }
            ownership = evaluate_dbt_workflow_graph_ownership(
                publish_model_ids_by_workflow=selected_unique_ids_by_workflow,
                selected_graph_ids_by_workflow=selected_graphs,
            )
        except (OSError, ValueError):
            return (
                DbtPublishIssue(
                    code=DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
                    message="The dbt selected graph could not be evaluated safely",
                    path=path.as_posix(),
                    remediation="Run `dbt parse` with the pinned toolchain, then retry.",
                ),
            )
        if ownership.issues:
            nodes = manifest.get("nodes")
            node_mapping = nodes if isinstance(nodes, Mapping) else {}
            return tuple(
                DbtPublishIssue(
                    code=issue.code,
                    message=issue.message,
                    path=_node_path(
                        node_mapping.get(issue.unique_id),
                        fallback=issue.path,
                    ),
                    remediation=issue.remediation,
                )
                for issue in ownership.issues
            )
        reports = tuple(
            evaluate_dbt_sqlserver_selected_graph(
                manifest,
                selected_graph,
            )
            for _workflow, selected_graph in sorted(selected_graphs.items())
        )
        return tuple(
            DbtPublishIssue(
                code=issue.code,
                message=issue.message,
                path=issue.path,
                remediation=issue.remediation,
            )
            for report in reports
            for issue in report.issues
        )

    def validate_semantic_refresh(
        self,
        manifest_path: str | Path,
        selected_unique_ids_by_workflow: Mapping[str, tuple[str, ...]],
        *,
        require_target_independence: bool = False,
    ) -> tuple[DbtPublishIssue, ...]:
        """Validate V2 roots using the platform-injected lifecycle config."""

        try:
            manifest = _effective_semantic_refresh_manifest(self._manifest_reader(Path(manifest_path)))
            return tuple(
                issue
                for workflow, roots in sorted(selected_unique_ids_by_workflow.items())
                for issue in _semantic_refresh_issues(
                    manifest,
                    roots=roots,
                    selected_graph=semantic_refresh_preview_selected_graph(manifest, roots),
                    require_target_independence=require_target_independence,
                )
            )
        except (OSError, ValueError):
            return (
                DbtPublishIssue(
                    code="DPONE_DBT_V2_COMPILE_UNVERIFIED",
                    message="The semantic-refresh graph could not be proven from the immutable dbt artifact",
                    path=str(manifest_path),
                    remediation="Run pinned `dbt compile`, preserve target/manifest.json, then retry.",
                ),
            )


def _effective_semantic_refresh_manifest(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    """Overlay only the platform-owned lifecycle field for V2 proof."""

    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        raise ValueError("dbt manifest nodes are missing")
    effective_nodes: dict[str, Any] = {}
    for unique_id, raw_node in nodes.items():
        if not isinstance(raw_node, Mapping):
            effective_nodes[str(unique_id)] = raw_node
            continue
        config = raw_node.get("config")
        effective_config = dict(config) if isinstance(config, Mapping) else {}
        effective_config["incremental_strategy"] = "dpone_scope_merge"
        effective_nodes[str(unique_id)] = {**raw_node, "config": effective_config}
    return {**manifest, "nodes": effective_nodes}


def _semantic_refresh_issues(
    manifest: Mapping[str, Any],
    *,
    roots: tuple[str, ...],
    selected_graph: tuple[str, ...],
    require_target_independence: bool,
) -> tuple[DbtPublishIssue, ...]:
    closure = prove_mutation_closure(
        manifest,
        selected_graph_unique_ids=selected_graph,
        selected_mutating_node_ids=roots,
    )
    issues = [_proof_issue(issue) for issue in closure.issues]
    nodes = manifest.get("nodes")
    macros = manifest.get("macros", {})
    if not isinstance(nodes, Mapping) or not isinstance(macros, Mapping):
        raise ValueError("semantic refresh proof sections are missing")
    for unique_id in roots:
        node = nodes.get(unique_id)
        if not isinstance(node, Mapping):
            raise ValueError("semantic refresh root is absent")
        issues.extend(
            _semantic_refresh_model_definition_issues(
                node,
                macros,
                require_target_independence=require_target_independence,
            )
        )
    return tuple(issues)


def _semantic_refresh_model_definition_issues(
    node: Mapping[str, Any],
    macros: Mapping[str, Any],
    *,
    require_target_independence: bool,
) -> list[DbtPublishIssue]:
    unique_id = str(node.get("unique_id") or "")
    depends_on = node.get("depends_on")
    macro_ids = depends_on.get("macros") if isinstance(depends_on, Mapping) else None
    required_macros = tuple(sorted(macro_ids)) if isinstance(macro_ids, Sequence) else ()
    try:
        macro_sources = resolve_manifest_macro_source_closure(
            root_macro_ids=tuple(str(item) for item in required_macros),
            macros=macros,
        )
    except ValueError:
        macro_sources = {}
    raw = node.get("raw_code")
    source = prove_raw_jinja_closure(
        model_raw_sql=raw if isinstance(raw, str) else "",
        macro_sources=macro_sources,
        required_macro_ids=tuple(macro_sources) if macro_sources else tuple(str(item) for item in required_macros),
        allowed_vars=("dpone_data_interval_end", "dpone_data_interval_start"),
        maximum_source_bytes=1024 * 1024,
    )
    result = [_proof_issue(issue, fallback_unique_id=unique_id) for issue in source.issues]
    compiled = node.get("compiled_code")
    database = node.get("database")
    schema = node.get("schema")
    alias = node.get("alias") or node.get("name")
    if not all(isinstance(item, str) and item for item in (compiled, database, schema, alias)):
        result.append(
            DbtPublishIssue(
                code="DPONE_DBT_V2_COMPILE_UNVERIFIED",
                message="The immutable compiled SQL or target identity is unavailable",
                path=str(node.get("original_file_path") or unique_id),
                remediation="Run `dbt compile` with the pinned V2 toolchain, then retry.",
            )
        )
        return result
    compiled_targets = node.get("compiled_code_by_target")
    target_sql = (
        dict(compiled_targets)
        if require_target_independence and isinstance(compiled_targets, Mapping)
        else (
            {}
            if require_target_independence
            else {"manifest_preview": str(compiled), "manifest_preview_recheck": str(compiled)}
        )
    )
    sql = prove_target_independent_compiled_sql(
        target_sql,
        forbidden_relations=((str(database), str(schema), str(alias)),),
    )
    result.extend(_proof_issue(issue, fallback_unique_id=unique_id) for issue in sql.issues)
    return result


def _proof_issue(
    issue: Any,
    *,
    fallback_unique_id: str | None = None,
) -> DbtPublishIssue:
    unique_id = issue.unique_id or fallback_unique_id
    return DbtPublishIssue(
        code=issue.code,
        message=issue.message,
        path=unique_id or issue.field,
        remediation=(
            "Use one target-independent SQL SELECT with the certified V2 model/lifecycle policy; "
            "platform evidence marked UNVERIFIED must be restored before execution."
        ),
    )


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_DBT_SELECTION_OUTPUT_BYTES:
            raise ValueError("dbt parse manifest is missing, unsafe, or oversized")
        payload = path.read_bytes()
    except OSError as exc:
        raise ValueError("dbt parse manifest could not be read") from exc
    try:
        return strict_json_object(payload)
    except StrictJsonError as exc:
        raise ValueError("dbt parse manifest must be a strict JSON object") from exc


def _node_path(value: object, *, fallback: str) -> str:
    if not isinstance(value, Mapping):
        return fallback
    for field in ("original_file_path", "patch_path"):
        candidate = value.get(field)
        if isinstance(candidate, str) and candidate:
            return candidate
    return fallback


__all__ = [
    "DbtSqlserverPreviewGraphPolicyValidator",
    "MAX_DBT_SELECTION_OUTPUT_BYTES",
    "manifest_preview_selected_graph",
    "semantic_refresh_preview_selected_graph",
    "require_supported_graph",
]
