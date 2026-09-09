"""Canonical build-plane result for one dbt-authoritative workflow selection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_graph_contract import dbt_publish_logical_target, expected_dbt_run_result_ids
from dpone.contracts.dbt_invocation import DbtInvocationTarget
from dpone.contracts.dbt_sqlserver_graph_policy import evaluate_dbt_sqlserver_selected_graph
from dpone.contracts.dbt_sqlserver_graph_policy_contract import dbt_sqlserver_graph_contract_sha256
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

_EXECUTABLE_PREFIXES = ("model.", "seed.", "snapshot.", "test.", "unit_test.")


@dataclass(frozen=True, slots=True)
class ResolvedDbtSelection:
    """Exact selected graph and emitted result IDs frozen into a lock."""

    selectors: tuple[str, ...]
    selected_graph_unique_ids: tuple[str, ...]
    expected_run_result_unique_ids: tuple[str, ...]
    graph_contract_sha256: str
    authority: str
    invocation_target: DbtInvocationTarget | None = None

    def __post_init__(self) -> None:
        if self.invocation_target is not None and not isinstance(self.invocation_target, DbtInvocationTarget):
            raise ValueError("dbt invocation target is invalid")
        if self.authority not in {"dbt_cli", "manifest_preview"}:
            raise ValueError("dbt selection authority is invalid")
        for name in (
            "selectors",
            "selected_graph_unique_ids",
            "expected_run_result_unique_ids",
        ):
            values = getattr(self, name)
            if (
                not isinstance(values, tuple)
                or not values
                or any(not isinstance(item, str) or not item for item in values)
                or tuple(sorted(values)) != values
                or len(values) != len(set(values))
            ):
                raise ValueError(f"dbt selection {name} must be sorted unique tokens")
        if not set(self.expected_run_result_unique_ids).issubset(self.selected_graph_unique_ids):
            raise ValueError("dbt expected run-results must be part of the selected graph")
        if not is_canonical_sha256_digest(self.graph_contract_sha256):
            raise ValueError("dbt graph contract fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class DbtSelectionPlan:
    """Pure selection intent shared by preview and the certified CLI adapter.

    Captured bytes retain the original comparison input; a successful completion
    validates graph policy and result membership, not a dbt invocation or SQL run.
    The adapter retains command execution, bounded acquisition and toolchain checks.
    """

    manifest: Mapping[str, Any]
    manifest_bytes: bytes
    selected_unique_ids: tuple[str, ...]
    selectors: tuple[str, ...]

    @classmethod
    def from_manifest(cls, payload: bytes, selected_unique_ids: tuple[str, ...]) -> DbtSelectionPlan:
        try:
            manifest = strict_json_object(payload)
        except StrictJsonError as exc:
            raise ValueError("dbt selection input must be a strict JSON object") from exc
        selectors = _selectors_for_unique_ids(manifest, selected_unique_ids)
        return cls(manifest, payload, selected_unique_ids, selectors)

    def require_matching_manifest(self, parsed_manifest: bytes) -> None:
        """Compare captured and freshly parsed semantics, excluding dbt volatility."""

        if _manifest_semantic_fingerprint(parsed_manifest) != _manifest_semantic_fingerprint(self.manifest_bytes):
            raise ValueError("supplied dbt manifest does not describe the captured project snapshot")

    def complete(
        self,
        manifest: Mapping[str, Any],
        selected_graph: tuple[str, ...],
        *,
        authority: str,
        invocation_target: DbtInvocationTarget | None = None,
    ) -> ResolvedDbtSelection:
        """Apply common graph safety before the historical authority-specific error."""

        require_supported_graph(
            manifest,
            selected_graph,
            expected_logical_target=dbt_publish_logical_target(manifest, self.selected_unique_ids),
        )
        expected_results = expected_dbt_run_result_ids(manifest, selected_graph)
        if not set(self.selected_unique_ids).issubset(expected_results):
            raise ValueError(
                "a publish-enabled model cannot be ephemeral"
                if authority == "manifest_preview"
                else "dbt selection omitted a publish-enabled model"
            )
        return ResolvedDbtSelection(
            selectors=self.selectors,
            selected_graph_unique_ids=selected_graph,
            expected_run_result_unique_ids=expected_results,
            graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(manifest, selected_graph),
            authority=authority,
            invocation_target=invocation_target,
        )


def manifest_preview_selected_graph(
    manifest: Mapping[str, Any],
    selected_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Return the deterministic executable closure for preview validation."""

    parent_map = _adjacency(manifest.get("parent_map"))
    child_map = _adjacency(manifest.get("child_map"))
    selected = set(selected_unique_ids)
    pending = list(selected_unique_ids)
    while pending:
        current = pending.pop()
        for parent in parent_map.get(current, ()):
            if parent.startswith(_EXECUTABLE_PREFIXES) and parent not in selected:
                selected.add(parent)
                pending.append(parent)
    for unique_id in tuple(selected):
        selected.update(child for child in child_map.get(unique_id, ()) if child.startswith(("test.", "unit_test.")))
    return tuple(sorted(selected))


def semantic_refresh_preview_selected_graph(
    manifest: Mapping[str, Any],
    selected_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Return exact V2 model roots plus compile-frozen read-only tests only."""

    child_map = _adjacency(manifest.get("child_map"))
    selected = set(selected_unique_ids)
    for unique_id in selected_unique_ids:
        selected.update(child for child in child_map.get(unique_id, ()) if child.startswith(("test.", "unit_test.")))
    return tuple(sorted(selected))


def require_supported_graph(
    manifest: Mapping[str, Any],
    selected_graph: tuple[str, ...],
    *,
    expected_logical_target: tuple[str, str] | None = None,
) -> None:
    """Raise one stable public error for the first unsupported capability."""

    report = evaluate_dbt_sqlserver_selected_graph(
        manifest,
        selected_graph,
        expected_logical_target=expected_logical_target,
    )
    if report.issues:
        issue = report.issues[0]
        raise DbtPublishingError(
            issue.code,
            issue.message,
            path=issue.path,
            remediation=issue.remediation,
        )


def _adjacency(value: object) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        raise ValueError("dbt manifest selection maps are missing")
    result: dict[str, tuple[str, ...]] = {}
    for key, raw_items in value.items():
        if (
            not isinstance(key, str)
            or not isinstance(raw_items, Sequence)
            or isinstance(raw_items, str | bytes)
            or any(not isinstance(item, str) for item in raw_items)
        ):
            raise ValueError("dbt manifest selection map is invalid")
        result[key] = tuple(raw_items)
    return result


def _selectors_for_unique_ids(
    manifest: Mapping[str, Any],
    selected_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        raise ValueError("dbt manifest nodes are unavailable for exact selection")
    selectors: list[str] = []
    for unique_id in selected_unique_ids:
        node = nodes.get(unique_id)
        fqn = node.get("fqn") if isinstance(node, Mapping) else None
        if (
            not isinstance(fqn, Sequence)
            or isinstance(fqn, str | bytes)
            or not fqn
            or any(
                not isinstance(part, str) or not part or any(character.isspace() for character in part) for part in fqn
            )
        ):
            raise ValueError("publish-enabled model has no safe exact dbt FQN")
        selectors.append(f"+fqn:{'.'.join(fqn)}")
    if len(selectors) != len(set(selectors)):
        raise ValueError("publish-enabled models have colliding dbt FQN selectors")
    return tuple(sorted(selectors))


def _manifest_semantic_fingerprint(payload: bytes) -> str:
    try:
        value = strict_json_object(payload)
    except StrictJsonError as exc:
        raise ValueError("dbt manifest is invalid JSON") from exc
    normalized = dict(value)
    metadata = normalized.get("metadata")
    if isinstance(metadata, Mapping):
        stable_metadata = dict(metadata)
        for field in (
            "env",
            "generated_at",
            "invocation_args_dict",
            "invocation_id",
            "invocation_started_at",
            "run_started_at",
            "send_anonymous_usage_stats",
            "user_id",
        ):
            stable_metadata.pop(field, None)
        normalized["metadata"] = stable_metadata
    normalized.pop("invocation_id", None)
    normalized = _without_dbt_parse_timestamps(normalized)
    raw = json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _without_dbt_parse_timestamps(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_dbt_parse_timestamps(item) for key, item in value.items() if key != "created_at"}
    if isinstance(value, list):
        return [_without_dbt_parse_timestamps(item) for item in value]
    return value


__all__ = [
    "ResolvedDbtSelection",
    "DbtSelectionPlan",
    "manifest_preview_selected_graph",
    "semantic_refresh_preview_selected_graph",
    "require_supported_graph",
]
