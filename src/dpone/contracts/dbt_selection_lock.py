"""Immutable dbt graph selection identity."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import (
    canonical_fingerprint,
    contract_error,
    require_digest,
    require_strict_mapping,
    require_strings,
    require_text,
    require_token,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)

DBT_SELECTION_LOCK_SCHEMA = "dpone.dbt-selection-lock.v1"
_SELECTION_ERROR = "DPONE_DBT_SELECTION_INVALID"
_SELECTION_KEYS = frozenset(
    "schema manifest_sha256 toolchain_sha256 invocation_context_sha256 "
    "graph_policy_id graph_policy_sha256 graph_contract_sha256 selectors selected_graph_unique_ids "
    "expected_run_result_unique_ids publish_model_unique_ids "
    "selection_sha256".split()
)


def _selection_values(
    values: tuple[str, ...],
    field_name: str,
) -> tuple[str, ...]:
    if not isinstance(values, tuple) or not values:
        raise contract_error(
            _SELECTION_ERROR,
            f"{field_name} must be a non-empty tuple",
        )
    normalized = tuple(sorted(require_token(item, field_name, _SELECTION_ERROR) for item in values))
    if len(normalized) != len(set(normalized)):
        raise contract_error(
            _SELECTION_ERROR,
            f"{field_name} contains duplicate values",
        )
    return normalized


@dataclass(frozen=True, slots=True)
class DbtSelectionLock:
    """Exact selected dbt graph and expected runtime result identities."""

    manifest_sha256: str
    toolchain_sha256: str
    invocation_context_sha256: str
    graph_policy_id: str
    graph_policy_sha256: str
    graph_contract_sha256: str
    selectors: tuple[str, ...]
    selected_graph_unique_ids: tuple[str, ...]
    expected_run_result_unique_ids: tuple[str, ...]
    publish_model_unique_ids: tuple[str, ...]
    selection_sha256: str
    schema: str = DBT_SELECTION_LOCK_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_SELECTION_LOCK_SCHEMA:
            raise contract_error(
                _SELECTION_ERROR,
                "dbt selection schema is invalid",
            )
        require_digest(
            self.manifest_sha256,
            "manifest sha256",
            _SELECTION_ERROR,
        )
        require_digest(
            self.toolchain_sha256,
            "toolchain sha256",
            _SELECTION_ERROR,
        )
        require_digest(
            self.invocation_context_sha256,
            "invocation context sha256",
            _SELECTION_ERROR,
        )
        require_token(
            self.graph_policy_id,
            "graph policy id",
            _SELECTION_ERROR,
        )
        require_digest(
            self.graph_policy_sha256,
            "graph policy sha256",
            _SELECTION_ERROR,
        )
        if (
            self.graph_policy_id != DBT_SQLSERVER_GRAPH_POLICY_ID
            or self.graph_policy_sha256 != DBT_SQLSERVER_GRAPH_POLICY_SHA256
        ):
            raise contract_error(
                _SELECTION_ERROR,
                "dbt graph policy does not match the supported SQL Server policy",
            )
        require_digest(
            self.graph_contract_sha256,
            "graph contract sha256",
            _SELECTION_ERROR,
        )
        object.__setattr__(
            self,
            "selectors",
            _selection_values(self.selectors, "selectors"),
        )
        object.__setattr__(
            self,
            "selected_graph_unique_ids",
            _selection_values(
                self.selected_graph_unique_ids,
                "selected_graph_unique_ids",
            ),
        )
        object.__setattr__(
            self,
            "expected_run_result_unique_ids",
            _selection_values(
                self.expected_run_result_unique_ids,
                "expected_run_result_unique_ids",
            ),
        )
        object.__setattr__(
            self,
            "publish_model_unique_ids",
            _selection_values(
                self.publish_model_unique_ids,
                "publish_model_unique_ids",
            ),
        )
        if not set(self.expected_run_result_unique_ids).issubset(self.selected_graph_unique_ids):
            raise contract_error(
                _SELECTION_ERROR,
                "expected run-results must be part of the selected graph",
            )
        if not set(self.publish_model_unique_ids).issubset(self.expected_run_result_unique_ids):
            raise contract_error(
                _SELECTION_ERROR,
                "publish models must be expected run-results nodes",
            )
        require_digest(
            self.selection_sha256,
            "selection sha256",
            _SELECTION_ERROR,
        )
        if self.selection_sha256 != canonical_fingerprint(self._unsigned()):
            raise contract_error(
                _SELECTION_ERROR,
                "dbt selection fingerprint differs from its content",
            )

    @classmethod
    def build(
        cls,
        *,
        manifest_sha256: str,
        toolchain_sha256: str,
        invocation_context_sha256: str,
        graph_policy_id: str,
        graph_policy_sha256: str,
        graph_contract_sha256: str,
        selectors: tuple[str, ...],
        selected_graph_unique_ids: tuple[str, ...],
        expected_run_result_unique_ids: tuple[str, ...],
        publish_model_unique_ids: tuple[str, ...],
    ) -> DbtSelectionLock:
        """Build a lock with a canonical semantic fingerprint."""

        selectors = _selection_values(selectors, "selectors")
        selected_graph_unique_ids = _selection_values(
            selected_graph_unique_ids,
            "selected_graph_unique_ids",
        )
        expected_run_result_unique_ids = _selection_values(
            expected_run_result_unique_ids,
            "expected_run_result_unique_ids",
        )
        publish_model_unique_ids = _selection_values(
            publish_model_unique_ids,
            "publish_model_unique_ids",
        )
        payload = _selection_dict(
            manifest_sha256,
            toolchain_sha256,
            invocation_context_sha256,
            graph_policy_id,
            graph_policy_sha256,
            graph_contract_sha256,
            selectors,
            selected_graph_unique_ids,
            expected_run_result_unique_ids,
            publish_model_unique_ids,
        )
        return cls(
            manifest_sha256,
            toolchain_sha256,
            invocation_context_sha256,
            graph_policy_id,
            graph_policy_sha256,
            graph_contract_sha256,
            selectors,
            selected_graph_unique_ids,
            expected_run_result_unique_ids,
            publish_model_unique_ids,
            canonical_fingerprint(payload),
        )

    @classmethod
    def from_mapping(cls, value: object) -> DbtSelectionLock:
        """Parse and validate a strict serialized selection lock."""

        raw = require_strict_mapping(
            value,
            "selection_lock",
            _SELECTION_KEYS,
            _SELECTION_ERROR,
        )
        return cls(
            require_text(
                raw.get("manifest_sha256"),
                "manifest_sha256",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("toolchain_sha256"),
                "toolchain_sha256",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("invocation_context_sha256"),
                "invocation_context_sha256",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("graph_policy_id"),
                "graph_policy_id",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("graph_policy_sha256"),
                "graph_policy_sha256",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("graph_contract_sha256"),
                "graph_contract_sha256",
                _SELECTION_ERROR,
            ),
            require_strings(
                raw.get("selectors"),
                "selectors",
                _SELECTION_ERROR,
            ),
            require_strings(
                raw.get("selected_graph_unique_ids"),
                "selected_graph_unique_ids",
                _SELECTION_ERROR,
            ),
            require_strings(
                raw.get("expected_run_result_unique_ids"),
                "expected_run_result_unique_ids",
                _SELECTION_ERROR,
            ),
            require_strings(
                raw.get("publish_model_unique_ids"),
                "publish_model_unique_ids",
                _SELECTION_ERROR,
            ),
            require_text(
                raw.get("selection_sha256"),
                "selection_sha256",
                _SELECTION_ERROR,
            ),
            require_text(raw.get("schema"), "schema", _SELECTION_ERROR),
        )

    def _unsigned(self) -> dict[str, object]:
        return _selection_dict(
            self.manifest_sha256,
            self.toolchain_sha256,
            self.invocation_context_sha256,
            self.graph_policy_id,
            self.graph_policy_sha256,
            self.graph_contract_sha256,
            self.selectors,
            self.selected_graph_unique_ids,
            self.expected_run_result_unique_ids,
            self.publish_model_unique_ids,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the canonical public mapping."""

        return {
            **self._unsigned(),
            "selection_sha256": self.selection_sha256,
        }


def _selection_dict(
    manifest_sha256: str,
    toolchain_sha256: str,
    invocation_context_sha256: str,
    graph_policy_id: str,
    graph_policy_sha256: str,
    graph_contract_sha256: str,
    selectors: tuple[str, ...],
    selected_graph_unique_ids: tuple[str, ...],
    expected_run_result_unique_ids: tuple[str, ...],
    publish_model_unique_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema": DBT_SELECTION_LOCK_SCHEMA,
        "manifest_sha256": manifest_sha256,
        "toolchain_sha256": toolchain_sha256,
        "invocation_context_sha256": invocation_context_sha256,
        "graph_policy_id": graph_policy_id,
        "graph_policy_sha256": graph_policy_sha256,
        "graph_contract_sha256": graph_contract_sha256,
        "selectors": list(selectors),
        "selected_graph_unique_ids": list(selected_graph_unique_ids),
        "expected_run_result_unique_ids": list(expected_run_result_unique_ids),
        "publish_model_unique_ids": list(publish_model_unique_ids),
    }


__all__ = ["DBT_SELECTION_LOCK_SCHEMA", "DbtSelectionLock"]
