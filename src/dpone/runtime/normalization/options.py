"""Configuration for nested row normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.normalization.guardrails import NormalizationGuardrails
from dpone.runtime.normalization.policies import PathPolicyResolver
from dpone.runtime.normalization.snapshot_factory import ChildSnapshotStoreOptions


@dataclass(frozen=True, slots=True)
class RawLandingOptions:
    """Optional raw + normalized dual-write settings."""

    enabled: bool = False
    table_suffix: str = "__raw"
    payload_column: str = "payload"

    @classmethod
    def from_config(cls, config: Any) -> RawLandingOptions:
        if isinstance(config, bool):
            return cls(enabled=config)
        if not isinstance(config, dict):
            return cls()
        return cls(
            enabled=bool(config.get("enabled", False)),
            table_suffix=str(config.get("table_suffix", "__raw")),
            payload_column=str(config.get("payload_column", "payload")),
        )


@dataclass(frozen=True, slots=True)
class NestedAtomicityOptions:
    """Atomicity policy for root/child load packages."""

    mode: str = "staging_commit"
    on_failure: str = "rollback_staged"

    @classmethod
    def from_config(cls, config: Any) -> NestedAtomicityOptions:
        if not isinstance(config, dict):
            return cls()
        return cls(
            mode=str(config.get("mode", "staging_commit")),
            on_failure=str(config.get("on_failure", "rollback_staged")),
        )


@dataclass(frozen=True, slots=True)
class NestedChildQualityOptions:
    """Quality policy for generated child tables."""

    duplicate_child_key: str = "fail"
    orphan_child_rows: str = "fail"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "duplicate_child_key",
            _child_quality_policy(
                self.duplicate_child_key,
                option="duplicate_child_key",
            ),
        )
        object.__setattr__(
            self,
            "orphan_child_rows",
            _child_quality_policy(
                self.orphan_child_rows,
                option="orphan_child_rows",
            ),
        )

    @classmethod
    def from_config(cls, config: Any) -> NestedChildQualityOptions:
        if not isinstance(config, dict):
            return cls()
        return cls(
            duplicate_child_key=_child_quality_policy(
                config.get("duplicate_child_key", "fail"),
                option="duplicate_child_key",
            ),
            orphan_child_rows=_child_quality_policy(
                config.get("orphan_child_rows", "fail"),
                option="orphan_child_rows",
            ),
        )


@dataclass(frozen=True, slots=True)
class NestedNormalizationOptions:
    """Options controlling dlt-like nested object normalization."""

    enabled: bool = False
    nested_level: int = 32
    table_separator: str = "__"
    scalar_list_value_column: str = "value"
    preserve_nested_json: bool = False
    include_empty_tables: bool = False
    path_policies: PathPolicyResolver = field(default_factory=PathPolicyResolver)
    guardrails: NormalizationGuardrails = field(default_factory=NormalizationGuardrails)
    raw_landing: RawLandingOptions = field(default_factory=RawLandingOptions)
    hierarchy_contract: dict[str, Any] = field(default_factory=dict)
    materialization: str = "memory"
    spill_output_dir: str | None = None
    spill_output_format: str = "jsonl"
    child_snapshot_store: ChildSnapshotStoreOptions = field(default_factory=ChildSnapshotStoreOptions)
    atomicity: NestedAtomicityOptions = field(default_factory=NestedAtomicityOptions)
    child_quality: NestedChildQualityOptions = field(default_factory=NestedChildQualityOptions)

    @classmethod
    def from_config(cls, config: Any) -> NestedNormalizationOptions:
        if config is None:
            return cls()
        if isinstance(config, cls):
            return config
        if isinstance(config, bool):
            return cls(enabled=config)
        if not isinstance(config, dict):
            raise TypeError("normalization options must be a mapping, boolean, or NestedNormalizationOptions")
        raw_config = dict(config)
        nested = raw_config.get("nested")
        if isinstance(nested, dict):
            raw_config = {**raw_config, **nested}
        elif isinstance(nested, bool):
            raw_config["enabled"] = nested
        nested_level = raw_config.get("nested_level", raw_config.get("max_depth", 32))
        return cls(
            enabled=bool(raw_config.get("enabled", False)),
            nested_level=int(nested_level),
            table_separator=str(raw_config.get("table_separator", "__")),
            scalar_list_value_column=str(raw_config.get("scalar_list_value_column", "value")),
            preserve_nested_json=bool(raw_config.get("preserve_nested_json", False)),
            include_empty_tables=bool(raw_config.get("include_empty_tables", False)),
            path_policies=PathPolicyResolver.from_config(raw_config),
            guardrails=NormalizationGuardrails.from_config(raw_config.get("guardrails")),
            raw_landing=RawLandingOptions.from_config(raw_config.get("raw_landing")),
            hierarchy_contract=dict(raw_config.get("hierarchy_contract", {}) or {}),
            materialization=str(raw_config.get("materialization", "memory")),
            spill_output_dir=str(raw_config["spill_output_dir"]) if raw_config.get("spill_output_dir") else None,
            spill_output_format=str(raw_config.get("spill_output_format", raw_config.get("spill_format", "jsonl"))),
            child_snapshot_store=ChildSnapshotStoreOptions.from_config(
                raw_config.get("child_snapshot_store", raw_config.get("snapshot_store"))
            ),
            atomicity=NestedAtomicityOptions.from_config(raw_config.get("atomicity")),
            child_quality=NestedChildQualityOptions.from_config(raw_config.get("child_quality")),
        )


def _child_quality_policy(value: object, *, option: str) -> str:
    policy = str(value or "fail").strip().lower()
    if policy not in {"fail", "warn", "skip"}:
        raise ValueError(f"Unsupported nested child_quality.{option} policy `{policy}`; expected fail, warn, or skip")
    return policy
