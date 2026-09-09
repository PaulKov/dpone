"""Reusable target table-settings physical design contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Protocol

TableSettingValue = str | int | float | bool
SettingClass = Literal["table_engine_setting", "insert_setting", "format_insert_setting", "insert_select_setting"]
SupportLevel = Literal["supported", "escape_hatch", "wrong_context", "unknown"]

_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CLICKHOUSE_NULLABLE_KEY_DOCS = "https://clickhouse.com/docs/operations/settings/merge-tree-settings#allow_nullable_key"
_CLICKHOUSE_PRIMARY_KEY_DOCS = (
    "https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#primary-keys-and-indexes-in-queries"
)


@dataclass(frozen=True, slots=True)
class TableSettingsOptions:
    """Normalized immutable target table settings."""

    settings: Mapping[str, TableSettingValue] = field(default_factory=dict)
    source_path: str = "physical_design.storage.<target>.table_settings"

    @classmethod
    def from_config(
        cls,
        raw: Mapping[str, Any] | None,
        *,
        field_prefix: str = "physical_design.storage.<target>.table_settings",
    ) -> TableSettingsOptions:
        if raw is None:
            return cls(source_path=field_prefix)
        if not isinstance(raw, Mapping):
            raise ValueError(f"{field_prefix} must be an object")
        return cls(
            settings=_normalize_settings(raw, field_prefix),
            source_path=field_prefix,
        )

    @classmethod
    def from_physical_design(
        cls,
        options: Mapping[str, Any] | None,
        *,
        target: str,
    ) -> TableSettingsOptions:
        physical = _mapping((options or {}).get("physical_design"))
        storage = _mapping(_mapping(physical.get("storage")).get(target))
        return cls.from_config(
            _mapping_or_none(storage.get("table_settings")),
            field_prefix=f"physical_design.storage.{target}.table_settings",
        )


@dataclass(frozen=True, slots=True)
class PhysicalSettingRule:
    """Target setting registry entry."""

    setting_class: str
    support_level: SupportLevel
    risk: str | None = None
    docs_url: str | None = None
    recommendation: str | None = None
    alternative_path: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedTableSetting:
    """Explainable physical table-setting decision."""

    key: str
    value: TableSettingValue
    source_path: str
    setting_class: str
    support_level: SupportLevel
    risk: str | None = None
    docs_url: str | None = None
    recommendation: str | None = None
    alternative_path: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True, slots=True)
class PhysicalSettingRegistry:
    """Target-aware table-setting rules."""

    target: str
    rules: Mapping[str, PhysicalSettingRule] = field(default_factory=dict)

    @classmethod
    def clickhouse(cls) -> PhysicalSettingRegistry:
        return cls(
            target="clickhouse",
            rules={
                "allow_nullable_key": PhysicalSettingRule(
                    setting_class="table_engine_setting",
                    support_level="escape_hatch",
                    risk="nullable_key_escape_hatch",
                    docs_url=_CLICKHOUSE_NULLABLE_KEY_DOCS,
                    recommendation=(
                        "Prefer physical_design.storage.clickhouse.nullability.mode="
                        "non_nullable_by_default for ORDER BY/PRIMARY KEY columns when possible."
                    ),
                ),
                "index_granularity": PhysicalSettingRule(
                    setting_class="table_engine_setting",
                    support_level="supported",
                    docs_url="https://clickhouse.com/docs/operations/settings/merge-tree-settings#index_granularity",
                ),
                "async_insert": _insert_setting("clickhouse_bulk.insert_settings.async_insert"),
                "wait_for_async_insert": _insert_setting("clickhouse_bulk.insert_settings.wait_for_async_insert"),
                "max_insert_block_size": _insert_setting("clickhouse_bulk.insert_settings.max_insert_block_size"),
                "input_format_null_as_default": PhysicalSettingRule(
                    setting_class="format_insert_setting",
                    support_level="wrong_context",
                    recommendation="Use physical_design.storage.clickhouse.nullability.null_handling instead.",
                    alternative_path="physical_design.storage.clickhouse.nullability.null_handling",
                ),
                "insert_null_as_default": PhysicalSettingRule(
                    setting_class="insert_select_setting",
                    support_level="wrong_context",
                    recommendation="Use physical_design.storage.clickhouse.nullability.null_handling instead.",
                    alternative_path="physical_design.storage.clickhouse.nullability.null_handling",
                ),
            },
        )

    def rule_for(self, key: str) -> PhysicalSettingRule:
        return self.rules.get(
            key,
            PhysicalSettingRule(
                setting_class="unknown_target_setting",
                support_level="unknown",
                recommendation=f"{self.target} will validate this table setting at CREATE TABLE execution time.",
            ),
        )


@dataclass(frozen=True, slots=True)
class PhysicalSettingClassifier:
    """Classify normalized table settings for one target."""

    registry: PhysicalSettingRegistry

    def resolve(self, options: TableSettingsOptions) -> tuple[ResolvedTableSetting, ...]:
        decisions = tuple(self._decision(options, key, value) for key, value in options.settings.items())
        wrong = next((decision for decision in decisions if decision.support_level == "wrong_context"), None)
        if wrong is not None:
            raise ValueError(_wrong_context_message(self.registry.target, wrong))
        return decisions

    def _decision(self, options: TableSettingsOptions, key: str, value: TableSettingValue) -> ResolvedTableSetting:
        rule = self.registry.rule_for(key)
        return ResolvedTableSetting(
            key=key,
            value=value,
            source_path=f"{options.source_path}.{key}",
            setting_class=rule.setting_class,
            support_level=rule.support_level,
            risk=rule.risk,
            docs_url=rule.docs_url,
            recommendation=rule.recommendation,
            alternative_path=rule.alternative_path,
        )


@dataclass(frozen=True, slots=True)
class PhysicalDesignAdvisor:
    """Render human-facing guidance from resolved physical settings."""

    def warnings(self, settings: Sequence[ResolvedTableSetting]) -> tuple[str, ...]:
        warnings: list[str] = []
        for setting in settings:
            if setting.support_level == "escape_hatch":
                warnings.append(_escape_hatch_warning(setting))
            elif setting.support_level == "unknown":
                warnings.append(
                    f"{setting.key} is not in the built-in {setting.setting_class} registry; "
                    "the target database will validate it during CREATE TABLE execution."
                )
        return tuple(warnings)


class TargetTableSettingsDialect(Protocol):
    """Target-specific table-settings renderer."""

    def resolve(self, options: TableSettingsOptions) -> tuple[ResolvedTableSetting, ...]:
        """Classify target table settings before rendering."""

    def render_settings(self, options: TableSettingsOptions) -> str:
        """Return target SQL settings clause, or an empty string."""


def _insert_setting(path: str) -> PhysicalSettingRule:
    return PhysicalSettingRule(
        setting_class="insert_setting",
        support_level="wrong_context",
        recommendation=f"Use sink.options.{path} instead.",
        alternative_path=path,
    )


def _validate_setting_name(key: str, field_prefix: str) -> bool:
    if not _SETTING_NAME.fullmatch(key):
        raise ValueError(f"Invalid table setting name {field_prefix}.{key}")
    return True


def _normalize_settings(raw: Mapping[str, Any], field_prefix: str) -> dict[str, TableSettingValue]:
    normalized: dict[str, TableSettingValue] = {}
    for raw_key, value in raw.items():
        key = str(raw_key)
        _validate_setting_name(key, field_prefix)
        normalized[key] = _table_setting_value(value, f"{field_prefix}.{key}")
    return dict(sorted(normalized.items()))


def _table_setting_value(value: Any, field_path: str) -> TableSettingValue:
    if isinstance(value, bool | int | float | str):
        return value
    raise ValueError(f"{field_path} must be a scalar string, number, integer, or boolean value")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("table_settings must be an object")
    return value


def _wrong_context_message(target: str, setting: ResolvedTableSetting) -> str:
    return (
        f"Invalid {target} table setting: {setting.key}. "
        f"{setting.key} is a {setting.setting_class}, not a table setting. "
        f"Use sink.options.{setting.alternative_path} instead."
    )


def _escape_hatch_warning(setting: ResolvedTableSetting) -> str:
    if setting.key == "allow_nullable_key":
        return (
            "allow_nullable_key enables Nullable expressions in ORDER BY / PRIMARY KEY. "
            "ClickHouse supports this only with allow_nullable_key, but strongly discourages it. "
            "Prefer non_nullable_by_default for key columns when possible. "
            f"See {_CLICKHOUSE_PRIMARY_KEY_DOCS}."
        )
    return setting.recommendation or f"{setting.key} is an escape-hatch physical table setting."


__all__ = [
    "PhysicalDesignAdvisor",
    "PhysicalSettingClassifier",
    "PhysicalSettingRegistry",
    "ResolvedTableSetting",
    "TableSettingValue",
    "TableSettingsOptions",
    "TargetTableSettingsDialect",
]
