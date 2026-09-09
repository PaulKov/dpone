from __future__ import annotations

import importlib
import importlib.util
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullabilityPolicy
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver


@dataclass(frozen=True, slots=True)
class QuestionMarkNullabilityDialect:
    """Tiny fake target dialect for architecture contract tests."""

    def is_nullable(self, target_type: str) -> bool:
        return target_type.endswith("?")

    def strip_nullable(self, target_type: str) -> str:
        return target_type.removesuffix("?")


@dataclass(frozen=True, slots=True)
class TypeDecision:
    target_type: str


class FakeSourceMapper:
    def __init__(self, mapped_types: dict[str, str]) -> None:
        self.mapped_types = mapped_types

    def resolve_column(self, column: str, source_type: str) -> TypeDecision:
        del column
        return TypeDecision(self.mapped_types[source_type])


def _nullability_module() -> Any:
    module_name = "dpone.runtime.physical_design.nullability"
    assert importlib.util.find_spec(module_name), "generic runtime nullability taxonomy module is missing"
    return importlib.import_module(module_name)


def test_generic_nullability_policy_is_reusable_for_non_clickhouse_type_syntax() -> None:
    module = _nullability_module()
    options = module.NullabilityOptions.from_config(
        {
            "mode": "non_nullable_by_default",
            "null_handling": "default",
            "columns": {
                "comment": {"mode": "preserve_source"},
            },
        }
    )
    policy = module.NullabilityPolicy(dialect=QuestionMarkNullabilityDialect())

    amount = policy.resolve_options(options=options, column="amount", mapped_type="decimal?")
    comment = policy.resolve_options(options=options, column="comment", mapped_type="text?")

    assert amount.target_type == "decimal"
    assert amount.target_nullable is False
    assert amount.decision_source == "physical_design"
    assert comment.target_type == "text?"
    assert comment.target_nullable is True
    assert comment.decision_source == "source_mapping"


def test_clickhouse_nullability_policy_is_a_sink_adapter_over_generic_policy() -> None:
    module = _nullability_module()
    policy = ClickHouseNullabilityPolicy()

    decision = policy.resolve_options(
        options=policy.options_from_config(
            {
                "mode": "non_nullable_by_default",
                "null_handling": "default",
            }
        ),
        column="payload",
        mapped_type="Nullable(String)",
    )

    assert decision.target_type == "String"
    assert policy.generic_policy.__class__ is module.NullabilityPolicy


def test_clickhouse_physical_resolver_accepts_any_source_mapper_contract() -> None:
    load_config = type(
        "LoadConfigStub",
        (),
        {
            "options": {
                "physical_design": {
                    "storage": {
                        "clickhouse": {
                            "nullability": {
                                "mode": "non_nullable_by_default",
                                "null_handling": "default",
                            }
                        }
                    }
                }
            }
        },
    )()
    resolver = ClickHousePhysicalColumnTypeResolver()
    mapper = FakeSourceMapper({"external.optional_text": "Nullable(String)"})

    resolved = resolver.resolve(
        load_config=load_config,
        type_mapper=mapper,
        column="value",
        source_type="external.optional_text",
    )

    assert resolved == "String"
