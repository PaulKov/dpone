"""Scenario catalog for executable benchmark certification."""

from __future__ import annotations

from dataclasses import dataclass

from tools.oss_benchmark.certification_runtime_models import ScenarioSpec

SCENARIO_CHOICES = ("all", "nested-lineage", "incremental", "cdc-replay", "schema-evolution", "artifact-contract")


@dataclass(frozen=True, slots=True)
class ScenarioCatalog:
    """Thin catalog for selecting release certification scenarios."""

    scenarios: tuple[ScenarioSpec, ...]

    @classmethod
    def default(cls) -> ScenarioCatalog:
        """Return the default local, credential-free certification catalog."""

        return cls(
            (
                ScenarioSpec(
                    scenario_id="nested-lineage",
                    category="nested_lineage",
                    runner="python_api",
                    description="Validate dlt-like nested id, parent id, root id and list-index lineage.",
                ),
                ScenarioSpec(
                    scenario_id="incremental",
                    category="incremental_sync",
                    runner="python_api",
                    description="Validate deterministic incremental insert/update/delete state convergence.",
                ),
                ScenarioSpec(
                    scenario_id="cdc-replay",
                    category="cdc_replay",
                    runner="python_api",
                    description="Validate local CDC-like replay idempotency and checkpoint semantics.",
                ),
                ScenarioSpec(
                    scenario_id="schema-evolution",
                    category="schema_evolution",
                    runner="python_api",
                    description="Validate additive schema evolution and incompatible change blocking.",
                ),
                ScenarioSpec(
                    scenario_id="artifact-contract",
                    category="artifact_contract",
                    runner="cli",
                    description="Validate CLI availability and generated artifact contract shape.",
                ),
            )
        )

    def select(self, scenario: str) -> tuple[ScenarioSpec, ...]:
        """Select one scenario or the whole catalog."""

        if scenario == "all":
            return self.scenarios
        selected = tuple(item for item in self.scenarios if item.scenario_id == scenario)
        if not selected:
            raise ValueError(f"Unknown certification scenario: {scenario}")
        return selected
