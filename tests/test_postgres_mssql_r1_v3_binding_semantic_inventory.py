"""Independent behavioral denominator for the restricted Binding V2 compiler.

The JSON registry is evidence input, never the source of the expected pair
universe.  This module derives that universe independently from the approved
phase sets and requires every constructible case to be bound to a black-box
public-entrypoint scenario by one of the two focused provider modules.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable
from itertools import combinations
from pathlib import Path
from typing import Literal

import pytest

ObservedClass = Literal["typed_rejection", "invariant_preserved"]
InputObserver = Callable[[tuple[tuple[str, str], ...]], None]
Scenario = Callable[[str, str, InputObserver], ObservedClass]

READER_PHASES = tuple(f"{value:02d}" for value in range(1, 22))
FACTORY_PHASES = ("01", "02", "07", "10", "11", "12", "13", "14", "16")
SOLE_EXCLUSION = "reader:03:04"
PHASE_REASONS = {
    "01": "exact_type_violation",
    "02": "canonical_size_exceeded",
    "03": "wrong_domain",
    "04": "wrong_version",
    "05": "malformed_canonical_bytes",
    "06": "unsupported_target_profile",
    "07": "identifier_invalid",
    "08": "column_count_invalid",
    "09": "ordinal_invalid",
    "10": "mapping_coverage_invalid",
    "11": "dependency_mismatch",
    "12": "authority_splice",
    "13": "target_key_invalid",
    "14": "authority_splice",
    "15": "stage_shape_invalid",
    "16": "buffer_plan_invalid",
    "17": "module_set_invalid",
    "18": "template_mismatch",
    "19": "signer_policy_mismatch",
    "20": "permission_widening",
    "21": "signature_mismatch",
}

_REGISTRY_PATH = Path("docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-cases.json")
_REGISTRY = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
_CASES = tuple(_REGISTRY["cases"])
BINDING_V2_CASE_IDS = tuple(item["case_id"] for item in _CASES)
_BY_ID = {item["case_id"]: item for item in _CASES}
_SCENARIOS: dict[str, Scenario] = {}


def _expected_ids() -> tuple[str, ...]:
    return tuple(
        f"{entrypoint}:{earlier}:{later}"
        for entrypoint, phases in (("reader", READER_PHASES), ("factory", FACTORY_PHASES))
        for earlier, later in combinations(phases, 2)
    )


def bind_phase_pair_scenario(entrypoint: str, scenario: Scenario) -> None:
    """Bind one entrypoint's independent public scenario exactly once."""

    assert entrypoint in {"reader", "factory"}
    assert entrypoint not in _SCENARIOS
    _SCENARIOS[entrypoint] = scenario


def _load_scenarios() -> None:
    for module in (
        "tests.test_postgres_mssql_r1_v3_binding_contract",
        "tests.test_postgres_mssql_r1_v3_binding_mutation_inventory",
    ):
        importlib.import_module(module)
    assert set(_SCENARIOS) == {"reader", "factory"}


@pytest.fixture
def binding_case_observer(request: pytest.FixtureRequest) -> Callable[[str, ObservedClass], None]:
    called = False

    def observe(case_id: str, observed: ObservedClass) -> None:
        nonlocal called
        assert not called
        assert case_id in _BY_ID
        assert observed == _BY_ID[case_id]["expected_class"]
        request.node.user_properties.append(("binding_v2_semantic_observation", (case_id, observed)))
        called = True

    return observe


@pytest.fixture
def binding_input_observer(request: pytest.FixtureRequest) -> InputObserver:
    called = False

    def observe(inputs: tuple[tuple[str, str], ...]) -> None:
        nonlocal called
        assert not called
        assert inputs
        assert len({name for name, _ in inputs}) == len(inputs)
        assert all(len(value) == 64 for _, value in inputs)
        request.node.user_properties.append(("binding_v2_input_vector", inputs))
        called = True

    return observe


@pytest.mark.parametrize("case_id", BINDING_V2_CASE_IDS, ids=BINDING_V2_CASE_IDS)
def test_binding_phase_pair(
    case_id: str,
    binding_case_observer: Callable[[str, ObservedClass], None],
    binding_input_observer: InputObserver,
) -> None:
    item = _BY_ID[case_id]
    earlier, later = item["earlier_phase"], item["later_phase"]
    assert item["coverage"] == [
        f"{item['entrypoint']}.phase_{earlier}",
        f"{item['entrypoint']}.phase_{later}",
        "earlier_wins",
        "delete_earlier_selects_later",
        "delete_later_selects_earlier",
    ]
    if case_id == SOLE_EXCLUSION:
        assert item["status"] == "excluded"
        assert item["fixture_id"] == "mutually-exclusive-domain-version"
        binding_input_observer((("exclusion_proof", hashlib.sha256(case_id.encode()).hexdigest()),))
        observed: ObservedClass = "invariant_preserved"
    else:
        _load_scenarios()
        observed = _SCENARIOS[item["entrypoint"]](earlier, later, binding_input_observer)
    binding_case_observer(case_id, observed)


def test_registry_is_exact_closed_pair_authority() -> None:
    expected = _expected_ids()
    assert _REGISTRY["contract_version"] == "dpone-postgres-mssql-r1-v3-binding-v2-case-registry-1"
    assert len(READER_PHASES) == 21
    assert FACTORY_PHASES == ("01", "02", "07", "10", "11", "12", "13", "14", "16")
    assert len(expected) == 246
    assert len(set(expected)) == 246
    assert BINDING_V2_CASE_IDS == expected
    assert len(_CASES) == 246
    assert sum(item["status"] == "constructible" for item in _CASES) == 245
    assert tuple(item["case_id"] for item in _CASES if item["status"] == "excluded") == (SOLE_EXCLUSION,)
    assert len({item["nodeid"] for item in _CASES}) == 246
    assert all(item["nodeid"].endswith(f"[{item['case_id']}]") for item in _CASES)


def test_failure_phase_reason_authority_is_closed() -> None:
    assert tuple(PHASE_REASONS) == READER_PHASES
    assert len(set(PHASE_REASONS.values())) == 20
    assert PHASE_REASONS["12"] == PHASE_REASONS["14"] == "authority_splice"
