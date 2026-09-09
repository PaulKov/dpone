from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dpone_ci_quality_timing_test", ROOT / "tools/ci_quality_timing.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _samples(*, durations: tuple[float, float, float] = (600.0, 610.0, 619.0)) -> list[dict[str, object]]:
    return [
        {
            "schema_version": 1,
            "head_sha": "a" * 40,
            "repository": "PaulKov/dpone",
            "workflow_path": ".github/workflows/ci.yml",
            "workflow_sha256": "b" * 64,
            "run_id": index + 1,
            "run_attempt": 1,
            "python_version": "3.12",
            "critical_path_seconds": duration,
            "status": "PASS",
            "collection": {"node_count": 1, "population_sha256": "c" * 64, "shard_count": 8},
            "coverage": {
                "config_sha256": "d" * 64,
                "input_artifacts": [{"name": "quality-shard-3.12-0", "sha256": "e" * 64}],
                "ratchet_status": "PASS",
            },
        }
        for index, duration in enumerate(durations)
    ]


def test_decision_uses_maximum_as_conservative_three_sample_p95() -> None:
    result = _load().performance_decision(_samples())

    assert result["p95_seconds"] == 619.0
    assert result["status"] == "PASS"


def test_decision_fails_at_the_strict_target_boundary() -> None:
    result = _load().performance_decision(_samples(durations=(600.0, 610.0, 720.0)))

    assert result["status"] == "FAIL"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda samples: samples.pop(),
        lambda samples: samples.__setitem__(1, {**samples[1], "head_sha": "b" * 40}),
        lambda samples: samples.__setitem__(1, {**samples[1], "run_id": 1}),
        lambda samples: samples.__setitem__(1, {**samples[1], "status": "UNVERIFIED"}),
        lambda samples: samples.__setitem__(1, {**samples[1], "workflow_sha256": "f" * 64}),
    ],
)
def test_decision_rejects_incomplete_or_untrusted_evidence(
    mutate: Callable[[list[dict[str, object]]], object],
) -> None:
    module = _load()
    samples = _samples()
    mutate(samples)

    with pytest.raises(ValueError):
        module.performance_decision(samples)
