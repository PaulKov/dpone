from __future__ import annotations

import copy
from types import SimpleNamespace
from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import load_module

gate = load_module(
    "dpone_release_candidate_evidence_publication_receipt_test",
    "tools/agent_policy/release_candidate_evidence_gate.py",
)

RELEASE_WORKFLOW = ".github/workflows/release.yml"
RUNTIME_WORKFLOW = ".github/workflows/runtime-image.yml"
RELEASE_CREATED_AT = "2026-08-13T10:01:00Z"
RUNTIME_CREATED_AT = "2026-08-13T10:00:00Z"
RUNS = [
    {
        "workflow_path": RELEASE_WORKFLOW,
        "run_id": 901,
        "run_attempt": 3,
        "created_at": RELEASE_CREATED_AT,
    },
    {
        "workflow_path": RUNTIME_WORKFLOW,
        "run_id": 902,
        "run_attempt": 2,
        "created_at": RUNTIME_CREATED_AT,
    },
]


def _selected(**overrides: Any) -> SimpleNamespace:
    paired_runs = copy.deepcopy(overrides.pop("paired_publication_runs", RUNS))
    values = {
        "paired_publication_runs": paired_runs,
        "publication_workflow_path": RELEASE_WORKFLOW,
        "publication_run_id": 901,
        "publication_run_attempt": 3,
        "publication_cutoff": RUNTIME_CREATED_AT,
        "publication_pair_sha256": gate.codec.canonical_json_sha256(paired_runs),
        **overrides,
    }
    return SimpleNamespace(**values)


def test_pair_projection_is_closed_sorted_self_describing_and_digest_bound() -> None:
    selected = _selected()

    paired_runs, pair_sha256 = gate._publication_receipt_identity(selected)

    assert paired_runs == RUNS
    assert [run["workflow_path"] for run in paired_runs] == sorted(gate.source.PUBLICATION_WORKFLOW_PATHS)
    assert all(frozenset(run) == gate.contract.PAIRED_PUBLICATION_RUN_KEYS for run in paired_runs)
    assert selected.publication_cutoff == min(run["created_at"] for run in paired_runs)
    assert pair_sha256 == gate.codec.canonical_json_sha256(paired_runs)


def test_pair_projection_rejects_unknown_nested_fields() -> None:
    runs = copy.deepcopy(RUNS)
    runs[0]["status"] = "in_progress"

    with pytest.raises(ValueError, match="field set is invalid.*unknown=.*status"):
        gate._publication_receipt_identity(_selected(paired_publication_runs=runs))


@pytest.mark.parametrize("runs", [RUNS[:1], list(reversed(RUNS))])
def test_pair_projection_requires_exactly_two_approved_runs_in_path_order(
    runs: list[dict[str, Any]],
) -> None:
    with pytest.raises(ValueError, match="two approved workflows in path order"):
        gate._publication_receipt_identity(_selected(paired_publication_runs=runs))


def test_pair_projection_requires_current_caller_to_match_its_run() -> None:
    with pytest.raises(ValueError, match="caller does not match"):
        gate._publication_receipt_identity(_selected(publication_run_id=999))


def test_pair_projection_accepts_the_exact_runtime_workflow_caller() -> None:
    selected = _selected(
        publication_workflow_path=RUNTIME_WORKFLOW,
        publication_run_id=902,
        publication_run_attempt=2,
    )

    paired_runs, _pair_sha256 = gate._publication_receipt_identity(selected)

    assert next(run for run in paired_runs if run["workflow_path"] == RUNTIME_WORKFLOW) == RUNS[1]


def test_pair_projection_requires_cutoff_to_equal_exact_minimum() -> None:
    with pytest.raises(ValueError, match="cutoff is not the exact minimum"):
        gate._publication_receipt_identity(_selected(publication_cutoff=RELEASE_CREATED_AT))


def test_pair_projection_rejects_a_relabelled_digest() -> None:
    with pytest.raises(ValueError, match="digest does not match"):
        gate._publication_receipt_identity(_selected(publication_pair_sha256="sha256:" + "0" * 64))
