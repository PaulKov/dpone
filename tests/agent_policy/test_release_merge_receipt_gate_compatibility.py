from tests.agent_policy.test_release_merge_receipt_gate import REPOSITORY, RUN_ID, _adapter, _check, _verify


def test_release_gate_accepts_exact_actions_run_url() -> None:
    adapter = _adapter()
    adapter.checks = [_check(details_url=f"https://github.com/{REPOSITORY}/actions/runs/{RUN_ID}")]

    assert _verify(adapter).workflow_run_id == RUN_ID
