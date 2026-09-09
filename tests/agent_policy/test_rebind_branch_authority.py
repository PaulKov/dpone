"""A replacement ruleset may change identity, never reviewed protection semantics."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from tools.agent_policy.rebind_branch_authority import rebind

ROOT = Path(__file__).parents[2]


def inputs():
    policy = (ROOT / ".agents/policy/github-branch-protection.yml").read_bytes()
    baseline = json.loads((ROOT / ".agents/policy/github-branch-protection.release-authority.json").read_bytes())
    live = deepcopy(baseline["ruleset_projection"])
    live["conditions"] = {"ref_name": live["conditions"]}
    live["bypass_actors"] = baseline["privileged_baseline"]["bypass_actors"]
    live["id"] += 1
    history = {"version_id": baseline["privileged_baseline"]["version_id"] + 1, "state": deepcopy(live)}
    history["state"]["updated_at"] = None
    return policy, baseline, live, history


def test_rebind_changes_only_identity_and_preserves_all_controls():
    policy, baseline, live, history = inputs()
    updated, derived = rebind(policy, baseline, live, history)
    old, new = yaml.safe_load(policy), yaml.safe_load(updated)
    old["ruleset"]["id"] = new["ruleset"]["id"]
    old["ruleset"]["version_id"] = new["ruleset"]["version_id"]
    assert old == new
    assert derived["ruleset_id"] == live["id"]
    assert derived["required_check_producers"] == baseline["required_check_producers"]


@pytest.mark.parametrize("mutation", ["check", "bypass", "history", "policy"])
def test_rebind_rejects_changed_or_unbound_authority(mutation):
    policy, baseline, live, history = inputs()
    if mutation == "check":
        live["rules"].pop()
        history["state"] = deepcopy(live)
    elif mutation == "bypass":
        live["bypass_actors"] = []
        history["state"] = deepcopy(live)
    elif mutation == "history":
        history["state"]["id"] += 1
    else:
        policy += b"\n"
    with pytest.raises(ValueError):
        rebind(policy, baseline, live, history)


def test_candidate_pair_is_complete_idempotent_and_validated(tmp_path):
    from tools.agent_policy.rebind_branch_authority import publish_candidate
    from tools.agent_policy.release_authority_baseline import load_release_authority_baseline

    policy, baseline = rebind(*inputs())
    output = tmp_path / "candidate"
    publish_candidate(output, policy, baseline)
    publish_candidate(output, policy, baseline)
    validated = load_release_authority_baseline(
        output / "github-branch-protection.release-authority.json", policy_path=output / "github-branch-protection.yml"
    )
    assert validated.ruleset_id == baseline["ruleset_id"]


def test_private_second_write_failure_leaves_no_partial_candidate(tmp_path, monkeypatch):
    from tools.agent_policy.rebind_branch_authority import publish_candidate

    policy, baseline = rebind(*inputs())
    output = tmp_path / "candidate"
    write = Path.write_bytes

    def fail(path, value):
        if path.name.endswith(".json"):
            raise OSError("injected second write failure")
        return write(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "write_bytes", fail)
        with pytest.raises(OSError):
            publish_candidate(output, policy, baseline)
        assert not output.exists()
    publish_candidate(output, policy, baseline)
    assert len(list(output.iterdir())) == 2
