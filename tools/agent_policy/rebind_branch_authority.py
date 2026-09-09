"""Rebind an unchanged reviewed protection policy to an observed replacement ruleset."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from copy import deepcopy
from pathlib import Path

import yaml
from tools.agent_policy.branch_protection import validate_policy
from tools.agent_policy.governance_live_ruleset import (
    live_ruleset_projection,
    observable_ruleset_projection,
)
from tools.agent_policy.release_authority_baseline import load_release_authority_baseline

from dpone.runtime.immutable_local_tree import materialize_immutable_local_tree


def rebind(policy_bytes: bytes, baseline: dict, live: dict, revision: dict) -> tuple[bytes, dict]:
    """Permit identity/revision changes only; never alter a protection decision."""
    policy = yaml.safe_load(policy_bytes)
    if not validate_policy(policy, label="reviewed").ok:
        raise ValueError("reviewed policy is invalid")
    if hashlib.sha256(policy_bytes).hexdigest() != baseline.get("policy_sha256"):
        raise ValueError("reviewed baseline does not bind the policy")
    live = deepcopy(live)
    live["version_id"] = revision["version_id"]
    historical = live_ruleset_projection(revision["state"])
    observed = live_ruleset_projection(live)
    revision_fields = {"updated_at", "version_id"}
    if {k: v for k, v in historical.items() if k not in revision_fields} != {
        k: v for k, v in observed.items() if k not in revision_fields
    }:
        raise ValueError("ruleset history state does not bind the observed ruleset")
    projection = live_ruleset_projection(live)
    previous = {**baseline["ruleset_projection"], **baseline["privileged_baseline"]}
    identity = {"id", "updated_at", "version_id"}
    if {k: v for k, v in projection.items() if k not in identity} != {
        k: v for k, v in previous.items() if k not in identity
    }:
        raise ValueError("replacement ruleset changes reviewed protection controls")
    rule = policy["ruleset"]
    text = policy_bytes.decode("utf-8")
    for key in ("id", "version_id", "updated_at"):
        old = json.dumps(rule[key]) if isinstance(rule[key], str) else str(rule[key])
        new = json.dumps(projection[key]) if isinstance(projection[key], str) else str(projection[key])
        if text.count(f"  {key}: {old}\n") != 1:
            raise ValueError("policy identity field is not uniquely encoded")
        text = text.replace(f"  {key}: {old}\n", f"  {key}: {new}\n")
    updated = text.encode("utf-8")
    if not validate_policy(yaml.safe_load(updated), label="candidate").ok:
        raise ValueError("candidate policy is invalid")
    result = deepcopy(baseline)
    result.update(
        policy_sha256=hashlib.sha256(updated).hexdigest(),
        ruleset_id=projection["id"],
        ruleset_projection=observable_ruleset_projection(projection),
        privileged_baseline={key: projection[key] for key in ("bypass_actors", "updated_at", "version_id")},
        policy_projection_sha256=hashlib.sha256(
            json.dumps(projection, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )
    return updated, result


def publish_candidate(output: Path, policy: bytes, baseline: dict) -> None:
    """Validate the pair privately, then publish a complete immutable candidate."""
    baseline_bytes = (json.dumps(baseline, indent=2, sort_keys=True) + "\n").encode()
    names = ("github-branch-protection.yml", "github-branch-protection.release-authority.json")
    with tempfile.TemporaryDirectory() as directory:
        stage = Path(directory)
        (stage / names[0]).write_bytes(policy)
        (stage / names[1]).write_bytes(baseline_bytes)
        load_release_authority_baseline(stage / names[1], policy_path=stage / names[0])
        materialize_immutable_local_tree(
            output,
            dict(zip(names, (policy, baseline_bytes), strict=True)),
            allowed_parent=output.parent,
            root=output.parent,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--live-ruleset", type=Path, required=True)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    load_release_authority_baseline(args.baseline, policy_path=args.policy)
    policy, baseline = rebind(
        args.policy.read_bytes(),
        json.loads(args.baseline.read_bytes()),
        json.loads(args.live_ruleset.read_bytes()),
        json.loads(args.history.read_bytes()),
    )
    publish_candidate(args.output_directory, policy, baseline)
    print("Protection controls unchanged; copy both candidate files, commit and verify live exact-head checks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
