"""Read-only proof of the approved staged/committed whole-tree transfer.

Run with --source H --phase staged|committed --output /absolute/external.json.
This producer never creates branches, applies changes, commits or publishes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

BASE = "fa20f554bab3024f240b92aadf176527ee967925"
BRANCH = "codex/dda-06-reviewed-delivery"


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def observe_transfer(source: str, phase: str) -> dict:
    """Bind exact tree equality to the pinned parent and recorded source commit."""
    if phase not in {"staged", "committed"}:
        raise ValueError("invalid_transfer_phase")
    source = git("rev-parse", "--verify", f"{source}^{{commit}}")
    head = git("rev-parse", "HEAD")
    source_tree = git("rev-parse", f"{source}^{{tree}}")
    head_tree = git("rev-parse", "HEAD^{tree}")
    staged_tree = git("write-tree")
    branch = git("branch", "--show-current")
    parents = git("rev-list", "--parents", "-n", "1", "HEAD").split()[1:]
    checks = {
        "approved_branch": branch == BRANCH,
        "no_unstaged_changes": git("diff", "--name-only") == "",
        "staged_tree_equals_reviewed_source": staged_tree == source_tree,
    }
    if phase == "staged":
        checks["head_is_pinned_upstream"] = head == BASE
    else:
        checks["sole_parent_is_pinned_upstream"] = parents == [BASE]
        checks["committed_tree_equals_reviewed_source"] = head_tree == source_tree
        checks["source_provenance_in_commit"] = (
            f"Reviewed-source: {source}" in git("show", "-s", "--format=%B", "HEAD").splitlines()
        )
    return {
        "schema_version": 1,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "phase": phase,
        "source_commit": source,
        "source_tree": source_tree,
        "pinned_upstream": BASE,
        "head": head,
        "parents": parents,
        "head_tree": head_tree,
        "staged_tree": staged_tree,
        "branch": branch,
        "checks": checks,
        "producer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--phase", choices=("staged", "committed"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(git("rev-parse", "--show-toplevel")).resolve()
    if not args.output.is_absolute() or args.output.resolve().is_relative_to(root):
        raise ValueError("transfer_evidence_must_be_outside_checkout")
    report = observe_transfer(args.source, args.phase)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(report["status"])
    raise SystemExit(report["status"] != "PASS")


if __name__ == "__main__":
    main()
