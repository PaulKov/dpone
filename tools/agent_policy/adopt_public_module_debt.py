"""Produce a provenance-only candidate from the audited public repository root."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from dpone.metrics.module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    decode_module_size_baseline,
    exact_sha,
)
from dpone.metrics.module_size_public_snapshot import (
    PUBLIC_LEDGER,
    PUBLIC_LEDGER_SHA256,
    PUBLIC_ROOT,
    PublicSnapshotDebt,
)
from dpone.metrics.module_size_write import write_baseline_if_unchanged


def adopt(repo: Path, head: str, *, write: bool) -> dict[str, object]:
    """Generate an unaccepted candidate; the ordinary exact-head gate certifies it."""
    head = exact_sha(head, label="adoption head")

    def git(*args: str) -> bytes:
        try:
            return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise ModuleSizeBaselineError("Adoption Git identity is unavailable") from exc

    if git("rev-parse", "HEAD").decode().strip() != head:
        raise ModuleSizeBaselineError("Adoption head differs from checked-out HEAD")
    git("merge-base", "--is-ancestor", PUBLIC_ROOT, head)
    audited = PublicSnapshotDebt.load(lambda path: git("show", f"{PUBLIC_ROOT}:{path}"))
    expected = git("show", f"{head}:{PUBLIC_LEDGER}")
    target = repo / PUBLIC_LEDGER
    if target.is_symlink() or target.read_bytes() != expected:
        raise ModuleSizeBaselineError("Adoption requires unchanged committed ledger bytes")
    current = decode_module_size_baseline(expected, source="adoption exact head")
    entries = []
    for prior in current.entries:
        candidate = replace(prior, baseline_commit=PUBLIC_ROOT)
        original = next((entry for entry in audited.baseline.entries if entry.path == prior.path), None)
        retry_prior = replace(prior, baseline_commit=original.baseline_commit) if original is not None else None
        if not audited.permits(candidate, prior) and not (
            prior.baseline_commit == PUBLIC_ROOT and audited.permits(candidate, retry_prior)
        ):
            raise ModuleSizeBaselineError("Adoption cannot change debt membership, metadata or caps")
        entries.append(candidate)
    changed = tuple(entries) != current.entries
    report = {
        "schema": "dpone.public-module-debt-adoption.v1",
        "status": "CANDIDATE_WRITTEN" if write and changed else "NO_CHANGE" if not changed else "PLANNED",
        "accepted": False,
        "historical_ancestry": "UNVERIFIED",
        "public_root": PUBLIC_ROOT,
        "public_ledger_sha256": PUBLIC_LEDGER_SHA256,
        "evaluated_head": head,
        "previous_ledger_sha256": sha256(expected).hexdigest(),
        "sources": [
            {
                "path": entry.path,
                "historical_baseline_commit": entry.baseline_commit,
                "source_sha256": dict(audited.source_digests)[entry.path],
                "max_lines": entry.max_lines,
                "max_sloc": entry.max_sloc,
            }
            for entry in audited.baseline.entries
        ],
        "next_action": "Commit the candidate and run the ordinary exact-head module-size gate.",
    }
    if write and changed:
        write_baseline_if_unchanged(
            repo_root=repo,
            baseline_path=target,
            baseline=ModuleSizeBaseline(tuple(entries)),
            expected_bytes=expected,
            expected_head_sha=head,
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    try:
        report = adopt(args.repo, args.head_sha, write=args.write)
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "FAIL", "accepted": False, "error": str(exc)}))
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
