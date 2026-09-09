"""Generate the audited clean-root ledger candidate; never return certification.

Run from a clean committed successor of the pinned root. The default is a dry
run. ``--write-baseline`` uses the canonical locked compare-and-swap writer.
Commit/review the candidate, then rerun the ordinary exact-base/head gate.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from dpone.adapters.project_authoring_lock import project_authoring_lock
from dpone.metrics import module_size_root_migration as migration
from dpone.metrics.module_size import ModuleSizeThresholds, analyze_module_sizes
from dpone.metrics.module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    decode_module_size_baseline,
    encode_module_size_baseline,
)
from dpone.metrics.module_size_policy import (
    ModuleSizeGitContext,
    resolve_module_size_git_context,
    validate_module_size_baseline,
)
from dpone.metrics.module_size_snapshot import ModuleSizeHeadSnapshot, load_module_size_head_snapshot
from dpone.metrics.module_size_write import persist_baseline_candidate, ratchet_module_size_baseline
from dpone.ports.project_authoring_lock import ProjectAuthoringLockError


def build_candidate(
    repo_root: Path,
    head_sha: str,
    *,
    as_of: date,
) -> tuple[ModuleSizeBaseline, ModuleSizeHeadSnapshot, ModuleSizeGitContext]:
    """Validate immutable inputs, reanchor and tighten, then verify every gate."""

    repo_root = repo_root.absolute()
    baseline_path = repo_root / migration.BASELINE_PATH
    package_dir = repo_root / "src/dpone"
    context = resolve_module_size_git_context(
        repo_root=repo_root,
        baseline_path=baseline_path,
        base_ref=migration.ROOT_POLICY.root_commit,
        head_ref=head_sha,
    )
    snapshot = load_module_size_head_snapshot(
        repo_root=repo_root,
        package_dir=package_dir,
        baseline_path=baseline_path,
        head_sha=head_sha,
    )
    anchor = migration.load_root_migration(
        repo_root=repo_root,
        base_sha=context.base_sha,
        head_sha=context.head_sha,
        previous=context.previous_baseline,
    )
    if anchor is None:
        raise ModuleSizeBaselineError("The exact audited root migration is unavailable")
    original = decode_module_size_baseline(snapshot.baseline, source="exact migration head")
    candidate = migration.reanchor_candidate_entries(original, anchor)
    budgets = yaml.safe_load(snapshot.quality_budgets)["global"]
    thresholds = ModuleSizeThresholds(
        warn_lines=budgets["warn_loc"],
        max_lines=budgets["max_loc"],
        warn_sloc=budgets["warn_sloc"],
        max_sloc=budgets["max_sloc"],
    )
    sources = snapshot.source_bytes_by_path
    measured = analyze_module_sizes(
        package_dir,
        repo_root=repo_root,
        thresholds=thresholds,
        baseline=candidate.entries,
        warning_debt_requires_baseline=True,
        source_bytes_by_path=sources,
    )
    # Candidate construction can tighten/remove entries. It grants no authority:
    # the resulting ledger must pass the complete verifier immediately below.
    candidate = ratchet_module_size_baseline(
        candidate,
        report=measured,
        thresholds=thresholds,
        bootstrap=False,
        bootstrap_commit=anchor.root_commit,
        git_context=context,
        policy_issues=(),
    )
    issues = validate_module_size_baseline(
        candidate,
        repo_root=repo_root,
        git_context=context,
        as_of=as_of,
        accepted_adr_text_by_path=snapshot.accepted_adr_text_by_path,
        trusted_module_paths=frozenset(sources),
        current_module_sizes={item.path: (item.lines, item.sloc) for item in measured.items},
        warning_thresholds=(thresholds.warn_lines, thresholds.warn_sloc or 0),
        budget_limits=(budgets["warn_loc"], budgets["max_loc"], budgets["warn_sloc"], budgets["max_sloc"]),
    )
    verified = analyze_module_sizes(
        package_dir,
        repo_root=repo_root,
        thresholds=thresholds,
        baseline=candidate.entries,
        policy_issues=issues,
        warning_debt_requires_baseline=True,
        source_bytes_by_path=sources,
    )
    if not verified.ok:
        raise ModuleSizeBaselineError("Root migration candidate does not satisfy exact caps and all policy gates")
    return candidate, snapshot, context


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--head-ref", required=True, help="Exact committed successor SHA, never the root itself")
    parser.add_argument("--write-baseline", action="store_true", help="Persist candidate; still not certification")
    args = parser.parse_args(argv)
    try:
        candidate, snapshot, context = build_candidate(
            args.repo_root,
            args.head_ref,
            as_of=datetime.now(timezone.utc).date(),  # noqa: UP017 -- matches the existing Python 3.10 policy API
        )
        changed = encode_module_size_baseline(candidate) != snapshot.baseline
        status = "CANDIDATE_READY" if changed else "NO_CHANGE"
        if args.write_baseline and changed:
            persist_baseline_candidate(
                repo_root=args.repo_root.absolute(),
                package_dir=args.repo_root.absolute() / "src/dpone",
                baseline_path=args.repo_root.absolute() / migration.BASELINE_PATH,
                baseline=candidate,
                snapshot=snapshot,
                snapshot_loader=load_module_size_head_snapshot,
                authoring_lock=project_authoring_lock,
            )
            status = "CANDIDATE_WRITTEN"
        payload = {
            "schema_version": "dpone.module-size-root-migration-candidate.v1",
            "ok": False,
            "status": status,
            "changed": bool(args.write_baseline and changed),
            "base_sha": context.base_sha,
            "head_sha": context.head_sha,
            "baseline_path": migration.BASELINE_PATH,
            "entries": len(candidate.entries),
            "next_action": "Review and commit the candidate, then run the ordinary exact-base/head gate.",
        }
    except (ModuleSizeBaselineError, ProjectAuthoringLockError) as exc:
        payload = {
            "schema_version": "dpone.module-size-root-migration-candidate.v1",
            "ok": False,
            "status": "REJECTED",
            "configuration_error": str(exc),
        }
    print(json.dumps(payload, sort_keys=True))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
