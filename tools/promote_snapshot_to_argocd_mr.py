from __future__ import annotations

import argparse
import json
from pathlib import Path

from dpone.services.ci.argocd_promote import (
    PromoteSnapshotSettings,
    load_snapshot_values,
    promote_snapshot_to_argocd,
)

DEFAULT_VALUES_FILE = ".helm/overrides/airflow-dev-dpone-snapshot.yaml"
DEFAULT_BRANCH_PREFIX = "auto/dpone-airflow-dev"
DEFAULT_TARGET_DIR = "/opt/airflow/.dpone-pkgs"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote dpone snapshot package to argocd-main via Git + MR")
    parser.add_argument(
        "--snapshot-env", default="snapshot.env", help="Path to dotenv artifact from publish_dev_snapshot"
    )
    parser.add_argument("--package-spec", default=None, help="Override package spec (e.g. dpone==1.2.3.dev45)")
    parser.add_argument("--repo-url", default=None, help="Git URL of argocd-main")
    parser.add_argument("--repo-dir", default=None, help="Use an existing local clone instead of cloning repo-url")
    parser.add_argument("--project-id", default=None, help="GitLab project ID of argocd-main for MR API")
    parser.add_argument("--target-branch", default="master", help="Target branch in argocd-main")
    parser.add_argument("--snapshot-file", default=DEFAULT_VALUES_FILE, help="Relative path to snapshot override YAML")
    parser.add_argument("--branch-prefix", default=DEFAULT_BRANCH_PREFIX, help="Prefix for generated source branch")
    parser.add_argument("--push-user", default=None, help="Git user for authenticated push")
    parser.add_argument("--push-token", default=None, help="Git token for authenticated push")
    parser.add_argument("--api-url", default=None, help="GitLab API v4 URL (defaults to CI_API_V4_URL)")
    parser.add_argument("--api-token", default=None, help="Token used to create/update MR (defaults to push token)")
    parser.add_argument("--git-author-name", default="dpone-ci-bot")
    parser.add_argument("--git-author-email", default="dpone-ci-bot@local")
    parser.add_argument("--commit-message", default=None)
    parser.add_argument("--mr-title", default=None)
    parser.add_argument("--mr-description", default=None)
    parser.add_argument("--mr-label", action="append", default=[], help="Repeatable MR label")
    parser.add_argument("--target-dir", default=DEFAULT_TARGET_DIR, help="Value to write into targetDir")
    parser.add_argument("--install-mode", default="snapshot")
    parser.add_argument("--disabled", action="store_true", help="Write enabled=false in snapshot file")
    parser.add_argument("--no-open-mr", action="store_true")
    parser.add_argument("--draft-mr", action="store_true")
    parser.add_argument("--remove-source-branch", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def _to_payload(result) -> dict[str, object]:
    return {
        "branch_name": result.branch_name,
        "changed": result.changed,
        "created_file": result.created_file,
        "repo_dir": str(result.repo_dir),
        "values_file": str(result.values_file),
        "commit_message": result.commit_message,
        "mr_title": result.mr_title,
        "package_spec": result.package_spec,
        "mr_url": result.mr_url,
        "dry_run": result.dry_run,
        "existed_mr": result.existed_mr,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    values = load_snapshot_values(args.snapshot_env)
    package_spec = args.package_spec or values.package_spec
    settings = PromoteSnapshotSettings(
        package_spec=package_spec,
        snapshot_version=values.snapshot_version,
        snapshot_sha=values.snapshot_sha,
        repo_url=args.repo_url,
        repo_dir=Path(args.repo_dir) if args.repo_dir else None,
        target_branch=args.target_branch,
        values_file=args.snapshot_file,
        branch_prefix=args.branch_prefix,
        push_user=args.push_user,
        push_token=args.push_token,
        git_author_name=args.git_author_name,
        git_author_email=args.git_author_email,
        commit_message=args.commit_message,
        mr_title=args.mr_title,
        mr_description=args.mr_description,
        mr_labels=tuple(args.mr_label),
        project_id=args.project_id,
        api_url=args.api_url,
        api_token=args.api_token,
        open_mr=not args.no_open_mr,
        draft_mr=args.draft_mr,
        remove_source_branch=args.remove_source_branch,
        target_dir=args.target_dir,
        install_mode=args.install_mode,
        enabled=not args.disabled,
        dry_run=args.dry_run,
    )
    result = promote_snapshot_to_argocd(settings)
    payload = _to_payload(result)
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"package_spec: {result.package_spec}")
        print(f"branch_name:  {result.branch_name}")
        print(f"values_file:  {result.values_file}")
        print(f"changed:      {result.changed}")
        print(f"dry_run:      {result.dry_run}")
        if result.mr_url:
            print(f"mr_url:       {result.mr_url}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
