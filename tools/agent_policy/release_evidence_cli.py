"""CLI for shape-B evidence store lease + attest/draft staging."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


support = _load_sibling("dpone_agent_release_evidence_cli_support", "release_evidence_cli_support.py")
observe = _load_sibling("dpone_agent_release_evidence_cli_observe", "release_evidence_cli_observe.py")

_DEFAULT_JOBS = {
    "acquire-lease": "admit-and-lease",
    "capture-snapshot-a": "admit-and-lease",
    "attest-draft-dry-run": "attest-and-draft",
    "stage-draft-live": "attest-and-draft",
    "authorize-publication": "authorize-publication",
    "pypi-inventory-observe": "observe-publication",
    "immutable-inventory-observe": "observe-publication",
    "trusted-publisher-inventory-observe": "observe-publication",
    "draft-inventory-observe": "observe-publication",
    "release-lease": "release-lease",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    acquire = sub.add_parser("acquire-lease", help="Append LEASE_ACQUIRED to the B2 stream")
    _add_common_args(acquire)

    snap_a = sub.add_parser("capture-snapshot-a", help="Append bootstrap GOVERNANCE_SNAPSHOT A")
    _add_common_args(snap_a)
    snap_a.add_argument("--owner", default="PaulKov")
    snap_a.add_argument("--repo", default="dpone")
    snap_a.add_argument("--github-token-env", default="GITHUB_TOKEN")
    snap_a.add_argument("--snapshot-gap-seconds", type=float, default=5.0)

    flow = sub.add_parser(
        "attest-draft-dry-run",
        help="Append MUTATION_INTENT→ATTESTATION→BUNDLE→DRAFT dry-run receipts",
    )
    _add_common_args(flow)
    flow.add_argument(
        "--bootstrap-dist",
        action="store_true",
        help="Use one synthetic wheel subject for bootstrap (no real artifacts)",
    )

    live = sub.add_parser(
        "stage-draft-live",
        help="Attest metadata + create real GitHub draft (no PyPI publish)",
    )
    _add_common_args(live)
    live.add_argument("--owner", default="PaulKov")
    live.add_argument("--repo", default="dpone")
    live.add_argument("--subject-file", required=True, type=Path)
    live.add_argument("--attestation-bundle", required=True, type=Path)
    live.add_argument("--attestation-url", default="")
    live.add_argument("--github-token-env", default="GITHUB_TOKEN")

    authz = sub.add_parser(
        "authorize-publication",
        help="Snapshot B bootstrap + AUTHORIZED receipt (no publish/PyPI)",
    )
    _add_common_args(authz)
    authz.add_argument("--owner", default="PaulKov")
    authz.add_argument("--repo", default="dpone")
    authz.add_argument("--github-token-env", default="GITHUB_TOKEN")
    authz.add_argument("--snapshot-gap-seconds", type=float, default=5.0)

    pypi_obs = sub.add_parser(
        "pypi-inventory-observe",
        help="Read-only PyPI JSON inventory under AUTHORIZED (never upload)",
    )
    _add_common_args(pypi_obs)
    pypi_obs.add_argument("--expected-json", type=Path, default=None)
    pypi_obs.add_argument("--index-url", default="https://pypi.org/")

    imm_obs = sub.add_parser(
        "immutable-inventory-observe",
        help="Observe GitHub immutable-releases setting (never enable/mutate)",
    )
    _add_common_args(imm_obs)
    imm_obs.add_argument("--owner", default="PaulKov")
    imm_obs.add_argument("--repo", default="dpone")
    imm_obs.add_argument("--github-token-env", default="GITHUB_TOKEN")

    tp_obs = sub.add_parser(
        "trusted-publisher-inventory-observe",
        help="Observe Integrity publisher claims / TP binding (never rebind)",
    )
    _add_common_args(tp_obs)
    tp_obs.add_argument("--index-url", default="https://pypi.org/")

    draft_obs = sub.add_parser(
        "draft-inventory-observe",
        help="Re-read staged draft by exact release ID (never publish)",
    )
    _add_common_args(draft_obs)
    draft_obs.add_argument("--owner", default="PaulKov")
    draft_obs.add_argument("--repo", default="dpone")
    draft_obs.add_argument("--github-token-env", default="GITHUB_TOKEN")

    release_lease = sub.add_parser("release-lease", help="Append LEASE_RELEASED (no public delete)")
    _add_common_args(release_lease)
    release_lease.add_argument("--reason", default="BOOTSTRAP_COMPLETE")

    args = parser.parse_args(argv)
    if args.command == "attest-draft-dry-run" and not args.bootstrap_dist:
        parser.error("--bootstrap-dist is required until real candidate inventory wiring exists")

    store = support.build_store(args)
    ids = support.release_ids(args)
    prod = support.producer(default_job=_DEFAULT_JOBS.get(args.command, args.command))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: UP017

    runners = {
        "acquire-lease": support.run_acquire_lease,
        "capture-snapshot-a": support.run_capture_snapshot_a,
        "attest-draft-dry-run": support.run_attest_draft_dry_run,
        "stage-draft-live": support.run_stage_draft_live,
        "authorize-publication": support.run_authorize_publication,
        "pypi-inventory-observe": observe.run_pypi_inventory_observe,
        "immutable-inventory-observe": observe.run_immutable_inventory_observe,
        "trusted-publisher-inventory-observe": observe.run_trusted_publisher_inventory_observe,
        "draft-inventory-observe": observe.run_draft_inventory_observe,
        "release-lease": support.run_release_lease,
    }
    runner = runners.get(args.command)
    if runner is None:
        parser.error(f"unsupported command {args.command}")
        return 2
    return int(runner(store, ids, args, prod, now))


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tag", required=True, help="Canonical release tag, e.g. v0.73.2")
    parser.add_argument("--repository-id", required=True, type=int)
    parser.add_argument("--ttl-seconds", type=int, default=300)
    parser.add_argument("--retention-days", type=int, default=365)
    parser.add_argument(
        "--policy-sha256",
        default="sha256:" + ("0" * 64),
        help="Frozen policy digest; bootstrap may use zeros until v2 cutover",
    )
    parser.add_argument("--tag-object-sha", default="0" * 40)
    parser.add_argument("--peeled-commit-sha", default="0" * 40)
    parser.add_argument("--dry-memory", action="store_true", help="Use in-memory store")


if __name__ == "__main__":
    raise SystemExit(main())
