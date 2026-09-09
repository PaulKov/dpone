"""Create a deterministic CI shadow plan from a fixture/event JSON file."""

from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.ci.shadow import (
    build_plan,
    changed_paths_from_git,
    event_from_pull_request_payload,
    read_object,
    write_create_new,
)

parser = argparse.ArgumentParser()
event_source = parser.add_mutually_exclusive_group(required=True)
event_source.add_argument("--event", type=Path)
event_source.add_argument("--github-event", type=Path)
parser.add_argument("--policy", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--repository-root", type=Path)
parser.add_argument("--merge-sha")
args = parser.parse_args()
if args.github_event:
    if args.merge_sha is None:
        parser.error("--merge-sha is required with --github-event")
    event = event_from_pull_request_payload(read_object(args.github_event), merge_sha=args.merge_sha)
else:
    assert args.event is not None
    event = read_object(args.event)
if args.repository_root is not None:
    event["changed_paths"] = changed_paths_from_git(
        repository_root=args.repository_root,
        base_sha=str(event.get("base_sha", "")),
        head_sha=str(event.get("head_sha", "")),
    )
write_create_new(args.output, build_plan(event, read_object(args.policy)))
