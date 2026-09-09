"""Fold CI shadow provider results into untrusted immutable claims."""

from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.ci.shadow import build_claims, read_object, write_create_new

parser = argparse.ArgumentParser()
parser.add_argument("--plan", type=Path, required=True)
parser.add_argument("--jobs", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
write_create_new(args.output, build_claims(read_object(args.plan), read_object(args.jobs)))
