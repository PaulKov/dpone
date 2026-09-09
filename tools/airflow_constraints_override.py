"""Derive an auditable Airflow constraints file for one provider override.

Apache Airflow constraints intentionally freeze provider versions. Compatibility
certification sometimes needs to exercise a newer provider against the same
Airflow core and dependency set. This tool removes exactly one named provider
pin, preserves every other source byte, and records both file digests plus the
requested replacement version. The replacement remains an explicit installer
argument; it is never hidden in the derived file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

EVIDENCE_SCHEMA = "dpone.airflow-constraints-provider-override.v1"
_EXACT_PIN = re.compile(rb"^[ \t]*([A-Za-z0-9][A-Za-z0-9_.-]*)[ \t]*==[ \t]*([^;# \t\r\n]+)(?:[ \t]*(?:;|#).*)?[ \t]*$")


def _normalized_distribution(value: str) -> str:
    normalized = re.sub(r"[-_.]+", "-", value.strip()).lower()
    if not normalized or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", normalized):
        raise ValueError("distribution must be a non-empty normalized Python distribution name")
    return normalized


def _validated_version(value: str, *, field: str) -> str:
    version = value.strip()
    if not version or re.search(r"\s", version):
        raise ValueError(f"{field} must be a non-empty version without whitespace")
    return version


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def derive_provider_override_constraints(
    source: bytes,
    *,
    distribution: str,
    requested_version: str,
) -> tuple[bytes, dict[str, Any]]:
    """Return source bytes without exactly one provider pin and audit evidence."""

    target = _normalized_distribution(distribution)
    requested = _validated_version(requested_version, field="requested_version")
    retained: list[bytes] = []
    removed: list[tuple[str, str]] = []

    for line in source.splitlines(keepends=True):
        candidate = line.rstrip(b"\r\n")
        match = _EXACT_PIN.fullmatch(candidate)
        if match is not None:
            name = match.group(1).decode("ascii")
            if _normalized_distribution(name) == target:
                constrained_version = _validated_version(
                    match.group(2).decode("ascii"),
                    field="constrained_version",
                )
                removed.append((candidate.decode("utf-8"), constrained_version))
                continue
        retained.append(line)

    if len(removed) != 1:
        raise ValueError(f"expected exactly one exact pin for {target!r}; found {len(removed)}")

    derived = b"".join(retained)
    removed_requirement, constrained_version = removed[0]
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "status": "PASS",
        "distribution": target,
        "constrained_version": constrained_version,
        "requested_version": requested,
        "removed_requirement": removed_requirement,
        "source_sha256": _sha256(source),
        "derived_sha256": _sha256(derived),
        "preserved_line_count": len(retained),
    }
    return derived, evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--requested-version", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        source = args.source.read_bytes()
        derived, evidence = derive_provider_override_constraints(
            source,
            distribution=args.distribution,
            requested_version=args.requested_version,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(derived)
        args.evidence.write_text(
            json.dumps(evidence, separators=(",", ":"), sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"airflow constraints override failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
