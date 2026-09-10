"""Audit integration-owned paths separately from reviewed imported checkpoints."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path

import yaml

PLANNING = "f3682940f8864563cde0e6b6ecee60f746b49020"
# Reviewed provenance recorded in dependencies.md; imported artifacts are not
# reclassified as DDA-06-owned writes merely because this branch contains them.
IMPORTED_PREFIXES = (
    "85e598c",
    "4edfb91",
    "e10d48e",
    "642874b",
    "c1a88ad",
    "0b526c5",
    "27a1252",
    "3dea445",
    "3f37418",
    "ad9d96b",
    "61bcebc",
    "a029878",
    "ad5c117",
    "4851bd3",
    "76ee6da",
    "9795f01",
    "d8b09de",
    "039fe201",
    "e0fbad8",
    "aadf384",
    "b8ae950",
    "94bbd3c",
    "7912df9",
    "957c239",
    "7fcdabd",
    "3e57875",
    "006e0a1",
    "9aa2a0e",
    "9b69be0",
    "f7ae00c",
    "d406dbf",
    "5cc2296",
    "518cc16",
    "f09af01",
    "5761738",
    "38f6603",
    "b5ad9d2",
    "33b7ad3",
)
FROZEN = {
    "docs/feature-design-data-delivery-acceleration-v1.md",
    "docs/feature-design-clickhouse-mssql-bounded-native-v1.md",
    "docs/data-delivery-acceleration-tasks.md",
}


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def matches(path: str, roots: list[str]) -> bool:
    return any(path == root or path.startswith(root.rstrip("/") + "/") for root in roots)


def changed_paths(commit: str) -> list[str]:
    return git("diff-tree", "--no-commit-id", "--name-only", "-r", commit).splitlines()


def patch_identity(commit: str, path: str | None = None) -> str:
    arguments = ["show", "--format=", "--binary", "--unified=0", commit]
    if path is not None:
        arguments.extend(["--", path])
    patch = subprocess.check_output(["git", *arguments], text=True)
    return subprocess.check_output(["git", "patch-id", "--verbatim"], input=patch, text=True).split()[0]


def reviewed_import(commit: str, original: str) -> bool:
    paths = changed_paths(commit)
    if paths != changed_paths(original):
        return False
    if patch_identity(commit) == patch_identity(original):
        return True
    return all(
        patch_identity(commit, path) == patch_identity(original, path)
        # An explicitly resolved conflict may adopt the exact reviewed file,
        # while its patch differs because the integration parent was different.
        or git("ls-tree", commit, "--", path) == git("ls-tree", original, "--", path)
        for path in paths
    )


def main() -> None:
    contract_path = Path("test_artifacts/delivery-acceleration/planning-amendments/dda-06-architecture-remediation.yml")
    raw = contract_path.read_bytes()
    contract = yaml.safe_load(raw)
    shared = contract["integrator_owned_paths"]
    allowed = contract["owned_paths"] + shared
    violations, owned, imported = [], set(), []
    reviewed = {git("rev-parse", prefix) for prefix in IMPORTED_PREFIXES}
    for commit in git("rev-list", "--reverse", f"{PLANNING}..HEAD").splitlines():
        provenance = re.search(
            r"\(cherry picked from commit ([a-f0-9]{40})\)", git("show", "-s", "--format=%B", commit)
        )
        if provenance and provenance[1] in reviewed:
            verified = reviewed_import(commit, provenance[1])
            imported.append({"commit": commit, "original": provenance[1], "reviewed_delta_verified": verified})
            if not verified:
                violations.append({"commit": commit, "reason": "Imported delta differs from reviewed checkpoint"})
            continue
        paths = changed_paths(commit)
        for path in paths:
            owned.add(path)
            forbidden = matches(path, contract["forbidden_paths"]) and not matches(path, shared)
            if path in FROZEN or not matches(path, allowed) or forbidden:
                violations.append({"commit": commit, "path": path})
    report = {
        "schema_version": 1,
        "source_commit": git("rev-parse", "HEAD"),
        "contract": str(contract_path),
        "contract_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "FAIL" if violations else "PASS",
        "owned_paths": sorted(owned),
        "reviewed_imports_excluded_from_own_path_audit": imported,
        "violations": violations,
    }
    print(json.dumps(report, indent=2))
    raise SystemExit(bool(violations))


if __name__ == "__main__":
    main()
