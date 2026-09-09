"""Validate reusable dbt self-service workflow inputs before shell expansion."""

from __future__ import annotations

import os
import re
import sys

_SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[0-9A-Za-z._+-]*)?$")
_PYTHON = re.compile(r"^3\.(11|12)$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$")
_REL_PATH = re.compile(r"^(?!.*(?:^|/)\.\.(?:/|$))(?!/)[A-Za-z0-9._@+/-]+$")
_ARTIFACT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:[^\s\"'`$\\]{1,1024}$")
_IMAGE = re.compile(r"^[A-Za-z0-9._/-]+@[sS][hH][aA]256:[0-9a-f]{64}$")
_CONFIG_MAP = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$")


def _optional(pattern: re.Pattern[str]) -> re.Pattern[str]:
    return re.compile(rf"^(?:{pattern.pattern[1:-1]})?$")


_RULES: dict[str, re.Pattern[str]] = {
    "DPONE_VERSION": _SEMVER,
    "AIRFLOW_VERSION": _SEMVER,
    "PYTHON_VERSION": _PYTHON,
    "DPONE_PROJECT_DIR": _REL_PATH,
    "DPONE_PUBLISH_POLICY": _REL_PATH,
    "DPONE_AIRFLOW_BASE_URL": _optional(_URI),
    "DPONE_RELEASE_ID": _DIGEST,
    "DPONE_DEPLOYMENT_ID": _DIGEST,
    "EXPECTED_DEV_DEPLOYMENT_ID": _DIGEST,
    "DPONE_EXPECTED_CURRENT": _optional(_DIGEST),
    "DPONE_RELEASE_REPOSITORY": _REPO,
    "PROD_REPOSITORY": _REPO,
    "DPONE_RELEASE_SOURCE_COMMIT": _GIT_SHA,
    "DPONE_RELEASE_SOURCE_REF": _REF,
    "DPONE_DEV_EVIDENCE_SOURCE_COMMIT_EXPECTED": _GIT_SHA,
    "DPONE_DEV_EVIDENCE_SOURCE_REF": _REF,
    "DPONE_RUNTIME_IMAGE_REF": _IMAGE,
    "DPONE_RUNTIME_IMAGE_DIGEST": _DIGEST,
    "DPONE_ARTIFACT_REGISTRY_REF": _ARTIFACT,
    "DPONE_REGISTRY_URI": _URI,
    "REGISTRY_CONFIG_MAP_NAME": _CONFIG_MAP,
    "REGISTRY_CONFIG_SHA256": _DIGEST,
    "TRUST_POLICY_CONFIG_MAP_NAME": _CONFIG_MAP,
    "TRUST_POLICY_SHA256": _DIGEST,
    "PROMOTION_DESCRIPTOR_PATH": _REL_PATH,
    "PROD_MIRROR_PATH": _REL_PATH,
    "PROD_SOURCE_SNAPSHOT_PATH": _REL_PATH,
    "PROD_DESCRIPTOR_PATH": _REL_PATH,
    "PROD_BASE_BRANCH": _REF,
    "MIRROR_PATH": _REL_PATH,
    "SNAPSHOT_PATH": _REL_PATH,
    "DESCRIPTOR_PATH": _REL_PATH,
    "DEV_EVIDENCE_ARTIFACT_NAME": _ARTIFACT,
    "RELEASE_ARTIFACT_NAME": _ARTIFACT,
}


def main(argv: list[str] | None = None) -> int:
    names = argv[1:] if argv is not None else sys.argv[1:]
    if not names:
        print("usage: validate_workflow_inputs.py ENV_NAME [ENV_NAME...]", file=sys.stderr)
        return 2
    failures: list[str] = []
    for name in names:
        pattern = _RULES.get(name)
        if pattern is None:
            failures.append(f"{name}: no allowlist rule")
            continue
        value = os.environ.get(name)
        if value is None:
            failures.append(f"{name}: missing")
            continue
        if not pattern.fullmatch(value):
            failures.append(f"{name}: rejected by allowlist")
    if failures:
        for item in failures:
            print(item, file=sys.stderr)
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
