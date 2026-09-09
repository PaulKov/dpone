"""Recheck exact-commit GitHub authority from a closed verifier artifact.

The ordinary release gate reads policy bytes from ``git show``.  A protected
publisher intentionally has no checkout, so this adapter accepts only the
policy file staged beside itself and proves that its bytes match an earlier
successful exact-commit authorization receipt before reading live GitHub
state again.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, NamedTuple

MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_POLICY_BYTES = 1024 * 1024
POLICY_FILENAME = "github-branch-protection.yml"
BASELINE_FILENAME = "github-branch-protection.release-authority.json"
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


release_gate = _load_sibling("dpone_breakglass_release_commit_gate", "release_commit_gate.py")
authority_baseline = _load_sibling(
    "dpone_breakglass_release_authority_baseline",
    "release_authority_baseline.py",
)


class AuthorizedPolicy(NamedTuple):
    """Policy and producer identities bound by the authorization receipt."""

    ruleset_id: int
    context_names: tuple[str, ...]
    policy_sha256: str
    report_sha256: str
    integration_ids: dict[str, int]
    policy_projection_sha256: str
    projection: dict[str, Any]
    privileged_baseline: dict[str, Any]


class AuthorizedRuleset(NamedTuple):
    """Observable authority plus its frozen privileged baseline."""

    projection: dict[str, Any]
    privileged_baseline: dict[str, Any]


def _bounded_bytes(path: Path, *, limit: int, label: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be one regular file")
    size = path.stat().st_size
    if size <= 0 or size > limit:
        raise ValueError(f"{label} size is outside its closed limit")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) != size or len(raw) > limit:
        raise ValueError(f"{label} changed while it was read or exceeded its closed limit")
    return raw


def _json_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be one UTF-8 JSON object") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be one JSON object")
    return payload


def load_authorized_policy(*, report_path: Path, repository: str, commit_sha: str) -> AuthorizedPolicy:
    """Bind the staged policy bytes to one prior successful gate receipt."""

    report_raw = _bounded_bytes(
        report_path,
        limit=MAX_RECEIPT_BYTES,
        label="authorization required-check receipt",
    )
    report = _json_object(report_raw, label="authorization required-check receipt")
    policy_path = Path(__file__).with_name(POLICY_FILENAME)
    baseline = authority_baseline.load_release_authority_baseline(
        Path(__file__).with_name(BASELINE_FILENAME),
        policy_path=policy_path,
    )
    expected = {
        "schema_version": 1,
        "status": "PASS",
        "decision": "GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "policy_path": ".agents/policy/github-branch-protection.yml",
        "policy_sha256": baseline.policy_sha256,
        "ruleset_id": baseline.ruleset_id,
    }
    if any(report.get(field) != value for field, value in expected.items()):
        raise ValueError("authorization required-check receipt does not bind the closed policy and release commit")
    if report.get("blockers") != []:
        raise ValueError("authorization required-check receipt contains blockers")
    contexts = report.get("contexts")
    if not isinstance(contexts, list) or not contexts:
        raise ValueError("authorization required-check contexts are unavailable")
    integration_ids: dict[str, int] = {}
    for context in contexts:
        if not isinstance(context, dict) or context.get("status") != "PASS":
            raise ValueError("authorization required-check context is not successful")
        name = context.get("context")
        integration_id = context.get("integration_id")
        if (
            not isinstance(name, str)
            or not name
            or name in integration_ids
            or isinstance(integration_id, bool)
            or not isinstance(integration_id, int)
            or integration_id <= 0
        ):
            raise ValueError("authorization required-check producer identity is invalid")
        integration_ids[name] = integration_id
    context_names = tuple(sorted(integration_ids))
    if integration_ids != baseline.integration_ids:
        raise ValueError("authorization required-check producers differ from sealed release authority")
    return AuthorizedPolicy(
        ruleset_id=baseline.ruleset_id,
        context_names=context_names,
        policy_sha256=baseline.policy_sha256,
        report_sha256=hashlib.sha256(report_raw).hexdigest(),
        integration_ids=integration_ids,
        policy_projection_sha256=baseline.policy_projection_sha256,
        projection=baseline.projection,
        privileged_baseline=baseline.privileged,
    )


def _reconcile_producers(payload: dict[str, Any], authorized: AuthorizedPolicy) -> None:
    contexts = payload.get("contexts")
    if not isinstance(contexts, list):
        raise ValueError("fresh required-check contexts are unavailable")
    current: dict[str, int] = {}
    for context in contexts:
        if not isinstance(context, dict) or context.get("status") != "PASS":
            raise ValueError("fresh required-check context is not successful")
        name = context.get("context")
        integration_id = context.get("integration_id")
        if (
            not isinstance(name, str)
            or name in current
            or isinstance(integration_id, bool)
            or not isinstance(integration_id, int)
            or integration_id <= 0
        ):
            raise ValueError("fresh required-check producer identity is invalid")
        current[name] = integration_id
    if current != authorized.integration_ids:
        raise ValueError("fresh required-check producer identities differ from authorization")


def _authorized_ruleset_projection(
    path: Path,
    *,
    repository: str,
    commit_sha: str,
    authorized: AuthorizedPolicy,
) -> AuthorizedRuleset:
    raw = _bounded_bytes(path, limit=MAX_RECEIPT_BYTES, label="authorization ruleset snapshot")
    payload = _json_object(raw, label="authorization ruleset snapshot")
    expected = {
        "schema": "dpone.release_ruleset_snapshot.v1",
        "status": "PASS",
        "decision": "GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "ruleset_id": authorized.ruleset_id,
        "policy_sha256": authorized.policy_sha256,
        "required_check_report_sha256": authorized.report_sha256,
        "policy_projection_sha256": authorized.policy_projection_sha256,
        "blockers": [],
    }
    if any(payload.get(field) != value for field, value in expected.items()):
        raise ValueError("authorization ruleset snapshot does not bind the exact release checks")
    projection = payload.get("projection")
    if not isinstance(projection, dict):
        raise ValueError("authorization ruleset projection is unavailable")
    try:
        canonical_projection = release_gate.live_ruleset.observable_ruleset_projection(projection)
    except (TypeError, ValueError) as exc:
        raise ValueError("authorization ruleset projection is invalid") from exc
    if canonical_projection != projection:
        raise ValueError("authorization ruleset projection is not canonical")
    if projection != authorized.projection:
        raise ValueError("authorization ruleset projection differs from sealed release authority")
    privileged = payload.get("privileged_baseline")
    if privileged != authorized.privileged_baseline:
        raise ValueError("authorization privileged ruleset differs from sealed release authority")
    return AuthorizedRuleset(projection=authorized.projection, privileged_baseline=authorized.privileged_baseline)


def _failure(*, repository: str, commit_sha: str, message: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "FAIL",
        "decision": "NO-GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "contexts": [],
        "blockers": [{"code": "CLOSED_AUTHORITY_INVALID", "message": message}],
    }


def main(argv: list[str] | None = None) -> int:
    """Emit a credential-free fresh exact-commit report for merge reconciliation."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--authorization-report", required=True, type=Path)
    parser.add_argument("--authorization-ruleset-report", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=15.0)
    args = parser.parse_args(argv)
    repository = args.repo
    commit_sha = args.commit_sha
    try:
        if REPOSITORY.fullmatch(repository) is None or FULL_SHA.fullmatch(commit_sha) is None:
            raise ValueError("release repository or commit identity is invalid")
        token = os.environ.get(args.github_token_env)
        if not token:
            raise ValueError("approved GitHub token is required")
        authorized = load_authorized_policy(
            report_path=args.authorization_report,
            repository=repository,
            commit_sha=commit_sha,
        )
        authorized_ruleset = _authorized_ruleset_projection(
            args.authorization_ruleset_report,
            repository=repository,
            commit_sha=commit_sha,
            authorized=authorized,
        )
        observed_projections: list[dict[str, Any]] = []

        def fetcher(**kwargs: Any) -> Any:
            snapshot = release_gate.fetch_live_snapshot(**kwargs)
            projection = snapshot.ruleset_projection
            if not isinstance(projection, dict):
                raise ValueError("fresh live ruleset projection is unavailable")
            for field in ("bypass_actors", "version_id"):
                observed = projection.get(field)
                if observed is not None and observed != authorized_ruleset.privileged_baseline[field]:
                    raise ValueError(f"fresh live privileged ruleset {field} differs from authorization")
            observed_projections.append(release_gate.live_ruleset.observable_ruleset_projection(projection))
            return snapshot

        report = release_gate.poll_release_gate(
            repository,
            commit_sha,
            authorized.ruleset_id,
            authorized.context_names,
            token,
            args.timeout_seconds,
            args.poll_interval_seconds,
            fetcher=fetcher,
            policy_sha256=authorized.policy_sha256,
            policy_projection=None,
        )
        payload = report.to_payload()
        if payload["status"] == "PASS":
            _reconcile_producers(payload, authorized)
            if not observed_projections or observed_projections[-1] != authorized_ruleset.projection:
                raise ValueError("fresh live ruleset projection differs from authorization")
        payload["authorization_report_sha256"] = authorized.report_sha256
    except (OSError, RuntimeError, TypeError, UnicodeError, ValueError) as exc:
        payload = _failure(repository=repository, commit_sha=commit_sha, message=str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["AuthorizedPolicy", "load_authorized_policy"]
