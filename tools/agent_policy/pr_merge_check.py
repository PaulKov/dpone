"""Project one immutable merge-closure receipt onto its exact integration commit."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CHECK_NAME = "Agent PR receipt"
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
POSITIVE_INTEGER = re.compile(r"^[1-9][0-9]*$")


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


github_api = _load_sibling("dpone_agent_pr_merge_check_github_api", "pr_receipt_github_api.py")
merge_receipt = _load_sibling("dpone_agent_pr_merge_check_receipt", "pr_merge_receipt.py")
merge_ownership = _load_sibling(
    "dpone_agent_pr_merge_check_ownership",
    "pr_merge_check_ownership.py",
)


@dataclass(frozen=True)
class MergeCheckProjection:
    """A bounded Checks API request derived from event and receipt authority."""

    integration_commit_sha: str
    conclusion: str
    blocker_codes: tuple[str, ...]
    request: dict[str, Any]


def derive_projection(
    *,
    event: dict[str, Any],
    receipt_path: Path,
    producer_exit_code_path: Path,
    repository: str,
    run_id: str,
    run_attempt: str,
) -> MergeCheckProjection:
    """Build a success or failure check request without reconstructing receipt semantics."""

    integration_sha = _event_integration_sha(event, repository)
    _require_positive(run_id, "workflow run id")
    _require_positive(run_attempt, "workflow run attempt")
    blockers = (
        *_receipt_blockers(
            receipt_path,
            repository=repository,
            integration_sha=integration_sha,
            run_id=run_id,
            run_attempt=run_attempt,
        ),
        *_producer_exit_blockers(producer_exit_code_path),
    )
    conclusion = "failure" if blockers else "success"
    status_label = "FAIL" if blockers else "PASS"
    summary = (
        f"Immutable merge-closure receipt {status_label} for integration commit "
        f"`{integration_sha}`. Workflow run {run_id}, attempt {run_attempt}."
    )
    if blockers:
        summary += f" Blockers: {', '.join(blockers)}. Inspect the retained receipt artifact."
    request = {
        "name": CHECK_NAME,
        "head_sha": integration_sha,
        "status": "completed",
        "conclusion": conclusion,
        "details_url": f"https://github.com/{repository}/actions/runs/{run_id}",
        "external_id": f"agent-pr-merge-closure:{run_id}:{run_attempt}",
        "output": {
            "title": f"Agent PR merge closure: {status_label}",
            "summary": summary,
        },
    }
    return MergeCheckProjection(
        integration_commit_sha=integration_sha,
        conclusion=conclusion,
        blocker_codes=blockers,
        request=request,
    )


def publish_projection(
    projection: MergeCheckProjection,
    *,
    repository: str,
    token: str,
    poster: Callable[..., dict[str, Any]] = github_api.post_github_json,
    updater: Callable[..., dict[str, Any]] = github_api.patch_github_json,
) -> tuple[int, int]:
    """Create the exact-commit check and validate the provider response."""

    initial_request = {
        **projection.request,
        "conclusion": "failure",
        "output": {
            "title": "Agent PR merge closure: validating",
            "summary": (
                "The exact-commit check remains failed until the immutable receipt and provider response are validated."
            ),
        },
    }
    response = poster(
        f"repos/{repository}/check-runs",
        token=token,
        payload=initial_request,
    )
    check_run_id, app_id = _validated_provider_identity(
        response,
        projection=projection,
        repository=repository,
        expected_conclusion="failure",
    )
    if projection.conclusion == "failure":
        return check_run_id, app_id
    update_request = {key: value for key, value in projection.request.items() if key != "head_sha"}
    endpoint = f"repos/{repository}/check-runs/{check_run_id}"
    try:
        response = updater(endpoint, token=token, payload=update_request)
        final_check_run_id, final_app_id = _validated_provider_identity(
            response,
            projection=projection,
            repository=repository,
            expected_conclusion=projection.conclusion,
        )
        if (final_check_run_id, final_app_id) != (check_run_id, app_id):
            raise RuntimeError("GitHub check update changed the provider identity.")
    except (RuntimeError, ValueError):
        updater(
            endpoint,
            token=token,
            payload={
                "status": "completed",
                "conclusion": "failure",
                "output": {
                    "title": "Agent PR merge closure: provider validation failed",
                    "summary": "The provider response could not be bound to the immutable merge-closure receipt.",
                },
            },
        )
        raise
    return check_run_id, app_id


def _validated_provider_identity(
    response: dict[str, Any],
    *,
    projection: MergeCheckProjection,
    repository: str,
    expected_conclusion: str,
) -> tuple[int, int]:
    app = response.get("app")
    check_run_id = _positive_int(response.get("id"))
    app_id = _positive_int(app.get("id")) if isinstance(app, dict) and app.get("slug") == "github-actions" else None
    provider_details_url = f"https://github.com/{repository}/runs/{check_run_id}" if check_run_id is not None else None
    accepted_details_urls = {projection.request["details_url"], provider_details_url}
    response_matches = (
        response.get("name") == projection.request["name"]
        and response.get("head_sha") == projection.integration_commit_sha
        and response.get("status") == "completed"
        and response.get("conclusion") == expected_conclusion
        and response.get("details_url") in accepted_details_urls
        and response.get("external_id") == projection.request["external_id"]
    )
    if not response_matches or check_run_id is None or app_id is None:
        raise RuntimeError("GitHub check publication response does not match the exact projection.")
    return check_run_id, app_id


def _event_integration_sha(event: dict[str, Any], repository: str) -> str:
    if not REPOSITORY.fullmatch(repository):
        raise ValueError("repository must use the owner/name form")
    pull_request = event.get("pull_request")
    if event.get("action") != "closed" or not isinstance(pull_request, dict) or pull_request.get("merged") is not True:
        raise ValueError("event must be an immutable merged pull_request closed event")
    base = pull_request.get("base")
    base_repo = base.get("repo") if isinstance(base, dict) else None
    event_repository = event.get("repository")
    identities = (
        base_repo.get("full_name") if isinstance(base_repo, dict) else None,
        event_repository.get("full_name") if isinstance(event_repository, dict) else None,
    )
    if any(identity != repository for identity in identities):
        raise ValueError("event repository identity does not match the canonical repository")
    integration_sha = pull_request.get("merge_commit_sha")
    if not isinstance(integration_sha, str) or not FULL_SHA.fullmatch(integration_sha):
        raise ValueError("event integration commit SHA must be full lowercase hexadecimal")
    return integration_sha


def _receipt_blockers(
    receipt_path: Path,
    *,
    repository: str,
    integration_sha: str,
    run_id: str,
    run_attempt: str,
) -> tuple[str, ...]:
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError):
        return ("MERGE_RECEIPT_UNAVAILABLE",)
    except json.JSONDecodeError:
        return ("MERGE_RECEIPT_INVALID",)
    if not isinstance(payload, dict):
        return ("MERGE_RECEIPT_INVALID",)
    try:
        merge_receipt._validate_receipt_payload(payload)
    except ValueError:
        return ("MERGE_RECEIPT_INVALID",)
    blockers: list[str] = []
    if payload.get("repository") != repository:
        blockers.append("REPOSITORY_MISMATCH")
    if payload.get("integration_commit_sha") != integration_sha:
        blockers.append("INTEGRATION_SHA_MISMATCH")
    if payload.get("status") != "PASS":
        blockers.append("MERGE_RECEIPT_FAILED")
    producer = payload.get("producer")
    expected_producer = {
        "workflow": "Agent PR receipt",
        "run_id": run_id,
        "run_attempt": run_attempt,
    }
    if producer != expected_producer:
        blockers.append("PRODUCER_IDENTITY_MISMATCH")
    if payload.get("status") == "PASS" and payload.get("binding_id") != merge_receipt.compute_binding_id(payload):
        blockers.append("BINDING_ID_MISMATCH")
    return tuple(blockers)


def _producer_exit_blockers(path: Path) -> tuple[str, ...]:
    try:
        exit_code = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return ("PRODUCER_EXIT_CODE_UNAVAILABLE",)
    if exit_code != "0":
        return ("PRODUCER_EXIT_CODE_MISMATCH",)
    return ()


def _require_positive(value: str, label: str) -> None:
    if not POSITIVE_INTEGER.fullmatch(value):
        raise ValueError(f"{label} must be a positive integer")


def _positive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def main(argv: list[str] | None = None) -> int:
    """Publish one credential-free exact-commit check projection report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--producer-exit-code", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--root", default=Path("."), type=Path)
    parser.add_argument("--protected-base-ref", action="append")
    args = parser.parse_args(argv)
    try:
        event = json.loads(args.event.read_text(encoding="utf-8"))
        if not isinstance(event, dict):
            raise ValueError("workflow event must be a JSON object")
        ownership = merge_ownership.classify_event_ownership(
            event,
            root=args.root,
            repository=args.repository,
            protected_base_refs=args.protected_base_ref or ["master"],
        )
        if ownership.classification == "transitive":
            report = {
                "status": "N/A",
                "check_name": CHECK_NAME,
                "integration_commit_sha": ownership.integration_commit_sha,
                "reviewed_head_sha": ownership.reviewed_head_sha,
                "direct_reviewed_head_sha": ownership.direct_reviewed_head_sha,
                "message": ("Transitively included PR does not own or publish the shared integration check."),
            }
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0
        projection = derive_projection(
            event=event,
            receipt_path=args.receipt,
            producer_exit_code_path=args.producer_exit_code,
            repository=args.repository,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
        )
        token = os.environ.get(args.github_token_env)
        if not token:
            raise ValueError("approved GitHub token is required")
        check_run_id, app_id = publish_projection(
            projection,
            repository=args.repository,
            token=token,
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
        report: dict[str, Any] = {
            "status": "FAIL",
            "check_name": CHECK_NAME,
            "message": "Exact integration check publication failed.",
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1
    report = {
        "status": "PASS" if projection.conclusion == "success" else "FAIL",
        "check_name": CHECK_NAME,
        "check_run_id": check_run_id,
        "github_app_id": app_id,
        "integration_commit_sha": projection.integration_commit_sha,
        "projected_conclusion": projection.conclusion,
        "blocker_codes": list(projection.blocker_codes),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if projection.conclusion == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
