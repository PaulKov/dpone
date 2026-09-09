"""Validate pull-request receipt evidence for agent control-plane changes."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

Status = Literal["PASS", "FAIL", "N/A"]
CHECKED_BOX = r"(?:x|X)"
OWNER_REVIEW = re.compile(rf"- \[{CHECKED_BOX}\] Owner reviewed the final diff and accepts the change\.")
REQUIRED_CHECKS = re.compile(rf"- \[{CHECKED_BOX}\] Required GitHub checks are green on the reviewed head commit\.")
ADMIN_BYPASS = re.compile(rf"- \[{CHECKED_BOX}\] Admin bypass was not used(?:,|\.)")
GOVERNANCE_RECEIPT = re.compile(
    rf"- \[{CHECKED_BOX}\] Agent governance receipt is attached when agent controls changed\."
)
GOVERNANCE_RECEIPT_REFERENCE = re.compile(r"\bagent[_-]governance[_-]gate(?:\.json)?\b", re.IGNORECASE)
REPOSITORY = re.compile(r"^[^/\s]+/[^/\s]+$")


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling policy module when this file is executed by path."""

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


github_receipt = load_sibling("dpone_agent_pr_receipt_github", "pr_receipt_github.py")
path_evidence = load_sibling("dpone_agent_pr_path_evidence", "path_evidence.py")
pr_evidence_chain = load_sibling("dpone_agent_pr_evidence_chain", "pr_evidence_chain.py")
pr_traceability = load_sibling("dpone_agent_pr_traceability", "pr_traceability.py")
wait_policy = load_sibling("dpone_agent_pr_receipt_wait", "pr_receipt_wait.py")
GitHubArtifactEvidence = github_receipt.GitHubArtifactEvidence
GitHubArtifactAttestationEvidence = github_receipt.GitHubArtifactAttestationEvidence
GitHubCheckEvidence = github_receipt.GitHubCheckEvidence
GitHubEvidence = github_receipt.GitHubEvidence
GOVERNANCE_ARTIFACT_NAME = github_receipt.GOVERNANCE_ARTIFACT_NAME
SELF_CHECK_NAME = github_receipt.SELF_CHECK_NAME
fetch_github_evidence = github_receipt.fetch_github_evidence


@dataclass
class PullRequestReceiptResult:
    """Machine-readable PR receipt validation result."""

    status: Status
    control_surface_changed: bool
    changed_paths: list[str]
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    github_evidence: Any | None = None
    traceability: Any | None = None
    evidence_chain: Any | None = None
    wait: Any | None = None

    @property
    def ok(self) -> bool:
        return self.status != "FAIL"


def validate_pr_receipt(
    *,
    body: str,
    changed_paths: list[str],
    github_evidence: Any | None = None,
    require_github_evidence: bool = False,
    require_github_attestation: bool = False,
    repository: str | None = None,
    pull_request_number: int | None = None,
    self_check_name: str = SELF_CHECK_NAME,
    governance_artifact_name: str = GOVERNANCE_ARTIFACT_NAME,
) -> PullRequestReceiptResult:
    """Validate owner and evidence attestations for control-plane PRs."""

    control_surface = load_sibling("dpone_agent_control_surface_pr_receipt", "control_surface.py")
    normalized_paths = sorted({control_surface.normalize_path(path) for path in changed_paths if path.strip()})
    if not control_surface.control_surface_changed(normalized_paths):
        return PullRequestReceiptResult(
            status="N/A",
            control_surface_changed=False,
            changed_paths=normalized_paths,
            warnings=["No agent control-surface paths changed."],
            github_evidence=github_evidence,
            traceability=None,
            evidence_chain=None,
        )

    errors: list[str] = []
    selected_governance_artifact: Any | None = None
    expected_attestation_source_ref: str | None = None
    if require_github_attestation:
        if repository is None or REPOSITORY.fullmatch(repository) is None:
            errors.append("Canonical owner/repository is required for GitHub attestation validation.")
        if pull_request_number is None or isinstance(pull_request_number, bool) or pull_request_number <= 0:
            errors.append("Positive pull-request number is required for GitHub attestation validation.")
        else:
            expected_attestation_source_ref = f"refs/pull/{pull_request_number}/merge"
    traceability = pr_traceability.extract_traceability(
        body,
        owner_attestation=pr_traceability.OwnerAttestation(
            owner_review=OWNER_REVIEW.search(body) is not None,
            required_checks=REQUIRED_CHECKS.search(body) is not None,
            admin_bypass=ADMIN_BYPASS.search(body) is not None,
            governance_receipt=GOVERNANCE_RECEIPT.search(body) is not None,
        ),
        governance_receipt_referenced=GOVERNANCE_RECEIPT_REFERENCE.search(body) is not None,
    )
    _require_pattern(body, OWNER_REVIEW, "Missing checked owner review attestation.", errors)
    _require_pattern(body, REQUIRED_CHECKS, "Missing checked required GitHub checks attestation.", errors)
    _require_pattern(body, ADMIN_BYPASS, "Missing checked admin-bypass attestation.", errors)
    _require_pattern(body, GOVERNANCE_RECEIPT, "Missing checked agent governance receipt attestation.", errors)
    errors.extend(pr_traceability.validate_traceability_payload(traceability))
    if not GOVERNANCE_RECEIPT_REFERENCE.search(body):
        errors.append("PR body must reference agent_governance_gate.json or agent-governance-gate evidence.")
    if (require_github_evidence or require_github_attestation) and github_evidence is None:
        errors.append("Live GitHub evidence is required for agent-control PR receipt validation.")
    if github_evidence is not None:
        errors.extend(github_evidence.errors)
        github_receipt.validate_required_check_evidence(github_evidence, self_check_name, errors)
        selected_governance_artifact = github_receipt.validate_governance_artifact_evidence(
            github_evidence,
            governance_artifact_name,
            errors,
            expected_changed_paths=normalized_paths,
            control_surface_changed=True,
            require_attestation=require_github_attestation,
            expected_attestation_repository=repository,
            expected_attestation_source_ref=expected_attestation_source_ref,
        )
    evidence_chain = pr_evidence_chain.evidence_chain_payload(
        github_evidence,
        self_check_name=self_check_name,
        selected_governance_artifact=selected_governance_artifact,
    )

    return PullRequestReceiptResult(
        status="FAIL" if errors else "PASS",
        control_surface_changed=True,
        changed_paths=normalized_paths,
        errors=errors,
        github_evidence=github_evidence,
        traceability=traceability,
        evidence_chain=evidence_chain,
    )


def result_payload(result: PullRequestReceiptResult) -> dict[str, Any]:
    """Convert a PR receipt result to a deterministic JSON payload."""

    payload = {
        "schema_version": 2,
        "status": result.status,
        "control_surface_changed": result.control_surface_changed,
        "changed_paths": result.changed_paths,
        "errors": result.errors,
        "warnings": result.warnings,
        "traceability": pr_traceability.traceability_payload(result.traceability),
        "github_evidence": github_receipt.evidence_payload(result.github_evidence),
        "evidence_chain": result.evidence_chain,
    }
    if result.wait is not None:
        payload["wait"] = result.wait
    return payload


def write_result(result: PullRequestReceiptResult, output: Path) -> None:
    """Write the PR receipt result JSON."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result_payload(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _require_pattern(body: str, pattern: re.Pattern[str], message: str, errors: list[str]) -> None:
    if not pattern.search(body):
        errors.append(message)


def _read_changed_paths(args: argparse.Namespace) -> list[str]:
    paths = list(args.changed_paths)
    if args.changed_paths_file:
        paths.extend(path_evidence.read_path_evidence(args.changed_paths_file))
    return paths


def _precondition_observation(
    evidence: GitHubEvidence | None,
    *,
    control_surface_changed: bool,
    self_check_name: str,
    governance_artifact_name: str,
    require_github_attestation: bool,
) -> Any:
    """Classify retryable indexing lag without retrying terminal evidence failures."""

    if evidence is None or evidence.errors:
        return wait_policy.Observation.terminal("live GitHub evidence is unavailable")
    required = sorted({check for check in evidence.required_checks if check != self_check_name})
    if not required:
        return wait_policy.Observation.terminal("no live required checks were discovered")
    observed = github_receipt.github_validation._observed_checks_by_name(evidence)
    for check in required:
        item = observed.get(check)
        if item is None:
            return wait_policy.Observation.pending(f"required check '{check}' is not registered")
        if item.successful:
            continue
        state = item.observed_state.lower()
        if state in {"queued", "in_progress", "pending", "requested", "waiting"}:
            return wait_policy.Observation.pending(f"required check '{check}' is {state}")
        return wait_policy.Observation.terminal(f"required check '{check}' is {state}")
    if control_surface_changed:
        artifact = next(
            (
                item
                for item in evidence.artifacts
                if item.name == governance_artifact_name and item.workflow_run_head_sha == evidence.head_sha
            ),
            None,
        )
        if artifact is None:
            return wait_policy.Observation.pending("governance artifact is not indexed")
        if require_github_attestation:
            attestation = artifact.attestation
            if attestation is None:
                return wait_policy.Observation.pending("governance artifact attestation is not indexed")
            if getattr(attestation, "status", None) != "PASS":
                if any(
                    "no verified attestations" in str(error).lower() for error in getattr(attestation, "errors", [])
                ):
                    return wait_policy.Observation.pending("governance artifact attestation is not indexed")
                return wait_policy.Observation.terminal("governance artifact attestation is invalid")
    return wait_policy.Observation.ready()


def _wait_for_live_prerequisites(args: argparse.Namespace, changed_paths: list[str]) -> tuple[Any, str | None]:
    """Wait for same-head prerequisites and refresh the body from that exact head."""

    if args.wait_timeout_seconds == 0:
        return None, None
    if not args.repo or not args.head_sha or not args.pull_request_number:
        raise ValueError("bounded receipt waiting requires --repo, --head-sha and --pull-request-number")
    token = os.environ.get(args.github_token_env)
    if not token:
        raise RuntimeError(f"{args.github_token_env} is required for bounded receipt waiting")
    control_surface = load_sibling("dpone_agent_control_surface_pr_wait", "control_surface.py")
    control_changed = control_surface.control_surface_changed(changed_paths)

    def fetch_head() -> str:
        return github_receipt.fetch_pull_request_snapshot(
            repo=args.repo, pull_request_number=args.pull_request_number, token=token
        ).head_sha

    def observe() -> Any:
        evidence = github_receipt.github_evidence_from_args(args)
        return _precondition_observation(
            evidence,
            control_surface_changed=control_changed,
            self_check_name=SELF_CHECK_NAME,
            governance_artifact_name=args.artifact_name,
            require_github_attestation=args.require_github_attestation,
        )

    outcome = wait_policy.wait_for_exact_head(
        expected_head=args.head_sha,
        fetch_head=fetch_head,
        observe=observe,
        now=time.monotonic,
        sleep=time.sleep,
        timeout_seconds=args.wait_timeout_seconds,
        initial_backoff_seconds=args.wait_initial_backoff_seconds,
        max_backoff_seconds=args.wait_max_backoff_seconds,
    )
    if outcome.state != "READY":
        return outcome, None
    snapshot = github_receipt.fetch_pull_request_snapshot(
        repo=args.repo, pull_request_number=args.pull_request_number, token=token
    )
    if snapshot.head_sha != args.head_sha:
        return wait_policy.WaitOutcome(
            "STALE_HEAD", outcome.attempts, outcome.elapsed_seconds, "pull-request head changed before body refresh"
        ), None
    return outcome, snapshot.body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--body-file", required=True, type=Path)
    parser.add_argument("--changed-paths", nargs="*", default=[])
    parser.add_argument("--changed-paths-file", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--repo")
    parser.add_argument("--head-sha")
    parser.add_argument("--pull-request-number", type=int)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--ruleset-id", type=int)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--artifact-name", default=GOVERNANCE_ARTIFACT_NAME)
    parser.add_argument("--require-github-evidence", action="store_true")
    parser.add_argument("--require-github-attestation", action="store_true")
    parser.add_argument("--github-attestation-signer-workflow")
    parser.add_argument("--wait-timeout-seconds", type=float, default=0)
    parser.add_argument("--wait-initial-backoff-seconds", type=float, default=5)
    parser.add_argument("--wait-max-backoff-seconds", type=float, default=60)
    args = parser.parse_args(argv)

    changed_paths = _read_changed_paths(args)
    try:
        wait_outcome, refreshed_body = _wait_for_live_prerequisites(args, changed_paths)
    except (RuntimeError, ValueError) as exc:
        wait_outcome = wait_policy.WaitOutcome("TERMINAL_FAILURE", 0, 0.0, str(exc))
        refreshed_body = None
    github_evidence = github_receipt.github_evidence_from_args(args)
    result = validate_pr_receipt(
        body=refreshed_body if refreshed_body is not None else args.body_file.read_text(encoding="utf-8"),
        changed_paths=changed_paths,
        github_evidence=github_evidence,
        require_github_evidence=args.require_github_evidence,
        require_github_attestation=args.require_github_attestation,
        repository=args.repo,
        pull_request_number=args.pull_request_number,
        governance_artifact_name=args.artifact_name,
    )
    if wait_outcome is not None:
        result.wait = {
            "state": wait_outcome.state,
            "attempts": wait_outcome.attempts,
            "elapsed_seconds": wait_outcome.elapsed_seconds,
            "reason": wait_outcome.reason,
        }
        if wait_outcome.state != "READY":
            result.status = "FAIL"
            result.errors.append(f"Exact-head prerequisite wait ended as {wait_outcome.state}: {wait_outcome.reason}.")
        elif args.repo and args.pull_request_number:
            token = os.environ.get(args.github_token_env)
            try:
                final_head = github_receipt.fetch_pull_request_snapshot(
                    repo=args.repo, pull_request_number=args.pull_request_number, token=token or ""
                ).head_sha
            except (RuntimeError, ValueError) as exc:
                result.status = "FAIL"
                result.errors.append(f"Unable to recheck current pull-request head: {exc}")
            else:
                if final_head != args.head_sha:
                    result.status = "FAIL"
                    result.errors.append("Pull-request head changed after receipt evidence validation.")
    if args.output:
        write_result(result, args.output)

    payload = result_payload(result)
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in result.warnings:
            print(f"WARNING: {warning}")
        for error in result.errors:
            print(f"ERROR: {error}")
        print(f"Agent PR receipt validation: {result.status}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
