"""Render and preflight agent-control pull-request bodies."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

Status = Literal["PASS", "FAIL", "N/A"]
Phase = Literal["draft", "final"]


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


control_surface = load_sibling("dpone_agent_control_surface_pr_body", "control_surface.py")
pr_receipt = load_sibling("dpone_agent_pr_receipt_pr_body", "pr_receipt.py")
pr_traceability = load_sibling("dpone_agent_pr_traceability_pr_body", "pr_traceability.py")


@dataclass(frozen=True)
class ValidationRow:
    """One Markdown validation-evidence table row."""

    check: str
    status: str
    command: str
    notes: str


@dataclass(frozen=True)
class PreflightResult:
    """Local PR body preflight result."""

    status: Status
    phase: Phase
    control_surface_changed: bool
    changed_paths: list[str]
    errors: list[str]
    warnings: list[str]
    traceability: Any | None

    @property
    def ok(self) -> bool:
        return self.status != "FAIL"

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "control_surface_changed": self.control_surface_changed,
            "changed_paths": self.changed_paths,
            "errors": self.errors,
            "warnings": self.warnings,
            "traceability": pr_traceability.traceability_payload(self.traceability),
        }


DEFAULT_VALIDATION_ROWS = (
    ValidationRow(
        check="Focused tests",
        status="UNVERIFIED",
        command="uv run pytest tests/agent_policy -q",
        notes="pending local execution before final PR body",
    ),
    ValidationRow(
        check="Agent governance gate",
        status="UNVERIFIED",
        command=(
            "uv run python tools/agent_policy/governance_gate.py --base-ref origin/master "
            "--output test_artifacts/agent-policy/agent_governance_gate.json"
        ),
        notes="pending GitHub CI artifact for final reviewed head",
    ),
)


def render_body(
    *,
    approved_source: str,
    problem: str = "Agent-control PR body needs local receipt preflight.",
    solution: str = "Generate and check the exact Agent PR receipt grammar before GitHub CI.",
    impact: str = "Maintainers avoid avoidable body-only receipt failures.",
    in_scope: Sequence[str] = ("Agent PR body generation and preflight.",),
    non_goals: Sequence[str] = ("Runtime ETL behavior changes.",),
    validation_rows: Sequence[ValidationRow] = DEFAULT_VALIDATION_ROWS,
    governance_receipt: str = "agent_governance_gate.json / agent-governance-gate pending final CI.",
) -> str:
    """Render a PR body that uses the exact Agent PR receipt grammar."""

    scope_lines = "\n".join(f"- In scope: {_cell_text(item)}" for item in in_scope)
    non_goal_lines = "\n".join(f"- Non-goals: {_cell_text(item)}" for item in non_goals)
    validation_table = "\n".join(_validation_row(row) for row in validation_rows)
    return f"""## Summary

- Problem: {_cell_text(problem)}
- Solution: {_cell_text(solution)}
- User/developer impact: {_cell_text(impact)}

## Design and scope

- Approved specification or issue: {_cell_text(approved_source)}
{scope_lines}
{non_goal_lines}
- Owned/shared paths and integrator (when parallel): integrator owns PR body and agent-policy helper changes.

## Public contracts and compatibility

- [ ] No public-contract change
- [x] Backward-compatible change
- [ ] Deprecation/migration documented
- [ ] Breaking change explicitly approved with ADR/migration

Affected surfaces: artifacts-evidence / agent governance tooling.

## Algorithm and failure behavior

Local draft preflight checks traceability grammar and validation evidence
statuses before opening the PR. Final preflight checks owner attestation and
governance receipt reference before rerunning the GitHub `Agent PR receipt`
workflow. Live required-check and artifact freshness remain verified by GitHub
CI.

## Documentation and user journey

- [x] Tutorial/how-to/reference/runbook updated as applicable
- [x] First-time-user CJM checked
- [x] CLI/Python/YAML examples validated
- [ ] Architecture, schemas, diagrams, and cross-links updated
- [ ] Compatibility and migration docs updated

## Market research and differentiation

- Relevant comparators and official source dates/versions: GitHub PR templates,
  `pull_request` events, and artifact digest metadata checked 2026-07-13.
- Adopted/rejected patterns: keep GitHub CI authoritative; add local preflight
  only for deterministic Markdown grammar.
- Measurable advantage: fewer body-only `Agent PR receipt` reruns for
  agent-control PRs.

## Validation evidence

| Check | Status | Command/workflow | Artifact/notes |
|---|---|---|---|
{validation_table}

## Owner attestation

Use this section when no independent reviewer exists.

- [ ] Owner reviewed the final diff and accepts the change.
- [ ] Required GitHub checks are green on the reviewed head commit.
- [ ] Admin bypass was not used.
- [ ] Agent governance receipt is attached when agent controls changed.

Agent governance receipt: {_cell_text(governance_receipt)}

## Risks, operations, and rollback

- Silent data loss/duplication risk: N/A, agent governance tooling only.
- Security/secret risk: no credentials, secrets, or live GitHub writes are added.
- Observability/runbook impact: PR-body diagnosis is available locally before CI.
- Rollback plan: revert the helper, tests, and docs.
- Remaining uncertainty or follow-up: live evidence remains GitHub-only.

## Reviewer focus

Please prioritize receipt grammar compatibility, no false PASS, documentation
accuracy, and module-size guard compliance.
""".strip()


def check_body(*, body: str, changed_paths: list[str], phase: Phase) -> PreflightResult:
    """Preflight a PR body for agent-control changes."""

    normalized_paths = sorted({control_surface.normalize_path(path) for path in changed_paths if path.strip()})
    if not control_surface.control_surface_changed(normalized_paths):
        return PreflightResult(
            status="N/A",
            phase=phase,
            control_surface_changed=False,
            changed_paths=normalized_paths,
            errors=[],
            warnings=["No agent control-surface paths changed."],
            traceability=None,
        )

    if phase == "final":
        receipt_result = pr_receipt.validate_pr_receipt(body=body, changed_paths=normalized_paths)
        return PreflightResult(
            status=receipt_result.status,
            phase=phase,
            control_surface_changed=True,
            changed_paths=normalized_paths,
            errors=list(receipt_result.errors),
            warnings=list(receipt_result.warnings),
            traceability=receipt_result.traceability,
        )

    traceability = pr_traceability.extract_traceability(
        body,
        owner_attestation=pr_traceability.OwnerAttestation(
            owner_review=False,
            required_checks=False,
            admin_bypass=False,
            governance_receipt=False,
        ),
        governance_receipt_referenced=pr_receipt.GOVERNANCE_RECEIPT_REFERENCE.search(body) is not None,
    )
    errors = pr_traceability.validate_traceability_payload(traceability)
    return PreflightResult(
        status="FAIL" if errors else "PASS",
        phase=phase,
        control_surface_changed=True,
        changed_paths=normalized_paths,
        errors=errors,
        warnings=[],
        traceability=traceability,
    )


def _validation_row(row: ValidationRow) -> str:
    command = _cell_text(row.command)
    command_cell = f"`{command}`" if command else ""
    return f"| {_cell_text(row.check)} | {_cell_text(row.status)} | {command_cell} | {_cell_text(row.notes)} |"


def _cell_text(value: str) -> str:
    return " ".join(value.replace("|", "/").split())


def _parse_validation_row(value: str) -> ValidationRow:
    parts = [part.strip() for part in value.split("|")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("validation rows must use 'check|status|command|notes'")
    return ValidationRow(check=parts[0], status=parts[1], command=parts[2], notes=parts[3])


def _read_changed_paths(args: argparse.Namespace) -> list[str]:
    paths = list(args.changed_paths)
    if args.changed_paths_file:
        paths.extend(args.changed_paths_file.read_text(encoding="utf-8").splitlines())
    return paths


def _write_or_print(value: str, output: Path | None) -> None:
    if output is None:
        print(value)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(value + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    render = subparsers.add_parser("render", help="Render an Agent PR receipt-compatible PR body.")
    render.add_argument("--approved-source", required=True)
    render.add_argument("--problem", default="Agent-control PR body needs local receipt preflight.")
    render.add_argument("--solution", default="Generate and check the exact Agent PR receipt grammar before GitHub CI.")
    render.add_argument("--impact", default="Maintainers avoid avoidable body-only receipt failures.")
    render.add_argument("--in-scope", action="append", default=[])
    render.add_argument("--non-goal", action="append", default=[])
    render.add_argument("--validation-row", action="append", type=_parse_validation_row, default=[])
    render.add_argument(
        "--governance-receipt", default="agent_governance_gate.json / agent-governance-gate pending final CI."
    )
    render.add_argument("--output", type=Path)

    check = subparsers.add_parser("check", help="Preflight an Agent PR body.")
    check.add_argument("--phase", choices=("draft", "final"), required=True)
    check.add_argument("--body-file", required=True, type=Path)
    check.add_argument("--changed-paths", nargs="*", default=[])
    check.add_argument("--changed-paths-file", type=Path)
    check.add_argument("--format", choices=("text", "json"), default="text")
    check.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    if args.command == "render":
        body = render_body(
            approved_source=args.approved_source,
            problem=args.problem,
            solution=args.solution,
            impact=args.impact,
            in_scope=tuple(args.in_scope) or ("Agent PR body generation and preflight.",),
            non_goals=tuple(args.non_goal) or ("Runtime ETL behavior changes.",),
            validation_rows=tuple(args.validation_row) or DEFAULT_VALIDATION_ROWS,
            governance_receipt=args.governance_receipt,
        )
        _write_or_print(body, args.output)
        return 0

    result = check_body(
        body=args.body_file.read_text(encoding="utf-8"),
        changed_paths=_read_changed_paths(args),
        phase=args.phase,
    )
    payload = result.as_payload()
    if args.format == "json":
        output = json.dumps(payload, indent=2, sort_keys=True)
        _write_or_print(output, args.output)
    else:
        lines = [*(f"WARNING: {warning}" for warning in result.warnings)]
        lines.extend(f"ERROR: {error}" for error in result.errors)
        lines.append(f"Agent PR body preflight ({result.phase}): {result.status}")
        _write_or_print("\n".join(lines), args.output)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
