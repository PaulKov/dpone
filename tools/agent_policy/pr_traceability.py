"""Traceability checks for agent-control pull-request receipts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

APPROVED_SOURCE = re.compile(
    r"^\s*-\s*Approved specification or issue:\s*(?P<value>.*)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
TRACEABLE_SOURCE = re.compile(
    r"(#\d+|GH-\d+|https?://\S+|docs/[\w./#-]+|docs/adr/[\w./#-]+|ADR[-\s]?\d*)",
    re.IGNORECASE,
)
VALIDATION_HEADING = "## validation evidence"
ALLOWED_STATUSES = frozenset({"PASS", "FAIL", "SKIP", "N/A", "UNVERIFIED"})
STATUSES_REQUIRING_REASON = frozenset({"SKIP", "N/A", "UNVERIFIED"})
PLACEHOLDER_SOURCES = frozenset(
    {
        "",
        "-",
        "none",
        "not applicable",
        "tbd",
        "todo",
        "n/a",
        "na",
        "<issue>",
        "<spec>",
        "<github issue/pr, approved spec, adr, or n/a: reason>",
    }
)


@dataclass(frozen=True)
class ValidationEvidenceRow:
    """A parsed validation evidence row with a recognized status."""

    check: str
    status: str
    command: str
    notes: str

    def as_payload(self) -> dict[str, str]:
        return {
            "check": self.check,
            "status": self.status,
            "command": self.command,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class NonPassReason:
    """Reason attached to a non-pass validation evidence row."""

    check: str
    status: str
    reason: str

    def as_payload(self) -> dict[str, str]:
        return {
            "check": self.check,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class OwnerAttestation:
    """Checked owner-attestation state from the PR body."""

    owner_review: bool
    required_checks: bool
    admin_bypass: bool
    governance_receipt: bool

    def as_payload(self) -> dict[str, bool]:
        return {
            "owner_review": self.owner_review,
            "required_checks": self.required_checks,
            "admin_bypass": self.admin_bypass,
            "governance_receipt": self.governance_receipt,
        }


@dataclass(frozen=True)
class Traceability:
    """Structured PR-body traceability for the receipt artifact."""

    approved_source: str | None
    approved_source_kind: str
    validation_rows: tuple[ValidationEvidenceRow, ...]
    validation_statuses: tuple[str, ...]
    non_pass_reasons: tuple[NonPassReason, ...]
    owner_attestation: OwnerAttestation
    governance_receipt_referenced: bool

    def as_payload(self) -> dict[str, Any]:
        return {
            "approved_source": self.approved_source,
            "approved_source_kind": self.approved_source_kind,
            "validation_rows": [row.as_payload() for row in self.validation_rows],
            "validation_statuses": list(self.validation_statuses),
            "non_pass_reasons": [reason.as_payload() for reason in self.non_pass_reasons],
            "owner_attestation": self.owner_attestation.as_payload(),
            "governance_receipt_referenced": self.governance_receipt_referenced,
        }


def validate_traceability(body: str) -> list[str]:
    """Validate source and evidence traceability in an agent-control PR body."""

    return validate_traceability_payload(
        extract_traceability(
            body,
            owner_attestation=OwnerAttestation(
                owner_review=False,
                required_checks=False,
                admin_bypass=False,
                governance_receipt=False,
            ),
            governance_receipt_referenced=False,
        )
    )


def extract_traceability(
    body: str,
    *,
    owner_attestation: OwnerAttestation,
    governance_receipt_referenced: bool,
) -> Traceability:
    """Extract structured traceability from a PR body."""

    source = _approved_source(body)
    rows = _validation_evidence_rows(body)
    return Traceability(
        approved_source=source,
        approved_source_kind=_source_kind(source),
        validation_rows=rows,
        validation_statuses=_validation_statuses(rows),
        non_pass_reasons=_non_pass_reasons(rows),
        owner_attestation=owner_attestation,
        governance_receipt_referenced=governance_receipt_referenced,
    )


def validate_traceability_payload(traceability: Traceability) -> list[str]:
    """Validate parsed PR traceability."""

    errors: list[str] = []
    if not _source_is_valid(traceability.approved_source):
        errors.append(
            "Approved specification or issue is required for agent-control PRs and must be a link, issue id, "
            "docs path, ADR, or 'N/A: reason'."
        )

    if not traceability.validation_rows:
        errors.append("Validation evidence must include at least one PASS/FAIL/SKIP/N/A/UNVERIFIED status row.")
        return errors

    missing_reason = sorted(
        {row.status for row in traceability.validation_rows if _requires_reason(row) and not _has_reason(row.notes)}
    )
    if missing_reason:
        statuses = ", ".join(missing_reason)
        errors.append(f"Validation evidence with {statuses} must include a reason in Artifact/notes.")
    return errors


def traceability_payload(traceability: Traceability | None) -> dict[str, Any] | None:
    """Convert traceability into a deterministic JSON payload."""

    return traceability.as_payload() if traceability is not None else None


def _approved_source(body: str) -> str | None:
    match = APPROVED_SOURCE.search(body)
    if match is None:
        return None
    return match.group("value").strip()


def _source_is_valid(value: str | None) -> bool:
    if value is None:
        return False
    normalized = _normalize_source(value)
    if normalized in PLACEHOLDER_SOURCES:
        return False
    if normalized.startswith("n/a"):
        return _has_na_reason(value)
    return TRACEABLE_SOURCE.search(value) is not None


def _source_kind(value: str | None) -> str:
    if value is None:
        return "unknown"
    normalized = _normalize_source(value)
    if normalized.startswith("n/a") or normalized.startswith("na:"):
        return "na"
    if re.search(r"(#\d+|GH-\d+)", value, re.IGNORECASE):
        return "issue"
    if re.search(r"(docs/adr/|ADR[-\s]?\d*)", value, re.IGNORECASE):
        return "adr"
    if re.search(r"docs/[\w./#-]+", value, re.IGNORECASE):
        return "docs"
    if re.search(r"https?://\S+", value, re.IGNORECASE):
        return "url"
    return "unknown"


def _normalize_source(value: str) -> str:
    return value.strip().strip("`").strip().lower()


def _has_na_reason(value: str) -> bool:
    prefix, separator, reason = value.partition(":")
    if separator != ":" or prefix.strip().lower() not in {"n/a", "na"}:
        return False
    return _has_reason(reason)


def _validation_evidence_rows(body: str) -> tuple[ValidationEvidenceRow, ...]:
    section = _section(body, VALIDATION_HEADING)
    rows: list[ValidationEvidenceRow] = []
    for line in section.splitlines():
        cells = _table_cells(line)
        for index, cell in enumerate(cells):
            status = _normalize_status(cell)
            if status in ALLOWED_STATUSES:
                rows.append(
                    ValidationEvidenceRow(
                        check=_cell(cells, 0),
                        status=status,
                        command=_strip_inline_code(_cell(cells, index + 1)),
                        notes=_cell(cells, -1),
                    )
                )
                break
    return tuple(rows)


def _validation_statuses(rows: tuple[ValidationEvidenceRow, ...]) -> tuple[str, ...]:
    statuses: list[str] = []
    for row in rows:
        if row.status not in statuses:
            statuses.append(row.status)
    return tuple(statuses)


def _non_pass_reasons(rows: tuple[ValidationEvidenceRow, ...]) -> tuple[NonPassReason, ...]:
    return tuple(
        NonPassReason(check=row.check, status=row.status, reason=row.notes)
        for row in rows
        if row.status in STATUSES_REQUIRING_REASON
    )


def _section(body: str, heading: str) -> str:
    selected: list[str] = []
    in_section = False
    for line in body.splitlines():
        stripped = line.strip().lower()
        if stripped == heading:
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if in_section:
            selected.append(line)
    return "\n".join(selected)


def _table_cells(line: str) -> tuple[str, ...]:
    if "|" not in line:
        return ()
    stripped = line.strip()
    if set(stripped) <= {"|", "-", ":", " "}:
        return ()
    return tuple(cell.strip() for cell in stripped.strip("|").split("|"))


def _cell(cells: tuple[str, ...], index: int) -> str:
    try:
        return cells[index].strip()
    except IndexError:
        return ""


def _strip_inline_code(value: str) -> str:
    return value.strip().strip("`").strip()


def _normalize_status(value: str) -> str:
    normalized = value.strip().strip("`").upper()
    if normalized == "NA":
        return "N/A"
    return normalized


def _requires_reason(row: ValidationEvidenceRow) -> bool:
    return row.status in STATUSES_REQUIRING_REASON


def _has_reason(value: str) -> bool:
    normalized = re.sub(r"\s+", " ", value.strip())
    if len(normalized) < 10:
        return False
    return normalized.lower() not in {"not applicable", "no reason", "todo", "tbd"}
