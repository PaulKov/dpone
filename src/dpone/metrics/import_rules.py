from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from .import_rule_catalog import default_import_rules
from .import_rule_models import ImportRule, ImportRuleReport, ImportRuleViolation
from .python_imports import build_module_files, iter_import_occurrences


def _matches_any(value: str, prefixes: Sequence[str]) -> bool:
    return any(value == prefix or value.startswith(prefix + ".") for prefix in prefixes)


def evaluate_import_rules(
    package_dir: Path,
    *,
    package_name: str = "dpone",
    rules: Sequence[ImportRule] | None = None,
) -> ImportRuleReport:
    rules = tuple(rules or default_import_rules())
    module_files = build_module_files(package_dir, package_name=package_name)
    violations: list[ImportRuleViolation] = []

    for occ in iter_import_occurrences(package_dir, package_name=package_name, module_files=module_files):
        for rule in rules:
            if not _matches_any(occ.source_module, rule.source_prefixes):
                continue
            if rule.allowed_source_prefixes and _matches_any(occ.source_module, rule.allowed_source_prefixes):
                continue
            if rule.allowed_target_prefixes and _matches_any(occ.target_module, rule.allowed_target_prefixes):
                continue
            if not _matches_any(occ.target_module, rule.forbidden_prefixes):
                continue
            violations.append(
                ImportRuleViolation(
                    rule_id=rule.id,
                    rule_description=rule.description,
                    source_module=occ.source_module,
                    source_path=occ.source_path.as_posix(),
                    imported=occ.target_module,
                    lineno=occ.lineno,
                    statement=occ.statement,
                )
            )

    violations.sort(key=lambda v: v.sort_key())
    return ImportRuleReport(rules=rules, violations=tuple(violations))


def format_import_rule_report_text(report: ImportRuleReport, *, package_dir: Path | None = None) -> str:
    if report.ok:
        return "✅ Import rules OK: no architectural import violations found."

    lines: list[str] = []
    lines.append(f"Import rule violations: {report.violation_count}")
    current_rule = ""
    for violation in report.violations:
        if violation.rule_id != current_rule:
            if lines:
                lines.append("")
            current_rule = violation.rule_id
            lines.append(f"[{violation.rule_id}] {violation.rule_description}")
        path = violation.source_path
        if package_dir:
            try:
                path = Path(path).relative_to(package_dir.parent).as_posix()
            except Exception:
                pass
        lines.append(f"- {path}:{violation.lineno}: {violation.source_module} -> {violation.imported}")
        lines.append(f"    {violation.statement}")
    return "\n".join(lines)


def format_import_rule_report_jsonable(report: ImportRuleReport) -> dict:
    return {
        "ok": report.ok,
        "violation_count": report.violation_count,
        "rules": [
            {
                "id": rule.id,
                "description": rule.description,
                "source_prefixes": list(rule.source_prefixes),
                "forbidden_prefixes": list(rule.forbidden_prefixes),
                "allowed_source_prefixes": list(rule.allowed_source_prefixes),
                "allowed_target_prefixes": list(rule.allowed_target_prefixes),
            }
            for rule in report.rules
        ],
        "violations": [
            {
                "rule_id": violation.rule_id,
                "rule_description": violation.rule_description,
                "source_module": violation.source_module,
                "source_path": violation.source_path,
                "imported": violation.imported,
                "lineno": violation.lineno,
                "statement": violation.statement,
            }
            for violation in report.violations
        ],
    }
