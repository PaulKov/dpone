from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ImportRule:
    id: str
    source_prefixes: tuple[str, ...]
    forbidden_prefixes: tuple[str, ...]
    description: str
    allowed_source_prefixes: tuple[str, ...] = ()
    allowed_target_prefixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ImportRuleViolation:
    rule_id: str
    rule_description: str
    source_module: str
    source_path: str
    imported: str
    lineno: int
    statement: str

    def sort_key(self) -> tuple[str, int, str, str]:
        return (self.source_path, self.lineno, self.rule_id, self.imported)


@dataclass(frozen=True)
class ImportRuleReport:
    rules: tuple[ImportRule, ...]
    violations: tuple[ImportRuleViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    @property
    def violation_count(self) -> int:
        return len(self.violations)
