"""Immutable contracts shared by hermetic test parsers, services, and adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint

HERMETIC_TEST_KIND = "dpone.test.v1"
MAX_SCHEMA_ASSERTIONS = 1_000
MAX_EXPECTATION_ISSUES = MAX_SCHEMA_ASSERTIONS + 3
SUPPORTED_HERMETIC_STRATEGIES = frozenset({"full_refresh", "incremental_append", "incremental_merge"})
SCHEMA_TYPES = frozenset({"null", "bool", "int64", "float64", "string", "array", "object", "mixed"})
_HERMETIC_STRATEGY_FIELDS = frozenset({"mode", "unique_key", "duplicate_policy", "only_new_rows"})
_HERMETIC_SINK_OPTION_FIELDS = frozenset({"batch_size", "log_sample_rows", "unique_key"})
_UNMODELED_PROCESS_FIELDS = frozenset(
    {"hooks", "physical_design", "quality", "quarantine", "schema_contract", "schema_evolution"}
)
_UNMODELED_EXECUTION_OPTIONS = frozenset(
    {"custom_predicate", "dedup_expression", "dedup_target", "micro_batch_commit", "with_dedup"}
)
_COVERAGE = {
    "level": "hermetic_contract",
    "includes": [
        "canonical_authoring",
        "fixture_validation",
        "connector_neutral_strategy_semantics",
    ],
    "excludes": ["connectors", "credentials", "vendor_sql", "transport", "physical_ddl"],
}


class HermeticTestError(ValueError):
    """A stable, row-safe failure at the hermetic test boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        exit_code: int = 2,
        stage: str = "test",
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.stage = stage
        self.path = path


@dataclass(frozen=True, slots=True)
class HermeticTestLimits:
    """Per-test limits that may only tighten the implementation hard caps."""

    max_bytes: int = 10 * 1024 * 1024
    max_rows: int = 10_000
    max_line_bytes: int = 1024 * 1024
    timeout_seconds: int = 30


@dataclass(frozen=True, slots=True)
class HermeticInputContract:
    """Input and optional initial-target fixture references."""

    fixture: str
    initial_target_fixture: str | None = None
    format: str = "jsonl"


@dataclass(frozen=True, slots=True)
class HermeticExpectationContract:
    """Safe assertions over final temporary-target metadata and content."""

    rows: int
    rejected_rows: int = 0
    schema: tuple[tuple[str, str], ...] = ()
    output_fixture: str | None = None
    match: str | None = None

    def schema_mapping(self) -> dict[str, str]:
        return dict(self.schema)


@dataclass(frozen=True, slots=True)
class HermeticTestContract:
    """Normalized ``dpone.test.v1`` authoring contract."""

    name: str
    pipeline: str
    process: str | None
    input: HermeticInputContract
    expect: HermeticExpectationContract
    limits: HermeticTestLimits
    normalized_payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HermeticFixture:
    """Rows and identity derived from the same bounded bytes."""

    path: str
    sha256: str
    byte_count: int
    rows: tuple[dict[str, Any], ...]

    def metadata(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "bytes": self.byte_count,
            "rows": len(self.rows),
        }


@dataclass(frozen=True, slots=True)
class HermeticExecutionPlan:
    """Connector-neutral load semantics negotiated before fixture execution."""

    mode: str
    unique_key: tuple[str, ...] = ()
    duplicate_policy: str = "fail"


@dataclass(frozen=True, slots=True)
class HermeticExecutionResult:
    """Immutable final temporary-target snapshot."""

    rows: tuple[dict[str, Any], ...]


def hermetic_execution_plan(process: Mapping[str, Any]) -> HermeticExecutionPlan:
    """Return a pure execution plan only for semantics modeled by hermetic v1."""

    if any(_configured(process.get(field)) for field in _UNMODELED_PROCESS_FIELDS):
        raise _unsupported_execution()
    reconciliation = process.get("reconciliation")
    if reconciliation is not None and reconciliation is not False:
        raise _unsupported_execution()
    transforms = process.get("transforms")
    if transforms is not None and transforms != () and transforms != []:
        raise _unsupported_execution()
    source = process.get("source")
    if not isinstance(source, Mapping):
        raise _unsupported_execution()
    raw_source_options = source.get("options")
    source_options = raw_source_options if isinstance(raw_source_options, Mapping) else {}
    if any(field in source or field in source_options for field in ("query", "sql_file")) or (
        set(source_options) & _UNMODELED_EXECUTION_OPTIONS
    ):
        raise _unsupported_execution()
    sink = process.get("sink")
    if not isinstance(sink, Mapping):
        raise _unsupported_execution()
    raw_sink_options = sink.get("options")
    sink_options = raw_sink_options if isinstance(raw_sink_options, Mapping) else {}
    if (
        not set(sink_options) <= _HERMETIC_SINK_OPTION_FIELDS
        or _nested_normalization_enabled(process)
        or _nested_normalization_enabled(sink_options)
    ):
        raise _unsupported_execution()
    strategy = sink.get("strategy")
    if not isinstance(strategy, Mapping):
        raise _unsupported_execution()
    mode = str(strategy.get("mode") or "").strip()
    if (
        mode not in SUPPORTED_HERMETIC_STRATEGIES
        or not set(strategy) <= _HERMETIC_STRATEGY_FIELDS
        or strategy.get("duplicate_policy", "fail") != "fail"
    ):
        raise _unsupported_execution()
    raw_unique_key = source_options.get("unique_key") or strategy.get("unique_key") or sink_options.get("unique_key")
    unique_key = _hermetic_unique_key(raw_unique_key) if mode == "incremental_merge" else ()
    if mode == "incremental_merge" and not unique_key:
        raise _unsupported_execution()
    if mode == "incremental_append" and bool(strategy.get("only_new_rows", False)):
        raise _unsupported_execution()
    return HermeticExecutionPlan(mode=mode, unique_key=unique_key, duplicate_policy="fail")


def hermetic_execution_supported(process: Mapping[str, Any]) -> bool:
    """Return whether one process can produce an executable hermetic v1 test."""

    try:
        hermetic_execution_plan(process)
    except HermeticTestError:
        return False
    return True


def _nested_normalization_enabled(container: Mapping[str, Any]) -> bool:
    normalization = container.get("normalization")
    if not isinstance(normalization, Mapping):
        return False
    nested = normalization.get("nested")
    if isinstance(nested, Mapping):
        return nested.get("enabled") is True
    return nested is True


def _configured(value: object) -> bool:
    return value not in (None, False, (), [], {})


def _hermetic_unique_key(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str) and raw.strip():
        columns = tuple(part.strip() for part in raw.split(",") if part.strip())
        if columns:
            return columns
    if isinstance(raw, list) and raw and all(isinstance(item, str) and item.strip() for item in raw):
        return tuple(item.strip() for item in raw)
    raise _unsupported_execution()


def _unsupported_execution() -> HermeticTestError:
    return HermeticTestError(
        "DPONE_TEST_EXECUTION_UNSUPPORTED",
        "Selected process uses behavior outside hermetic test v1 coverage.",
        stage="test_plan",
    )


def hermetic_error(
    code: str,
    message: str,
    *,
    stage: str,
    path: str | None = None,
) -> dict[str, Any]:
    """Build a minimal ``dpone.error.v1`` without unsafe exception details."""

    payload: dict[str, Any] = {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": stage,
        "severity": "error",
        "message": message,
        "fixes": [],
        "docs_url": _hermetic_error_docs_url(code),
    }
    if path:
        payload["path"] = path
    return payload


def _hermetic_error_docs_url(code: str) -> str:
    if code.startswith("DPONE_TEST_"):
        return f"docs/errors/DPONE_TEST.md#{code.lower()}"
    return f"docs/errors/{code}.md"


@dataclass(frozen=True, slots=True)
class HermeticTestReport:
    """One metadata-only hermetic test result."""

    test_id: str
    name: str
    status: str
    execution_status: str
    data_outcome: str
    pipeline: dict[str, Any]
    process: dict[str, Any]
    fixtures: dict[str, Any]
    temporary_target: dict[str, Any]
    expectations: tuple[dict[str, Any], ...] = ()
    errors: tuple[dict[str, Any], ...] = ()
    duration_ms: int = 0
    exit_code: int = 0

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": "dpone.test-report.v1",
            "test_id": self.test_id,
            "name": self.name,
            "status": self.status,
            "execution_status": self.execution_status,
            "data_outcome": self.data_outcome,
            "coverage": {key: list(value) if isinstance(value, list) else value for key, value in _COVERAGE.items()},
            "pipeline": dict(self.pipeline),
            "process": dict(self.process),
            "fixtures": dict(self.fixtures),
            "temporary_target": dict(self.temporary_target),
            "expectations": [dict(item) for item in self.expectations],
            "errors": [dict(item) for item in self.errors],
            "duration_ms": self.duration_ms,
        }


@dataclass(frozen=True, slots=True)
class HermeticTestSuiteReport:
    """Ordered aggregate used by local output and CI."""

    tests: tuple[HermeticTestReport, ...]
    duration_ms: int
    exit_code: int
    input_paths: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.exit_code == 0

    @property
    def status(self) -> str:
        if self.passed:
            return "passed"
        return "blocked" if any(test.status == "blocked" for test in self.tests) else "failed"

    @property
    def counts(self) -> dict[str, int]:
        return {
            "total": len(self.tests),
            "passed": sum(test.status == "passed" for test in self.tests),
            "failed": sum(test.status == "failed" for test in self.tests),
            "blocked": sum(test.status == "blocked" for test in self.tests),
        }

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": "dpone.test-suite-report.v1",
            "passed": self.passed,
            "status": self.status,
            "counts": self.counts,
            "tests": [test.to_jsonable() for test in self.tests],
            "duration_ms": self.duration_ms,
        }


__all__ = [
    "HERMETIC_TEST_KIND",
    "MAX_EXPECTATION_ISSUES",
    "MAX_SCHEMA_ASSERTIONS",
    "SCHEMA_TYPES",
    "SUPPORTED_HERMETIC_STRATEGIES",
    "HermeticExecutionPlan",
    "HermeticExecutionResult",
    "HermeticExpectationContract",
    "HermeticFixture",
    "HermeticInputContract",
    "HermeticTestContract",
    "HermeticTestError",
    "HermeticTestLimits",
    "HermeticTestReport",
    "HermeticTestSuiteReport",
    "canonical_fingerprint",
    "hermetic_error",
    "hermetic_execution_plan",
    "hermetic_execution_supported",
]
