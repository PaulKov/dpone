"""Application service for credential-free ``dpone.test.v1`` execution."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.hermetic_test_memory import InMemoryHermeticStrategyExecutor
from dpone.contracts.hermetic_test import (
    HermeticFixture,
    HermeticTestContract,
    HermeticTestError,
    HermeticTestReport,
    HermeticTestSuiteReport,
    canonical_fingerprint,
    hermetic_error,
)
from dpone.manifest.hermetic_test import (
    AuthoringCompiler,
    compile_hermetic_pipeline,
    hermetic_execution_plan,
    load_jsonl_fixture,
    parse_hermetic_test_contract,
)
from dpone.services.hermetic_test_assertions import evaluate_expectations, infer_json_schema
from dpone.services.hermetic_test_project import (
    HermeticProjectError,
    HermeticTestProject,
    default_test_name,
    display_target,
)

if TYPE_CHECKING:
    from dpone.ports.hermetic_test import HermeticStrategyExecutor

_SEVERITY = {0: 0, 1: 1, 2: 2, 4: 3, 5: 4}


class HermeticTestService:
    """Discover, compile, execute, and aggregate hermetic tests."""

    def __init__(
        self,
        *,
        root: Path,
        compiler: AuthoringCompiler | None = None,
        executor: HermeticStrategyExecutor | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._compiler = compiler
        self._executor = executor or InMemoryHermeticStrategyExecutor()
        self._clock = clock or time.monotonic

    def run(self, target: str | Path) -> HermeticTestSuiteReport:
        started = self._clock()
        project = HermeticTestProject(self._root)
        try:
            paths = project.discover(target)
        except (HermeticTestError, HermeticProjectError) as exc:
            result = self._blocked_report(display_target(target), started=started, error=exc)
            return HermeticTestSuiteReport(
                (result,), _elapsed_ms(self._clock, started), result.exit_code, project.input_paths
            )
        self._protect_suite_inputs(paths, project=project)
        results: list[HermeticTestReport] = []
        for path in paths:
            result = self._run_one(path, project=project)
            results.append(result)
            if result.exit_code in {4, 5}:
                break
        exit_code = max((result.exit_code for result in results), key=lambda item: _SEVERITY[item], default=2)
        return HermeticTestSuiteReport(
            tuple(results), _elapsed_ms(self._clock, started), exit_code, project.input_paths
        )

    @staticmethod
    def _protect_suite_inputs(paths: tuple[str, ...], *, project: HermeticTestProject) -> None:
        """Track every declared input before fail-fast execution can stop the suite."""

        for test_path in paths:
            try:
                raw_test = project.read_yaml(test_path)
            except (HermeticTestError, HermeticProjectError):
                continue
            for raw_reference in _declared_fixture_references(raw_test):
                _protect_reference(test_path, raw_reference, project=project)
            raw_pipeline = raw_test.get("pipeline")
            if not isinstance(raw_pipeline, str):
                continue
            pipeline_path = _protect_reference(test_path, raw_pipeline, project=project)
            if pipeline_path is None:
                continue
            try:
                pipeline = project.read_yaml(pipeline_path)
                project.track_declared_authoring_inputs(pipeline_path, pipeline)
            except (HermeticTestError, HermeticProjectError):
                continue

    def _run_one(self, test_path: str, *, project: HermeticTestProject) -> HermeticTestReport:
        started = self._clock()
        name = default_test_name(test_path)
        pipeline_path = ""
        process_summary: dict[str, Any] = {}
        fallback_id = canonical_fingerprint({"contract": "dpone.test.v1", "test_path": test_path})
        try:
            raw_test = project.read_yaml(test_path)
            contract = parse_hermetic_test_contract(raw_test, default_name=name)
            name = contract.name
            fixture_paths = self._fixture_paths(test_path, contract, project=project)
            project.track_inputs(fixture_paths.values())
            pipeline_path = project.resolve_reference(test_path, contract.pipeline)
            pipeline = project.read_yaml(pipeline_path)
            project.track_declared_authoring_inputs(pipeline_path, pipeline)
            compilation = compile_hermetic_pipeline(
                pipeline,
                source_path=self._root / pipeline_path,
                project_root=self._root,
                compiler=self._compiler,
            )
            project.track_inputs(dependency.path for dependency in compilation.dependencies)
            process = _select_process(compilation.processes, contract.process)
            plan = hermetic_execution_plan(process)
            process_summary = {"name": str(process["name"]), "strategy": plan.mode}
            fixtures = self._load_fixtures(fixture_paths, contract, project=project)
            test_id = canonical_fingerprint(
                {
                    "contract_version": "dpone.test.v1",
                    "normalized_test_manifest": contract.normalized_payload,
                    "pipeline_semantic_fingerprint": compilation.semantic_fingerprint,
                    "canonical_selected_process": process,
                    "input_fixture_sha256": fixtures["input"].sha256,
                    "initial_target_sha256_or_null": _fixture_digest(fixtures.get("initial_target")),
                    "expected_output_sha256_or_null": _fixture_digest(fixtures.get("expected_output")),
                }
            )
            deadline = started + contract.limits.timeout_seconds
            execution = self._executor.execute(
                plan,
                fixtures["input"].rows,
                _fixture_rows(fixtures.get("initial_target")),
                cancelled=lambda: self._clock() > deadline,
            )
            expected = fixtures.get("expected_output")
            failures = evaluate_expectations(
                execution.rows,
                expected_rows=contract.expect.rows,
                expected_rejected_rows=contract.expect.rejected_rows,
                expected_schema=contract.expect.schema,
                expected_output=expected.rows if expected is not None else None,
            )
            schema = infer_json_schema(execution.rows)
            passed = not failures
            return HermeticTestReport(
                test_id=test_id,
                name=name,
                status="passed" if passed else "failed",
                execution_status="succeeded",
                data_outcome="passed" if passed else "failed_quality_gate",
                pipeline={"path": pipeline_path, "semantic_fingerprint": compilation.semantic_fingerprint},
                process=process_summary,
                fixtures={key: fixture.metadata() for key, fixture in fixtures.items()},
                temporary_target={
                    "uri": "tmp://dpone-tests/" + test_id.replace(":", "-"),
                    "rows": len(execution.rows),
                    "rejected_rows": 0,
                    "schema": schema,
                },
                expectations=failures,
                duration_ms=_elapsed_ms(self._clock, started),
                exit_code=0 if passed else 1,
            )
        except (HermeticTestError, HermeticProjectError) as exc:
            return self._blocked_report(
                name,
                started=started,
                error=exc,
                test_id=fallback_id,
                pipeline_path=pipeline_path,
                process=process_summary,
            )
        except Exception:  # noqa: BLE001 - public report must not expose internal exception text.
            internal = HermeticTestError(
                "DPONE_TEST_INTERNAL_ERROR",
                "Hermetic test failed because of an internal error.",
                exit_code=5,
                stage="test_internal",
            )
            return self._blocked_report(name, started=started, error=internal, test_id=fallback_id)

    def _load_fixtures(
        self,
        paths: Mapping[str, str],
        contract: HermeticTestContract,
        *,
        project: HermeticTestProject,
    ) -> dict[str, HermeticFixture]:
        fixtures: dict[str, HermeticFixture] = {}
        for role, path in paths.items():
            content = project.read_fixture(path, max_bytes=contract.limits.max_bytes)
            fixtures[role] = load_jsonl_fixture(content, path=path, limits=contract.limits)
        return fixtures

    @staticmethod
    def _fixture_paths(
        test_path: str,
        contract: HermeticTestContract,
        *,
        project: HermeticTestProject,
    ) -> dict[str, str]:
        refs = {"input": contract.input.fixture}
        if contract.input.initial_target_fixture is not None:
            refs["initial_target"] = contract.input.initial_target_fixture
        if contract.expect.output_fixture is not None:
            refs["expected_output"] = contract.expect.output_fixture
        return {role: project.resolve_reference(test_path, ref) for role, ref in refs.items()}

    def _blocked_report(
        self,
        name: str,
        *,
        started: float,
        error: HermeticTestError | HermeticProjectError,
        test_id: str | None = None,
        pipeline_path: str = "",
        process: dict[str, Any] | None = None,
    ) -> HermeticTestReport:
        return HermeticTestReport(
            test_id=test_id or canonical_fingerprint({"contract": "dpone.test.v1", "name": name}),
            name=name,
            status="blocked",
            execution_status="skipped",
            data_outcome="unknown",
            pipeline={"path": pipeline_path, "semantic_fingerprint": None},
            process=process or {},
            fixtures={},
            temporary_target={"uri": None, "rows": 0, "rejected_rows": 0, "schema": {}},
            errors=(hermetic_error(error.code, str(error), stage=error.stage, path=error.path),),
            duration_ms=_elapsed_ms(self._clock, started),
            exit_code=error.exit_code,
        )


def _select_process(processes: tuple[Mapping[str, Any], ...], requested: str | None) -> Mapping[str, Any]:
    if requested is None:
        if len(processes) != 1:
            raise HermeticTestError(
                "DPONE_TEST_PROCESS_AMBIGUOUS",
                "Pipeline has multiple processes; set the exact process name in the test manifest.",
                stage="test_plan",
            )
        return processes[0]
    matches = tuple(process for process in processes if str(process.get("name") or "") == requested)
    if len(matches) != 1:
        raise HermeticTestError(
            "DPONE_TEST_PROCESS_NOT_FOUND",
            "The requested process was not found exactly once in the compiled pipeline.",
            stage="test_plan",
        )
    return matches[0]


def _fixture_digest(value: HermeticFixture | None) -> str | None:
    return value.sha256 if value is not None else None


def _fixture_rows(value: HermeticFixture | None) -> tuple[dict[str, Any], ...]:
    return value.rows if value is not None else ()


def _elapsed_ms(clock: Callable[[], float], started: float) -> int:
    return max(0, int((clock() - started) * 1000))


def _declared_fixture_references(payload: Mapping[str, Any]) -> tuple[str, ...]:
    references: list[str] = []
    input_block = payload.get("input")
    if isinstance(input_block, Mapping):
        fixture = input_block.get("fixture")
        if isinstance(fixture, str):
            references.append(fixture)
        initial_target = input_block.get("initial_target")
        if isinstance(initial_target, Mapping):
            initial_fixture = initial_target.get("fixture")
            if isinstance(initial_fixture, str):
                references.append(initial_fixture)
    expect = payload.get("expect")
    if isinstance(expect, Mapping):
        output_fixture = expect.get("output_fixture")
        if isinstance(output_fixture, str):
            references.append(output_fixture)
    return tuple(references)


def _protect_reference(
    test_path: str,
    raw_reference: str,
    *,
    project: HermeticTestProject,
) -> str | None:
    try:
        relative_path = project.resolve_reference(test_path, raw_reference)
        project.track_inputs((relative_path,))
    except HermeticProjectError:
        return None
    return relative_path


__all__ = ["HermeticTestService"]
