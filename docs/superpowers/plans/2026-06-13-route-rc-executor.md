# Route RC Executor

## Goal

Add an opt-in executor for `route_rc_orchestration.json` receipts. The command
must be safe by default, dry-run first, and reusable for any future
`source -> sink -> strategy` route because it consumes the stable orchestration
contract rather than route-specific branches.

## Architecture

- `dpone.ops.routes.rc_executor_models`
  - Public JSON/Markdown contract for execution receipts.
  - Step, artifact, command result, policy decision, and report models.
- `dpone.ops.routes.rc_executor_runner`
  - Thin shell process runner protocol and default subprocess implementation.
  - No route knowledge and no dependency on runtime ETL `ProcessRunner`.
- `dpone.ops.routes.rc_executor_redaction`
  - Shared redaction helper for command text and captured output.
- `dpone.ops.routes.rc_executor_policy`
  - Pure fail-closed policy over executed/planned steps and artifacts.
- `dpone.ops.route_rc_executor`
  - Facade service that loads orchestration JSON, redacts commands, runs steps
    only when `execute=True`, collects expected artifacts, and writes receipts.
- CLI `dpone ops route-rc-execute`
  - Argument parsing only: `--orchestration-json`, `--output-dir`,
    `--execute`, timeout/retry/redaction options, and output format.

## Safety Contract

- Default mode is dry-run; it must never call the command runner.
- Execute mode requires `--execute`.
- Every required step must exit with code `0`.
- Every required expected artifact must exist after execution.
- Command text, stdout, and stderr tails are redacted before writing artifacts.
- Timeout and retry controls are explicit and testable.
- The service never opens database connections directly; Docker/vendor work is
  performed by the commands from the orchestration receipt.

## Test Plan

1. RED: service dry-run plans commands without calling the runner.
2. RED: execute mode runs through DI, collects artifacts, and redacts secrets.
3. RED: non-zero exit and missing artifacts block the release candidate.
4. RED: CLI delegates all execution options and remains dry-run by default.
5. RED: docs contract requires user docs, developer docs, architecture links,
   workflow wiring, CLI reference, and source-sink matrix mentions.
6. GREEN: implement models, runner, redaction, policy, service, CLI/catalog
   wiring, docs, and workflow dry-run gate.
7. VERIFY: focused pytest, docs contract tests, ruff, mypy on the new modules,
   mkdocs build with warning-as-error environment.

## Implementation Notes

- Reuse `RouteKey` and route profile metadata from the orchestration JSON.
- Treat orchestration step `path` values and artifact index values as expected
  artifacts.
- Keep command runner as an injectable protocol so tests can simulate Docker
  runs without local services.
- Do not add route-specific behavior for `postgres -> mssql` or
  `mssql -> clickhouse`; the first live Docker path is an operator/workflow
  composition, not a branch in service code.
