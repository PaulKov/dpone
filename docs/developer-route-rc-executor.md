# Developer Route RC Executor

The route RC executor is the execution layer for
`route_rc_orchestration.json`. It keeps route release-candidate execution
separate from runtime connectors and from the orchestration service that
generates the command train.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.route_rc_executor` | `RouteReleaseCandidateExecutorService` facade. Loads orchestration JSON, normalizes steps, executes only when requested, collects artifacts, evaluates policy, and writes reports. |
| `dpone.ops.routes.rc_executor_models` | Stable public models for `route_rc_execution.json` and Markdown rendering. |
| `dpone.ops.routes.rc_executor_runner` | `CommandProcessRunner` protocol, `CommandProcessResult`, and the subprocess-backed default runner. |
| `dpone.ops.routes.rc_executor_redaction` | Shared redaction helper for command text and captured output. |
| `dpone.ops.routes.rc_executor_policy` | Pure fail-closed policy for command results and expected artifacts. |

## Design rules

- Default mode is dry-run; command execution requires `execute=True`.
- The service has no route-specific branches.
- The service does not open database connections.
- The service does not import runtime source or sink connectors.
- Docker, pytest, and vendor-live work happens only through commands already
  recorded in `route_rc_orchestration.json`.
- Command execution is dependency-injected through `CommandProcessRunner`.
- Redaction is applied before any command text, stdout, or stderr is written to
  `route_rc_execution.json`.

## Extension rules

Future source -> sink routes should only need:

1. A matrix-backed route profile.
2. Route readiness/certification evidence.
3. A `route_rc_orchestration.json` receipt with ordered commands and expected
   artifacts.

Do not add route-specific code to `RouteReleaseCandidateExecutorService`.
If a future route needs a different execution backend, add a small
`CommandProcessRunner` implementation and inject it from the calling context.

## Testing contract

Required coverage:

- Dry-run never calls the runner.
- Execute mode calls the runner through DI.
- Non-zero exit codes block the report.
- Timeouts block the report.
- Missing required artifacts block the report.
- Redaction removes explicit secrets and common key/value secret patterns.
- CLI tests verify that `dpone ops route-rc-execute` remains a thin adapter.
- Docs tests require user docs, developer docs, architecture docs, CI docs,
  workflow wiring, CLI reference, and source-sink matrix links.

## Public receipt

`route_rc_execution.json` uses schema `dpone.route_rc_executor.v1` and includes:

- release, profile, route, and input orchestration path;
- mode and executed flag;
- step command status, attempts, timeout, exit code, stdout/stderr tails, and
  step artifact checksum;
- artifact collection status and SHA-256 checksum;
- blockers, warnings, score, and next actions.

Keep this schema additive. Downstream release gates and human review runbooks
can depend on existing field names.
