# Route Release Candidate Executor

`dpone ops route-rc-execute` dry-runs or executes a previously generated
`route_rc_orchestration.json` receipt. It is the opt-in execution bridge after
[Route release candidate orchestrator](route-rc-orchestrator.md): the
orchestrator builds the reproducible command train, and the executor either
plans that train or runs it against the local Docker-live environment.

The command is route-generic. The first industrial routes are
`mssql -> clickhouse` and `postgres -> mssql`, but the executor does not encode
either route. It consumes the stable `source -> sink -> strategy` receipt and
therefore works for future matrix-backed routes without new command logic.

## Dry-run

Dry-run is the default and never calls the process runner.

```bash
uv run dpone ops route-rc-execute \
  --orchestration-json test_artifacts/live_certification/route-rc/mssql-to-clickhouse/route_rc_orchestration.json \
  --output-dir test_artifacts/live_certification/route-rc-execution/mssql-to-clickhouse \
  --format json
```

Artifacts:

```text
route_rc_execution.json
route_rc_execution.md
```

The dry-run receipt records the route, release, planned commands, expected
artifacts, timeout settings, retry settings, and redacted command text. Use it
before enabling `--execute` in CI or on a developer machine.

## Execute

Execution is opt-in.

```bash
uv run dpone ops route-rc-execute \
  --orchestration-json test_artifacts/live_certification/route-rc/mssql-to-clickhouse/route_rc_orchestration.json \
  --output-dir test_artifacts/live_certification/route-rc-execution/mssql-to-clickhouse \
  --execute \
  --timeout-seconds 1800 \
  --max-attempts 1 \
  --redact "$MSSQL_PASSWORD" \
  --format json
```

When `--execute` is set, the executor runs each required command from the
orchestration receipt in order. It captures stdout/stderr tails, applies
redaction, performs artifact collection, computes SHA-256 checksums, and writes
`route_rc_execution.json`.

## Safety

- Default mode is dry-run.
- `--execute` is required before any command is run.
- Required commands fail closed on non-zero exit code.
- Required commands fail closed on timeout.
- Required expected artifacts fail closed when missing after execution.
- Command text, stdout, and stderr tails are redacted before being written.
- The executor does not open database connections directly.

## Operator runbook

Use this operator runbook when `route_rc_execution.json` is blocked.

Failure: `*.command_failed`.

1. Open `route_rc_execution.json`.
2. Read the failing step stdout/stderr tails.
3. Fix the upstream route command or Docker-live service issue.
4. Re-run `dpone ops route-rc-execute --execute`.

Failure: `*.timed_out`.

1. Check Docker service health and runner capacity.
2. Increase `--timeout-seconds` only after confirming the command is making
   progress.
3. Re-run with the same `route_rc_orchestration.json` receipt.

Failure: `*.artifact_missing`.

1. Open the expected artifact path in `route_rc_execution.json`.
2. Re-run the missing upstream command manually or with `--execute`.
3. Do not attach the route release candidate to review until the artifact exists
   and the checksum is non-zero.

## GitHub Actions

`.github/workflows/live-certification.yml` always writes dry-run
`route_rc_execution` receipts for generated route release candidates. Set the
manual workflow input `execute_route_rc_commands=true` to execute the command
train against the disposable Docker-live services before they are stopped.

The ordinary OSS CI path remains credential-free. Vendor-live credentials are
still controlled by the existing vendor-live workflow inputs and secrets.

## Related docs

- [Route release candidate orchestrator](route-rc-orchestrator.md)
- [Route live certification](route-live-certification.md)
- [Route release gate](route-release-gate.md)
- [Developer route RC executor](developer-route-rc-executor.md)
