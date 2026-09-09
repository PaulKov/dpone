# Feature design: self-service CLI and diagnostics fail-closed hardening

- Status: APPROVED
- Owner: dpone maintainers
- Issue: global Airflow self-service review for v0.73.1
- Target release: next patch after 0.73.1
Last verified: 2026-07-18

## Executive summary

The Airflow self-service beginner path is functional, but the v0.73.1 global
review found correctness gaps at mode boundaries and in machine-readable
diagnostics:

- `airflow explain` can read two different `current` deployments while one
  response is being assembled;
- safe-sample mode accepts ordinary-run options that it does not execute and
  can create artifacts before reporting an unrelated live-readiness failure;
- an invalid materialized deployment can still produce top-level
  `passed: true` and exit code `0`;
- selected safe-sample output claims an unregistered schema;
- generated beginner commands omit required options;
- recovery output and shell commands are incomplete or unsafe to copy.

This bug fix makes self-service commands deterministic, fail-closed before side
effects, schema-valid and executable as documented. It does not add a new
authoring mode or runtime capability.

## Personas and journey

| Persona | Goal | Current failure | Success signal |
|---|---|---|---|
| New data engineer | Follow five commands without hidden semantics. | Generated reference omits required flags and can enter ordinary execution. | Every rendered command is complete and executable. |
| CI author | Treat JSON status and exit code as authoritative. | Invalid operator diagnostics can return exit `0` with `passed: true`. | Aggregate health, errors and exit code agree. |
| Airflow operator | Diagnose exactly one active deployment. | Promotion can mix diagnostics from deployment A with artifact state from B. | One immutable active-index snapshot drives the whole response. |
| Security reviewer | Prove invalid arguments cannot trigger work. | Ignored run arguments can reach safe-sample planning and filesystem writes. | Incompatible options fail with exit `2` before any side effect. |

Normal journey remains:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow
dpone check pipelines/orders_daily
dpone airflow preview orders_daily
dpone run pipelines/orders_daily --sample 1000 --target temporary
```

`dpone airflow explain orders_daily` remains diagnostic and is not added to the
normal five-command path.

## Scope

### In scope

- Read and validate the active Airflow deployment index once per explain call.
- Reuse that immutable snapshot for operator diagnostics and artifact state.
- Make invalid operator diagnostics affect aggregate explain status, errors and
  exit code.
- Reject every ordinary-run-only option in safe-sample mode before filesystem,
  network, secret, registry or runtime side effects.
- Register and validate `dpone.selected-safe-sample-report.v1`.
- Include evidence and recovery guidance in human safe-sample output.
- Render generated beginner commands with all required options.
- Shell-quote every generated fix command.
- Return CLI usage exit code `2` for bare `dpone init`.
- Synchronize documented output with actual output.

### Non-goals

- Splitting the `run` command into a breaking new top-level command.
- Changing normal-run execution semantics.
- Adding live connector capabilities.
- Decomposing all large CLI and documentation modules in this patch.
- Reading Airflow metadata DB, Variables, Connections, Vault or remote cache
  during `airflow explain`.

## Public contract

### Explain snapshot and aggregate status

One invocation resolves `.dpone-cache/current` once and reads one bounded,
confined `airflow-index.json` snapshot. The parsed immutable snapshot is passed
to both operator diagnostics and artifact-state projection.

The response remains `dpone.airflow-explain.v1` and has one aggregate meaning:

```yaml
passed: false
exit_code: 1
errors:
  - code: DPONE_AIRFLOW_INDEX_INVALID
operator_diagnostics:
  status: invalid
  summary:
    failed: 1
```

Rules:

- authoring validation failure has priority and preserves its existing exit
  code;
- otherwise any failed operator diagnostic sets `passed: false`, adds a
  structured error and returns exit `1`;
- warnings do not fail the command;
- `artifact_state.published_deployment_id` and
  `operator_diagnostics.deployment_id` must be equal whenever both are present;
- missing cache remains a planned state and is not an error;
- index bytes are read through the existing confinement and size contracts;
- no second lookup of `current` is allowed during the call.

### Safe-sample mode boundary

Safe-sample mode is selected when either `--sample` or `--target` is present.
It accepts only:

- pipeline path and global output options;
- `--sample`;
- `--target`;
- selectors explicitly supported by the selected-safe-sample contract;
- safe-sample authorization/policy options already defined by the command.

The following ordinary-run options are rejected when explicitly supplied:

```text
--registry
--dag-id
--execution-date
--data-interval-start
--data-interval-end
--retry-attempts
--retry-backoff-seconds
```

Validation occurs before directory creation, authoring compilation,
release/deployment materialization, registry access, network access, secret
resolution or evidence writes.

Failure contract:

```yaml
schema: dpone.error.v1
code: DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID
stage: cli_validation
severity: error
exit_code: 2
```

The error lists only explicitly supplied incompatible option names. Values are
not echoed because they may contain paths or sensitive identifiers.

### Selected safe-sample report

`dpone.selected-safe-sample-report.v1` becomes a registered public JSON Schema.
Both success and failure output validate against it. The schema defines:

- selection inputs and explanation;
- selected/skipped pipeline results;
- aggregate status and exit code;
- nested safe-sample reports;
- structured errors;
- evidence paths as project-relative public paths.

Unknown fields are rejected unless an existing compatibility envelope
explicitly requires extension fields.

### Human output and fixes

- A planned or completed safe-sample result prints its project-relative
  evidence path when one exists.
- A blocked result prints at least one actionable recovery line or documentation
  link.
- JSON, text and Markdown apply the same redaction policy.
- A fix marked `safe` must be directly copyable. Commands are built as argument
  vectors and rendered with `shlex.join` or equivalent POSIX-safe quoting.
- Paths with spaces, quotes, semicolons and command substitutions remain one
  inert argument.

### Generated beginner reference

The public-contract reference renderer consumes `required_options` rather than
discarding it. Its five commands are semantically equivalent to the canonical
baseline. In particular:

```text
dpone init pipeline <name> --recipe <recipe> --airflow
dpone run <path> --sample <rows> --target temporary
```

Generated-reference tests parse the rendered commands and assert required
options, not only file synchronization.

### CLI exit codes

The existing stable contract remains:

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | validation or check failed |
| 2 | invalid CLI usage/configuration |
| 3 | live dependency unavailable |
| 4 | security or safety violation |
| 5 | internal error |

Bare `dpone init` is invalid CLI usage and returns `2`.

## Detailed algorithms

### Immutable explain snapshot

1. Run authoring inspection without remote I/O.
2. Resolve `current` once using the confined cache-pointer resolver.
3. Read at most the configured index size into immutable bytes.
4. Parse and validate one mapping; retain its source classification.
5. Build operator diagnostics from those exact bytes/mapping.
6. Build artifact state from the same mapping.
7. Compare the two projected identities as an internal invariant.
8. Merge authoring and operator health into one result.
9. Render only project-relative public paths.

If the pointer changes after step 2, the current invocation continues using the
already pinned snapshot. A later invocation observes the new deployment.

### Safe-sample argument validation

1. Parse argv with argparse without application side effects.
2. Determine whether safe-sample mode was selected.
3. Inspect parser-preserved explicit-option metadata; do not infer explicit use
   from a default value.
4. Intersect explicit options with the ordinary-run-only set.
5. When non-empty, return one structured usage error and exit `2`.
6. Do not instantiate safe-sample services or create directories.
7. Otherwise normalize safe-sample arguments and continue existing planning.

The implementation must distinguish an explicitly supplied value from the same
parser default. A sentinel/default-suppression or a pre-dispatch argv option
set is acceptable; comparing normalized values with defaults is not.

### Aggregate explain health

1. Preserve authoring errors and their exit code.
2. Collect failed operator checks from the immutable diagnostics result.
3. Convert each failed check into a redacted `dpone.error.v1`-compatible error.
4. Deduplicate by stable code/entity.
5. If authoring passed and operator failures exist, set exit `1`.
6. Set `passed` only when both authoring and operator diagnostics pass.
7. Keep planned/no-cache diagnostics successful.

## State, failure and side-effect ordering

- Explain is read-only and parse-safe.
- Invalid safe-sample arguments are terminal before any durable or external
  effect.
- No cache/release/deployment/evidence file may be created for argument
  validation failures.
- Existing atomic preview and deployment publication contracts remain
  unchanged.
- No secret-bearing value, absolute path or traceback is added to error output.

## Compatibility and migration

- Correct five-command invocations are unchanged.
- Scripts that combined safe-sample flags with ignored ordinary-run flags now
  fail with exit `2`; remove those incompatible options.
- Invalid deployment diagnostics now cause a non-zero explain result. This is a
  fail-closed correction of an internally contradictory machine contract.
- Existing selected report payloads remain compatible once validated by the
  newly registered schema.
- Legacy `dpone init` with valid legacy arguments remains supported; only the
  argument-less invocation changes from exit `1` to the documented exit `2`.

## Security

- All cache reads remain confined, bounded and symlink-safe.
- Argument validation does not echo option values.
- Fix rendering treats every user-controlled path as one shell argument.
- Human and machine output use the central redaction policy.
- Schema errors do not serialize tracebacks or absolute filesystem paths.
- Diagnostic reads perform zero network, DB, secret, Airflow metadata or cache
  refresh calls.

## Test plan

### Explain

- Atomic pointer switch between diagnostic and artifact projection cannot mix
  deployment IDs.
- Invalid JSON, checksum/contract failure and unsafe pointer produce exit `1`,
  `passed: false` and structured errors.
- Missing cache remains planned with exit `0`.
- Exactly one active-index read occurs.
- No network/DB/secret calls occur.

### Safe sample

- Each incompatible option fails independently with exit `2`.
- All incompatible options together produce one stable error.
- No cache, evidence or temporary target artifacts are created.
- Valid safe-sample options retain behavior.
- Text, Markdown and JSON remain redacted.
- Fix commands preserve spaces and metacharacters as inert path content.

### Schema and docs

- `dpone gitops schema show dpone.selected-safe-sample-report.v1` succeeds.
- Success and failure selected reports validate against the registered schema.
- Generated five-command reference contains every required option.
- Generated references, CLI reference and strict MkDocs build pass.
- Documented sample outputs match executable fixtures.

## Documentation impact

- Correct First DAG expected output.
- Correct `airflow-self-service.md` preview claims.
- Add policy error pages to navigation where applicable.
- Make the generated public-contract reference executable.
- Keep advanced recovery details in linked runbooks rather than expanding the
  beginner tutorial.

## Observability and acceptance

- Zero internally mixed deployment IDs in explain stress tests.
- One hundred percent of explicit incompatible-option cases fail before side
  effects.
- One hundred percent of selected reports validate against their advertised
  schema.
- One hundred percent of generated golden-path commands include their required
  options.
- At least five new users per milestone remain the usability sample.

## Market comparison

N/A. This patch repairs dpone's own documented CLI, schema and parse-safety
contracts. It does not introduce a new competitive capability or product claim.

## Rollout

1. Land focused tests and the immutable snapshot/mode-boundary fixes.
2. Register the selected report schema and strengthen generated references.
3. Update human output and docs.
4. Run focused CLI/Airflow tests, then all non-live, docs and package gates.
5. Mark live Airflow/connector execution `UNVERIFIED` unless an approved
   environment is actually used.

## Approval

This bug specification is APPROVED for implementation. Any request to change
normal-run semantics, add a new command, perform remote work during explain, or
weaken the no-side-effect boundary requires a new design review.
