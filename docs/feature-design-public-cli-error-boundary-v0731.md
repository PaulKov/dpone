# Feature design: public CLI error and redaction boundary

- Status: APPROVED
- Owner: dpone maintainers
- Issue: global Airflow self-service review for v0.73.1
- Target release: next patch after 0.73.1
Last verified: 2026-07-18

## Executive summary

The public `run` and self-service `check` paths do not currently have one
end-to-end safe error boundary. Final run rendering redacts common secrets, but
the ETL processor logs the raw exception and complete load config first.
`check` has no generic exception adapter and can let an unexpected exception
escape the CLI. Structured self-service payloads also redact secret keys but
not absolute paths recursively.

This bug fix makes every public output channel fail closed:

- runtime logs receive a safe classification and redacted context;
- returned payloads are recursively redacted for secrets and physical paths;
- unexpected `check` failures become `dpone.error.v1` with exit code `5`;
- known stable error codes and existing non-sensitive diagnostics are
  preserved;
- tracebacks and exception objects remain internal and are never serialized.

## Scope

### In scope

- Add one dependency-free recursive public-value redactor for secrets and
  POSIX/Windows absolute paths.
- Apply it at Airflow self-service output serialization.
- Ensure run exception and failed-result output use the same boundary.
- Prevent ETL processor error/end logging from receiving raw exception text or
  full `LoadConfig`.
- Add a generic structured boundary around `dpone check`.
- Preserve exit codes `1`, `3`, and `4` for returned known results.
- Use exit code `5` and `DPONE_INTERNAL_CHECK_FAILED` for unexpected check
  exceptions.
- Add end-to-end CLI tests for stdout, stderr and logging capture.

### Non-goals

- Hiding safe logical identifiers such as pipeline id, connection ref or error
  code.
- Persisting raw tracebacks in public evidence.
- Adding a remote diagnostic service.
- Changing connector exception types.
- Claiming that all internal operator logs are confidential storage.

## Public contract

### Recursive redaction

`redact_public_value(value)` recursively:

1. replaces values under sensitive keys with `[REDACTED]`;
2. applies text secret/URI/private-key redaction to every string;
3. replaces POSIX and Windows absolute paths with `$ABSOLUTE_PATH`;
4. preserves mappings, lists, tuples, numbers, booleans and null shape;
5. is idempotent and has no I/O.

Logical relative paths and non-secret IDs remain visible.

### Check errors

An unexpected exception becomes:

```yaml
passed: false
exit_code: 5
errors:
  - schema: dpone.error.v1
    code: DPONE_INTERNAL_CHECK_FAILED
    stage: check
    severity: error
    message: Check failed unexpectedly. Use the trace id with platform support.
    trace_id: <safe-id>
```

Rules:

- no exception class, traceback, secret value or physical path appears in
  stdout/stderr;
- text and JSON describe the same stable code and trace id;
- service construction or execution failure creates no cache/deployment files;
- returned `SelfServiceResult` codes remain authoritative and are not remapped;
- Markdown remains unsupported by the current parser and fails with argparse
  exit `2` before service construction.

### Run errors and runtime logs

- The original exception is re-raised internally so retry and caller behavior
  remain compatible.
- `ETLProcessor` stores and logs only a redacted public message.
- The complete string representation of `LoadConfig` is never passed to an
  error logger.
- `run` emits one final public failure document in the selected format.
- Known exception `.code` values are preserved.
- Unknown exceptions use the existing generic class/message form after
  redaction; introducing a new generic run code is deferred to a separate
  compatibility decision.

## Detailed algorithm

### Runtime

1. Catch the primary runtime exception.
2. Preserve the exception object for internal control flow and re-raise.
3. Derive `safe_error = redact_absolute_paths(redact_text(str(exc)))`.
4. Update run state/evidence only through existing safe adapters.
5. Append `safe_error` to the in-memory result used by end logging.
6. Log `safe_error` with a bounded logical process/run identifier, never
   `str(load_config)`.
7. Let `finally` log only the already-redacted result.

### Self-service output

1. Convert `SelfServiceResult` to a plain mapping.
2. Run the complete mapping through `redact_public_value`.
3. Render JSON or command-specific text from that same redacted mapping.
4. Never render from the original object after step 2.

### Unexpected check failure

1. Execute the selected check service inside the command boundary.
2. On an unexpected exception, generate one local trace id.
3. Send only safe exception type/classification and trace id to the logger.
4. Build one failed `SelfServiceResult` with exit code `5`.
5. Pass it through the shared output adapter.
6. Return `5`.

## Compatibility

- Successful and expected-failure payload shapes remain compatible.
- Secret or absolute-path strings previously visible become redaction tokens.
- Unexpected `check` failures change from an unhandled traceback/process error
  to the documented structured internal-error contract.
- Runtime retries and original exception propagation remain unchanged.

## Security and failure semantics

- Redaction is a final public boundary, not a replacement for avoiding secret
  acquisition.
- Raw exceptions are not written to release/deployment/evidence artifacts.
- Private diagnostic logging must not serialize traceback frames on default CLI
  stderr handlers.
- A redaction failure is fail-closed: emit only a generic internal error.
- Repeated rendering produces the same payload except for a newly generated
  trace id at the initial exception boundary.

## Test plan

- Text, Markdown and JSON run exception output contains no traceback, secret,
  private-key marker, URI credential, query token or absolute path.
- Failed returned run reports receive recursive redaction.
- Runtime logger spies never receive raw exception text or `str(load_config)`.
- `check` expected exit codes `1`, `3`, `4` remain unchanged.
- Unexpected `check` returns exit `5`, one structured error and matching trace
  id in text/JSON.
- `check --format md` exits `2` before service construction.
- Recursive redaction is idempotent for nested mappings/lists/tuples.
- No output-boundary test performs network, secret, cache or database I/O.

## Documentation impact

- Add `DPONE_INTERNAL_CHECK_FAILED` to the static error catalog.
- Document that trace id is a support correlation identifier, not a traceback
  handle exposed to end users.
- Keep troubleshooting examples free of physical local paths.

## Market comparison

N/A. This patch repairs an internal security and CLI correctness contract and
does not introduce a competitive feature claim.

## Approval

This bug specification is APPROVED. Persisting raw exception details,
introducing a remote diagnostic sink, or changing stable expected-failure exit
codes requires a separate design review.
