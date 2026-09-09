# Hermetic test errors

This catalog covers stable `dpone.error.v1` failures emitted by `dpone test`.
Reports and terminal output intentionally omit row values and internal
exception details.

## DPONE_TEST_MANIFEST_INVALID

The test or pipeline YAML is malformed, exceeds its parser budget, contains an
unknown field, or violates `dpone.test.v1`. Validate it against
`src/dpone/schema/test-manifest.schema.json` and rerun `dpone test`.

## DPONE_TEST_INPUT_NOT_FOUND

A test, pipeline, or fixture reference does not exist as a regular project
file. Correct the relative path; do not create an external symlink.

## DPONE_TEST_INPUT_UNAVAILABLE

An existing test, pipeline, or fixture could not be read because of permissions
or a transient filesystem failure. The run stops with safety exit code `4` and
does not expose the operating-system error. Restore safe read access and rerun.

## DPONE_TEST_INPUT_LIMIT_EXCEEDED

A YAML authoring input exceeds the fixed 1 MiB read budget. Split the pipeline
through supported folder authoring rather than increasing the test reader.

## DPONE_TEST_FIXTURE_INVALID

A JSONL record is blank, not an object, invalid UTF-8/JSON, has duplicate keys,
exceeds the 64-level nesting limit, contains a non-finite number, or contains
an integer outside signed `int64`.

## DPONE_TEST_FIXTURE_LIMIT_EXCEEDED

The fixture exceeds its configured bytes, rows, or line-size limit. Keep a
small representative fixture and use integration or performance tests for
volume behavior.

## DPONE_TEST_PATH_UNSAFE

A path is absolute, traverses outside the project, contains backslashes, is a
symlink, or is not a regular file. Move the input under the project and use a
POSIX relative path.

## DPONE_TEST_SUITE_UNAVAILABLE

The project tests directory exists but could not be enumerated under the
confined filesystem policy. Check local permissions and directory health; the
underlying operating-system error and path are intentionally redacted.

## DPONE_TEST_SUITE_NOT_FOUND

The requested test or pipeline has no matching direct `tests/*.test.yaml`
contract. Create the test or pass an existing test, pipeline, or project path.

## DPONE_TEST_SUITE_EMPTY

The project `tests/` directory contains no direct hermetic test manifests.
Add a `dpone.test.v1` file or select a more specific existing target.

## DPONE_TEST_SUITE_LIMIT_EXCEEDED

The selected suite contains more than 100 direct test manifests. Split the CI
selection into bounded project or pipeline targets.

## DPONE_TEST_PIPELINE_INVALID

The primary authoring source could not be found or compiled. Run `dpone check`
on the pipeline and resolve its structured authoring diagnostics first.

## DPONE_TEST_PROCESS_AMBIGUOUS

The pipeline contains more than one process and the test does not select one.
Set `process` to the exact compiled process name.

## DPONE_TEST_PROCESS_NOT_FOUND

The selected process does not exist exactly once. Inspect the primary source
and update the test's `process` value.

## DPONE_TEST_EXECUTION_UNSUPPORTED

The selected process contains SQL, transforms, an unsupported strategy,
schema evolution, quality/schema-contract policy, hooks, vendor execution
options, or merge semantics outside hermetic v1. Use the connector/integration
test layer; the hermetic runner does not approximate unimplemented behavior.

## DPONE_TEST_DUPLICATE_KEY

An incremental-merge input or initial-target fixture repeats a unique key.
Correct the fixture; v1 always uses `duplicate_policy: fail`.

## DPONE_TEST_UNIQUE_KEY_MISSING

An incremental-merge row lacks a required non-null unique-key field. Correct
the fixture or the pipeline's reviewed unique-key declaration.

## DPONE_TEST_UNIQUE_KEY_INVALID

An incremental-merge unique key is an array or object. Use scalar key fields.

## DPONE_TEST_TIMEOUT

Execution exceeded `limits.timeout_seconds`. Reduce the fixture or investigate
an implementation regression. Do not raise the limit above the 300-second hard
ceiling.

## DPONE_TEST_INTERNAL_ERROR

An unexpected implementation failure was safely redacted. Preserve the JSON
report and traceback from a trusted development environment, then report a bug.

## DPONE_TEST_OUTPUT_WRITE_FAILED

The requested CI report could not be completed and atomically published. Check
directory permissions and available storage, then rerun. dpone removes the
temporary file and does not print the output path or operating-system error.

## DPONE_TEST_OUTPUT_UNSAFE

The requested report target is an authoring input, fixture, symlink,
non-regular path, or a differing existing file. Choose a new path or explicitly
remove the reviewed generated artifact first. An identical text/Markdown report
or semantically identical JSON report is accepted as a no-op; no existing file
is replaced. The text/Markdown SHA-256 footer protects report integrity but does
not establish path ownership. dpone never uses `--force` to bypass this
protection. Report targets cannot use authoring/input suffixes `.yaml`, `.yml`,
`.jsonl`, or `.sql`.

## Expectation failures

`DPONE_TEST_EXPECTED_ROWS_MISMATCH`,
`DPONE_TEST_EXPECTED_REJECTED_ROWS_MISMATCH`,
`DPONE_TEST_EXPECTED_SCHEMA_MISMATCH`, and
`DPONE_TEST_EXPECTED_OUTPUT_MISMATCH` use exit code `1`: execution completed,
but the observed metadata/content identity did not match the reviewed
expectation. Fix the pipeline or update the expectation deliberately.
