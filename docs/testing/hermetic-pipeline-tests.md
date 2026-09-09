# Hermetic pipeline tests

`dpone test` checks a pipeline's canonical authoring and load-strategy semantics
without a database, Airflow, Vault, Kubernetes, or connector SDK. Every pipeline
created by `dpone init pipeline` includes a passing starter test and a two-row
JSONL fixture.

External recipe scaffolding first negotiates this capability for every expanded
process. A recipe with any process using SQL, transforms, `strategy: auto`,
`only_new_rows: true`, or an unsupported merge policy fails before writing files with
`DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED`; choose a compatible recipe or cover
those integration semantics in a route test.

Use this layer for fast local feedback and pull-request checks. It is not route
certification: it does not prove vendor SQL, network access, physical DDL,
transport behavior, or production credentials.

## First test

Create a pipeline and run the generated test:

```bash
dpone init project --airflow
dpone init pipeline orders_daily \
  --recipe mssql-to-clickhouse-incremental
dpone check orders_daily
dpone airflow preview orders_daily
dpone test orders_daily
```

Expected summary:

```text
dpone test: PASS
- schema: dpone.test-suite-report.v1
- tests: 1 (1 passed, 0 failed, 0 blocked)
- orders_daily_happy_path: passed
- strategy: incremental_merge
- rows: 2
- coverage: hermetic contract
- journey: offline golden path complete
```

No connection configuration is required. Fix the generated fixture and
expectations as the pipeline evolves, then commit them with the primary
authoring source.

When `--output` is used, dpone atomically creates a new complete report. If the
target already contains the exact same text/Markdown report or a semantically
identical JSON report, the command succeeds as a no-op and does not touch the
file. JSON comparison ignores only suite and per-test `duration_ms`; a pipeline
column with that name remains ordinary report data. Every differing existing
file is rejected, including a prior dpone report. Choose a new output path or
explicitly remove the reviewed generated artifact before
publishing changed output. The SHA-256 footer on text and Markdown reports is
an integrity marker, not proof that dpone owns the path.

The command refuses to overwrite a pipeline, test manifest, fixture, symlink,
non-regular path, or unrelated user file. `.yaml`, `.yml`, `.jsonl`, and `.sql`
are always treated as authoring or input formats and cannot be report targets;
every path under `tests/fixtures/` is protected regardless of suffix. A target
created concurrently with equivalent content is a no-op; a differing target
created or changed after validation is rejected without mutation.
Input reads use descriptor walks on POSIX and verified Win32 handles on Windows;
reparse points and final paths outside the project are rejected.

## Files

The scaffold creates:

```text
pipelines/orders_daily/pipeline.yaml
tests/orders_daily.test.yaml
tests/fixtures/orders_daily.input.jsonl
```

The test manifest is an authoring source, while reports are generated CI
artifacts. A minimal contract is:

```yaml
kind: dpone.test.v1
name: orders_daily_happy_path
pipeline: ../pipelines/orders_daily/pipeline.yaml
input:
  fixture: fixtures/orders_daily.input.jsonl
  format: jsonl
expect:
  rows: 2
  rejected_rows: 0
  schema:
    id: int64
```

`pipeline` and fixture paths are resolved relative to the test file and must
remain inside the project root. Symlinks, absolute paths, backslashes, path
traversal, malformed YAML/JSONL, duplicate JSON keys, non-finite numbers, and
values outside signed `int64` fail closed.

`expect.schema` accepts at most 1,000 columns so every possible mismatch report
remains valid against `dpone.test-report.v1`.

## Supported semantics

Hermetic v1 executes these connector-neutral final-state rules:

| Strategy | Final temporary target |
| --- | --- |
| `full_refresh` | input rows replace the initial target |
| `incremental_append` | input rows append to the initial target; `only_new_rows` is not modeled |
| `incremental_merge` | input rows replace matching unique keys and add new keys |

`incremental_merge` requires a non-empty scalar or list unique key and
`duplicate_policy: fail`. A comma-delimited scalar such as `id, region` is
normalized to the same ordered composite key used by runtime. Legacy placement
uses the same runtime precedence: `source.options`, then `sink.strategy`, then
`sink.options`. Duplicate, null,
missing, array, or object key values are errors. `incremental_append` with
`only_new_rows: true`, SQL queries, transforms, enabled reconciliation,
connector behavior, a `duplicate_policy` other than `fail` on any strategy,
vendor/transaction strategy options such as `merge_policy`, `diff`,
`mutations_sync`, or `micro_batch_commit`, schema evolution, and other
unmodeled sink options, process quality gates, schema contracts, quarantine,
and hooks return `DPONE_TEST_EXECUTION_UNSUPPORTED`; the runner never guesses.
Hermetic v1 accepts only `unique_key`, `batch_size`, and `log_sample_rows` in
`sink.options` because they do not add final-state policy.

The canonical compiler still resolves classic, flow, folder, and recipe
authoring, but hermetic compilation does not read or hash declared SQL content:
SQL cannot execute in v1 and is rejected during capability negotiation.

Use an initial target to test append or merge:

```yaml
input:
  fixture: fixtures/orders_daily.input.jsonl
  initial_target:
    fixture: fixtures/orders_daily.target.jsonl
```

Use a complete expected output when row count and logical schema are not
enough:

```yaml
expect:
  rows: 3
  output_fixture: fixtures/orders_daily.expected.jsonl
  match: exact_unordered
```

`exact_unordered` ignores row order but preserves duplicate counts. Reports
contain only mismatch metadata, never fixture row values.

## Limits

Defaults are intentionally bounded and may only be tightened:

```yaml
limits:
  max_bytes: 10485760
  max_rows: 10000
  max_line_bytes: 1048576
  timeout_seconds: 30
```

The hard timeout ceiling is 300 seconds. Limit and path violations use exit
code `4`, stop unsafe work, and do not write a partial report file. A failed
write, flush, or filesystem sync also removes its named temporary report.

## Targets and reports

Run one test, a pipeline's matching test, or all direct project tests:

```bash
dpone test tests/orders_daily.test.yaml
dpone test pipelines/orders_daily
dpone test .
```

Write an atomic JSON report for CI:

```bash
dpone test . --format json --output test-artifacts/hermetic-tests.json
```

Available formats are `text`, `json`, and `md`. JSON uses
`dpone.test-suite-report.v1` and embeds `dpone.test-report.v1` entries. Test
identity is content-addressed from the normalized test, canonical pipeline
semantics, selected process, and exact fixture digests, so duration and output
path do not change `test_id`.

## Exit codes

| Code | Meaning |
| ---: | --- |
| `0` | every test passed |
| `1` | execution succeeded but an expectation failed |
| `2` | authoring, fixture, process, or supported-semantics validation failed |
| `4` | path, resource, timeout, or other safety policy blocked execution |
| `5` | internal failure; report remains redacted |

For stable error meanings and next actions, see the
[hermetic error catalog](../errors/DPONE_TEST.md).

## CI boundary

Hermetic tests are safe for an unprivileged pull-request job:

```bash
uv run dpone test . --format json \
  --output test-artifacts/hermetic-tests.json
```

The import and execution path must not load `dpone.runtime`, Vault, Airflow, or
Kubernetes modules and performs no network, database, secret, Variable,
Connection, metadata-DB, or SQL dependency reads. Run route certification separately before
making a production-readiness claim.

## Troubleshooting

- `DPONE_TEST_PROCESS_AMBIGUOUS`: set `process` to the exact process name.
- `DPONE_TEST_EXECUTION_UNSUPPORTED`: reduce the test to supported v1
  connector-neutral semantics or use a connector/integration test.
- `DPONE_TEST_EXPECTED_*`: update the pipeline or reviewed expectation; the
  report shows counts/types, not data values.
- `DPONE_TEST_PATH_UNSAFE`: keep regular input files inside the project root;
  do not replace them with symlinks.
- `DPONE_TEST_INPUT_UNAVAILABLE`: restore safe read permissions or resolve the
  transient filesystem failure; dpone does not expose the raw OS error.
- `DPONE_TEST_FIXTURE_LIMIT_EXCEEDED`: shrink the fixture. Hermetic tests are
  behavioral examples, not load tests.
