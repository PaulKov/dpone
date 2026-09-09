# Runbook

This runbook covers troubleshooting and operational diagnostics for `dpone`. For implementation guidance, see [Developer integrations runbook](developer-integrations-runbook.md).

## 1. `dpone --help` fails because of an optional dependency

Check:

- A heavy import such as `google.cloud.*`, `clickhouse_driver`, `pyodbc`, or `confluent_kafka` was not added at module import time.
- CLI renderers do not import runtime-specific modules directly.
- Optional connector code is still behind lazy factories.

Useful commands:

```bash
dpone docs check-import-rules
uv run pytest tests/test_lazy_imports.py -q
```

Fix by moving the heavy import into the execution path and adding an optional-extra diagnostic if the dependency is missing.

## 2. `manifest explain` and `dag explain-*` disagree

Check that dependency semantics still come from `dpone.dag.edge_resolver`.

Fix rules:

- Change dependency semantics only in the edge resolver.
- Reuse the resolver from renderers and explain/report commands.
- Add a regression test that compares manifest, DAG, and report output.

## 3. CI fails on import or layer metrics

Check:

- A new cross-layer dependency was introduced.
- Runtime code leaked into manifest or CLI-only layers.
- A connector dependency was added to the base package instead of an extra.

Useful commands:

```bash
uv run python tools/check_import_rules.py
uv run python tools/check_layer_metrics.py
```

## 4. Generated developer metrics are stale

Regenerate and commit the resulting docs:

```bash
uv run dpone docs update-dev-metrics
uv run dpone docs check-compatibility --write-doc
```

Use `--check` in CI and without `--check` when intentionally updating generated sections.

## 5. `dag report` is too large

Use focused output:

```bash
dpone dag report manifest.yaml --format json --only-problems
dpone dag explain-task manifest.yaml --task-id landing__orders
```

For local analysis, write JSON to an artifact and inspect it with `jq`.

## 6. A task received an upstream dependency that is not in `depends_on`

The most common reason is task-group expansion or a convention-level dependency.

Check:

```bash
dpone manifest explain manifest.yaml --format md
dpone dag explain-task manifest.yaml --task-id <task_id>
```

Look at resolved defaults, registry provenance, and task-group expansion output.

## 7. Native MSSQL bulk load fails

Check:

- `bcp` is installed and available in `PATH` or configured through `bcp_path`.
- ODBC Driver 18 is installed.
- Target schema permissions allow creating staging tables.
- Field and row terminators do not appear in unsafe text values.

Useful commands:

```bash
dpone doctor --profile local
bcp -v
```

If native bulk cannot be used, enable `allow_pyodbc_fallback: true` explicitly and document the expected performance impact.

## 8. ClickHouse load is slow

Prefer direct TSV or HTTP streaming paths for large MSSQL or Postgres exports. Avoid Python row parsing when the source can export a native delimited file.

Check the performance plan:

```bash
dpone perf advise manifest.yaml
```

## 9. Kafka offsets advanced unexpectedly

Check the manifest option `source.options.offset_storage`:

- `dpone` stores offsets in the configured state backend only after sink success.
- `kafka` commits consumer offsets only after sink success.

Inspect state before replaying:

```bash
dpone state inspect --state-kind kafka_offsets --format md
```

## 10. Schema evolution failed

Safe changes are applied automatically by default. Breaking changes fail unless the manifest explicitly enables a compatible policy.

Check:

```bash
dpone plan manifest.yaml --format md
```

For incompatible type changes, choose one of:

- Manual migration.
- `on_type_change: new_column` with the `__dpone__nc__<column>` generated-column convention.

See [Schema evolution](schema-evolution.md).
