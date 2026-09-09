# Connector SDK

The connector SDK turns `dpone` community connector development into a repeatable package workflow: generate a connector package, fill thin source/sink/state classes, run certification, and publish evidence with the connector.

## When to use it

Use the SDK when a connector is not part of core `dpone`, but should still follow the same production contracts:

- optional imports without side effects;
- manifest examples that parse;
- staging-first sink behavior;
- schema and quality evidence;
- run artifacts and certification reports;
- clear docs and runbooks for users.

## Scaffold a connector

```bash
dpone connectors scaffold demo_api \
  --root ./community-connectors \
  --connector-type api \
  --capability source \
  --capability sink
```

The generated package is created at:

```text
community-connectors/dpone-connector-demo-api/
```

Key files:

| Path | Purpose |
| --- | --- |
| `pyproject.toml` | Installable connector package metadata. |
| `src/dpone_connector_demo_api/connector.py` | Shared connector config and helpers. |
| `src/dpone_connector_demo_api/source.py` | Source runtime skeleton. |
| `src/dpone_connector_demo_api/sink.py` | Sink runtime skeleton when `--capability sink` is used. |
| `examples/demo_api_to_postgres.yaml` | Copy/paste manifest starter. |
| `docs/demo_api.md` | User-facing connector guide and runbook. |
| `tests/test_demo_api_contract.py` | Import-safety contract test. |
| `certification/certification.yaml` | Required evidence contract for certification. |
| `.github/workflows/certification.yml` | Manual and PR certification workflow starter. |

## Capability flags

```bash
# Source only
dpone connectors scaffold demo_api --capability source

# Source + sink
dpone connectors scaffold demo_api --capability source --capability sink

# State backend connector
dpone connectors scaffold demo_state --connector-type database --capability state
```

Native transfer capabilities are declared separately from source/sink/state
roles. They describe the physical transport contracts a connector can certify:

```bash
dpone connectors scaffold warehouse_db \
  --connector-type database \
  --capability source \
  --capability sink \
  --native-capability stream_export \
  --native-capability stream_staging_load
```

Use source/sink/state for connector shape and `--native-capability` for
transport contracts. For example, a source-only PostgreSQL-like connector can
declare `--native-capability stream_export` without pretending it is also a
sink. Route certification later combines one source export capability, one sink
staging-load capability, and the codec contract.

Supported capabilities:

| Capability | What the scaffold creates |
| --- | --- |
| `source` | Extract class, source certification cases, source manifest example. |
| `sink` | Load class, sink certification cases, staging-first reminders. |
| `state` | State backend skeleton and state commit/rollback certification cases. |

Supported native transfer capabilities:

| Native capability | Meaning |
| --- | --- |
| `stream_export` | Source can produce a bounded byte stream for one transfer slice. |
| `file_export` | Source can materialize one bounded file slice. |
| `object_export` | Source can write one slice to an object-backed transfer store. |
| `stream_staging_load` | Sink can ingest a bounded stream into staging. |
| `file_staging_load` | Sink can ingest a bounded file artifact into staging. |
| `object_staging_load` | Sink can ingest a transfer-store object into staging. |

## Certification workflow

Run the generated package tests first:

```bash
cd community-connectors/dpone-connector-demo-api
uv sync
uv run pytest
```

Then render connector certification evidence:

```bash
dpone connectors certify --artifact-dir test_artifacts/connectors/demo_api
python certification/run_certification.py
```

The certification manifest defines required checks for the connector package. The generated defaults require import safety, manifest validation, docs, quality contracts, run artifacts, and benchmark evidence.

Native transfer capability certification is explicit and fail-closed:

```bash
dpone connectors certify \
  --profile static \
  --capability native_transfer.stream \
  --artifact-dir test_artifacts/connectors/warehouse_db \
  --format json
```

Short form for local static checks:
`dpone connectors certify --profile static --capability native_transfer.stream`.

The command reads `certification/certification.yaml` from the connector package
when it exists and writes:

- `connector-capability-certification.json`
- `connector-capability-certification.md`

If the requested stream capability is missing or incomplete, the report is
`blocked` and lists stable blocker codes such as
`native_transfer_stream_capability_missing`.

Connector capability evidence proves that one connector can export or load a
transport. Route transport certification proves that a concrete
`source -> sink -> codec -> staging` path is safe for a manifest:

```bash
dpone ops route-transport-certification \
  --manifest manifests/postgres_clickhouse_orders.yaml \
  --profile static \
  --artifact-dir .dpone/certification/postgres_clickhouse_orders \
  --format json
```

The command writes `native_transfer_route_certification.json` and
`native_transfer_route_certification.md`. Attach that JSON as
`native_transfer_route_certification` in route live certification and reference
it from manifests through
`source.options.native_transfer.execution.certification.artifact` when a
production route must run in `certified_only` mode.

## Publishing checklist

1. Keep network clients dependency-injected so unit tests can use fakes.
2. Keep optional dependencies in the connector package, not in core `dpone`.
3. Add at least one manifest example per supported strategy.
4. Add runbooks for credentials, rate limits, retries, schema drift, and failed quality gates.
5. Attach `test_artifacts/connectors/<connector>/certification_report.md` to releases.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| Import test fails because an optional dependency is missing | Move the import inside the method that needs it or add the dependency to the connector package. |
| Manifest example does not parse | Validate the YAML with `dpone manifest validate path/to/example.yaml` before publishing. |
| Sink writes directly to final target | Add a staging table/file step and finalizer; connector sinks must be staging-first. |
| Certification artifact is incomplete | Run `dpone connectors certify --artifact-dir ...` and include the generated report in CI artifacts. |
| Stream certification is blocked | Add the matching `--native-capability` to the scaffold or update `certification/certification.yaml`, then rerun `dpone connectors certify --profile static --capability native_transfer.stream`. |
