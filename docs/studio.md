# dpone Studio local API

Use the Studio API to inspect routes, recipes, pipelines, Airflow explanations,
and static plans without duplicating dpone policy in a UI.

Audience: data engineers evaluating Studio and platform engineers operating the
local development adapter.

Studio v1 is an API, not a production web product. The public
`PaulKov/dpone-studio` UI and `dpone-studio-assets` package are not released
yet and their release verdict is **NO-GO** until the human usability gate
passes. `dpone studio` therefore reports `ui_status: not_installed` instead of
opening a placeholder interface. Even when compatible assets are installed,
`ui_status: installed` means package compatibility only; `/api/v1/meta`
separately reports `usability_status: UNVERIFIED` and
`release_verdict: NO-GO`.

## Before you start

Use Python 3.11 or 3.12, install the current dpone checkout, and run commands
from a dpone project root:

```bash
uv sync
. .venv/bin/activate
dpone init project --airflow
dpone --version
```

The Studio bridge does not require Airflow, database credentials, Vault, or
network access for the read-only journey below.

## Shortest safe path

Discover a route before starting the server:

```bash
dpone recipe list \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge

dpone init pipeline orders_daily \
  --route mssql:clickhouse:incremental_merge
```

Start the local adapter:

```bash
dpone studio --serve
```

The startup log confirms `dpone Studio API listening on
http://127.0.0.1:8765`. Leave that terminal running and use a second terminal
on the same machine:

```bash
curl http://127.0.0.1:8765/healthz
curl http://127.0.0.1:8765/api/v1/capabilities
curl http://127.0.0.1:8765/api/v1/pipelines
```

Expected signals:

- `/healthz` returns `"status": "ok"`;
- `/api/v1/meta` reports `not_installed`, `installed`, or `incompatible` from
  the same compatibility-aware assets probe used by the CLI;
- capability records keep `support`, `certification`, and
  `evidence_status` separate; missing live proof remains `UNVERIFIED`;
- a new project can return an empty `items` array from `/api/v1/pipelines`;
- invalid input returns HTTP `4xx` with a `dpone.error.v1` envelope and
  structured `fixes[].argv` or `next_actions[].argv`.

The generated [OpenAPI 3.1 document](api/studio-openapi.json) is also available
at `/openapi.json`.

## What the API can do

| Endpoint | Purpose | Side effects |
| --- | --- | --- |
| `GET /api/v1/meta` | API mode, UI availability, snapshot identity | none |
| `GET /api/v1/capabilities` | connector, route, recipe, support, certification and evidence projection | none |
| `GET /api/v1/recipes` | recipe discovery for a typed client | none |
| `GET /api/v1/pipelines` | bounded pipeline summaries | none |
| `GET /api/v1/pipelines/{id}/explain` | one pipeline's Airflow/artifact state | none |
| `POST /api/v1/manifests/draft` | canonically validate an in-memory draft | temporary files only |
| `POST /api/v1/plans` | calculate a static dry-run plan | temporary files only |
| `POST /api/v1/checks/static` | run credential-free validation | none |

POST does not mean project mutation in v1. Studio does not edit authoring
sources, credentials, Vault paths, releases, deployments, or runtime
infrastructure.

Create and validate an in-memory draft:

```bash
curl -sS http://127.0.0.1:8765/api/v1/manifests/draft \
  -H 'Content-Type: application/json' \
  -d '{"source_type":"postgres","sink_type":"mssql","strategy":"incremental_merge"}'

curl -sS http://127.0.0.1:8765/api/v1/plans \
  -H 'Content-Type: application/json' \
  -d '{"manifest_path":"pipelines/orders_daily/pipeline.yaml"}'

curl -sS http://127.0.0.1:8765/api/v1/checks/static \
  -H 'Content-Type: application/json' \
  -d '{"pipeline_ref":"orders_daily"}'
```

The first command returns the canonical in-memory YAML and structured next
actions. The second and third commands are read-only and fail with a typed
`dpone.error.v1` envelope if the referenced source is absent or invalid. All
three operations use the same capability snapshot: changing an inline manifest
to an unknown source, sink, or strategy returns
`DPONE_ROUTE_NOT_SUPPORTED` instead of a misleading static plan or check
success. Plan and static-check validate the canonical resolved route, including
classic table-level overrides, and do not maintain a second raw-YAML route
parser. Effective certification-artifact references are validated from the
same compiled process objects and are rejected when they traverse or escape the
project root, including through a symlink.

Pipeline summaries deliberately expose two independent signals:

- `valid` reports whether the editable authoring source passed canonical static
  validation;
- `operational_status` is `ready`, `blocked`, or `not_applicable` and reports
  whether the pipeline's requested Airflow projection can be used.

A valid source can therefore be operationally blocked by missing or invalid
artifacts. Clients must not infer runtime readiness from `valid: true`.

## Reading capability status

Do not collapse these fields:

| Field | Question |
| --- | --- |
| connector `maturity` | How mature is the connector implementation? |
| connector `release_phase` | How stable is its public release surface? |
| route `support.status` | Does the runtime catalog support this source, sink and strategy? |
| certification `level` | What certification level has a complete route variant reached? |
| certification `evidence_status` | Is current evidence `PASS`, `FAIL`, `SKIP`, or `UNVERIFIED`? |
| beginner `recipe_available` | Can a new user scaffold this route now? |

Certification variants also identify transport, schema-evolution mode, and
Airflow/runtime mode. Missing, stale, malformed, skipped, or foreign-commit
evidence is `UNVERIFIED`; it is never silently promoted to `PASS`.

Studio, `dpone recipe`, `dpone connectors`, and the marketplace compatibility
view consume the same capability snapshot. Platform owners who publish
certification evidence configure its exact matrix, commit, directories, and
freshness policy in the
[project evidence authority](connector-certification.md#project-evidence-authority).
Studio does not discover or refresh those artifacts itself.

If an external recipe catalog cannot be validated, Studio keeps the bounded
built-in recipes visible and reports a non-passing catalog issue. It never
silently drops the issue or treats the fallback as a healthy capability
snapshot. Both `/api/v1/capabilities` and `/api/v1/recipes` expose that issue
and snapshot identity. Pipeline summaries likewise block every unknown or
unsupported process route before suggesting Airflow preview.

Security, remote-development controls, compatibility aliases, exit codes, and
troubleshooting live in the [Studio operations runbook](studio-operations.md).

## Customer journey

| Stage | User action | Success evidence |
| --- | --- | --- |
| Discover | filter `dpone recipe list` or `/api/v1/capabilities` | one supported scaffoldable route |
| Configure | run `dpone init pipeline --route ...` | idempotent authoring files |
| Validate | run `dpone check` or static API check | structured pass/errors |
| Preview | run `dpone airflow preview` | target-scoped DAG projection |
| Observe | inspect `/api/v1/pipelines/{id}/explain` | only that pipeline's artifact state |
| Recover | follow structured `next_actions[].argv` | no shell-string execution |
| Upgrade | regenerate the client from OpenAPI | canonical `/api/v1` routes only |

Next: [First Airflow DAG](getting-started/first-airflow-dag.md). Developers
should continue with [Studio API developer contract](developer-studio-api.md).
Human usability for this milestone remains `UNVERIFIED`; use the
[self-service usability evidence protocol](airflow-self-service-evidence.md)
with at least five first-time users before releasing the separate UI. The
Studio-specific evidence location is `test_artifacts/studio-usability/`.
Until that evidence passes, both the UI v0.1.0 and authoring v0.2 release
verdicts remain **NO-GO**.
