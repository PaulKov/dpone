# Managed-like UX for dpone

`dpone` includes self-service commands that help users diagnose their environment, generate manifests, preview execution plans, inspect state, write run artifacts, certify connectors, tune performance, and scaffold community connectors.

For Airflow, the default managed-like path is the
[indexed-v2 self-service plane](airflow-self-service.md). The
[GitOps Airflow runner pack](gitops-airflow-runner-pack.md) is an advanced
compatibility path for custom-image deployments, not the beginner or default
self-service workflow.

## Fast path

```bash
dpone doctor --profile local
dpone init \
  --source-type postgres \
  --sink-type mssql \
  --source-connection pg_src \
  --sink-connection mssql_dwh \
  --source-schema public \
  --source-table orders \
  --target-schema landing \
  --target-table orders \
  --strategy incremental_merge \
  --unique-key id \
  --out examples/batch/landing_orders.batch.yaml
dpone plan examples/batch/landing_orders.batch.yaml --selector public.orders
dpone perf advise examples/batch/landing_orders.batch.yaml --selector public.orders
```

## Commands

- `dpone doctor` checks local readiness: Python, optional extras, Docker, `bcp`, `sqlcmd`, ClickHouse tools, Kafka/Schema Registry hints, and fix commands.
- `dpone init` generates a batch manifest bundle, `.env.example`, and smoke command.
- `dpone plan` renders a dry-run execution plan without changing targets.
- `dpone run-report` generates manual/synthetic JSON, Markdown, and HTML run
  artifacts under `.dpone/runs` by default. It does not read or summarize
  `dpone run` output.
- `dpone state` exposes safe inspect/reset/export/replay/compare UX. Destructive operations preview unless `--yes` is supplied.
- `dpone connectors` lists, certifies, and scaffolds connector SDK skeletons.
- `dpone perf advise` recommends native bulk paths, partitioning, and delivery tuning.
- `dpone studio` prints local Studio API metadata by default; `--serve` starts
  the bounded headless local HTTP API. The public UI is not released yet, so
  metadata reports `ui_status: not_installed`.

## Manifest additions

All managed UX sections are optional and backward-compatible:

```yaml
quality:
  gates:
    - id: source_target_rows
      type: row_count_reconciliation
      severity: warning
      tolerance:
        mode: pct
        value: 0.1

observability:
  artifacts:
    enabled: true
    formats: [json, md, html]
  opentelemetry:
    enabled: false
  prometheus:
    enabled: false

performance:
  advisor:
    enabled: true

certification:
  connector_status: experimental
```

Manifest `quality.gates` configures runtime load governance. The old
`/api/quality/check` endpoint is a retirement-only Studio compatibility
surface; new clients must use `POST /api/v1/checks/static` for static pipeline
validation and the ordinary `dpone check`/runtime quality workflow for
executable gates. The legacy endpoint evaluates lightweight checks only over
rows supplied in its request; it is not manifest authoring and does not create
runtime gates. `/api/manifests/draft` is also a deprecated alias of
`/api/v1/manifests/draft`; both translate the compatibility
`quality_checks` request into canonical `quality.gates` in the generated
manifest. It never writes deprecated manifest `quality.checks`. The
capabilities response distinguishes executable manifest gates from declared
fail-closed gate types; a draft warning names any selected gate that cannot
yet execute.

## Safety defaults

- `dpone plan` is dry-run only.
- `dpone state reset` and `dpone state replay-from` require `--yes` to execute; without it they return a preview payload.
- Secrets are redacted in doctor, plan, run artifact, certification, and scaffold diagnostics.
- Studio is local-only in this version and has no SaaS backend.

## Studio architecture

The framework owns a local API bridge and shared application services. A future
Nuxt/Vue frontend will be a typed OpenAPI client; it will not own connector,
route, manifest, quality, or certification policy. No public
`PaulKov/dpone-studio` release is claimed by this version.

## Studio API endpoints

Start the local Studio API bridge:

```bash
dpone studio --serve --host 127.0.0.1 --port 8765
```

- `/healthz` and `/openapi.json` are public local endpoints.
- `/api/v1/capabilities` projects connector, route, recipe and evidence axes.
- `/api/v1/pipelines` and `/api/v1/pipelines/{id}/explain` are target-scoped.
- `/api/v1/manifests/draft`, `/api/v1/plans`, and `/api/v1/checks/static` are
  non-mutating bounded POST queries.
- Legacy `/api/*` endpoints are deprecated compatibility operations. Some map
  to v1 routes; others are retirement-only and have no v1 replacement.

See the [Studio local API guide](studio.md) for security, status codes and
migration.

## Run artifacts

A run artifact contains timeline, state before/after, quality results, schema evolution notes, reconciliation metrics, and redacted diagnostics. Use JSON for automation, Markdown for repo evidence, and HTML for local review.

## Connector SDK

```bash
dpone connectors scaffold demo_api --root ./community-connectors
```

The scaffold includes connector/source/sink skeletons, a contract test, and a docs template. Fill in auth, capabilities, examples, and certification evidence before publishing a community connector.

## Production checklist

- Run `dpone doctor --profile production` on the execution host.
- Run `dpone plan` and inspect staging/shadow/schema/state sections.
- Run `dpone perf advise` for large tables or Kafka batches.
- Configure canonical `quality.gates` for runtime correctness gates.
- Store run artifacts for every release or live certification run.
