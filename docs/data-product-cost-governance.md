# Data Product Cost, Capacity and Quota Guardrails

`dpone data product cost` adds opt-in FinOps evidence to the data product
release control plane. It answers whether a release/run stays inside declared
budget, table growth, staging, query-duration and concurrency guardrails before
the bundle is promoted.

## Manifest

```yaml
sink:
  options:
    data_product:
      id: analytics.orders
      owner: data-platform
      tier: gold
      criticality: high

      cost_governance:
        enabled: true
        mode: gate
        profile: prod_strict
        currency: USD
        stale_evidence_policy: block

        ownership:
          cost_center: data-platform-prod
          require_owner_cost_center: true

        budgets:
          monthly_max_cost: 1500
          per_run_max_cost: 25
          per_release_max_cost_delta: 0.15

        capacity:
          max_table_growth_ratio: 0.25
          max_staging_gb: 100
          max_query_duration_ms: 300000
          max_concurrent_runs: 4

        rates:
          storage_gb_month: 0.023
          staging_gb_day: 0.01
          query_tb_scanned: 5.0
          run_minute: 0.02
```

Default `cost_governance.enabled` is `false`. V1 uses manifest rates, local
runtime artifacts, registry history and optional ClickHouse read-only metadata.
It never calls cloud billing APIs and never mutates target state.

## Workflow

```bash
dpone data product cost plan \
  --manifest manifests/orders.yaml \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --format json \
  --output .dpone/data-products/orders.cost-plan.json

dpone data product cost evaluate \
  --plan .dpone/data-products/orders.cost-plan.json \
  --runtime-artifact .dpone/runs/orders/latest-run.json \
  --registry .dpone/schema-migration/registry/registry.json \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --format json \
  --output .dpone/data-products/orders.cost-evaluation.json

dpone data product cost gate \
  --evaluation .dpone/data-products/orders.cost-evaluation.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.cost-gate.json
```

Use `forecast` to compare the latest evaluation to historical cost artifacts,
and `report` to render an operator-friendly Markdown summary.

## Artifacts

- `dpone.data_product_cost_plan.v1`
- `dpone.data_product_cost_evaluation.v1`
- `dpone.data_product_cost_gate.v1`
- `dpone.data_product_cost_forecast.v1`
- `dpone.data_product_cost_report.v1`

Bundle artifact kinds:

- `data_product_cost_gate`
- `data_product_cost_forecast`
- `data_product_cost_report`

Evidence registry stages:

- `cost_planned`
- `cost_evaluated`
- `cost_gate_passed`
- `capacity_forecasted`
- `cost_report_rendered`

## Gate Semantics

`advisory` never blocks and converts blockers to warnings. `stage` blocks
missing required cost evidence and unbounded full refresh when configured.
`prod_strict` blocks per-run/monthly budget breaches, missing cost center,
unbounded full refresh and configured capacity breaches. `regulated` also
requires owner metadata and registry lifecycle evidence.

## Industrial Comparison

- dlt gives schema contract/evolution ergonomics; dpone adds release-bound
  FinOps gates and capacity evidence after schema acceptance.
- Airbyte focuses on connector/runtime safety; dpone adds product-level
  budget/capacity decisions before deploy.
- Fivetran exposes usage and platform logs; dpone turns usage evidence into
  deterministic pre-release gates.
- dbt has artifact-first freshness/state workflows; dpone applies the same
  repeatable artifact model to cost and capacity.
- Informatica, Pentaho and SSIS provide monitoring/execution history; dpone
  upgrades passive reports into typed cost controls, bundle gates and registry
  evidence.
