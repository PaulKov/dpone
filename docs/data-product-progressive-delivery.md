# Data Product Progressive Delivery

`dpone data product rollout` adds evidence-first progressive delivery for data
products. It answers how a release should move through staging, canary and full
production rings, which gates are required at each step, whether shadow
validation is green, and whether promotion should continue, hold or recommend
rollback.

V1 is offline and evidence-plane only. It does not shift traffic, mutate target
databases, modify schedulers, call catalogs or create tickets. Promotion is a
deterministic artifact that can be required by bundle, registry, governance and
compliance workflows.

## Manifest

```yaml
sink:
  options:
    data_product:
      id: analytics.orders
      owner: data-platform
      tier: gold
      criticality: high

      progressive_delivery:
        enabled: true
        mode: gate
        profile: prod_strict
        stale_evidence_policy: block

        rings:
          - id: staging
            order: 10
            required_gates:
              - schema_contract_gate
              - data_product_assertion_gate
              - data_product_cost_gate

          - id: canary
            order: 20
            max_consumers: 3
            consumer_selector:
              owners: [data-platform]
              criticality: [low, medium]
            required_gates:
              - data_product_slo_gate
              - data_product_access_gate
              - data_product_connection_rotation_gate

          - id: prod_full
            order: 30
            required_gates:
              - data_product_release_closeout_gate
              - data_product_compliance_gate
              - data_product_policy_gate

        shadow_validation:
          enabled: true
          required_for: [canary, prod_full]
          compare:
            row_count_delta_max: 0.001
            null_key_delta_max: 0
            duplicate_key_delta_max: 0
            typed_hash: warning

        rollback:
          auto_hold_on:
            - shadow_failed
            - ring_gate_blocked
            - slo_blocked
            - assertion_blocked
            - cost_blocked
          require_authority_gate: true
```

Default `progressive_delivery.enabled` is `false`. When enabled, the default
mode is `gate` and the default profile is `prod_strict`.

## Workflow

```bash
dpone data product rollout plan \
  --manifest manifests/orders.yaml \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --evidence-dir .dpone/data-products/orders \
  --format json \
  --output .dpone/data-products/orders.rollout-plan.json

dpone data product rollout shadow-validate \
  --plan .dpone/data-products/orders.rollout-plan.json \
  --runtime-artifact .dpone/runs/orders/latest-run.json \
  --baseline-runtime-artifact .dpone/runs/orders/previous-run.json \
  --format json \
  --output .dpone/data-products/orders.shadow-validation.json

dpone data product rollout ring gate \
  --plan .dpone/data-products/orders.rollout-plan.json \
  --ring canary \
  --shadow-validation .dpone/data-products/orders.shadow-validation.json \
  --evidence-dir .dpone/data-products/orders \
  --format json \
  --output .dpone/data-products/orders.canary-ring-gate.json

dpone data product rollout promote \
  --ring-gate .dpone/data-products/orders.canary-ring-gate.json \
  --target-ring prod_full \
  --authority-gate .dpone/data-products/orders.authority-gate.json \
  --format json \
  --output .dpone/data-products/orders.rollout-promotion.json

dpone data product rollout report \
  --promotion .dpone/data-products/orders.rollout-promotion.json \
  --format md \
  --output .dpone/data-products/orders.rollout-report.md
```

## Artifacts

- `dpone.data_product_rollout_plan.v1`
- `dpone.data_product_shadow_validation.v1`
- `dpone.data_product_ring_gate.v1`
- `dpone.data_product_rollout_promotion.v1`
- `dpone.data_product_rollout_report.v1`

Bundle artifact kinds:

- `data_product_ring_gate`
- `data_product_shadow_validation`
- `data_product_rollout_promotion`
- `data_product_rollout_report`

Evidence registry stages:

- `rollout_planned`
- `shadow_validated`
- `rollout_ring_gate_passed`
- `rollout_promoted`
- `rollout_held`
- `rollout_rollback_recommended`

## Gate Semantics

`advisory` never blocks and converts blockers to warnings. `stage` blocks
missing required gates and blocked shadow validation. `prod_strict` blocks stale
evidence, blocked gates, missing required shadow validation and skipped rings.
`regulated` also expects authority, policy and compliance evidence before final
promotion.

Promotion emits `promoted` only when the source ring gate is `allowed` or
`warning` and required authority evidence is present. Otherwise it emits `held`
with blockers and next actions. Rollback is a recommendation artifact in V1;
actual rollback still uses the existing migration rollback/remediation paths.

## Industrial Comparison

- dlt controls schema evolution with contract modes; dpone keeps
  contract-as-code ergonomics and adds release-ring evidence after contract
  acceptance: [dlt schema contracts](https://dlthub.com/docs/general-usage/schema-contracts).
- Airbyte reviews schema changes at connection/stream level; dpone moves the
  decision to product rings, downstream consumers and shadow evidence:
  [Airbyte schema changes](https://docs.airbyte.com/platform/using-airbyte/schema-change-management).
- Fivetran exposes platform logs and operational metadata; dpone turns rollout
  decisions into deterministic pre/post-release artifacts:
  [Fivetran Platform Connector](https://fivetran.com/docs/logs/fivetran-platform).
- Informatica emphasizes data quality and observability governance; dpone binds
  those signals directly to ring promotion and hold decisions:
  [Informatica Data Quality](https://www.informatica.com/products/data-quality.html).
- Pentaho Operations Mart and SSIS Catalog provide operational history; dpone
  upgrades passive reporting into typed rollout gates and registry evidence:
  [Pentaho Operations Mart](https://docs.pentaho.com/pdia-admin/10.2-admin/optimize-the-pentaho-system/monitoring-system-performance/pentaho-operations-mart),
  [SSIS Catalog](https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog?view=sql-server-ver17).
- Argo Rollouts provides canary and analysis for Kubernetes traffic; dpone takes
  the progressive-delivery pattern and makes it data-product/evidence-native:
  [Argo Rollouts](https://argo-rollouts.readthedocs.io/en/stable/).
- dbt exposures document downstream dependencies; dpone uses downstream
  evidence to require gates before ring promotion:
  [dbt exposures](https://docs.getdbt.com/docs/build/exposures).
