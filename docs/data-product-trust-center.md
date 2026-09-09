# Data Product Trust Center, Evidence Lake and Query API

`dpone data product trust` adds an opt-in evidence plane over data product
release artifacts. It indexes local manifests, bundle evidence, registry
records and data-product artifacts, then renders a trust snapshot, gate and
operator report.

V1 is offline and render-only. It never starts a server, calls external catalog
APIs, mutates targets, or publishes to Slack/Jira/SCM systems.

## Manifest

```yaml
sink:
  options:
    data_product:
      id: analytics.orders
      owner: data-platform
      tier: gold
      criticality: high

      trust_center:
        enabled: true
        mode: gate
        profile: prod_strict
        stale_evidence_policy: block
        stale_after_seconds: 86400
        required_domains:
          - contract
          - quality
          - governance
          - compliance
          - access
          - connection_security
          - cost
          - rollout
        include_artifacts:
          - schema_contract_gate
          - data_product_assertion_gate
          - data_product_policy_gate
          - data_product_compliance_gate
          - data_product_access_gate
          - data_product_connection_rotation_gate
          - data_product_cost_gate
          - data_product_ring_gate
```

Default `trust_center.enabled` is `false`; disabled products emit no-op
artifacts and do not affect existing commands.

## Workflow

```bash
dpone data product trust lake index \
  --manifests "manifests/**/*.yaml" \
  --registry .dpone/schema-migration/registry/registry.json \
  --evidence-dir .dpone/data-products \
  --format json \
  --output .dpone/trust/evidence-lake-index.json

dpone data product trust query \
  --index .dpone/trust/evidence-lake-index.json \
  --product-id analytics.orders \
  --domain compliance \
  --format table

dpone data product trust snapshot \
  --index .dpone/trust/evidence-lake-index.json \
  --product-id analytics.orders \
  --profile prod_strict \
  --format json \
  --output .dpone/trust/orders.trust-snapshot.json

dpone data product trust gate \
  --snapshot .dpone/trust/orders.trust-snapshot.json \
  --profile prod_strict \
  --format json \
  --output .dpone/trust/orders.trust-gate.json

dpone data product trust report \
  --snapshot .dpone/trust/orders.trust-snapshot.json \
  --gate .dpone/trust/orders.trust-gate.json \
  --format md \
  --output .dpone/trust/orders.trust-report.md
```

Use `trust export` to render local JSON, DataHub-like, OpenLineage-like or
OPA-decision-log-like payloads. The export command writes artifacts only and
does not perform network delivery.

## Artifacts

- `dpone.data_product_evidence_lake_index.v1`
- `dpone.data_product_trust_query_result.v1`
- `dpone.data_product_trust_snapshot.v1`
- `dpone.data_product_trust_gate.v1`
- `dpone.data_product_trust_report.v1`
- `dpone.data_product_trust_export.v1`

Bundle artifact kinds:

- `data_product_trust_gate`
- `data_product_trust_report`
- `data_product_trust_export`

Evidence registry stages:

- `trust_lake_indexed`
- `trust_query_executed`
- `trust_snapshot_rendered`
- `trust_gate_passed`
- `trust_report_rendered`
- `trust_export_rendered`

## Gate Semantics

`advisory` never blocks and converts blockers to warnings. `stage` blocks
malformed or missing core evidence. `prod_strict` blocks missing required
domains, blocked domain evidence and trust score below the configured threshold.
`regulated` also requires complete owner, authority, signature, compliance,
audit and access evidence where configured.

The Trust Center gate does not override domain gates. It summarizes the evidence
plane and fails closed when required proof is incomplete.
