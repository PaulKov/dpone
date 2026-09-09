# Data Product Remediation Runbooks

`dpone data product remediation` turns blocked Trust Center and data-product
gate evidence into a deterministic operator runbook. It does not repair targets
by itself unless the opt-in execution layer is enabled and invoked with
`--execute`. The default flow is evidence-plane only: it classifies failures,
routes owners, renders safe handoff commands, gates whether the plan is
actionable, and verifies fresh closeout evidence after the underlying domain
gates are rerun.

## Manifest

```yaml
sink:
  options:
    data_product:
      id: analytics.orders
      owner: data-platform
      tier: gold
      criticality: high

      remediation:
        enabled: true
        mode: gate
        profile: prod_strict
        stale_evidence_policy: block
        require_trust_gate: true
        closeout_requires_fresh_evidence: true

        execution:
          enabled: true
          mode: gate
          profile: prod_strict
          require_remediation_gate: true
          require_authority_gate: true
          require_lock: true
          require_idempotency_key: true
          unresolved_command_policy: block
          dry_run_status: warning
          command_timeout_seconds: 300
          allowed_command_prefixes:
            - [dpone, data, product, assertions]
            - [dpone, data, product, slo]
```

Default `remediation.enabled` is `false`; disabled products emit no-op artifacts
and do not affect existing commands. Default `remediation.execution.enabled` is
also `false`; enabling remediation runbooks does not enable execution receipts
unless the nested execution config is present.

## Workflow

```bash
dpone data product remediation plan \
  --manifest manifests/orders.yaml \
  --trust-snapshot .dpone/trust/orders.trust-snapshot.json \
  --trust-gate .dpone/trust/orders.trust-gate.json \
  --evidence-dir .dpone/data-products/orders \
  --format json \
  --output .dpone/data-products/orders.remediation-plan.json

dpone data product remediation runbook render \
  --plan .dpone/data-products/orders.remediation-plan.json \
  --format md \
  --output .dpone/data-products/orders.remediation-runbook.md

dpone data product remediation gate \
  --plan .dpone/data-products/orders.remediation-plan.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.remediation-gate.json

dpone data product remediation closeout \
  --plan .dpone/data-products/orders.remediation-plan.json \
  --evidence-dir .dpone/data-products/orders/fresh \
  --format json \
  --output .dpone/data-products/orders.remediation-closeout.json

dpone data product remediation report \
  --gate .dpone/data-products/orders.remediation-gate.json \
  --closeout .dpone/data-products/orders.remediation-closeout.json \
  --format md \
  --output .dpone/data-products/orders.remediation-report.md
```

## Controlled Execution Receipts

The execution subcommands convert approved remediation actions into a bounded,
auditable execution receipt. They are intentionally conservative:

- placeholders such as `<assertion-plan>` must be supplied through a parameter
  artifact;
- commands must match `allowed_command_prefixes`;
- `run` is a dry-run unless `--execute` is passed;
- real execution requires the configured idempotency key, lock and authority
  evidence;
- stdout/stderr are represented by short snippets and SHA-256 digests.

```bash
dpone data product remediation execution plan \
  --manifest manifests/orders.yaml \
  --remediation-plan .dpone/data-products/orders.remediation-plan.json \
  --remediation-gate .dpone/data-products/orders.remediation-gate.json \
  --authority-gate .dpone/authority/orders.authority-gate.json \
  --parameters .dpone/data-products/orders.remediation-params.json \
  --format json \
  --output .dpone/data-products/orders.remediation-execution-plan.json

dpone data product remediation execution run \
  --plan .dpone/data-products/orders.remediation-execution-plan.json \
  --idempotency-key orders-remediation-20260713 \
  --lock .dpone/locks/orders-remediation-lock.json \
  --execute \
  --format json \
  --output .dpone/data-products/orders.remediation-execution-run.json

dpone data product remediation execution certify \
  --run .dpone/data-products/orders.remediation-execution-run.json \
  --evidence-dir .dpone/data-products/orders/fresh \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.remediation-execution-certificate.json

dpone data product remediation execution report \
  --certificate .dpone/data-products/orders.remediation-execution-certificate.json \
  --format md \
  --output .dpone/data-products/orders.remediation-execution-report.md
```

## Artifacts

- `dpone.data_product_remediation_plan.v1`
- `dpone.data_product_remediation_runbook.v1`
- `dpone.data_product_remediation_gate.v1`
- `dpone.data_product_remediation_closeout.v1`
- `dpone.data_product_remediation_report.v1`
- `dpone.data_product_remediation_execution_plan.v1`
- `dpone.data_product_remediation_execution_run.v1`
- `dpone.data_product_remediation_execution_certificate.v1`
- `dpone.data_product_remediation_execution_report.v1`

## Safety Model

- Base remediation performs no target mutation, scheduler mutation, SCM write,
  catalog write, ticket write, or notification delivery.
- Commands in the runbook are deterministic handoff templates; operators still
  execute the underlying domain workflow and attach fresh evidence.
- Execution V1 only runs allowlisted `dpone` commands through `subprocess`
  with `shell=False`, and only when `--execute` is present.
- Execution is intentionally not a generic shell runner. Non-allowlisted
  commands, unresolved placeholders, missing locks and missing idempotency keys
  fail closed in strict profiles.
- Closeout blocks stale evidence when `closeout_requires_fresh_evidence` is
  true, so rerunning the same blocked gate cannot close a remediation campaign.
- Unknown blocker codes fail closed in `gate` mode until a deterministic catalog
  mapping exists.

## Bundle And Registry

Bundle policy can require `data_product_remediation_gate`. The evidence registry
accepts remediation lifecycle stages:

- `data_product_remediation_planned`
- `data_product_remediation_runbook_rendered`
- `data_product_remediation_gate_passed`
- `data_product_remediation_closed`
- `data_product_remediation_report_rendered`
- `data_product_remediation_execution_planned`
- `data_product_remediation_executed`
- `data_product_remediation_execution_certified`
- `data_product_remediation_execution_report_rendered`

Bundle policy can also require
`data_product_remediation_execution_certificate` when a production or regulated
workflow needs proof that the approved repair command ran and produced fresh
expected evidence.
