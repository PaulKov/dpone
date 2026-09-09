---
title: Data Product SLO, assertions and incidents
---

# Data Product SLO, Assertions and Incident Contract

`dpone data product assertions` and `dpone data product slo` turn a routed
dataset into a production data API with explicit quality tests, measurable SLOs
release-closeout evidence, policy-as-code release governance and audit-ready
compliance control mapping. The feature is opt-in and does not mutate targets.
It reads local artifacts, migration evidence registry records, runtime run
summaries and, when configured, a read-only ClickHouse connection.

Use it after schema contract, consumer, migration, post-apply and release-watch
evidence has been produced:

```text
schema contract gate
-> consumer gate/test/adoption evidence
-> migration bundle/gate/registry
-> post-apply/watch evidence
-> assertion plan/evaluate/gate
-> data product SLO evaluation
-> SLO gate
-> error budget gate
-> incident lifecycle / route payload if needed
-> release closeout gate
-> policy evaluate / waiver / gate
-> compliance controls plan/evaluate/gate
-> compliance audit package
-> audit archive / verify / retention / legal hold
-> access classify / entitlements / privacy impact / gate
-> governance export render / publish verification
-> fleet evaluate/gate/report/export
```

## Manifest

```yaml
sink:
  options:
    data_product:
      id: analytics.orders
      owner: data-platform
      tier: gold
      criticality: high
      policy:
        enabled: true
        mode: gate
        profile: prod_strict
        store_backend: local_json
        store_uri: .dpone/policies/policy-registry.json
        packs:
          - id: production_release_guardrails
            version: 1.0.0
            owner: data-governance
            applies_to:
              product_tiers: [gold, regulated]
              environments: [prod]
            rules:
              - id: require_assertion_gate
                severity: critical
                require_artifacts: [data_product_assertion_gate]
              - id: block_fast_burn_release
                severity: critical
                require_status:
                  data_product_error_budget_gate: [allowed, warning]
        waivers:
          enabled: true
          max_duration_days: 14
          require_owner: true
          require_approval_evidence: true
          expired_policy: block
      authority:
        enabled: true
        mode: gate
        profile: prod_strict
        store_backend: local_json
        store_uri: .dpone/authority/authority-registry.json
        unknown_actor: block
        identities:
          - id: alice
            type: user
            owner: data-platform
            groups: [data-platform]
            roles: [change_author]
          - id: data-governance
            type: group
            owner: governance
            roles: [governance_approver]
        roles:
          - id: data_owner
            grants: [policy_waiver.approve, release_closeout.approve]
          - id: governance_approver
            grants: [policy_waiver.approve, regulated_release.approve]
          - id: change_author
            grants: [change.author]
        approval:
          quorum:
            policy_waiver:
              min_approvals: 2
              required_roles: [data_owner, governance_approver]
        separation_of_duties:
          prevent_self_approval: true
          disallow_same_actor_for: [requested_by, implemented_by, approved_by]
        signing:
          enabled: true
          algorithm: hmac_sha256
          key_env: DPONE_AUTHORITY_SIGNING_KEY
          required_for:
            - dpone.data_product_waiver.v1
      compliance:
        enabled: true
        mode: gate
        profile: regulated
        stale_evidence_policy: block
        stale_after_seconds: 2592000
        frameworks:
          - id: soc2
            version: 2026.1
            owner: security-governance
            controls:
              - id: CC7.2-data-quality-release
                title: Data product release evidence is reviewed and approved
                severity: critical
                require_artifacts:
                  - data_product_policy_gate
                  - data_product_authority_gate
                  - data_product_evidence_signature
                  - data_product_release_closeout_gate
                allowed_statuses:
                  data_product_policy_gate: [allowed, warning, waived]
                  data_product_authority_gate: [allowed, warning]
                  data_product_evidence_signature: [signed, verified]
                  data_product_release_closeout_gate: [allowed, warning]
              - id: CC9.2-data-quality-monitoring
                title: Data product quality and SLOs are continuously evaluated
                severity: high
                require_artifacts:
                  - data_product_assertion_gate
                  - data_product_slo_gate
                  - data_product_error_budget_gate
      audit_retention:
        enabled: true
        mode: gate
        profile: regulated
        stale_evidence_policy: block
        archive:
          store_backend: local_fs
          store_uri: .dpone/audit-archive
          layout: "{product_id}/{yyyy}/{mm}/{audit_archive_id}"
          include_artifacts:
            - data_product_audit_package
            - data_product_compliance_gate
            - data_product_policy_gate
            - data_product_authority_gate
          manifest_hash_chain: true
          merkle_root: true
        retention:
          min_days: 2555
          delete_after_days: 3650
          expired_policy: block
        legal_hold:
          enabled: true
          policy: block_delete
          require_reason: true
          require_authority_gate: true
      governance_export:
        enabled: true
        mode: gate
        profile: prod_strict
        stale_evidence_policy: block
        targets:
          - provider: datahub
            mode: render
          - provider: openmetadata
            mode: render
          - provider: openlineage
            mode: render
          - provider: opa
            mode: render
      access_governance:
        enabled: true
        mode: gate
        profile: regulated
        unknown_consumer: block
        stale_evidence_policy: block
        classification:
          default: internal
          columns:
            customer_email:
              class: pii
              glossary_terms: [EMAIL_PLAINTEXT]
              masking: hash
              lawful_basis_required: true
            amount:
              class: financial
              masking: none
        entitlements:
          - subject: finance.daily_margin
            type: consumer
            owner: finance-analytics
            purpose: finance_close
            actions: [read]
            columns: [amount, customer_id]
          - subject: support.ops_debug
            type: group
            owner: support
            purpose: support_debug
            lawful_basis: support_contract
            actions: [read]
            columns: [customer_email]
            masking_required: true
            masking: hash
        privacy:
          require_lawful_basis_for: [pii, regulated]
          block_sensitive_export_without_approval: true
          require_authority_gate_for: [pii, regulated]
      assertions:
        enabled: true
        mode: gate
        profile: prod_strict
        unknown_assertion_source: warn
        stale_after_seconds: 86400
        suites:
          - id: orders_contract_quality
            owner: data-platform
            severity: critical
            assertions:
              - id: order_id_not_null
                type: null_key
                columns: [order_id]
                max_failures: 0
              - id: order_id_unique
                type: duplicate_key
                columns: [order_id]
                max_failures: 0
              - id: amount_non_negative
                type: sql
                query: "SELECT count() AS failures FROM analytics.orders WHERE amount < 0"
                expect:
                  column: failures
                  equals: 0
              - id: freshness_15m
                type: freshness
                max_lag_seconds: 900
        imports:
          dbt_run_results: target/run_results.json
          great_expectations: .dpone/quality/gx-validation-results.json
          datahub_assertions: .dpone/catalog/datahub-assertions.json
          openmetadata_tests: .dpone/catalog/openmetadata-tests.json
      slo:
        enabled: true
        mode: gate
        profile: prod_strict
        objectives:
          freshness:
            max_lag_seconds: 900
          volume:
            min_rows: 1
            max_relative_delta: 0.001
          latency:
            max_run_duration_ms: 300000
          quality:
            max_duplicate_keys: 0
            max_null_keys: 0
            typed_hash: warning
          availability:
            max_failed_runs: 0
        consumers:
          require_critical_consumer_green: true
          unknown_consumer: warn
        incident:
          enabled: true
          severity_map:
            critical_consumer_failed: sev1
            freshness_breach: sev2
            volume_breach: sev2
            warning_only: sev3
        error_budget:
          enabled: true
          mode: gate
          profile: prod_strict
          objective_window: 30d
          windows:
            - name: fast_burn
              duration: 1h
              max_burn_rate: 14.4
            - name: slow_burn
              duration: 6h
              max_burn_rate: 6.0
            - name: monthly
              duration: 30d
              min_budget_remaining: 0.10
          release_policy:
            freeze_on_budget_exhausted: true
            block_risky_migration_on_fast_burn: true
        incident_lifecycle:
          enabled: true
          mode: gate
          require_ack_for_sev1: true
          require_resolution_evidence: true
          routing:
            enabled: true
            mode: render
            providers: [slack, jira, pagerduty]
      reliability_control_tower:
        enabled: true
        mode: gate
        profile: prod_strict
        fleet:
          unknown_product: warn
          stale_evidence_policy: block
          stale_after_seconds: 86400
          freeze_on:
            - sev1_open
            - fast_burn
            - budget_exhausted
            - release_closeout_blocked
        export:
          enabled: true
          targets: [prometheus, opentelemetry, openlineage, datahub]
        routing:
          enabled: true
          mode: dry_run
          providers: [slack, jira, pagerduty, webhook]
```

## CLI

```bash
dpone data product assertions plan \
  --manifest manifests/orders.yaml \
  --format json \
  --output .dpone/data-products/orders.assertion-plan.json

dpone data product assertions evaluate \
  --plan .dpone/data-products/orders.assertion-plan.json \
  --runtime-artifact .dpone/runs/orders/latest-run.json \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --format json \
  --output .dpone/data-products/orders.assertion-evaluation.json

dpone data product assertions gate \
  --evaluation .dpone/data-products/orders.assertion-evaluation.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.assertion-gate.json

dpone data product assertions report \
  --evaluation .dpone/data-products/orders.assertion-evaluation.json \
  --format md \
  --output .dpone/data-products/orders.assertion-report.md

dpone data product slo plan \
  --manifest manifests/orders.yaml \
  --contract-gate .dpone/schema-contracts/orders.contract-gate.json \
  --consumer-gate .dpone/schema-contracts/orders.consumer-gate.json \
  --assertion-gate .dpone/data-products/orders.assertion-gate.json \
  --format json \
  --output .dpone/data-products/orders.slo-plan.json

dpone data product slo evaluate \
  --plan .dpone/data-products/orders.slo-plan.json \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --runtime-artifact .dpone/runs/orders/latest-run.json \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --format json \
  --output .dpone/data-products/orders.slo-evaluation.json

dpone data product slo gate \
  --evaluation .dpone/data-products/orders.slo-evaluation.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.slo-gate.json

dpone data product incident report \
  --evaluation .dpone/data-products/orders.slo-evaluation.json \
  --slo-gate .dpone/data-products/orders.slo-gate.json \
  --format md \
  --output .dpone/data-products/orders.incident.md

dpone data product slo budget plan \
  --manifest manifests/orders.yaml \
  --slo-evaluation .dpone/data-products/orders.slo-evaluation.json \
  --format json \
  --output .dpone/data-products/orders.error-budget-plan.json

dpone data product slo budget evaluate \
  --plan .dpone/data-products/orders.error-budget-plan.json \
  --history .dpone/data-products/orders.slo-history.json \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --format json \
  --output .dpone/data-products/orders.error-budget-evaluation.json

dpone data product slo budget gate \
  --evaluation .dpone/data-products/orders.error-budget-evaluation.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.error-budget-gate.json

dpone data product incident lifecycle open \
  --slo-evaluation .dpone/data-products/orders.slo-evaluation.json \
  --slo-gate .dpone/data-products/orders.slo-gate.json \
  --budget-gate .dpone/data-products/orders.error-budget-gate.json \
  --format json \
  --output .dpone/data-products/orders.incident-lifecycle.json

dpone data product incident lifecycle ack \
  --incident .dpone/data-products/orders.incident-lifecycle.json \
  --actor data-platform \
  --format json \
  --output .dpone/data-products/orders.incident-ack.json

dpone data product incident lifecycle resolve \
  --incident .dpone/data-products/orders.incident-ack.json \
  --evidence .dpone/data-products/orders.slo-gate.json \
  --format json \
  --output .dpone/data-products/orders.incident-resolved.json

dpone data product incident route render \
  --incident .dpone/data-products/orders.incident-resolved.json \
  --provider slack \
  --format json \
  --output .dpone/data-products/orders.slack-route.json

dpone data product release closeout gate \
  --slo-gate .dpone/data-products/orders.slo-gate.json \
  --budget-gate .dpone/data-products/orders.error-budget-gate.json \
  --incident .dpone/data-products/orders.incident-resolved.json \
  --watch-certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --format json \
  --output .dpone/data-products/orders.release-closeout.json

dpone data product policy evaluate \
  --manifest manifests/orders.yaml \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --evidence-dir .dpone/data-products \
  --format json \
  --output .dpone/data-products/orders.policy-evaluation.json

dpone data product policy waiver request \
  --evaluation .dpone/data-products/orders.policy-evaluation.json \
  --rule-id require_assertion_gate \
  --reason "approved migration window" \
  --expires-at 2026-07-12T00:00:00Z \
  --format json \
  --output .dpone/data-products/orders.waiver-request.json

dpone data product policy waiver approve \
  --request .dpone/data-products/orders.waiver-request.json \
  --actor data-governance \
  --approval approvals/orders-policy.yaml \
  --format json \
  --output .dpone/data-products/orders.waiver.json

dpone data product policy gate \
  --evaluation .dpone/data-products/orders.policy-evaluation.json \
  --waiver .dpone/data-products/orders.waiver.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/orders.policy-gate.json

dpone data product policy report \
  --gate .dpone/data-products/orders.policy-gate.json \
  --format md \
  --output .dpone/data-products/orders.policy-report.md

dpone data product compliance controls plan \
  --manifest manifests/orders.yaml \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --evidence-dir .dpone/data-products/orders \
  --format json \
  --output .dpone/data-products/orders.compliance-plan.json

dpone data product compliance controls evaluate \
  --plan .dpone/data-products/orders.compliance-plan.json \
  --format json \
  --output .dpone/data-products/orders.compliance-evaluation.json

dpone data product compliance controls gate \
  --evaluation .dpone/data-products/orders.compliance-evaluation.json \
  --profile regulated \
  --format json \
  --output .dpone/data-products/orders.compliance-gate.json

dpone data product compliance audit package \
  --gate .dpone/data-products/orders.compliance-gate.json \
  --format md \
  --output .dpone/data-products/orders.audit-package.md

dpone data product audit archive plan \
  --manifest manifests/orders.yaml \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --evidence-dir .dpone/data-products/orders \
  --format json \
  --output .dpone/data-products/orders.audit-archive-plan.json

dpone data product audit archive run \
  --plan .dpone/data-products/orders.audit-archive-plan.json \
  --execute \
  --format json \
  --output .dpone/data-products/orders.audit-archive-run.json

dpone data product audit archive verify \
  --archive-run .dpone/data-products/orders.audit-archive-run.json \
  --format json \
  --output .dpone/data-products/orders.audit-archive-verification.json

dpone data product audit retention plan \
  --manifest manifests/orders.yaml \
  --archive-verification .dpone/data-products/orders.audit-archive-verification.json \
  --format md \
  --output .dpone/data-products/orders.audit-retention-plan.md

dpone data product audit legal-hold apply \
  --archive-run .dpone/data-products/orders.audit-archive-run.json \
  --reason "SOC2 evidence hold" \
  --authority-gate .dpone/authority/orders.authority-gate.json \
  --format json \
  --output .dpone/data-products/orders.legal-hold.json

dpone data product governance export plan \
  --manifest manifests/orders.yaml \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --evidence-dir .dpone/data-products \
  --targets datahub,openmetadata,openlineage,opa,json \
  --format json \
  --output .dpone/data-products/orders.governance-export-plan.json

dpone data product governance export render \
  --plan .dpone/data-products/orders.governance-export-plan.json \
  --target datahub \
  --format json \
  --output .dpone/data-products/orders.datahub-payload.json

dpone data product governance publish \
  --payload .dpone/data-products/orders.datahub-payload.json \
  --provider datahub \
  --connection .dpone/connections/datahub.json \
  --execute \
  --format json \
  --output .dpone/data-products/orders.governance-publish-receipt.json

dpone data product governance verify \
  --receipt .dpone/data-products/orders.governance-publish-receipt.json \
  --format json \
  --output .dpone/data-products/orders.governance-publish-verification.json

dpone data product fleet evaluate \
  --manifests "manifests/**/*.yaml" \
  --registry .dpone/schema-migration/registry.sqlite3 \
  --format json \
  --output .dpone/data-products/fleet.evaluation.json

dpone data product fleet gate \
  --evaluation .dpone/data-products/fleet.evaluation.json \
  --profile prod_strict \
  --format json \
  --output .dpone/data-products/fleet.gate.json

dpone data product fleet report \
  --evaluation .dpone/data-products/fleet.evaluation.json \
  --format md \
  --output .dpone/data-products/fleet.report.md

dpone data product fleet export \
  --evaluation .dpone/data-products/fleet.evaluation.json \
  --target prometheus \
  --format text \
  --output .dpone/data-products/fleet.prom

dpone data product incident route dry-run \
  --payload .dpone/data-products/orders.slack-route.json \
  --provider slack \
  --format json \
  --output .dpone/data-products/orders.route-receipt.json

dpone data product authority registry build \
  --manifest manifests/orders.yaml \
  --format json \
  --output .dpone/authority/orders.authority-registry.json

dpone data product authority check \
  --registry .dpone/authority/orders.authority-registry.json \
  --actor data-governance \
  --action policy_waiver.approve \
  --subject .dpone/data-products/orders.waiver-request.json \
  --format json \
  --output .dpone/authority/orders.authority-check.json

dpone data product authority quorum verify \
  --registry .dpone/authority/orders.authority-registry.json \
  --request .dpone/data-products/orders.waiver-request.json \
  --approval approvals/orders-owner.yaml \
  --approval approvals/orders-governance.yaml \
  --format json \
  --output .dpone/authority/orders.approval-quorum.json

dpone data product authority signature sign \
  --registry .dpone/authority/orders.authority-registry.json \
  --artifact .dpone/data-products/orders.waiver.json \
  --actor data-governance \
  --format json \
  --output .dpone/authority/orders.waiver.signature.json

dpone data product authority signature verify \
  --registry .dpone/authority/orders.authority-registry.json \
  --artifact .dpone/data-products/orders.waiver.json \
  --signature .dpone/authority/orders.waiver.signature.json \
  --format json \
  --output .dpone/authority/orders.signature-verification.json

dpone data product authority gate \
  --authority-check .dpone/authority/orders.authority-check.json \
  --approval-quorum .dpone/authority/orders.approval-quorum.json \
  --signature .dpone/authority/orders.waiver.signature.json \
  --profile prod_strict \
  --format json \
  --output .dpone/authority/orders.authority-gate.json

dpone data product access classify \
  --manifest manifests/orders.yaml \
  --schema-contract .dpone/schema-contracts/orders.contract.json \
  --format json \
  --output .dpone/data-products/orders.access-classification.json

dpone data product access entitlements plan \
  --manifest manifests/orders.yaml \
  --classification .dpone/data-products/orders.access-classification.json \
  --consumer-matrix .dpone/schema-contracts/orders.consumer-matrix.json \
  --format json \
  --output .dpone/data-products/orders.entitlement-plan.json

dpone data product privacy impact assess \
  --manifest manifests/orders.yaml \
  --entitlement-plan .dpone/data-products/orders.entitlement-plan.json \
  --authority-gate .dpone/authority/orders.authority-gate.json \
  --format json \
  --output .dpone/data-products/orders.privacy-impact.json

dpone data product access gate \
  --entitlement-plan .dpone/data-products/orders.entitlement-plan.json \
  --privacy-impact .dpone/data-products/orders.privacy-impact.json \
  --profile regulated \
  --format json \
  --output .dpone/data-products/orders.access-gate.json

dpone data product access report \
  --gate .dpone/data-products/orders.access-gate.json \
  --format md \
  --output .dpone/data-products/orders.access-report.md
```

## Policy and waivers

`dpone data product policy` is the governance layer above individual gates. It
does not replace assertion, SLO, error-budget, incident, closeout or fleet
commands. Instead it evaluates a versioned local policy pack against those
artifacts and returns one decision artifact that CI/CD can require.

Policy rules support two V1 checks:

- `require_artifacts`: the named artifact must be present and must not be in a
  blocked status.
- `require_status`: the named artifact must be present and its status must be
  one of the configured values.

Waivers are bounded exceptions. A waiver request can target only a failed or
warning policy rule. Approval requires a local approval artifact, an actor and an
expiry timestamp. Expired waivers never cover a failed rule in `gate` mode; they
become blockers so release closeout and fleet control tower can fail closed.

Recommended production flow:

```text
assertions/SLO/error-budget/incident/closeout evidence
-> policy evaluate
-> waiver request/approve only when risk is explicitly accepted
-> policy gate
-> bundle build --data-product-policy-gate
-> registry record --stage policy_gate_passed
-> fleet gate / release closeout
```

V1 is artifact-only. It does not create Jira, Slack, PagerDuty, GitHub or catalog
records. External systems can consume the waiver and policy report artifacts.

## Approval authority and signed evidence

`dpone data product authority` strengthens policy waivers and regulated release
closeout with local, deterministic authority evidence. It answers four questions
before a risk exception can be accepted:

- does the actor exist in the local authority registry;
- does one of the actor roles grant the requested action;
- do approval artifacts satisfy quorum and required role coverage;
- does separation-of-duties prevent the requester/implementer from approving
  their own exception.

Detached signatures make critical evidence tamper-evident. `digest_only`
records the canonical artifact hash; `hmac_sha256` uses the configured
environment key and never writes the secret into the signature artifact. V1 has
no SCM, Jira, Slack, PagerDuty, catalog or KMS writes. External approval systems
can produce local YAML/JSON artifacts that the quorum verifier consumes.

When `sink.options.data_product.authority.enabled: true`, policy gates can
fail-closed for strict profiles if a waiver is present but no valid
`data_product_authority_gate` was supplied. Existing waiver flows remain
backward-compatible when authority is disabled.

## Access classification, entitlements and privacy impact

`dpone data product access` and `dpone data product privacy` add an offline
access-governance layer before regulated release closeout. The commands answer
which columns are sensitive, who is allowed to read or export them, whether
masking is required, and whether a sensitive access path has purpose,
lawful-basis and authority evidence.

The V1 flow is render-only:

```text
manifest access_governance + schema contract
-> access classification
-> entitlement plan joined with consumer matrix
-> privacy impact assessment with optional authority gate
-> access gate
-> access report / bundle / registry / governance export
```

The taxonomy stays provider-neutral:

- `AccessClassificationBuilder` normalizes column classes such as `pii`,
  `financial`, `confidential` and `regulated`.
- `EntitlementPlanBuilder` joins classification, consumer matrix reads and
  declared entitlements into subject-column decisions.
- `PrivacyImpactAssessor` checks sensitive access for purpose, lawful basis,
  masking and authority evidence.
- `AccessGateEvaluator` applies `advisory`, `stage`, `prod_strict` and
  `regulated` fail-closed semantics.

`regulated` blocks missing entitlements, missing masking for sensitive fields,
missing purpose/lawful basis and missing authority evidence for configured
classes. `advisory` produces the same artifact shape but converts blockers to
warnings for rollout rehearsal. No V1 command mutates a target database, catalog
or access-control system.

### Access enforcement, masking and drift detection

`dpone data product access enforcement` turns the render-only access governance
evidence into a target-bound enforcement lifecycle:

```text
classification + entitlement plan + privacy impact + access/authority gates
-> access enforcement plan
-> apply --execute with approval / dry-run without --execute
-> drift inspect
-> enforcement certificate
-> bundle / registry / compliance / governance export
```

The V1 generic core is provider-neutral. It normalizes subject-column
requirements, masking requirements and row filters, then delegates target DDL
rendering to a dialect port. ClickHouse is the certified V1 dialect:

- column grants render as `GRANT SELECT(col...) ON db.table TO role`;
- row filters render as `CREATE ROW POLICY ... FOR SELECT USING ...`;
- masking uses a normal masked projection view by default, with native masking
  policy support left behind an explicit capability path.

`apply` is safe by default. Without `--execute`, it writes a dry-run run
artifact and performs no target mutation. With `--execute`, strict profiles
require approval, target fingerprint, lock evidence and authority evidence. The
drift inspector compares desired grants/masks/row filters with actual target
state evidence and blocks extra sensitive grants, missing masks and missing row
filters when `drift_policy: block`.

New artifacts are:

- `dpone.data_product_access_enforcement_plan.v1`
- `dpone.data_product_access_enforcement_run.v1`
- `dpone.data_product_access_drift_report.v1`
- `dpone.data_product_access_enforcement_certificate.v1`
- `dpone.data_product_access_enforcement_report.v1`

## Compliance control mapping and audit packages

`dpone data product compliance` maps framework controls such as SOC 2, ISO
27001, SOX or GDPR labels to concrete dpone evidence artifacts. V1 validates the
evidence mapping, freshness, signatures, authority refs and statuses; it does
not claim legal compliance or call external GRC systems.

The control flow is:

```text
manifest compliance framework
-> evidence index from bundle / registry / evidence-dir
-> controls evaluate
-> compliance gate
-> audit package
-> bundle/registry closeout
```

For `regulated`, dpone fails closed when critical/high/medium controls are
missing required artifacts, when evidence is stale under a blocking stale policy,
or when owner/signature/authority evidence is incomplete. `advisory` keeps the
same evidence payload but converts blockers to warnings for rollout rehearsals.

The audit package is a deterministic Markdown/JSON-compatible control matrix:
each row links a framework control to artifact kind, status, evidence id, hash,
blockers and warnings. Auditors can review one package instead of manually
walking registry records, bundle files and data-product evidence directories.

## Audit evidence retention, immutability and legal hold

`dpone data product audit` turns compliance and release evidence into a
checksummed local archive. It is still offline and provider-neutral: V1 writes
only to a local filesystem archive store, verifies bytes against the archive
manifest and emits retention/legal-hold artifacts. It never deletes evidence.

The archive flow is:

```text
audit package + governance/release evidence
-> archive plan with per-artifact hashes, hash chain and Merkle root
-> archive run with canonical JSON payloads and archive manifest
-> archive verify
-> retention plan
-> optional legal hold
```

`archive run` is dry-run unless `--execute` is present. `retention plan` marks
archives as retained, expired or held but does not perform cleanup. Legal hold
requires a reason and can require an authority gate, giving regulated users a
portable evidence path from policy decision to immutable archive.

## Governance export and catalog interop

`dpone data product governance` publishes the decisions made by policy,
compliance, assertion, SLO, budget, release-closeout and fleet gates into
catalog-friendly payloads. The command group keeps dpone as the release control
plane and treats DataHub, OpenMetadata, OpenLineage, OPA and portable JSON as
export targets.

The flow is intentionally split:

- `export plan` reads the manifest, evidence directory and optional registry,
  then binds product, contract and release evidence into a deterministic plan.
- `export render` converts the plan into one provider payload without network
  writes.
- `publish` is a dry run unless `--execute` is present. Execution requires a
  connection file and emits a receipt without secrets.
- `verify` validates the receipt, payload id/hash and idempotency key so CI can
  require a machine-readable closeout artifact.

V1 does not run background syncs and does not import catalog SDKs in generic
readiness modules. Provider-specific publishing is isolated behind the publish
port; render-only flows are the safe default for self-service CI.

## Bundle and registry integration

Policy artifacts are pack-bound release evidence:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --data-product-policy-gate .dpone/data-products/orders.policy-gate.json \
  --data-product-policy-report .dpone/data-products/orders.policy-report.md \
  --data-product-waiver .dpone/data-products/orders.waiver.json \
  --data-product-authority-gate .dpone/authority/orders.authority-gate.json \
  --data-product-approval-quorum .dpone/authority/orders.approval-quorum.json \
  --data-product-evidence-signature .dpone/authority/orders.waiver.signature.json \
  --data-product-compliance-gate .dpone/data-products/orders.compliance-gate.json \
  --data-product-audit-package .dpone/data-products/orders.audit-package.md \
  --data-product-audit-archive-verification \
    .dpone/data-products/orders.audit-archive-verification.json \
  --data-product-audit-retention-plan .dpone/data-products/orders.audit-retention-plan.json \
  --data-product-legal-hold .dpone/data-products/orders.legal-hold.json \
  --data-product-access-gate .dpone/data-products/orders.access-gate.json \
  --data-product-access-report .dpone/data-products/orders.access-report.md \
  --data-product-privacy-impact-assessment .dpone/data-products/orders.privacy-impact.json \
  --output-dir .dpone/schema-migration/review/orders

dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --environment prod \
  --stage compliance_gate_passed \
  --data-product-policy-gate .dpone/data-products/orders.policy-gate.json \
  --data-product-authority-gate .dpone/authority/orders.authority-gate.json \
  --data-product-approval-quorum .dpone/authority/orders.approval-quorum.json \
  --data-product-evidence-signature .dpone/authority/orders.waiver.signature.json \
  --data-product-compliance-gate .dpone/data-products/orders.compliance-gate.json \
  --data-product-audit-package .dpone/data-products/orders.audit-package.md \
  --data-product-audit-archive-verification \
    .dpone/data-products/orders.audit-archive-verification.json \
  --data-product-audit-retention-plan .dpone/data-products/orders.audit-retention-plan.json \
  --data-product-legal-hold .dpone/data-products/orders.legal-hold.json \
  --data-product-access-classification .dpone/data-products/orders.access-classification.json \
  --data-product-entitlement-plan .dpone/data-products/orders.entitlement-plan.json \
  --data-product-privacy-impact-assessment .dpone/data-products/orders.privacy-impact.json \
  --data-product-access-gate .dpone/data-products/orders.access-gate.json \
  --data-product-access-report .dpone/data-products/orders.access-report.md

dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --data-product-governance-publish-verification \
    .dpone/data-products/orders.governance-publish-verification.json \
  --output-dir .dpone/schema-migration/review/orders

dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --environment prod \
  --stage governance_publish_verified \
  --data-product-governance-publish-verification \
    .dpone/data-products/orders.governance-publish-verification.json
```

Registry stages for policy lifecycle are:
`policy_evaluated`, `waiver_requested`, `waiver_approved`, `waiver_expired` and
`policy_gate_passed`. Compliance lifecycle stages are:
`compliance_planned`, `compliance_evaluated`, `compliance_gate_passed` and
`audit_package_rendered`. Governance export stages are:
`governance_export_planned`, `governance_payload_rendered`,
`governance_published` and `governance_publish_verified`. Access governance
stages are: `access_classified`, `entitlements_planned`,
`privacy_impact_assessed`, `access_gate_passed`,
`access_enforcement_planned`, `access_enforced`,
`access_drift_inspected` and `access_enforcement_certified`.

## Industrial comparison

- dlt provides schema contract modes such as evolve, freeze and discard. dpone
  keeps contract-as-code ergonomics and adds cross-artifact release policy,
  bounded exceptions and audit control evidence.
- Airbyte provides schema review, notifications and audit logs. dpone keeps the
  review value but emits portable policy and waiver artifacts instead of relying
  on platform-only timeline state.
- Fivetran exposes platform logs and schema changelogs. dpone moves governance
  left into pre-release policy and compliance gates rather than post-hoc log
  inspection.
- Informatica emphasizes data governance policies and data quality. dpone
  expresses that governance as deterministic GitOps evidence.
- Pentaho supports operational reporting and metadata-driven execution. dpone
  replaces manual exception runbooks with typed waiver lifecycle artifacts.
- SSIS Catalog centralizes execution history, permissions and environments.
  dpone generalizes that discipline to target-neutral data product policy and
  compliance audit packages.
- DataHub, OpenMetadata, Apache Ranger and dbt model access provide catalog
  policies, classifications, masking/filter semantics and ownership models.
  dpone treats those systems as interop/export targets while keeping
  release-time column classification, entitlement and privacy impact evidence
  deterministic, offline and CI-verifiable.

The public schemas are in `docs/schemas/data-product/`:

- `data-product-assertion-plan.schema.json`
- `data-product-assertion-evaluation.schema.json`
- `data-product-assertion-gate.schema.json`
- `data-product-assertion-report.schema.json`
- `data-product-slo-plan.schema.json`
- `data-product-slo-evaluation.schema.json`
- `data-product-slo-gate.schema.json`
- `data-product-incident-report.schema.json`
- `data-product-error-budget-plan.schema.json`
- `data-product-error-budget-evaluation.schema.json`
- `data-product-error-budget-gate.schema.json`
- `data-product-incident-lifecycle.schema.json`
- `data-product-incident-route-payload.schema.json`
- `data-product-release-closeout-gate.schema.json`
- `data-product-fleet-evaluation.schema.json`
- `data-product-fleet-gate.schema.json`
- `data-product-fleet-report.schema.json`
- `data-product-reliability-export.schema.json`
- `data-product-route-delivery-receipt.schema.json`
- `data-product-governance-export-plan.schema.json`
- `data-product-governance-export-payload.schema.json`
- `data-product-governance-publish-receipt.schema.json`
- `data-product-governance-publish-verification.schema.json`
- `data-product-access-classification.schema.json`
- `data-product-entitlement-plan.schema.json`
- `data-product-privacy-impact-assessment.schema.json`
- `data-product-access-gate.schema.json`
- `data-product-access-report.schema.json`
- `data-product-access-enforcement-plan.schema.json`
- `data-product-access-enforcement-run.schema.json`
- `data-product-access-drift-report.schema.json`
- `data-product-access-enforcement-certificate.schema.json`
- `data-product-access-enforcement-report.schema.json`

## Semantics

`assertions plan` normalizes suites from
`sink.options.data_product.assertions`, validates supported V1 assertion types
and rejects unsafe custom SQL before any target access. Supported assertions are
`freshness`, `row_count`, `null_key`, `duplicate_key`, `accepted_values`,
`range`, `regex`, `typed_hash`, `schema`, `sql` and `imported`.

`assertions evaluate` checks offline evidence first: runtime freshness and row
counts, dbt run results, Great Expectations validation results, DataHub
assertions and OpenMetadata tests. When a target connection is supplied, V1 uses
read-only ClickHouse probes and SELECT-only custom SQL. Missing import sources
follow `unknown_assertion_source: allow|warn|block`.

`assertions gate` emits `allowed`, `warning` or `blocked`:

- `advisory` never blocks and converts blockers into warnings;
- `stage` blocks critical failed assertions;
- `prod_strict` blocks critical/high failures, stale or missing required
  evidence and unsafe SQL blockers;
- `regulated` additionally requires owner and lifecycle evidence.

`assertions report` renders a deterministic Markdown/JSON/text/table summary for
release reviews and incident notes.

`slo plan` binds the product id, owner, tier, objectives and optional contract
or consumer/assertion gate ids into `dpone.data_product_slo_plan.v1`.

`slo evaluate` checks configured objectives:

- freshness from runtime lag evidence;
- volume from runtime row counts or read-only target evidence;
- latency from runtime duration metrics;
- availability from failed-run counts and run status;
- quality from assertion gate status, duplicate/null key counters and typed-hash
  status;
- consumer impact from consumer gate summaries.

`slo gate` turns the evaluation into `allowed`, `warning` or `blocked`:

- `advisory` never blocks and converts blockers into warnings;
- `stage` is suitable for non-prod closeout;
- `prod_strict` blocks failed freshness, volume, latency, quality,
  availability and critical-consumer objectives;
- `regulated` is reserved for stricter evidence-completeness policies.

`incident report` is artifact-only in V1. It does not create Jira, Slack or
PagerDuty tickets. External systems can ingest the Markdown/JSON artifact.

`slo budget plan|evaluate|gate` turns a stream of SLO evaluations into
burn-rate and budget-remaining evidence. The evaluator supports short windows
for fast burn, longer windows for slow burn and monthly budget remaining checks.

`incident lifecycle` is append-only evidence. `open` deduplicates incidents by
product/objective/severity signals, `ack` records the operator/owner
acknowledgement and `resolve` requires green evidence or an accepted exception.

`incident route render` creates Slack/Jira/PagerDuty-shaped payloads, but never
writes to those systems in V1. The payload includes `network_writes: []` by
contract.

`release closeout gate` combines SLO, error budget, incident lifecycle and
optional watch/post-apply evidence into a single go/no-go artifact for CI and
release review.

`fleet evaluate` is the control-tower layer. It reads all configured data
product manifests and registry evidence, then normalizes product health into
`healthy`, `degraded`, `incident_active`, `frozen` or `unknown`. It is offline
in V1 and never opens target, Slack, Jira, PagerDuty, Prometheus, Grafana or
catalog connections.

`fleet gate` turns that snapshot into a release decision:

- `advisory` converts blockers into warnings;
- `stage` blocks only active Sev1 or missing critical evidence;
- `prod_strict` freezes releases on Sev1, fast burn, exhausted budget, stale
  critical evidence or blocked closeout;
- `regulated` additionally requires owner metadata and complete lifecycle
  evidence for critical/gold products.

`fleet report` renders an executive/operator Markdown report with release
freeze reasons and owner focus. `fleet export` renders offline Prometheus,
OpenTelemetry-style, OpenLineage-like, DataHub-like or normalized JSON
artifacts. It does not push metrics or write to external systems.

`incident route dry-run` validates a rendered route payload and produces
`dpone.data_product_route_delivery_receipt.v1` with `network_writes: []`.
External routing tools can consume this receipt without dpone holding provider
credentials.

## Bundle and registry integration

Production closeout evidence can be bundled and persisted:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --data-product-assertion-gate .dpone/data-products/orders.assertion-gate.json \
  --data-product-assertion-report .dpone/data-products/orders.assertion-report.json \
  --data-product-slo-gate .dpone/data-products/orders.slo-gate.json \
  --data-product-incident-report .dpone/data-products/orders.incident.json \
  --data-product-error-budget-gate .dpone/data-products/orders.error-budget-gate.json \
  --data-product-incident-lifecycle .dpone/data-products/orders.incident-resolved.json \
  --data-product-release-closeout-gate .dpone/data-products/orders.release-closeout.json \
  --data-product-fleet-gate .dpone/data-products/fleet.gate.json \
  --data-product-fleet-report .dpone/data-products/fleet.report.json \
  --data-product-reliability-export .dpone/data-products/fleet.prom.json \
  --data-product-route-delivery-receipt .dpone/data-products/orders.route-receipt.json \
  --data-product-governance-publish-verification \
    .dpone/data-products/orders.governance-publish-verification.json \
  --output-dir .dpone/schema-migration/review/orders

dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --data-product-assertion-gate .dpone/data-products/orders.assertion-gate.json \
  --data-product-assertion-report .dpone/data-products/orders.assertion-report.json \
  --data-product-slo-gate .dpone/data-products/orders.slo-gate.json \
  --data-product-error-budget-gate .dpone/data-products/orders.error-budget-gate.json \
  --data-product-incident-lifecycle .dpone/data-products/orders.incident-resolved.json \
  --data-product-release-closeout-gate .dpone/data-products/orders.release-closeout.json \
  --data-product-fleet-gate .dpone/data-products/fleet.gate.json \
  --data-product-fleet-report .dpone/data-products/fleet.report.json \
  --data-product-reliability-export .dpone/data-products/fleet.prom.json \
  --data-product-route-delivery-receipt .dpone/data-products/orders.route-receipt.json \
  --data-product-governance-publish-verification \
    .dpone/data-products/orders.governance-publish-verification.json \
  --environment prod \
  --stage fleet_gate_passed
```

New registry stages:

- `assertions_planned`
- `assertions_evaluated`
- `assertion_gate_passed`
- `assertion_incident_opened`
- `slo_planned`
- `slo_evaluated`
- `slo_gate_passed`
- `incident_opened`
- `incident_acknowledged`
- `incident_mitigated`
- `incident_resolved`
- `error_budget_planned`
- `error_budget_evaluated`
- `error_budget_gate_passed`
- `release_closeout_passed`
- `fleet_evaluated`
- `fleet_gate_passed`
- `fleet_release_frozen`
- `reliability_exported`
- `incident_route_prepared`
- `governance_export_planned`
- `governance_payload_rendered`
- `governance_published`
- `governance_publish_verified`
- `authority_registered`
- `authority_checked`
- `approval_quorum_verified`
- `evidence_signed`
- `evidence_signature_verified`
- `authority_gate_passed`
- `audit_archived`
- `audit_archive_verified`
- `retention_planned`
- `legal_hold_applied`

## Developer taxonomy

Generic modules remain provider-neutral:

- `DataProductAssertionOptions` parses immutable assertion suite policy.
- `DataProductAssertionPlanner` builds deterministic quality test plans.
- `DataProductAssertionEvaluator` evaluates offline/imported evidence and
  read-only target facts through injected service evidence.
- `DataProductAssertionGate` performs profile-aware quality go/no-go decisions.
- `AssertionReportRenderer` renders assertion evidence for release and incident
  review.
- `DataProductSloOptions` parses immutable manifest policy.
- `DataProductSloPlanner` builds deterministic plans.
- `DataProductSloEvaluator` evaluates offline/runtime evidence.
- `DataProductSloGate` performs profile-aware go/no-go decisions.
- `IncidentClassifier` maps SLO failures to severity and owner evidence.
- `DataProductErrorBudgetPlanner/Evaluator/Gate` computes burn-rate and
  budget-remaining release policy.
- `IncidentLifecycleReducer` maintains append-only incident state.
- `IncidentRouterPayloadRenderer` renders provider payloads without network
  writes.
- `ReleaseCloseoutGate` combines SLO, budget, incident and watch evidence.
- `FleetReliabilityOptions` parses fleet control-tower policy.
- `FleetReliabilityEvaluator` evaluates manifests and registry records into a
  deterministic fleet snapshot.
- `FleetReliabilityGate` makes fleet release/freeze decisions.
- `ReliabilityExportRenderer` renders reports and offline export artifacts.
- `IncidentRouteDryRunEvaluator` validates route payloads without network
  writes.
- `DataProductGovernanceExportOptions` parses provider export policy.
- `GovernanceEvidenceIndex` normalizes bundle, registry and local evidence refs.
- `GovernanceExportPlanner` builds product/contract-bound export plans.
- `GovernancePayloadRenderer` renders DataHub, OpenMetadata, OpenLineage, OPA
  and JSON payloads without network writes.
- `GovernancePublisher` emits explicit dry-run or execute receipts through a DI
  port without leaking secrets.
- `GovernancePublishVerifier` validates publish receipts for CI and registry.
- `AuthorityRegistryBuilder` normalizes local identities, roles, grants, quorum
  and signing policy into a deterministic registry.
- `AuthorityCheckEvaluator` checks actor/action/subject permission plus
  separation-of-duties.
- `ApprovalQuorumVerifier` deduplicates approval artifacts and verifies role
  coverage/minimum approvals.
- `EvidenceSigner` and `EvidenceSignatureVerifier` produce detached digest/HMAC
  evidence without leaking secrets.
- `AuthorityGate` combines authority, quorum and signature evidence into
  `allowed`, `warning` or `blocked`.
- `AuditEvidenceArchivePlanner/Archiver/Verifier` builds immutable archive
  plans, writes local canonical evidence and verifies hash chains.
- `AuditRetentionPlanner` emits non-destructive cleanup/retain decisions.
- `LegalHoldService` applies reasoned legal-hold evidence to archived packages.
- `DataProductSloFacade` is a thin file-IO boundary for CLI commands.

Generic readiness code must not import ClickHouse, DB clients, Airflow, SCM,
Jira, Slack, PagerDuty, Prometheus, Grafana or DataDog SDKs. Target access and
external writes stay behind service-layer adapters or external CI jobs.

## Industrial comparison

| System | Pattern | dpone behavior |
| --- | --- | --- |
| dlt | Schema contracts and evolution control accepted schema changes: [schema contracts](https://dlthub.com/docs/general-usage/schema-contracts). | Adds product-level SLO, policy, governance-export and release-freeze gates after schema acceptance. |
| Airbyte | Connection status, notifications, audit logs and timelines help operate syncs: [schema management](https://docs.airbyte.com/platform/using-airbyte/schema-change-management). | Produces portable reliability, governance and archive verification artifacts instead of platform-only connection state. |
| Fivetran | History Mode, platform connector logs and notifications expose sync/schema events: [History Mode](https://fivetran.com/docs/core-concepts/syncoverview/sync-modes/history-mode). | Moves product health, governance publishing and retention evidence into proactive, CI-verifiable release decisions. |
| Informatica | Data Quality and Observability focus on trusted data and monitoring: [Data Quality](https://www.informatica.com/products/data-quality.html). | Makes observability and governance decisions typed, offline-verifiable and bundle-ready. |
| Pentaho | Operations Mart aggregates execution and audit reporting: [Operations Mart](https://docs.pentaho.com/pdia-admin/10.2-admin/optimize-the-pentaho-system/monitoring-system-performance/pentaho-operations-mart). | Turns passive reports into deterministic SLO, policy, archive-retention and catalog-export evidence. |
| SSIS | Data Profiling Task and SSIS Catalog provide SQL Server execution/quality discipline. | Generalizes profiling/catalog discipline into target-neutral product reliability and governance evidence. |
| DataHub/OpenMetadata/OpenLineage/OPA | Catalogs and policy engines model assertions, incidents, lineage facets and decision logs. | dpone renders provider payloads while keeping release authority in checksummed bundle/registry artifacts. |
| OPA/DataHub/OpenMetadata access workflows | Policy bundles, decision logs, access workflows and roles/policies model who can approve what. | dpone keeps those semantics local and CI-verifiable with authority registry, quorum, SOD and signed evidence. |
| DataHub/OpenMetadata | Assertions and data quality tests model table, column, schema and custom SQL checks. | Imports their local evidence while keeping dpone as the release/control-plane contract. |
| Great Expectations/dbt | Checkpoints and dbt data tests produce validation artifacts; dbt source freshness checks freshness thresholds. | Imports validation output and extends it into multi-objective release closeout, error-budget and incident evidence. |
| OpenLineage | Column lineage identifies downstream dependencies: [column lineage facet](https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet/). | Uses consumer evidence to route SLO breaches to affected data API owners. |
