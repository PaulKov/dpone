# Schema migration control

`dpone schema migration` is the durable schema/DDL migration control plane for
manifest-owned target tables. It complements, not replaces, the existing
commands:

- `dpone schema physical-plan` plans desired target DDL.
- `dpone schema physical-diff` compares desired and actual physical state.
- `dpone schema contract ...` publishes semver data API contracts and checks
  consumer compatibility windows before DDL/apply.
- `dpone schema identity plan` explains semantic rename aliases and deprecation
  windows.
- `dpone schema expand-contract` builds an expand/backfill/contract runbook.
- `dpone schema migration ...` packages those decisions into ledgered migration
  evidence.

## Commands

Publish and gate a versioned schema contract before building a production pack:

```bash
dpone schema contract publish --manifest manifests/orders.yaml --format json
dpone schema contract gate \
  --manifest manifests/orders.yaml \
  --pack .dpone/schema-migration/orders.pack.json \
  --format json \
  --output .dpone/schema-contracts/orders.contract-gate.json
dpone schema contract consumers lineage \
  --manifest manifests/orders.yaml \
  --format json \
  --output .dpone/schema-contracts/orders.lineage.json
dpone schema contract consumers discover \
  --manifest manifests/orders.yaml \
  --lineage .dpone/schema-contracts/orders.lineage.json \
  --format json \
  --output .dpone/schema-contracts/orders.consumers.json
dpone schema contract consumers matrix \
  --manifest manifests/orders.yaml \
  --against analytics.orders@1.5.0 \
  --consumers .dpone/schema-contracts/orders.consumers.json \
  --lineage .dpone/schema-contracts/orders.lineage.json \
  --format json \
  --output .dpone/schema-contracts/orders.consumer-matrix.json
dpone schema contract consumers gate \
  --manifest manifests/orders.yaml \
  --pack .dpone/schema-migration/orders.pack.json \
  --matrix .dpone/schema-contracts/orders.consumer-matrix.json \
  --format json \
  --output .dpone/schema-contracts/orders.consumer-gate.json
dpone schema contract adoption plan \
  --manifest manifests/orders.yaml \
  --consumer-matrix .dpone/schema-contracts/orders.consumer-matrix.json \
  --compatibility-view-plan .dpone/schema-contracts/orders.compatibility-views.json \
  --format json \
  --output .dpone/schema-contracts/orders.adoption-plan.json
dpone schema contract adoption status \
  --plan .dpone/schema-contracts/orders.adoption-plan.json \
  --registry .dpone/schema-migration/registry/registry.json \
  --consumer-certification .dpone/schema-contracts/orders.consumer-certification.json \
  --format json \
  --output .dpone/schema-contracts/orders.adoption-status.json
dpone schema contract adoption gate \
  --status .dpone/schema-contracts/orders.adoption-status.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-contracts/orders.retirement-gate.json
```

When `schema_contract.adoption.enabled: true`, migration planning embeds an
`adoption_summary` next to the contract, consumer matrix and compatibility view
summaries. This lets PR reviewers see whether old pinned consumers are still
migrating before a compatibility view or expired alias is retired.

Build an immutable migration pack:

```bash
dpone schema migration plan \
  --manifest manifests/orders.yaml \
  --actual actual-clickhouse-physical.json \
  --format json \
  --output .dpone/schema-migration/orders.pack.json
```

Build a shadow-table migration pack for physical layout drift:

```bash
dpone schema migration plan \
  --manifest manifests/orders.yaml \
  --actual actual-clickhouse-physical.json \
  --strategy shadow \
  --format json \
  --output .dpone/schema-migration/orders.shadow-pack.json
```

Build an online-safe migration pack for mutable physical settings:

```bash
dpone schema migration plan \
  --manifest manifests/orders.yaml \
  --actual actual-clickhouse-physical.json \
  --strategy online_safe \
  --format json
```

Adopt an existing target table into the artifact ledger without changing user
tables:

```bash
dpone schema migration baseline \
  --manifest manifests/orders.yaml \
  --actual actual-clickhouse-physical.json \
  --ledger .dpone/schema-migration/ledger.json \
  --format json
```

Apply a reviewed migration pack into the ledger:

```bash
dpone schema migration apply \
  --plan .dpone/schema-migration/orders.pack.json \
  --actual actual-clickhouse-physical.json \
  --ledger .dpone/schema-migration/ledger.json \
  --approval approvals/orders.yaml \
  --format json
```

Execute a reviewed migration pack against the target and then write the ledger
record:

```bash
dpone schema migration apply \
  --plan .dpone/schema-migration/orders.pack.json \
  --actual actual-clickhouse-physical.json \
  --ledger .dpone/schema-migration/ledger.json \
  --approval approvals/orders.yaml \
  --execute \
  --target-connection .dpone/targets/clickhouse.json \
  --format json
```

Target connection JSON for ClickHouse V1:

```json
{
  "type": "clickhouse",
  "host": "127.0.0.1",
  "port": 9000,
  "database": "analytics",
  "user": "default",
  "password": "${CLICKHOUSE_PASSWORD}",
  "secure": false
}
```

## Environment promotion

`dpone schema migration` treats GitHub, GitLab, Bitbucket, and similar systems
as the review and deployment control plane. V1 does not call SCM APIs directly.
Instead it emits immutable JSON/Markdown artifacts that PR/MR pipelines can
upload, approve, and pass between jobs.

The key rule is: build one `MigrationPack`, certify it in the previous
environment, then promote the same `pack_id` into the next environment. Do not
rebuild the pack independently in stage and prod.

Environment contract:

```yaml
schema_version: dpone.schema_migration_environments.v1
chain: [dev, stage, prod]
policy:
  require_same_pack_id: true
  require_previous_certification: true
  require_impact_gate: true
  prod_requires_approval: true
environments:
  dev:
    ledger: .dpone/schema-migration/dev-ledger.json
    actual: actual/dev-clickhouse-physical.json
    target_connection:
      path: .dpone/targets/dev-clickhouse.json
  stage:
    ledger: .dpone/schema-migration/stage-ledger.json
    actual: actual/stage-clickhouse-physical.json
    target_connection:
      path: .dpone/targets/stage-clickhouse.json
  prod:
    ledger: .dpone/schema-migration/prod-ledger.json
    actual: actual/prod-clickhouse-physical.json
    target_connection:
      path: .dpone/targets/prod-clickhouse.json
```

`target_connection` must be a path/reference. Inline secrets such as
`password`, `token`, `secret`, `client_secret`, `private_key`, or `access_key`
are rejected so PR artifacts can stay reviewable.

Verify that a pack has reached stage:

```bash
dpone schema migration verify-env \
  --pack .dpone/schema-migration/orders.pack.json \
  --environment stage \
  --environment-contract .dpone/environments/orders.yaml \
  --format json
```

Create an immutable stage certification receipt:

```bash
dpone schema migration certify \
  --pack .dpone/schema-migration/orders.pack.json \
  --environment stage \
  --environment-contract .dpone/environments/orders.yaml \
  --format json \
  --output .dpone/schema-migration/orders.stage.cert.json
```

Promotion approval for production:

```yaml
schema_version: dpone.schema_migration_promotion_approval.v1
pack_id: sha256:...
from_environment: stage
to_environment: prod
approved_by: data-platform-owner
approved_at: 2026-06-21T12:00:00Z
expires_at: 2026-06-28T12:00:00Z
approved_risks:
  - prod_promotion
  - compatibility_breaking
```

Promote the certified pack:

```bash
dpone schema migration promote \
  --pack .dpone/schema-migration/orders.pack.json \
  --from stage \
  --to prod \
  --certificate .dpone/schema-migration/orders.stage.cert.json \
  --environment-contract .dpone/environments/orders.yaml \
  --approval approvals/orders-prod.yaml \
  --format json \
  --output .dpone/schema-migration/orders.stage-to-prod.promotion.json
```

## Migration rehearsal and certification

`dpone schema migration rehearse` proves that a reviewed `MigrationPack` can
execute in a controlled sandbox or stage target before production apply.
Rehearsal is separate from `migration apply`: it writes run/certificate
artifacts and does not write the migration ledger or evidence registry.

Build a rehearsal plan bound to the exact pack, optional bundle, environment,
and target connection artifact:

```bash
dpone schema migration rehearse plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --environment stage \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --output-dir .dpone/schema-migration/rehearsal/orders \
  --format json
```

The plan uses schema version `dpone.schema_migration_rehearsal_plan.v1` and
contains ordered executable operations, rollback operations when supported,
checks, blockers, warnings, and `rehearsal_plan_id`. V1 executes live rehearsal
for ClickHouse through the same `MigrationOperationExecutor` port used by
`migration apply --execute`. Other targets receive
`schema_migration_rehearsal.unsupported_target:<sink>` until certified.
Prod-like rehearsal targets are blocked by default with
`schema_migration_rehearsal.prod_environment_blocked` unless the connection
artifact explicitly opts into that unsafe rehearsal mode.

Run the rehearsal:

```bash
dpone schema migration rehearse run \
  --plan .dpone/schema-migration/rehearsal/orders/rehearsal-plan.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/rehearsal-run.json
```

Without `--execute`, `run` creates a dry-run receipt. With `--execute`, dpone
uses the target connection from the rehearsal plan and executes DDL, DML, and
validation operations in the sandbox target. The run artifact uses schema
version `dpone.schema_migration_rehearsal_run.v1` and records operation status,
result fingerprints, validation mismatches, rollback operations, duration,
rows validated, and executed operation count.

For shadow packs with rollback supported only before `contract`, rehearsal
executes through cutover and rollback-check, then defers the `contract` phase
with `schema_migration_rehearsal.contract_deferred_for_rollback_check`. That
keeps the sandbox proof reversible instead of proving rollback after the phase
that intentionally removes rollback safety.

### Rehearsal data fixture and quality profile

DDL/DML execution alone is not enough for production confidence. The optional
fixture/profile layer proves the rehearsal ran on representative data: enough
rows, a sufficiently wide schema, edge cases, key null/duplicate checks, typed
hashes, and nested parent/child integrity when the manifest exposes hierarchy.

Configure the fixture policy in the manifest or pass an override policy file:

```yaml
sink:
  options:
    physical_design:
      migration:
        rehearsal:
          data_fixture:
            mode: synthetic
            min_rows: 10000
            max_rows: 100000
            min_columns: 1
            include_edge_cases: true
            preserve_hierarchy: true
            seed: dpone-rehearsal-v1
          quality_profile:
            row_count: true
            typed_hash: true
            null_distribution: true
            distinct_count: true
            min_max: true
            duplicate_key: true
            null_key: true
            nested_parent_child: true
            performance_budget_ms: 300000
```

Build the fixture plan:

```bash
dpone schema migration rehearse fixture plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --manifest manifests/orders.yaml \
  --policy .dpone/schema-migration/rehearsal/fixture-policy.yaml \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/fixture-plan.json
```

The plan uses `dpone.schema_migration_fixture_plan.v1` and contains the
pack-bound target, fixture mode, row/column thresholds, key columns, hierarchy,
masking rules, enabled checks, blockers, warnings, and `fixture_plan_id`.

Build or seed fixture rows:

```bash
dpone schema migration rehearse fixture build \
  --plan .dpone/schema-migration/rehearsal/orders/fixture-plan.json \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/fixture-build.json
```

Without `--execute`, `fixture build` returns a dry-run receipt and does not
write the sandbox target. With `--execute`, V1 seeds ClickHouse through a
target-specific adapter behind the generic `TargetFixtureSeeder` port. Modes:

| Mode | Behavior |
| --- | --- |
| `none` | No fixture rows; allowed for advisory flows and backward compatibility. |
| `synthetic` | Deterministic rows with nullable/default/key/nested edge cases. |
| `artifact_sample` | Reads local JSONL/CSV rows from `artifact_path`. |
| `masked_sample` | Reads local sample rows, applies deterministic masking, then seeds. Direct source DB sampling is out of scope in V1. |

For masked samples, configured `hash` and `token` actions require the salt env
var, for example `DPONE_REHEARSAL_MASKING_SALT`. Sensitive configured values
are masked before target write and before evidence is emitted.

Profile fixture data before and after rehearsal:

```bash
dpone schema migration rehearse fixture profile \
  --fixture-build .dpone/schema-migration/rehearsal/orders/fixture-build.json \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --stage before \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/profile-before.json

dpone schema migration rehearse fixture profile \
  --fixture-build .dpone/schema-migration/rehearsal/orders/fixture-build.json \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --stage after \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/profile-after.json
```

Profiles use `dpone.schema_migration_data_profile.v1`. They include row count,
typed hash, null distribution, distinct counts, min/max, duplicate key, null key
and nested parent/child checks. Blocking examples include
`schema_migration_profile.duplicate_key:<columns>`,
`schema_migration_profile.null_key:<column>`, and
`schema_migration_profile.orphan_child:<parent_column>`.

Certify the run:

```bash
dpone schema migration rehearse certify \
  --run .dpone/schema-migration/rehearsal/orders/rehearsal-run.json \
  --fixture-build .dpone/schema-migration/rehearsal/orders/fixture-build.json \
  --before-profile .dpone/schema-migration/rehearsal/orders/profile-before.json \
  --after-profile .dpone/schema-migration/rehearsal/orders/profile-after.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/rehearsal/orders/rehearsal-certificate.json
```

Certificates use schema version
`dpone.schema_migration_rehearsal_certificate.v1` and return `certified`,
`warning`, or `blocked`. `advisory` may certify a dry-run as warning evidence;
`stage`, `prod_strict`, and `regulated` require execution evidence and block
with `schema_migration_rehearsal.execution_required` when `run` was created
without `--execute`. When fixture/profile artifacts are supplied, the
certificate records `fixture_build_id`, `before_profile_id`,
`after_profile_id`, and blocks on row loss, typed-hash drift, profile blockers,
or mismatched pack ids. Row-count drift is reported as
`schema_migration_rehearsal.row_count_drift`; typed-hash drift is reported as
`schema_migration_rehearsal.typed_hash_drift`. Render a human report with:

```bash
dpone schema migration rehearse report \
  --certificate .dpone/schema-migration/rehearsal/orders/rehearsal-certificate.json \
  --format md \
  --output .dpone/schema-migration/rehearsal/orders/rehearsal-certificate.md
```

Production recommendation:

```text
plan -> impact -> bundle build/verify/trust -> rehearse plan/run/certify
-> rehearse fixture plan/build/profile
-> bundle build --rehearsal-certificate --fixture-build --quality-profile
-> bundle gate -> registry record
-> migration apply --bundle-gate
```

Missing rehearsal evidence preserves backward compatibility unless a custom
bundle policy requires `rehearsal_certificate`, `fixture_build`, or
`quality_profile` as required artifacts.

## Production post-apply verification

`dpone schema migration post-apply` is the production closeout layer after
`migration apply --execute`. Rehearsal proves that a pack can execute in a
controlled stage target. Post-apply verification proves that the same `pack_id`
actually landed correctly in the applied target.

The command group is read-only. It never applies DDL/DML; only
`dpone schema migration apply --execute` mutates the target. V1 executes live
post-apply checks for ClickHouse through `ClickHousePostApplyVerifier`; other
targets return `schema_migration_post_apply.unsupported_target:<sink>` until
their read-only adapters are certified.

Configure post-apply checks in the manifest:

```yaml
sink:
  options:
    physical_design:
      migration:
        post_apply:
          enabled: true
          mode: gate
          profile: prod_strict
          verification:
            ledger_state: true
            physical_design: true
            canary_queries: true
            rollback_window: true
          canaries:
            - id: orders_count_positive
              type: sql
              owner: data-platform
              severity: critical
              query: "SELECT count() > 0 AS ok FROM analytics.orders"
              expect:
                column: ok
                equals: 1
```

Plan the verification after apply:

```bash
dpone schema migration post-apply plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --ledger .dpone/schema-migration/ledger.json \
  --manifest manifests/orders.yaml \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --environment prod \
  --format json \
  --output .dpone/schema-migration/post-apply/orders/post-apply-plan.json
```

The plan uses `dpone.schema_migration_post_apply_plan.v1`. It binds pack,
bundle, ledger, environment, target connection, enabled checks, canary queries,
and rollback metadata. Unsafe canary SQL is rejected before target access;
canaries are SELECT-only in V1 and block on mutating keywords such as `DROP`,
`ALTER`, `INSERT`, `DELETE`, `EXCHANGE`, and `RENAME`.

Run the read-only checks:

```bash
dpone schema migration post-apply run \
  --plan .dpone/schema-migration/post-apply/orders/post-apply-plan.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/post-apply/orders/post-apply-run.json
```

Without `--execute`, `run` emits a dry-run receipt and does not connect to the
target. With `--execute`, `PostApplyVerificationRunner` calls
`PostApplyTargetInspector` for actual physical state and `TargetCanaryExecutor`
for canary queries. ClickHouse uses system table introspection plus read-only
SELECT canaries. The run artifact uses
`dpone.schema_migration_post_apply_run.v1` and records physical drift, canary
results, metrics, and the rollback window.

Certify the result:

```bash
dpone schema migration post-apply certify \
  --run .dpone/schema-migration/post-apply/orders/post-apply-run.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/post-apply/orders/post-apply-certificate.json
```

The certificate uses `dpone.schema_migration_post_apply_certificate.v1` and
returns `verified`, `warning`, or `blocked`. `prod_strict` requires ledger,
physical design, high/critical canary, and rollback-window checks. A failed
critical canary emits `schema_migration_post_apply.canary_failed:<id>`;
physical drift emits `schema_migration_post_apply.physical_drift`; a missing
applied ledger record emits
`schema_migration_post_apply.ledger_applied_record_missing`.

Render a human closeout report:

```bash
dpone schema migration post-apply report \
  --certificate .dpone/schema-migration/post-apply/orders/post-apply-certificate.json \
  --format md \
  --output .dpone/schema-migration/post-apply/orders/post-apply-certificate.md
```

The certificate can be embedded into the review bundle and required by bundle
policy:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --post-apply-certificate .dpone/schema-migration/post-apply/orders/post-apply-certificate.json \
  --output-dir .dpone/schema-migration/review/orders \
  --attest
```

It can also be persisted directly as release evidence:

```bash
dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --post-apply-certificate .dpone/schema-migration/post-apply/orders/post-apply-certificate.json \
  --environment prod \
  --stage verified
```

Recommended production chain:

```text
plan -> impact -> bundle/trust/gate/diff -> rehearse/fixture/certify
-> registry approved/certified -> migration apply --execute
-> post-apply plan/run/certify -> registry verified -> release closeout
```

## Migration release watch

`dpone schema migration watch` is the read-only release watch layer after
post-apply verification. Rehearsal proves that the pack can run in a controlled
target. Post-apply proves that the production target is correct immediately
after `migration apply --execute`. Release watch proves that the same target
stays healthy during a configured watch window under SELECT-only canaries and
target telemetry.

Watch itself is recommend-only. It emits a deterministic decision `continue`,
`extend_watch`, `rollback_recommended`, or `rollback_required`, plus rollback
commands for evidence. Controlled target mutation belongs to
`dpone schema migration remediation`, which requires a pack-bound plan,
approval where configured, `--execute`, and a remediation certificate.

Configure watch in the manifest:

```yaml
sink:
  options:
    physical_design:
      migration:
        watch:
          enabled: true
          mode: gate
          profile: prod_strict
          window:
            duration: 60m
            interval: 5m
            min_successful_samples: 3
            max_failed_samples: 0
          verification:
            post_apply_recheck: true
            physical_design: true
            row_count: true
            canary_queries: true
            query_health: true
            rollback_window: true
          canaries:
            - id: orders_count_positive
              type: sql
              owner: data-platform
              severity: critical
              query: "SELECT count() > 0 AS ok FROM analytics.orders"
              expect:
                column: ok
                equals: 1
          query_health:
            lookback: 15m
            max_error_count: 0
            max_p95_ms: 5000
            max_read_rows: 100000000
          remediation:
            mode: recommend
            rollback_on:
              - critical_canary_failure
              - physical_drift
              - query_health_blocked
              - rollback_window_closing
```

Build the watch plan from a verified post-apply certificate:

```bash
dpone schema migration watch plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --post-apply-certificate .dpone/schema-migration/post-apply/orders/post-apply-certificate.json \
  --manifest manifests/orders.yaml \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --environment prod \
  --format json \
  --output .dpone/schema-migration/watch/orders/watch-plan.json
```

The plan uses `dpone.schema_migration_watch_plan.v1`. It blocks when the
post-apply certificate is for another `pack_id`, when `mode: gate` receives a
non-`verified` post-apply certificate, when canary SQL is unsafe, or when the
target adapter is not certified. `ClickHouseWatchProbe` is the V1 live adapter;
other targets return `schema_migration_watch.unsupported_target:<sink>` until
certified.

Run watch samples:

```bash
dpone schema migration watch run \
  --plan .dpone/schema-migration/watch/orders/watch-plan.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/watch/orders/watch-run.json
```

Without `--execute`, `watch run` emits a dry-run receipt and does not connect to
the target. With `--execute`, `MigrationWatchRunner` executes the configured
sample window. Each `MigrationWatchSample` may re-check physical design, run
data profile checks, execute SELECT-only canaries, read telemetry, and evaluate
the rollback window.

For ClickHouse query health, V1 reads `system.query_log` for the watched table
and configured lookback window. It computes query count, error count, p95 query
duration, max read rows, and max memory usage. If `system.query_log` is not
available, `advisory` and `stage` profiles warn; `prod_strict` and `regulated`
block when `query_health: true`.

Inspect current watch status:

```bash
dpone schema migration watch status \
  --run .dpone/schema-migration/watch/orders/watch-run.json \
  --format table
```

Certify the release watch:

```bash
dpone schema migration watch certify \
  --run .dpone/schema-migration/watch/orders/watch-run.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/watch/orders/watch-certificate.json
```

The certificate uses `dpone.schema_migration_watch_certificate.v1` and returns
`stable`, `warning`, or `blocked`. Failed critical/high canaries emit
`schema_migration_watch.canary_failed:<id>`. Physical drift emits
`schema_migration_watch.physical_drift`. Query health budget failures emit
`schema_migration_watch.query_health_blocked:<metric>`. Rollback-window closure
emits `schema_migration_watch.rollback_window_closing`.

Render a human closeout report:

```bash
dpone schema migration watch report \
  --certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --format md \
  --output .dpone/schema-migration/watch/orders/watch-certificate.md
```

Embed the watch certificate into the SCM review bundle:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --post-apply-certificate .dpone/schema-migration/post-apply/orders/post-apply-certificate.json \
  --watch-certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --output-dir .dpone/schema-migration/review/orders \
  --attest
```

Persist release watch evidence:

```bash
dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --watch-certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --environment prod \
  --stage watched
```

`watched` means the release has a watch certificate. `closed` means the release
has been accepted after watch and no rollback/remediation remains open.

Recommended production chain:

```text
plan -> impact -> bundle/trust/gate/diff -> rehearse/fixture/certify
-> registry approved/certified -> migration apply --execute
-> post-apply plan/run/certify -> registry verified
-> watch plan/run/certify -> optional remediation plan/apply/certify
-> registry watched/remediated/rollback_certified/closed -> release closeout
```

## Backup, snapshot, and restore evidence

`dpone schema migration backup` is the pack-bound backup/restore safety layer
before destructive or physical-layout migration apply. It complements
rehearsal, post-apply verification, release watch, and controlled remediation:

```text
backup plan/create -> restore plan/run -> backup certify
-> bundle build --backup-certificate -> bundle gate
-> remediation can use restore_from_backup if shadow contract removed retained backup
```

Configure it in the manifest:

```yaml
sink:
  options:
    physical_design:
      migration:
        backup:
          enabled: true
          mode: gate
          profile: prod_strict
          strategy: target_native
          require_for:
            - data_destructive
            - shadow_contract
            - physical_layout_change
            - direct_rename
          retention:
            min_days: 7
            delete_after: 30d
          restore_rehearsal:
            enabled: true
            environment: stage
            require_clean_target: true
          clickhouse:
            destination: "Disk('backups', 'dpone/{pack_id}/{table}.zip')"
            incremental: false
```

Build the backup plan. It emits
`dpone.schema_migration_backup_plan.v1` and classifies why backup is required:

```bash
dpone schema migration backup plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --manifest manifests/orders.yaml \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --environment prod \
  --format json \
  --output .dpone/schema-migration/backup/orders/backup-plan.json
```

Create the backup only with explicit execution and approval. Without
`--execute`, `create` emits a dry-run receipt and does not touch the target:

```bash
dpone schema migration backup create \
  --plan .dpone/schema-migration/backup/orders/backup-plan.json \
  --approval approvals/orders-backup.yaml \
  --execute \
  --format json \
  --output .dpone/schema-migration/backup/orders/backup-run.json
```

Restore rehearsal is sandbox/stage only by default. Standalone production
restore is blocked; production restore belongs to `dpone schema migration
remediation apply --execute` with approval.
If the restore plan targets `prod` or `production`, dpone emits
`schema_migration_backup.restore_prod_blocked` so users can route recovery
through controlled remediation instead of running an ungoverned direct restore.

```bash
dpone schema migration backup restore plan \
  --backup-run .dpone/schema-migration/backup/orders/backup-run.json \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --environment stage \
  --format json \
  --output .dpone/schema-migration/backup/orders/restore-plan.json

dpone schema migration backup restore run \
  --plan .dpone/schema-migration/backup/orders/restore-plan.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/backup/orders/restore-run.json
```

Certify and report backup evidence:

```bash
dpone schema migration backup certify \
  --backup-run .dpone/schema-migration/backup/orders/backup-run.json \
  --restore-run .dpone/schema-migration/backup/orders/restore-run.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/backup/orders/backup-certificate.json

dpone schema migration backup report \
  --certificate .dpone/schema-migration/backup/orders/backup-certificate.json \
  --format md \
  --output .dpone/schema-migration/backup/orders/backup-certificate.md
```

The certificate uses `dpone.schema_migration_backup_certificate.v1` and returns
`certified`, `warning`, or `blocked`. `prod_strict` and `regulated` require a
real backup and restore rehearsal. Bundle gates can require
`backup_certificate`, and registry stages `backup_planned`, `backup_created`,
`restore_rehearsed`, and `backup_certified` make the backup lifecycle auditable.

ClickHouse V1 renders native `BACKUP TABLE ... TO ...` and
`RESTORE TABLE ... FROM ... AS ...` operations through `ClickHouseBackupDialect`.
ClickHouse documents native BACKUP/RESTORE and recommends practicing restore,
not assuming backup is enough:
[ClickHouse backup overview](https://clickhouse.com/docs/operations/backup/overview)
and [ClickHouse disk/S3 backup details](https://clickhouse.com/docs/operations/backup/disk).

Industrial comparison:

| System | Pattern | dpone backup behavior |
| --- | --- | --- |
| dlt | Schema contracts/evolution govern runtime schema changes. | Adds target backup/restore proof for physical DDL, not only schema behavior. |
| Airbyte | Refresh can clear or re-sync destination data. | Adds pre-apply backup certificate and restore rehearsal before destructive migration. |
| Fivetran | History Mode/historical sync preserve destination views. | Makes restore readiness explicit, pack-bound, and CI-verifiable. |
| Informatica | Governance and platform/job controls are common. | Uses typed backup contracts instead of raw/manual backup SQL as primary UX. |
| Pentaho | Execute SQL Script can implement custom backup/restore. | Replaces ad hoc scripts with typed plans, approvals, certificates, and registry evidence. |
| SSIS | SSISDB centralizes deployment and execution history. | Adds GitOps backup/restore evidence tied to immutable schema migration packs. |

## Recovery point catalog

`dpone schema migration recovery` indexes certified backup evidence into a
queryable restore-point catalog. Backup certificates prove that one pack has a
backup and restore rehearsal. The recovery catalog answers operational release
questions across packs and environments:

- which restore point is the latest usable point for a target;
- whether an incremental chain still has a usable base;
- whether retention can safely clean up an old backup;
- whether a destructive/layout/direct-rename migration has enough restore
  coverage for the configured RPO/RTO policy;
- whether controlled remediation can use `restore_to_point` after shadow
  `contract` removed the retained shadow table.

Configure recovery in the manifest:

```yaml
sink:
  options:
    physical_design:
      migration:
        recovery:
          enabled: true
          mode: gate
          profile: prod_strict
          require_restore_point_for:
            - data_destructive
            - shadow_contract
            - physical_layout_change
            - direct_rename
          policy:
            max_rpo_seconds: 900
            require_restore_rehearsal: true
            require_chain_verification: true
            allow_incremental_without_base: false
            allow_expired_restore_point: false
          catalog:
            store_backend: sqlite
            store_uri: .dpone/schema-migration/recovery.sqlite3
          clickhouse:
            incremental:
              enabled: true
              base_selection: latest_usable
```

Record a backup certificate as a cataloged restore point:

```bash
dpone schema migration recovery point record \
  --backup-certificate .dpone/schema-migration/backup/orders/backup-certificate.json \
  --environment prod \
  --store-backend sqlite \
  --store-uri .dpone/schema-migration/recovery.sqlite3 \
  --format json \
  --output .dpone/schema-migration/recovery/orders/recovery-point.json
```

Read the latest usable restore point:

```bash
dpone schema migration recovery point latest \
  --target clickhouse.analytics.orders \
  --environment prod \
  --profile prod_strict \
  --store-backend sqlite \
  --store-uri .dpone/schema-migration/recovery.sqlite3 \
  --format json
```

Verify a full/incremental chain before a restore or release gate:

```bash
dpone schema migration recovery chain verify \
  --restore-point-id sha256:... \
  --require-restore-rehearsal \
  --store-uri .dpone/schema-migration/recovery.sqlite3 \
  --format json \
  --output .dpone/schema-migration/recovery/orders/chain-verification.json
```

Standalone restore remains non-prod by default. Production restore routes
through controlled remediation and requires approval plus `--execute`:

```bash
dpone schema migration recovery restore plan \
  --restore-point-id sha256:... \
  --target-connection .dpone/connections/clickhouse-stage.json \
  --environment stage \
  --store-uri .dpone/schema-migration/recovery.sqlite3 \
  --format json \
  --output .dpone/schema-migration/recovery/orders/restore-plan.json

dpone schema migration recovery restore run \
  --plan .dpone/schema-migration/recovery/orders/restore-plan.json \
  --execute \
  --format json \
  --output .dpone/schema-migration/recovery/orders/restore-run.json

dpone schema migration recovery restore certify \
  --restore-run .dpone/schema-migration/recovery/orders/restore-run.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/recovery/orders/restore-certificate.json
```

Plan retention cleanup without deleting anything:

```bash
dpone schema migration recovery retention plan \
  --target clickhouse.analytics.orders \
  --environment prod \
  --store-uri .dpone/schema-migration/recovery.sqlite3 \
  --format md
```

Recovery artifacts use these schema versions:
`dpone.schema_migration_recovery_point.v1`,
`dpone.schema_migration_recovery_catalog.v1`,
`dpone.schema_migration_recovery_chain_verification.v1`,
`dpone.schema_migration_recovery_restore_plan.v1`,
`dpone.schema_migration_recovery_restore_run.v1`,
`dpone.schema_migration_recovery_restore_certificate.v1`, and
`dpone.schema_migration_recovery_retention_plan.v1`.

ClickHouse V1 uses native BACKUP/RESTORE. Full backups start a new recovery
chain. Incremental backups reference a cataloged base restore point and render
`SETTINGS base_backup = Disk(...)`; chain verification blocks when the base is
missing, expired, blocked, or lacks required restore rehearsal evidence. V1 PITR
means a named cataloged recovery point before or after a migration event.
Arbitrary timestamp PITR is enabled only for future target dialects that can
prove transactional or time-log restore semantics.

Industrial comparison:

| System | Pattern | dpone recovery behavior |
| --- | --- | --- |
| dlt | Schema contracts/evolution decide whether schemas evolve/freeze/discard. | Adds restore-point evidence and recovery gates for physical DDL/data risk. |
| Airbyte | Refresh can reset/re-sync historical data. | Makes restore readiness explicit, rehearsed, pack-bound, and CI-verifiable before production mutation. |
| Fivetran | History Mode and platform logs preserve history/audit context. | Adds pre-apply restore-point selection, chain verification, and remediation integration. |
| Informatica | Enterprise governance often relies on platform runbooks. | Uses typed, portable recovery contracts instead of manual restore runbooks. |
| Pentaho | Execute SQL Script and Metadata Injection can script backup/restore. | Replaces ad hoc restore scripts with cataloged chains, policy gates, and retention plans. |
| SSIS | SSISDB centralizes deployment/execution history. | Adds GitOps recovery evidence tied to schema migration packs. |
| Liquibase/Flyway | Migration history and rollback are first-class concerns. | Extends rollback discipline into physical/data restore-point chains and rehearsal evidence. |

## Controlled rollback and remediation

`dpone schema migration remediation` is the canonical production rollback path
after release watch recommends or requires rollback. It does not replace the
legacy lightweight `schema migration rollback` receipt; it adds a governed
controlled remediation control plane with explicit capability classification,
approval preconditions, target execution evidence, and a certificate that can be
bundled and recorded in the evidence registry.

Configure remediation in the manifest:

```yaml
sink:
  options:
    physical_design:
      migration:
        remediation:
          enabled: true
          mode: gate
          profile: prod_strict
          execution:
            require_approval: true
            require_watch_certificate: true
            require_target_fingerprint: true
            require_lock: true
          rollback:
            strategy: controlled
            allow_after_contract: false
            retain_backup_required: true
          certify:
            physical_design: true
            canary_queries: true
            row_count: true
            rollback_window: true
```

Build the remediation plan from the exact migration pack, watch certificate,
ledger, manifest, and target connection:

```bash
dpone schema migration remediation plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --watch-certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --backup-certificate .dpone/schema-migration/backup/orders/backup-certificate.json \
  --ledger .dpone/schema-migration/ledger.json \
  --manifest manifests/orders.yaml \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --environment prod \
  --format json \
  --output .dpone/schema-migration/remediation/orders/remediation-plan.json
```

The plan uses `dpone.schema_migration_remediation_plan.v1`. In `gate` mode it
accepts a watch certificate only when the same `pack_id` is `blocked` with
`rollback_required`, or `warning` with `rollback_recommended`. It then
classifies rollback capability:

| Capability | Meaning |
| --- | --- |
| `online_safe` | Pack contains rollback DDL such as `ALTER TABLE ... RESET SETTING`. |
| `shadow_exchange` | Shadow cutover happened and retained backup can be swapped back. |
| `drop_shadow` | Shadow table exists before cutover and can be cleaned up. |
| `manual_only` | dpone can explain the state but will not execute mutation. |
| `unsupported` | Pack has no supported rollback contract. |
| `blocked_after_contract` | Shadow `contract` already removed retained backup evidence. |
| `restore_from_backup` | Shadow retained backup is gone, but a certified `backup_certificate` provides restore operations. |

When retained backup evidence is gone after shadow `contract`, the plan blocks
with `schema_migration_remediation.blocked_after_contract` unless a certified
`backup_certificate` for the same `pack_id` is supplied. dpone will not invent
a destructive rollback path.

Apply the plan only after approval. Without `--execute`, the command emits a
dry-run receipt and does not mutate the target:

```bash
dpone schema migration remediation apply \
  --plan .dpone/schema-migration/remediation/orders/remediation-plan.json \
  --approval approvals/orders-rollback.yaml \
  --execute \
  --format json \
  --output .dpone/schema-migration/remediation/orders/remediation-run.json
```

V1 certified execution is ClickHouse. `shadow_exchange` renders
`EXCHANGE TABLES actual AND retained_backup`; online-safe table-setting rollback
uses the rollback DDL stored in the pack, for example
`ALTER TABLE ... RESET SETTING index_granularity`. Other targets keep the same
ports and return `schema_migration_remediation.unsupported_target:<sink>` until
their dialects are certified.

Certify and report the remediation:

```bash
dpone schema migration remediation certify \
  --run .dpone/schema-migration/remediation/orders/remediation-run.json \
  --target-connection .dpone/connections/clickhouse-prod.json \
  --profile prod_strict \
  --format json \
  --output .dpone/schema-migration/remediation/orders/remediation-certificate.json

dpone schema migration remediation report \
  --certificate .dpone/schema-migration/remediation/orders/remediation-certificate.json \
  --format md \
  --output .dpone/schema-migration/remediation/orders/remediation-certificate.md
```

The certificate uses `dpone.schema_migration_remediation_certificate.v1` and
returns `certified`, `warning`, or `blocked`. `prod_strict` and `regulated`
require an executed `remediated` run; `advisory` may turn a dry-run certificate
into a warning for local review.

Bundle and persist remediation evidence:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --watch-certificate .dpone/schema-migration/watch/orders/watch-certificate.json \
  --remediation-certificate .dpone/schema-migration/remediation/orders/remediation-certificate.json \
  --output-dir .dpone/schema-migration/review/orders \
  --attest

dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --remediation-certificate .dpone/schema-migration/remediation/orders/remediation-certificate.json \
  --environment prod \
  --stage remediated
```

Registry stage `remediation_planned` means rollback/remediation was planned;
`remediated` means target mutation ran; `rollback_certified` means the
post-rollback certificate is available.

## Evidence bundle and SCM review

`dpone schema migration bundle` creates one provider-neutral review artifact
for GitHub, GitLab, Bitbucket, Azure DevOps, Jenkins, Argo CD, or any other
CI/CD system. V1 does not write PR/MR comments directly. SCM owns branch
protection, PR/MR approvals, environments, and deploy jobs; dpone owns pack
consistency, artifact integrity, schema/physical/impact evidence, and apply
gates.

Build an attested bundle from the reviewed migration pack plus optional impact,
environment, certification, rehearsal certificate, promotion, and approval
artifacts:

```bash
dpone schema migration bundle build \
  --pack .dpone/schema-migration/orders.pack.json \
  --impact .dpone/schema-impact/orders.impact.json \
  --environment-contract .dpone/environments/orders.yaml \
  --certificate .dpone/schema-migration/orders.stage.cert.json \
  --rehearsal-certificate .dpone/schema-migration/rehearsal/orders/rehearsal-certificate.json \
  --fixture-build .dpone/schema-migration/rehearsal/orders/fixture-build.json \
  --quality-profile .dpone/schema-migration/rehearsal/orders/profile-after.json \
  --promotion .dpone/schema-migration/orders.stage-to-prod.promotion.json \
  --approval approvals/orders-prod.yaml \
  --attest \
  --output-dir .dpone/schema-migration/review/orders \
  --format json
```

The command writes `.dpone/schema-migration/review/orders/bundle.json` with
schema version `dpone.schema_migration_bundle.v1`. The artifact includes
`bundle_id`, `pack_id`, target, artifact digests, blockers, warnings,
recommendations, summary, and optional `bundle_digest` attestation. The bundle
is deterministic and can be verified without database access.

Verify the bundle offline in CI:

```bash
dpone schema migration bundle verify \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --require-attestation \
  --format json \
  --output .dpone/schema-migration/review/orders/verification.json
```

The verification artifact uses schema version
`dpone.schema_migration_bundle_verification.v1`. It recomputes artifact
digests, reruns cross-artifact checks, verifies attestation when requested, and
exits non-zero when the bundle is blocked or internally inconsistent.

Build signed provenance for the verified bundle:

```bash
dpone schema migration bundle attest \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --output .dpone/schema-migration/review/orders/provenance.json \
  --signing-key-env DPONE_BUNDLE_SIGNING_KEY \
  --signing-key-id ci-hmac \
  --provenance-source github_actions \
  --repository https://github.com/acme/data-platform \
  --commit-sha "$GITHUB_SHA" \
  --ref "$GITHUB_REF" \
  --run-id "$GITHUB_RUN_ID" \
  --workflow ".github/workflows/schema-migration.yml" \
  --protected-ref \
  --format json
```

`bundle build --attest` proves bundle artifact integrity. `bundle attest`
adds SLSA/in-toto-style provenance: repository, ref, commit, workflow, CI run,
subject digest, and optional `HMAC-SHA256` local/dev signature. For public
identity-backed release trust, publish GitHub Artifact Attestations or
Sigstore/Cosign receipts as external evidence; dpone imports those receipts
offline rather than calling SCM APIs directly.

Verify the provenance against a trust policy:

```bash
dpone schema migration bundle trust verify \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --provenance .dpone/schema-migration/review/orders/provenance.json \
  --policy .dpone/schema-migration/trust-policy.prod.yaml \
  --signing-key-env DPONE_BUNDLE_SIGNING_KEY \
  --format json \
  --output .dpone/schema-migration/review/orders/trust-verification.json
```

Trust policy artifacts use schema version
`dpone.schema_migration_trust_policy.v1`. Provenance artifacts use
`dpone.schema_migration_provenance.v1`; trust receipts use
`dpone.schema_migration_trust_verification.v1` and return `trusted`,
`warning`, or `blocked`. A production policy can require allowed repositories,
allowed refs, protected refs, commit SHA, CI run metadata, allowed signature
key ids, and external attestation kinds.

Evaluate release policy for a PR/MR or deploy stage:

```bash
dpone schema migration bundle gate \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --profile prod_strict \
  --target-environment prod \
  --trust-verification .dpone/schema-migration/review/orders/trust-verification.json \
  --format json \
  --output .dpone/schema-migration/review/orders/bundle-gate.json
```

`bundle verify` and `bundle gate` intentionally answer different questions.
`bundle verify` proves integrity: SHA-256 digests, attestation, artifact drift,
and cross-artifact consistency. `bundle trust verify` proves trusted
provenance: the bundle was produced from an allowed repository/ref/commit/run
and signed or externally attested when policy requires it. `bundle gate` proves
release policy: enough evidence exists for `advisory`, `pr_review`,
`stage_certified`, `prod_strict`, or `regulated` flows. The gate receipt uses
schema version
`dpone.schema_migration_bundle_gate.v1` and returns `allowed`, `warning`, or
`blocked`. Use `--policy` with `dpone.schema_migration_bundle_policy.v1` when a
team needs a custom profile; CLI `--target-environment` overrides the policy
file for deployment jobs.

Built-in profile summary:

| Profile | Use case | Main requirements |
| --- | --- | --- |
| `advisory` | Local/dev review | Bundle is verifiable; missing optional evidence is a warning. |
| `pr_review` | PR/MR required check | Attestation, impact plan, and approval for impact-required risks. |
| `stage_certified` | Stage after apply | Attestation, impact plan, environment contract, and certification. |
| `prod_strict` | Production deploy | Attestation, impact, approval, environment contract, certification, promotion, optional `rehearsal_certificate` by custom policy, and no warnings. |
| `regulated` | High-governance release | `prod_strict` plus approval actor and non-expired approval checks. |

Compare an approved/base bundle with the current candidate:

```bash
dpone schema migration bundle diff \
  --base .dpone/schema-migration/review/orders-main/bundle.json \
  --head .dpone/schema-migration/review/orders-pr/bundle.json \
  --head-gate .dpone/schema-migration/review/orders-pr/bundle-gate.json \
  --require-attestation \
  --format md \
  --output .dpone/schema-migration/review/orders/diff.md
```

`bundle diff` uses schema version `dpone.schema_migration_bundle_diff.v1`.
It compares normalized migration pack, impact, approval, environment,
certification, promotion, gate, and bundle metadata evidence. Status is
`same`, `changed`, `breaking`, or `blocked`. Use this as the reviewer-first
artifact in PR/MR comments: it explains what changed and which high/critical
items require attention. The command is offline and never opens a database or
SCM connection.

The bundle commands have separate responsibilities. In short:
**verify vs gate vs diff vs review** means integrity check, release policy,
review delta, and full review rendering; `trust verify` is the provenance and
signature gate between integrity and release policy.

| Command | Question answered |
| --- | --- |
| `bundle verify` | Are bundle artifacts intact and pack-bound? |
| `bundle trust verify` | Was this bundle produced by trusted CI/repository/ref evidence? |
| `bundle gate` | Is there enough evidence for this release profile? |
| `bundle diff` | What changed between approved/base and current/head evidence? |
| `review render` | What full Markdown review can be posted to PR/MR? |

## Evidence registry and audit catalog

`dpone schema migration registry` persists review/release evidence after bundle
verification, trust verification, gate evaluation, and optional diff review.
It is intentionally separate from the migration ledger:

- migration ledger answers what was applied to the target;
- evidence registry answers what was reviewed, trusted, gated, approved, and
  available for audit.

This registry vs ledger boundary keeps target mutation facts separate from
review, approval, trust, and promotion evidence.

Record one reviewed bundle:

```bash
dpone schema migration registry record \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --gate .dpone/schema-migration/review/orders/bundle-gate.json \
  --trust .dpone/schema-migration/review/orders/trust-verification.json \
  --diff .dpone/schema-migration/review/orders/diff.json \
  --environment prod \
  --stage approved \
  --store-backend sqlite \
  --store-uri .dpone/schema-migration/registry.sqlite3 \
  --format json
```

V1 store backends:

| Backend | Use case |
| --- | --- |
| `local_json` | Default artifact store for PR jobs and simple local review. |
| `sqlite` | Shared-runner and long-lived audit store with indexed queries. |

Read history and latest approved evidence:

```bash
dpone schema migration registry history \
  --target clickhouse.analytics.orders \
  --environment prod \
  --format table

dpone schema migration registry latest \
  --target clickhouse.analytics.orders \
  --environment prod \
  --stage approved \
  --format json
```

Render an audit report for release notes, incident review, or compliance export:

```bash
dpone schema migration registry audit-report \
  --target clickhouse.analytics.orders \
  --from 2026-01-01 \
  --to 2026-06-22 \
  --format md \
  --output .dpone/schema-migration/review/orders/audit.md
```

`registry record` verifies the bundle with `MigrationBundleVerifier`, validates
that `gate`, `trust`, and `diff` receipts point to the same `pack_id` and
`bundle_id`, derives SCM metadata from trust evidence when present, computes a
stable `record_id`, and appends idempotently. Re-recording the same `record_id`
is a no-op; a different record for the same environment, stage, `pack_id`, and
`bundle_id` blocks with `evidence_registry.record_conflict`.

`registry history`, `registry latest`, and `registry audit-report` never open a
target database or call SCM APIs. `--verify-artifacts` is reserved for slower
future integrity rechecks; default queries are fast catalog reads.

Render a PR/MR-friendly Markdown review:

```bash
dpone schema migration review render \
  --bundle .dpone/schema-migration/review/orders/bundle.json \
  --format md \
  --output .dpone/schema-migration/review/orders/review.md
```

The review payload uses schema version `dpone.schema_migration_review.v1`.
`review.md` is safe to upload as a CI artifact, paste into a PR/MR, or publish
through native SCM comment actions. Direct GitHub/GitLab/Bitbucket API writers
can be added later over the same bundle/review contracts.

Apply in prod with environment guardrails:

```bash
dpone schema migration apply \
  --plan .dpone/schema-migration/orders.pack.json \
  --environment prod \
  --environment-contract .dpone/environments/orders.yaml \
  --promotion .dpone/schema-migration/orders.stage-to-prod.promotion.json \
  --bundle-gate .dpone/schema-migration/review/orders/bundle-gate.json \
  --execute \
  --format json
```

When `--environment` is used, dpone resolves the ledger and target connection
from the environment contract. If the environment policy requires previous
certification and the promotion receipt is missing or stale, `apply` blocks
with `migration_promotion.promotion_required`,
`migration_promotion.pack_id_mismatch`, or
`migration_promotion.environment_mismatch`.
If `--bundle-gate` is provided, `apply` also blocks when the gate receipt is
`blocked` or when its `pack_id` does not match the migration pack.

Recommended PR/MR flow:

1. In the feature branch, run `dpone schema migration plan` and commit or upload
   the pack as a CI artifact.
2. In PR/MR CI, run `dpone schema impact plan` and `dpone schema impact gate`.
3. Run `dpone schema migration bundle build`,
   `dpone schema migration bundle verify`, `dpone schema migration bundle gate`,
   and `dpone schema migration bundle diff`; then run
   `dpone schema migration review render`; upload `bundle.json`,
   `verification.json`, `bundle-gate.json`, `diff.md`, and `review.md`.
4. On merge to the stage branch/environment, run `apply`, then `verify-env` and
   `certify`; publish the certification artifact.
5. For prod, require repository approvals and an explicit promotion approval
   artifact; then run `promote`.
6. Production deploy uses `apply --environment prod --promotion ...` and blocks
   if the promoted pack is not the exact pack reviewed earlier.

Apply a shadow pack one phase at a time. Without `--execute`, this is
ledger-only apply for review systems. With `--execute`, dpone executes the SQL
operations for that phase and records `operation_status` evidence in the
ledger:

```bash
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase prepare --execute --target-connection .dpone/targets/clickhouse.json
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase create_shadow --execute --target-connection .dpone/targets/clickhouse.json
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase backfill --execute --target-connection .dpone/targets/clickhouse.json
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase validate --execute --target-connection .dpone/targets/clickhouse.json
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase cutover --execute --target-connection .dpone/targets/clickhouse.json
dpone schema migration apply --plan .dpone/schema-migration/orders.shadow-pack.json --phase contract --execute --target-connection .dpone/targets/clickhouse.json
```

Read migration history:

```bash
dpone schema migration history \
  --ledger .dpone/schema-migration/ledger.json \
  --format table
```

Rollback only a reversible pack:

```bash
dpone schema migration rollback \
  --plan .dpone/schema-migration/orders.pack.json \
  --ledger .dpone/schema-migration/ledger.json \
  --approval approvals/orders-rollback.yaml \
  --format md
```

V1 is intentionally conservative and explicit. `apply` defaults to ledger-only
mode for review systems and dry release gates. Target side effects require both
`--execute` and `--target-connection`; otherwise the command blocks with
`migration.target_connection_required`. This keeps accidental DDL out of normal
review workflows while allowing the CLI to execute approved DDL/DML when an
operator chooses the execution path.

## Shadow migration and cutover

Shadow migration is the governed path for drift that `physical-diff` classifies
as `shadow_required`: engine, partition expression, order/sorting key, primary
key, and selected unsafe physical column layout changes. The default remains
`block`; shadow migration is opt-in through the manifest or the CLI:

```yaml
sink:
  options:
    physical_design:
      migration:
        strategy: shadow
        shadow:
          cutover:
            mode: atomic_exchange
            retain_backup: true
          validation:
            row_count: true
            typed_hash: true
            duplicate_key: true
            sample_rows: 10000
          backfill:
            batch_size: 100000
            max_workers: 4
```

CLI `--strategy shadow` overrides the manifest for release and CI workflows.
`dpone run` does not start shadow migration automatically; runtime remains
fail-closed and points operators to `dpone schema migration plan --strategy
shadow`.

`--strategy online_safe` is narrower than “apply whatever changed”. It asks the
target migration dialect to emit only changes it marks as safe for online ALTER.
For ClickHouse V1 this means selected mutable MergeTree settings such as
`min_rows_for_wide_part`; create-time or unknown settings remain blockers even
though they may be valid in `CREATE TABLE ... SETTINGS`.

The shadow pack contains ordered phases:

| Phase | Purpose |
| --- | --- |
| `prepare` | Verifies fingerprint, migration lock, and atomic cutover capability. |
| `create_shadow` | Creates the new table with desired physical design. |
| `backfill` | Copies rows with explicit column projection into the shadow table. |
| `validate` | Emits row-count, typed-hash, duplicate-key, and nullability checks. |
| `cutover` | Swaps the original and shadow table names. |
| `contract` | Drops the retained old table when rollback is no longer needed. |

For ClickHouse V1, cutover uses `EXCHANGE TABLES` because ClickHouse documents
it as an atomic swap for supported database engines. Backfill uses explicit
`INSERT INTO shadow (...) SELECT ... FROM actual` projection. `RENAME` is not
the default cutover primitive because ClickHouse documents it separately from
atomic table exchange.

Rollback is supported only before `contract` when the old table is retained:
before cutover the rollback operation can drop the shadow table; after cutover
and before contract it exchanges tables back. After contract dpone blocks with
`migration.rollback_not_supported_after_contract`.

ClickHouse references:

- [EXCHANGE TABLES](https://clickhouse.com/docs/sql-reference/statements/exchange)
- [Atomic database engine](https://clickhouse.com/docs/engines/database-engines/atomic)
- [INSERT INTO SELECT](https://clickhouse.com/docs/sql-reference/statements/insert-into)
- [RENAME statement](https://clickhouse.com/docs/sql-reference/statements/rename)

## Schema identity in migration packs

When `sink.options.schema_identity.enabled: true`, `dpone schema migration plan`
embeds `identity_decisions` in the migration pack. Confirmed renames no longer
look like `drop old + add new`.

Default rename strategy is `expand_contract`: add canonical column, backfill
from alias, validate equality, and contract the old alias later. `runtime_alias`
produces projection-only evidence. `direct_rename` is an unsafe escape hatch and
emits a downstream-impact warning because consumers may still reference the old
column name.

## Algorithms

`plan`:

1. Loads the manifest.
2. Builds desired physical design with the existing `physical-plan` planner.
3. Optionally loads actual physical target state.
4. Runs `physical-diff` when `--actual` is provided.
5. Computes stable desired and actual fingerprints.
6. When `--strategy online_safe` or `--strategy shadow` is selected, runs
   physical reconciliation in auto-safe planning mode so dialect-approved
   setting drift appears as DDL instead of a blocker.
7. When `--strategy shadow` is selected, converts eligible physical blockers
   into ordered `phases` with target-specific SQL operations.
8. Emits `MigrationPack` with `pack_id`, strategy, changes, blockers, warnings,
   DDL, phases, and rollback metadata.
9. Returns exit code `0` even when the pack contains blockers, because planning
   succeeded and the blockers are evidence for review/apply gates.

`baseline`:

1. Loads desired plan and actual state.
2. Computes the actual fingerprint.
3. Appends a `baseline` record to the artifact ledger.
4. Does not mutate user tables.

`apply`:

1. Reads a `MigrationPack`.
2. When `--environment` is set, resolves ledger/target connection from the
   environment contract and runs the promotion gate before target mutation.
3. Optionally compares current `--actual` with the pack `actual_fingerprint`.
4. Blocks on fingerprint mismatch, promotion mismatch, or pack blockers.
5. For phased packs, requires `--phase` and enforces phase order from the
   artifact ledger.
6. In ledger-only mode, appends an `applied` or `phase_applied` record only for
   accepted packs.
7. In `--execute` mode, builds a `MigrationOperationExecutor` from
   `--target-connection` and executes DDL/DML before writing the ledger record.
   ClickHouse V1 uses the runtime `ClickHouseMigrationOperationExecutor`
   adapter.
8. Records `operation_status` evidence for every executed SQL operation:
   operation name, operation type, status, SQL text, row count, and result
   fingerprint.
   Large validation result sets are not copied into the ledger; dpone records
   `row_count`, `result_fingerprint`, and `result_truncated: true` instead.
9. Executes validation operations during the `validate` phase. Sequential
   validation pairs must match; a final unpaired validation must return no
   rows. Mismatches block with `migration.validation_mismatch`, and non-empty
   final validations block with `migration.validation_not_empty`.
10. Replaying an already applied phase is an idempotent no-op: the command
   returns `phase_already_applied` and does not append a duplicate ledger record.

`MigrationOperationExecutor` is a thin target execution port. It does not
classify risk and does not render SQL. It only executes SQL already present in
the reviewed `MigrationPack`. This keeps plan, policy, rendering, execution,
and ledger responsibilities separate.

`history` reads the artifact ledger and renders the audit trail.

`rollback` only proceeds when the pack explicitly says rollback is supported.
Non-reversible packs fail with `migration.rollback_not_supported`.

## Evidence shape

The pack schema is `dpone.schema_migration_pack.v1`. Important fields:

| Field | Meaning |
| --- | --- |
| `pack_id` | Stable checksum over target, fingerprints, changes, blockers, and DDL. |
| `desired_fingerprint` | Hash of desired manifest-derived physical plan. |
| `actual_fingerprint` | Hash of normalized actual state when `--actual` is provided. |
| `changes` | Normalized schema/physical drift decisions. |
| `blockers` | Reasons the pack cannot be applied automatically. |
| `ddl` | Generated DDL evidence. |
| `strategy` | `block`, `online_safe`, or `shadow`. |
| `phases` | Ordered shadow migration phase graph for phased packs. |
| `rollback` | Whether rollback is supported and which DDL would be used. |

Executed ledger records include `operations`. Each operation has
`operation_status` semantics through the `status` field:

```json
{
  "name": "backfill_shadow_table",
  "operation_type": "sql",
  "status": "executed",
  "sql": "INSERT INTO ... SELECT ...",
  "row_count": 0,
  "result_fingerprint": "sha256:..."
}
```

Environment-bound ledger records may also include:

| Field | Meaning |
| --- | --- |
| `environment` | Environment where the pack was applied or phase-applied. |
| `promotion_id` | Promotion receipt that authorized the environment apply. |
| `certification_id` | Optional environment certification receipt reference. |

Environment reports use
`dpone.schema_migration_environment_report.v1`, certifications use
`dpone.schema_migration_environment_certification.v1`, and promotion receipts
use `dpone.schema_migration_promotion.v1`.

Public JSON Schemas for CI validation:

- [Environment contract](schemas/schema-migration/environments.schema.json)
- [Environment certification](schemas/schema-migration/environment-certification.schema.json)
- [Promotion approval](schemas/schema-migration/promotion-approval.schema.json)
- [Promotion receipt](schemas/schema-migration/promotion.schema.json)
- [Rehearsal plan](schemas/schema-migration/rehearsal-plan.schema.json)
- [Rehearsal run](schemas/schema-migration/rehearsal-run.schema.json)
- [Rehearsal certificate](schemas/schema-migration/rehearsal-certificate.schema.json)
- [Fixture plan](schemas/schema-migration/fixture-plan.schema.json)
- [Fixture build](schemas/schema-migration/fixture-build.schema.json)
- [Data profile](schemas/schema-migration/data-profile.schema.json)
- [Post-apply plan](schemas/schema-migration/post-apply-plan.schema.json)
- [Post-apply run](schemas/schema-migration/post-apply-run.schema.json)
- [Post-apply certificate](schemas/schema-migration/post-apply-certificate.schema.json)
- [Watch plan](schemas/schema-migration/watch-plan.schema.json)
- [Watch run](schemas/schema-migration/watch-run.schema.json)
- [Watch certificate](schemas/schema-migration/watch-certificate.schema.json)
- [Remediation plan](schemas/schema-migration/remediation-plan.schema.json)
- [Remediation run](schemas/schema-migration/remediation-run.schema.json)
- [Remediation certificate](schemas/schema-migration/remediation-certificate.schema.json)
- [Recovery point](schemas/schema-migration/recovery-point.schema.json)
- [Recovery catalog](schemas/schema-migration/recovery-catalog.schema.json)
- [Recovery chain verification](schemas/schema-migration/recovery-chain-verification.schema.json)
- [Recovery restore plan](schemas/schema-migration/recovery-restore-plan.schema.json)
- [Recovery restore run](schemas/schema-migration/recovery-restore-run.schema.json)
- [Recovery restore certificate](schemas/schema-migration/recovery-restore-certificate.schema.json)
- [Recovery retention plan](schemas/schema-migration/recovery-retention-plan.schema.json)
- [Schema contract consumer lineage](schemas/schema-migration/schema-contract-consumer-lineage.schema.json)
- [Schema contract consumer test kit](schemas/schema-migration/schema-contract-consumer-test-kit.schema.json)
- [Schema contract consumer certification](schemas/schema-migration/schema-contract-consumer-certification.schema.json)
- [Schema contract compatibility view plan](schemas/schema-migration/schema-contract-compatibility-view-plan.schema.json)
- [Schema contract compatibility view gate](schemas/schema-migration/schema-contract-compatibility-view-gate.schema.json)
- [Schema contract compatibility view report](schemas/schema-migration/schema-contract-compatibility-view-report.schema.json)
- [Schema contract adoption plan](schemas/schema-migration/schema-contract-adoption-plan.schema.json)
- [Schema contract adoption status](schemas/schema-migration/schema-contract-adoption-status.schema.json)
- [Schema contract retirement gate](schemas/schema-migration/schema-contract-retirement-gate.schema.json)
- [Schema contract retirement plan](schemas/schema-migration/schema-contract-retirement-plan.schema.json)
- [Bundle policy](schemas/schema-migration/bundle-policy.schema.json)
- [Bundle gate](schemas/schema-migration/bundle-gate.schema.json)
- [Bundle diff](schemas/schema-migration/bundle-diff.schema.json)
- [Schema migration trust policy](schemas/schema-migration/schema-migration-trust-policy.schema.json)
- [Schema migration provenance](schemas/schema-migration/schema-migration-provenance.schema.json)
- [Schema migration trust verification](schemas/schema-migration/schema-migration-trust-verification.schema.json)
- [Evidence registry record](schemas/schema-migration/evidence-registry-record.schema.json)
- [Evidence registry](schemas/schema-migration/evidence-registry.schema.json)
- [Evidence registry query](schemas/schema-migration/evidence-registry-query.schema.json)
- [Evidence audit report](schemas/schema-migration/evidence-audit-report.schema.json)

## Industrial comparison

| System | Pattern | dpone behavior |
| --- | --- | --- |
| dlt | Schema evolution, pipeline state, and load traces. | Keeps inference/evolution ergonomics, then adds physical layout packs, executable rehearsal certificates, governed cutover, and queryable migration evidence. |
| Airbyte | Managed non-breaking schema propagation, connection timeline, and audit logging. | Keeps clear breaking/non-breaking UX, but adds immutable target fingerprints, pre-prod rehearsal proof, phase apply, rollback-before-contract, and offline evidence registry records. |
| Fivetran | Managed schema settings, platform connector logs, and schema/table change events. | Keeps self-service schema controls while exposing physical DDL evidence, rehearsal certification, approvals, provenance, and promotion chain before apply. |
| Informatica | Enterprise lineage, impact analysis, Operational Insights, command utilities, dynamic mappings, and data validation patterns. | Provides typed phase contracts, executable rehearsal validation, registry evidence, and portable audit reports instead of raw SQL as primary UX. |
| Pentaho | DDL/DML often lives in SQL script steps; Operations Mart aggregates execution history. | Keeps load/DDL separation but replaces ad hoc scripts with deterministic packs, rehearsal runs, dialects, ledgers, and evidence catalog reports. |
| SSIS | SSISDB catalog centralizes deployments, environments, execution history, and troubleshooting. | Borrows catalog/history discipline and adds GitOps evidence, rehearsal certificates, checksums, trust, schema impact, and provider-neutral registry queries. |
| Liquibase | Changelog table, locks, preconditions, update/rollback, and checksums. | Borrows ledger/fingerprint discipline and extends it from executed changes to reviewed/trusted/gated/promoted evidence. |
| Flyway | Schema history table, migration status, and checksums. | Borrows audit/checksum discipline and adds physical-design drift context plus pre-apply evidence registry. |
| GitHub/GitLab/Bitbucket | Branch protections, PR/MR approvals, environments, artifacts, and deploy jobs. | Keeps SCM as the review/deploy orchestrator while dpone provides deterministic pack, certification, promotion, and apply-gate artifacts. |

## Best practices

- Run `dpone schema migration plan` in CI before changing manifests that affect
  target physical design.
- Store migration packs with release evidence.
- Promote the same `pack_id` across dev, stage, and prod; do not rebuild packs
  per environment.
- Use repository approvals for production and bind them to
  `dpone.schema_migration_promotion_approval.v1`.
- Use `baseline` when adopting existing production tables.
- Pass `--actual` to `apply` whenever possible; fingerprint mismatch is the
  main guardrail against applying stale review artifacts.
- Treat `rollback_not_supported` as normal for many analytical DDL changes; plan
  shadow/expand-contract workflows instead.
- Use `--strategy shadow` for ClickHouse layout drift when `physical-diff`
  reports `shadow_required`.
- Do not use shadow migration as a row-transform feature. V1 supports identity,
  deterministic cast, nullable-new-column, and default-safe projections only.
