# Schema impact and dependency contract

`schema_impact` is an opt-in pre-apply gate for schema, DDL, physical-design,
identity, and shadow-migration packs. It answers one release question before
`dpone schema migration apply`: who can be affected, which risks need approval,
and whether the migration is allowed to continue.

It complements existing contracts:

- `schema evolution` decides whether the source and target schemas are
  compatible.
- `schema identity` decides whether a name change is a semantic rename.
- `physical-diff` decides whether target physical state drift exists.
- `schema migration` packages the final migration evidence.
- `schema impact` explains downstream blast radius and gates apply.

## Configuration

```yaml
sink:
  options:
    schema_impact:
      enabled: true
      mode: gate                 # observe | gate
      unknown_dependency: warn   # warn | block
      empty_graph: warn          # allow | warn | block

      approval:
        required_for:
          - compatibility_breaking
          - data_destructive
          - direct_rename
          - shadow_cutover

      sources:
        manifests: true
        dbt_manifest: target/manifest.json
        openlineage: .dpone/lineage/latest/openlineage.json
        manual:
          consumers:
            - id: finance.daily_margin
              type: dashboard
              owner: finance-analytics
              reads:
                - dataset: clickhouse.analytics.orders
                  columns: [amount, customer_id]

      owners:
        default: data-platform
```

If `schema_impact` is absent, dpone behaves exactly as before. If it is enabled,
`mode: gate` is the default.

## Commands

Build an impact plan from a reviewed migration pack:

```bash
dpone schema impact plan \
  --pack .dpone/schema-migration/orders.pack.json \
  --manifest manifests/orders.yaml \
  --format json \
  --output .dpone/schema-impact/orders.impact.json
```

Evaluate the gate:

```bash
dpone schema impact gate \
  --pack .dpone/schema-migration/orders.pack.json \
  --impact .dpone/schema-impact/orders.impact.json \
  --approval approvals/orders.yaml \
  --format text
```

When `schema_impact.enabled: true`, `dpone schema migration plan` embeds
`impact_summary` into the migration pack. `dpone schema migration apply` checks
that summary before target mutation.

## Risk taxonomy

| Risk tag | Meaning | Examples |
| --- | --- | --- |
| `compatibility_breaking` | A consumer contract can break, even if data is not deleted. | Type narrowing, `nullable -> not nullable`, public drop/rename, incompatible schema change. |
| `data_destructive` | Data, history, or rollback capability can be removed. | `DROP COLUMN`, `DROP TABLE`, `TRUNCATE`, shadow `contract`, deleting a deprecated alias. |
| `direct_rename` | Target rename without expand-contract alias lifecycle. | `ALTER TABLE ... RENAME COLUMN ...`. |
| `shadow_cutover` | Production table name switches to the shadow table after backfill and validation. | ClickHouse `EXCHANGE TABLES actual AND shadow`. |

`data_destructive` is also treated as `compatibility_breaking`. `shadow_cutover`
is separate because it is a production switch: the table name stays stable, but
the physical object behind it changes.

## Approval artifact

```yaml
pack_id: sha256:...
impact_plan_id: sha256:...
approved_by: finance-data-owner
approved_at: 2026-06-21T12:00:00Z
expires_at: 2026-06-28T12:00:00Z
approved_risks:
  - compatibility_breaking
  - shadow_cutover
```

Approvals are bound to both `pack_id` and `impact_plan_id`. A changed pack or
changed dependency graph invalidates the approval.

## How impact planning works

1. Load the immutable `MigrationPack`.
2. Extract changed subjects: columns, table layout, identity rename,
   `__dpone__nc__` expansion, shadow phases, and destructive contract actions.
3. Load dependency evidence from the current manifest, manual config, dbt
   manifest, OpenLineage artifact, and future provider ports.
4. Normalize identifiers to dataset and column refs.
5. Match changed subjects to direct and indirect consumers.
6. Classify severity and required risk approvals.
7. Emit `impact_plan_id`, impacted consumers, blockers, warnings, owner routing,
   and recommendations.

## Recommended CI/CD flow

```bash
dpone schema migration plan --manifest manifests/orders.yaml --format json --output .dpone/schema-migration/orders.pack.json
dpone schema impact plan --pack .dpone/schema-migration/orders.pack.json --manifest manifests/orders.yaml --format json --output .dpone/schema-impact/orders.impact.json
dpone schema impact gate --pack .dpone/schema-migration/orders.pack.json --impact .dpone/schema-impact/orders.impact.json --approval approvals/orders.yaml
dpone schema migration apply --plan .dpone/schema-migration/orders.pack.json --approval approvals/orders.yaml
```

Use `mode: observe` during adoption to collect evidence without blocking. Switch
production manifests to `mode: gate` when ownership and approval routing are in
place.

## Industrial comparison

| System | Pattern | dpone behavior |
| --- | --- | --- |
| dlt | Schema evolution and contracts handle added columns and type variants. See [dlt schema evolution](https://dlthub.com/docs/general-usage/schema-evolution). | Adds migration-pack impact evidence and pre-apply approval gates. |
| Airbyte | Schema changes can propagate, require review, or stop syncs. See [Airbyte schema change management](https://docs.airbyte.com/platform/using-airbyte/schema-change-management). | Keeps review UX, but ties downstream impact to immutable migration packs. |
| Fivetran | Schema handling modes and logs provide managed governance. See [schema config API](https://fivetran.com/docs/rest-api/api-reference/connection-schema) and [schema change logs](https://fivetran.com/docs/logs/troubleshooting/track-schema-changes). | Adds pre-change blast-radius evidence and pack-bound approvals. |
| Informatica | Enterprise catalog and dynamic mapping patterns support impact governance. | Provides typed contract-as-code and deterministic CLI evidence. |
| Pentaho | Metadata injection and SQL script steps allow dynamic DDL workflows. See [metadata injection](https://docs.pentaho.com/pdia-data-integration/pdi-transformation-steps-reference-overview/etl-metadata-injection) and [Execute SQL Script](https://docs.pentaho.com/pdia-data-integration/pdi-transformation-steps-reference-overview/execute-sql-script-cp). | Replaces ad hoc SQL control with typed dependency providers and guardrails. |
