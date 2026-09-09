# dbt to dpone self-service demo

This project demonstrates the authoring part of the beginner journey with five
terms: a **dbt model** selects a platform-owned **publish profile** and
**workflow**. In the production design, governed CI turns certified input into
an Airflow **DAG** and promotes the same immutable release across dev and prod
**environments**.

## Run the executable demo

This is an authoring demo. It validates resolved dbt metadata and deterministic
manifest handling without a database; it does not compile a production release
or certify MSSQL-to-ClickHouse, Airflow/Kubernetes runtime, or Cosmos
coexistence.

From the repository root:

```bash
uv sync --extra dbt-mssql
bash examples/dbt-inline-publishing/run_demo.sh
```

The source-checkout path requires Python `3.11` or `3.12` and `uv`. The locked
`dbt-mssql` extra installs dbt Core `1.12.3` and `dbt-sqlserver` `1.11.1`.
For fixture-only validation, dpone is still required but dbt is optional:

```bash
DPONE_DBT_DEMO_MODE=fixture bash examples/dbt-inline-publishing/run_demo.sh
```

The script never connects to a database. Normal runs write nothing into the
example tree: they use a temporary directory and remove it on exit. Maintainers
can deliberately refresh the checked-in manifest through the real parse
producer:

```bash
DPONE_DBT_DEMO_MODE=parse \
DPONE_DBT_DEMO_MANIFEST_OUTPUT=examples/dbt-inline-publishing/fixtures/manifest.v12.json \
bash examples/dbt-inline-publishing/run_demo.sh
```

The output option refuses fixture mode and symlink targets. Review the
generated diff; never hand-edit the manifest.

- When dbt Core and `dbt-sqlserver` are installed, it copies the project to a
  temporary directory, installs the local macro package there, performs a real
  `dbt parse`, and validates that generated manifest.
- When either dependency is missing, it clearly falls back to
  `fixtures/manifest.v12.json`, the deterministic offline fixture used by
  repository tests.

Force either path:

```bash
DPONE_DBT_DEMO_MODE=parse bash examples/dbt-inline-publishing/run_demo.sh
DPONE_DBT_DEMO_MODE=fixture bash examples/dbt-inline-publishing/run_demo.sh
```

`parse` fails with an actionable message if the SQL Server adapter is missing;
it never silently uses the fixture. The script prefers the repository
`.venv/bin` tools, then tools on `PATH`, then `uv run`.

The first production certification target is dbt Core `1.12.3` with
`dbt-sqlserver` `1.11.1`. A different locally installed version can exercise
the parse path, but does not become production-certified.

The example's `dbt_project.yml` contains the required literal SQL Server
adapter policy:

```yaml
flags:
  dbt_sqlserver_enable_safe_type_expansion: false
  dbt_sqlserver_use_dbt_transactions: true
  dbt_sqlserver_use_default_schema_concat: true
  dbt_sqlserver_use_native_string_types: true

models:
  dpone_dbt_demo:
    +as_columnstore: false
    +indexes: []
    +drop_unmanaged_indexes: false
    +prefer_single_alter_column: false
```

Do not replace these booleans with strings, Jinja, or environment variables,
and do not omit the explicit safe model defaults.
The incremental example also declares `incremental_strategy='merge'` and an
identifier-only composite `unique_key`; implicit adapter strategies and key
expressions are outside this preview policy. Both key columns are exact
enforced-contract columns with structural `not_null` constraints. Their
`data_tests: [not_null]` remain separate runtime assurance.

The checked-in SQL Server fixture includes the adapter's `javascript` macro
language extension. The official dbt v12 base schema lists only `sql` and
`python`, so the fixture path reports
`DPONE_DBT_MANIFEST_ADAPTER_SCHEMA_EXTENSION` as a compatibility warning. The
manifest still passes dpone's strict structural and publishing validation; the
warning is neither a certification result nor a suppressed schema failure.
Expected fixture-mode success ends with:

```text
dbt -> dpone publish: PASS
manifest schema: v12
published models: 2; workflows: 1
models:
- model.dpone_dbt_demo.competitive_pricing
- model.dpone_dbt_demo.competitive_pricing_history
- DAG__pricing__competitive_pricing__refresh: 2 model(s)
- WARNING DPONE_DBT_MANIFEST_ADAPTER_SCHEMA_EXTENSION: ...
Demo PASS: validated 2 publish-enabled models in 1 workflow.
```

Here `PASS` means only that `dpone dbt check` accepted the authoring fixture.
The demo does not configure the project-level `dpone.yaml`
`capability_discovery.certification_evidence` authority, and it never runs the
production-strict `dpone dbt compile`, an environment-owned runtime-evidence
producer, or the protected dev evidence finalizer.

## The two-command author loop

Prepare dbt and local package dependencies once. Then, from a normal dbt
project, the author loop is:

```bash
dbt deps  # only when packages.yml/dependencies.yml exists or changes
git add package-lock.yml  # commit the updated lock with the declaration
dbt parse
dpone dbt check .
```

`dbt parse` produces `target/manifest.json`. `dpone dbt check` discovers it and
validates the project flags, pinned adapter config/macro boundary, workflow
model ownership, selected graph, and publishing metadata without invoking dbt
again.
This local check does not resolve a deployment binding or prove runtime
preflight identity. Production performs those checks before `dbt build`; see
`docs/dbt-self-service-runtime-identity.md` and
`docs/dbt-self-service-errors.md`.
When package declarations exist, dpone also requires the generated and
committed `package-lock.yml`, verifies its dbt declaration hash, and requires the
resolved `packages-install-path`; it never runs `dbt deps` or uses the network
on the author's behalf. This example does not declare a package in its checked-in
source; `run_demo.sh` adds the repository-local package only to a temporary copy.

## What the author edits

- model SQL and `config.meta.dpone.publish`;
- model contracts and tests in `models/schema.yml`;
- normal dbt project metadata.

Keep each workflow's materialized upstream closure disjoint. Do not add
top-level `dispatch`, change or shadow execution-critical dbt/dbt-sqlserver
macros, or use unlisted adapter config. A selected model/test cannot call an
arbitrary custom macro. Model constraints remain empty; column constraints are
empty or `not_null` only, and merge-key columns require `not_null`. Express
other assertions as data or unit tests. Under eager selection, every model read
by a data test must remain inside the same workflow closure.

The smallest publishing declaration names:

```yaml
meta:
  dpone:
    publish:
      enabled: true
      profile: mssql_to_clickhouse_mart
      workflow: competitive_pricing
```

Platform owners manage the publish profile and workflow in
`dpone/dbt-publish-profiles.yml`. The optional `packages/dbt-dpone` macro only
returns the same metadata dictionary; direct `meta` remains the public contract.
Astronomer Cosmos is not installed or required by this demo.

The SQL uses `dpone_data_interval_start` and `dpone_data_interval_end` dbt vars
with deterministic local defaults. Airflow supplies the real interval for a
runtime execution, so a replay does not use wall-clock time.

## What CI and Airflow do

After the author opens one merge request, production-strict CI can create one
environment-neutral immutable release only when current project-authorized
route evidence reports `PASS` at `production-certified` or
`enterprise-certified` level. The dev and prod environments bind that same
release digest to different deployment identities. The generated DAG runs dbt
build/tests before the two publishing models; the model transfers may run in
parallel within workflow policy.

The checked-in publish profile is sample policy, not certification authority.
Capability discovery reads route evidence only from the bounded
`capability_discovery.certification_evidence` section of the consuming
project's `dpone.yaml`; missing, stale, skipped, mocked, or foreign-commit
evidence remains `UNVERIFIED`.

Do not commit `target`, logs, installed dbt packages, rendered profiles,
generated DAGs, releases, project bundles, or evidence. The local `.gitignore`
protects the common generated paths.

Continue with the
[five-minute tutorial](../../docs/dbt-inline-publishing.md), the
[promotion guide](../../docs/dbt-self-service-promotion.md), and the
[operations runbook](../../docs/dbt-self-service-runbook.md).
