---
title: dpone documentation
hide:
  - toc
---

<div class="dpone-hero" markdown>
<div class="dpone-eyebrow">Open-source batch ELT framework - Current source version v0.75.0</div>

# Build reliable data pipelines without hiding the machinery.

dpone is a production-oriented Python framework for moving data between databases, APIs, Kafka, and analytical targets with explicit state, staging-first loads, schema evolution, reconciliation, quality gates, and operational artifacts.

<div class="dpone-actions" markdown>
[Start quickstart](getting-started/quickstart.md){ .md-button .md-button--primary }
[Install dpone](getting-started/installation.md){ .md-button }
[Browse source -> sink guides](source-sink-matrix.md){ .md-button }
</div>
</div>

<div class="dpone-strip" markdown>
<div class="dpone-pill">Postgres</div>
<div class="dpone-pill">MSSQL</div>
<div class="dpone-pill">ClickHouse</div>
<div class="dpone-pill">BigQuery</div>
<div class="dpone-pill">Kafka</div>
<div class="dpone-pill">REST APIs</div>
</div>

## What dpone is built for

<div class="dpone-grid" markdown>
<div class="dpone-card" markdown>
### Staging-first loading
Every database sink uses staging or shadow-table flows before target promotion, so heavy writes are kept away from final tables until commit time.
</div>

<div class="dpone-card" markdown>
### Explicit state
XMin, Kafka offsets, CDC offsets, run state, and source cursors can be persisted in supported state backends instead of disappearing into logs.
</div>

<div class="dpone-card" markdown>
### Schema evolution
Safe additions and widening are automated by default. Breaking type changes can fail fast or route into `__dpone__nc__*` generated columns.
</div>

<div class="dpone-card" markdown>
### Operational UX
Doctor, plan, run reports, quality gates, state inspection, connector certification, and performance advice are first-class workflows.
</div>
</div>

## Fast paths

| Need | Start here |
| --- | --- |
| Install and smoke-test dpone | [Installation](getting-started/installation.md) |
| Run dpone with native MSSQL/ClickHouse tools in Docker | [Runtime Docker image](runtime-image.md) |
| Run the first manifest | [Quickstart](getting-started/quickstart.md) |
| Execute from CLI or Python | [Running pipelines](run.md) |
| Try a local database pipeline | [First local pipeline](getting-started/first-local-pipeline.md) |
| Create the first Airflow DAG preview | [First Airflow DAG](getting-started/first-airflow-dag.md) — Start from your route (source → sink) |
| Operate bounded stale runtime Pod cleanup | [Airflow runtime Pod retention](airflow-runtime-pod-retention.md) |
| Publish the first contracted dbt model | [First dbt mart](dbt-inline-publishing.md) |
| Configure database/API/Kafka credentials | [Connections and credentials](connections.md) |
| Choose a pipeline combination | [Source -> sink matrix](source-sink-matrix.md) |
| Pick append/upsert/replace semantics | [Load strategies](load-strategies.md) |
| Split nested JSON into root/child tables | [Nested normalization](nested-normalization.md) |
| Understand type conversion | [Type mapping matrix](type-mapping-matrix.md) |
| Control automatic type detection | [Type inference](type-inference.md) |
| Declare explicit column contracts | [Schema contracts](schema-contracts.md) |
| Manage planned renames and aliases | [Schema Identity](schema-identity.md) |
| Gate migration packs by downstream impact | [Schema impact](schema-impact.md) |
| Tune target DDL, indexes, compression, and storage | [Physical design](physical-design.md) |
| Enforce row contracts and quarantine bad rows | [Runtime data contracts](data-contract-runtime.md) |
| Keep contracts safe on streaming/native fast paths | [Streaming-safe contracts](runtime-fast-path-contracts.md) |
| Apply physical DDL safely | [Physical DDL apply](physical-ddl-apply.md) |
| Package schema and physical DDL changes into migration evidence, including shadow cutover, environment promotion, and PR/MR review bundles | [Schema migration control](schema-migration-control.md) |
| Use production-safe defaults | [Production profiles](production-profiles.md) |
| Bundle run evidence for certification | [Unified run evidence](unified-run-evidence.md) |
| Decide whether a minor/major release can ship | [Release evidence](release-evidence.md) |
| Use PostgreSQL as source, sink, or state | [PostgreSQL guide](postgres.md) |
| Use SQL Server as source, sink, or state | [MSSQL guide](mssql.md) |
| Use BigQuery as analytical sink or state backend | [BigQuery guide](bigquery.md) |
| Use ClickHouse as analytical source or sink | [ClickHouse guide](clickhouse.md) |
| Use bounded Kafka batch source/sink | [Kafka guide](kafka.md) |
| Use Postgres transaction-ID incremental extraction | [Postgres XMin](postgres-xmin.md) |
| Export Prometheus and OpenTelemetry runtime metrics | [Runtime observability](observability.md) |
| Gate data product assertions, freshness, access/privacy, volume, latency, consumers, incidents, error budgets, policy waivers and fleet release freeze evidence | [Data Product SLO, assertions and incidents](data-product-slo.md) |
| Turn blocked data product gates into safe owner-routed repair plans and controlled execution receipts | [Data Product Remediation Runbooks](data-product-remediation.md) |
| Operate certification, recovery, reconciliation, deployment, and catalog evidence | [Operational control plane](operational-control-plane.md) |
| Prove connectors with local-live/real-local/vendor-live gates | [Live certification](live-certification.md) |
| Stage large files through S3/GCS/Azure | [Object storage staging](object-storage-staging.md) |
| Certify route capabilities before source IO | [Route capability certification](route-capability-certification.md) |
| Produce SBOM/provenance/signing evidence | [Supply-chain evidence](supply-chain.md) |
| Run certification, contracts, quarantine, rollback, and marketplace controls | [`dpone ops`](ops-cli.md) |
| Prepare for production operations | [Production readiness](production-readiness.md) |
| Understand CI/CD and release automation | [CI/CD](ci-cd.md) |

## Install

```bash
pip install dpone
dpone --version
dpone -v
pip install "dpone[postgres,mssql,clickhouse,kafka,gcp,s3,azure,pandas,vault]"
```

## Local documentation preview

```bash
python3 -m pip install -r docs/requirements.txt
mkdocs serve
```

The GitHub Pages workflow builds the same site with `mkdocs build --strict` on every docs pull request and deploys from `master`.
