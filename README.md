# dpone

[![PyPI](https://img.shields.io/pypi/v/dpone.svg)](https://pypi.org/project/dpone/)
[![Python](https://img.shields.io/pypi/pyversions/dpone.svg)](https://pypi.org/project/dpone/)
[![CI](https://github.com/PaulKov/dpone/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/PaulKov/dpone/actions/workflows/ci.yml)
[![Docs](https://github.com/PaulKov/dpone/actions/workflows/pages.yml/badge.svg?branch=master)](https://paulkov.github.io/dpone/)
[![License](https://img.shields.io/pypi/l/dpone.svg)](LICENSE)

`dpone` is a Python ETL framework for declarative, YAML-driven data pipelines. It helps data teams describe sources, sinks, load strategies, dependencies, conventions, and operational checks as reusable configuration instead of one-off scripts.

The public package name, import name, GitHub repository name, and CLI name are all intentionally short: `dpone`.

Repository: https://github.com/PaulKov/dpone

Public install:

```bash
python3 -m pip install dpone
dpone --help
```

## First Airflow DAG

Create and verify a provider-loadable DAG without writing Airflow Python or
configuring credentials:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental
dpone check orders_daily
dpone airflow preview orders_daily
dpone test orders_daily
```

The five commands are the complete offline beginner journey. A live, bounded
sample is a separate platform-gated action. See
[First Airflow DAG](docs/getting-started/first-airflow-dag.md).

## Documentation map

Start here if you are evaluating or operating dpone:

- [Documentation index](docs/README.md)
- [CLI reference](docs/cli-reference.md)
- [Running pipelines from CLI and Python](docs/run.md)
- [Connector overview](docs/connectors.md)
- [Source -> sink matrix](docs/source-sink-matrix.md)
- [Manual integration matrix](docs/testing/manual-integration-matrix.md)
- [CI/CD](docs/ci-cd.md)
- [Testing runbooks](docs/testing/index.md)
- [Runtime observability](docs/observability.md)
- [Runtime decision audit](docs/runtime-decision-audit.md)
- [Object storage staging](docs/object-storage-staging.md)
- [Airflow artifact trust (fail-closed preview)](docs/airflow-artifact-trust.md)
- [Airflow exact-cache operations and recovery](docs/airflow-cache-sync.md)
- [Airflow Kubernetes cache deployment](docs/airflow-cache-kubernetes-deployment.md)
- [Airflow runtime Pod retention](docs/airflow-runtime-pod-retention.md)
- [dbt self-service platform workflows](docs/dbt-self-service-platform-workflows.md)
- [Supply-chain evidence](docs/supply-chain.md)
- [Release evidence](docs/release-evidence.md)
- [Load strategies](docs/load-strategies.md)
- [Nested normalization](docs/nested-normalization.md)
- [Load lineage](docs/load-lineage.md)
- [Type mapping matrix](docs/type-mapping-matrix.md)
- [Schema evolution](docs/schema-evolution.md)
- [Production readiness](docs/production-readiness.md)
- [Architecture](docs/architecture.md)


## What dpone gives you

- YAML manifests for single-process and batch ETL definitions.
- Built-in DAG/dependency inspection for pipeline debugging.
- Runtime abstractions for sources, sinks, connectors, state, reconciliation, and safe SQL logging.
- Optional integrations for PostgreSQL, MSSQL/SQL Server, ClickHouse, BigQuery/GCS, Kafka, pandas, Google Ads, and HashiCorp Vault.
- A CLI designed for self-service validation, rendering, explainability, and documentation checks.
- Compatibility shims for older import paths while the canonical package layout continues to stabilize.

## Installation

Install the core package from PyPI:

```bash
pip install dpone
dpone --version
dpone -v
```

Install common extras for local ETL development:

```bash
pip install "dpone[postgres,mssql,clickhouse,kafka,gcp,s3,azure,pandas,vault]"
```

Install everything currently published by the project:

```bash
pip install "dpone[full]"
```

With `uv`:

```bash
uv add dpone
uv add "dpone[full]"
```

Install the Kubernetes SDK only in the restricted platform environment that
runs Airflow Connection Secret GC or runtime-Pod retention:

```bash
pip install "dpone[kubernetes]"
```

## Optional extras

| Extra | Purpose |
| --- | --- |
| `postgres` | PostgreSQL connectivity via `psycopg` |
| `mssql` | Microsoft SQL Server connectivity via `pyodbc`; production bulk paths use external ODBC Driver 18 and `bcp` |
| `clickhouse` | ClickHouse connectivity |
| `gcp` | Google BigQuery and Google Cloud Storage support |
| `s3` | AWS S3 object storage staging support via `boto3` |
| `azure` | Azure Blob Storage plus workload identity via `azure-storage-blob` and `azure-identity` |
| `object_storage` | S3, GCS, and Azure Blob staging, including Azure workload identity |
| `kafka` | Kafka batch source/sink support via `confluent-kafka`, Schema Registry codecs, Avro, JSON Schema, and Protobuf helpers |
| `pandas` | DataFrame-based extract/load helpers |
| `vault` | HashiCorp Vault integration via public `vault-kv-client` |
| `kubernetes` | Metadata-only, namespace-scoped Airflow Connection Secret GC and runtime-Pod retention for platform operators |
| `google_ads` | Google Ads API support |
| `full` | All public extras above |

Vault support uses [`vault-kv-client`](https://github.com/PaulKov/vault-kv-client), published on PyPI as `vault-kv-client`. dpone imports the canonical `vault_kv_client` module only; legacy Vault import paths are not supported by dpone.

Credentials can come from environment variables, Airflow Connections,
HashiCorp Vault-compatible KV, or inline params for smoke tests. See
[Connections and credentials](docs/connections.md) for copy-paste examples for
Postgres, MSSQL, ClickHouse, BigQuery, Kafka, and REST API.

## Quick start

Create a batch manifest, for example [examples/batch/landing_postgres_to_bq.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_postgres_to_bq.batch.yaml):

```yaml
# yaml-language-server: $schema=../../src/dpone/schema/etl-batch-manifest.schema.json
kind: dpone.batch.v1
convention: landing_raw_v1
registry: ../registry/sources.yaml

vars:
  src_system: demo_source
  src_database: demo_db
  owner_team: data-platform
  owner_contact: data-platform@example.com
  sla: daily

defaults:
  source:
    type: postgres
    connection_type: vault
    connection_id: postgres-demo
    vault_path: postgres/demo-source
    options:
      batch_size: 100000
      export_format: csv

  sink:
    type: bigquery
    connection_type: vault
    connection_id: bigquery-demo
    vault_path: gcp/demo-project-prod/bq/service-account
    staging:
      schema: stg
    strategy:
      mode: full_refresh
      overwrite_type: exchange

schemas:
  public:
    tables:
      - core_city
```

Validate and render it:

```bash
dpone manifest validate examples/batch/landing_postgres_to_bq.batch.yaml \
  --profile landing_raw_v1 \
  --registry examples/registry/sources.yaml

dpone manifest render examples/batch/landing_postgres_to_bq.batch.yaml \
  --selector public.core_city \
  --registry examples/registry/sources.yaml
```

Inspect pipeline dependencies:

```bash
dpone dag report examples/batch/landing_postgres_to_bq.batch.yaml \
  --base-path . \
  --format json \
  --preset ci \
  --registry examples/registry/sources.yaml
```

## CLI overview

```bash
dpone --help
dpone manifest --help
dpone dag --help
dpone docs --help
```

Common commands:

```bash
dpone manifest list examples/batch/landing_postgres_to_bq.batch.yaml
dpone manifest validate examples/batch/landing_postgres_to_bq.batch.yaml --recursive
dpone manifest render examples/batch/landing_postgres_to_bq.batch.yaml --selector public.core_city
dpone manifest explain examples/batch/landing_postgres_to_bq.batch.yaml --selector public.core_city --why sink.table.schema
dpone dag list-edges examples/batch/landing_postgres_to_bq.batch.yaml --with-groups --with-refs
dpone dag explain-node examples/batch/landing_postgres_to_bq.batch.yaml --task public.core_city
dpone dag report examples/batch/landing_postgres_to_bq.batch.yaml --preset ci --format md
```

## Repository layout

```text
src/dpone/      Python package source code
docs/           User and developer documentation
examples/       Public example manifests and registries
tests/          Unit and integration tests
tools/          Local smoke and release helper scripts
```

Canonical imports live under:

- `dpone.manifest.*`
- `dpone.dag.*`
- `dpone.runtime.*`
- `dpone.contracts.*`
- `dpone.ports.*`
- `dpone.adapters.*`

Legacy paths such as `dpone.core.*`, `dpone.lib.*`, `dpone.source.*`, and `dpone.sink.*` are compatibility shims. Prefer canonical imports for new code.

## Local development

```bash
uv sync --all-extras
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
```

Build package artifacts:

```bash
uv build
```

Run the package smoke script from an installed environment:

```bash
python3 tools/package_smoke.py --project-root . --dpone-cmd dpone
```

## CI and releases

The OSS repository uses GitHub Actions as the primary automation path. See [CI/CD](docs/ci-cd.md) for the workflow map, detailed runbooks, artifacts, and developer guidance.

Key workflows:

- `.github/workflows/ci.yml` runs linting, formatting checks, type checks, tests, coverage, package build, and PostgreSQL XMin integration.
- `.github/workflows/pages.yml` builds and deploys the GitHub Pages documentation site from `master`.
- `.github/workflows/release.yml` builds and publishes tagged releases to PyPI.
- [.github/workflows/integration-matrix.yml](https://github.com/PaulKov/dpone/blob/master/.github/workflows/integration-matrix.yml) and [.github/workflows/connector-certification.yml](https://github.com/PaulKov/dpone/blob/master/.github/workflows/connector-certification.yml) provide manual/scheduled production-confidence gates.

Release tags use the format `vX.Y.Z`, for example:

```bash
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

Tagged releases use PyPI Trusted Publishing exclusively. The ordinary release
workflow never reads a PyPI API token. The
[OIDC-only release controller](docs/release.md) is the sole publisher: it
rebuilds only the immutable release tag and verifies the public archive bytes
after publication.

## Security

Never commit API tokens, PyPI tokens, GitHub tokens, Vault credentials, service-account JSON, or live vendor credentials. If a secret is ever pasted into an issue, chat, commit, or CI log, revoke it before publishing or pushing public history.

See [Security policy](SECURITY.md) for the vulnerability reporting process.


## License

`dpone` is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
