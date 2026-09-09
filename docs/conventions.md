# Conventions

This page defines naming and behavioral conventions used across `dpone`.

## Public package identity

- PyPI distribution: `dpone`
- Python import: `dpone`
- CLI entrypoint: `dpone`
- GitHub repository: `PaulKov/dpone`

## Source and sink names

Use lowercase names with explicit system identifiers:

```yaml
source:
  type: postgres
  connection_id: postgres_oltp

sink:
  type: mssql
  connection_id: mssql_dwh
```

## Table naming

Recommended landing names are stable and explicit:

```text
<layer>__<source_system>__<resource>
```

Examples:

- `landing__postgres__orders`
- `landing__mssql__customers`
- `landing__api__exchange_rates`
- `landing__kafka__orders_events`

## Framework namespace

The `__dpone__*` prefix is reserved for framework-generated columns and objects.

Current conventions:

- `__dpone__nc__<column>` - generated column for incompatible schema evolution when `on_type_change: new_column` is enabled.
- `__dpone__run_id` - optional run identifier column.
- `__dpone__loaded_at` - optional ingestion timestamp column.

Do not create business columns that start with `__dpone__`.

## Strategy names

Use canonical strategy names in manifests:

- `full_refresh`
- `incremental_append`
- `incremental_merge`
- `replace`
- `partition_replace`
- `snapshot_reconciliation`
- `xmin`
- `cdc`

See [Load strategies](load-strategies.md) for behavior and source/sink support.


## Documentation file names

Published documentation files and directories use lowercase kebab-case:

```text
docs/load-strategies.md
docs/source-sink/postgres-to-mssql.md
docs/getting-started/quickstart.md
```

Rules:

- Use lowercase letters, numbers, and hyphens only for published Markdown paths.
- Use `.md` lowercase extension.
- Prefer stable descriptive names over acronyms-only names when adding new pages.
- Keep ecosystem convention files such as root `README.md`, `CHANGELOG.md`, `LICENSE`, and the excluded GitHub-facing `docs/README.md` unchanged.
- Update `mkdocs.yml`, Markdown links, tests, and workflow references in the same change when renaming a page.

This keeps GitHub Pages URLs stable across case-sensitive filesystems, shell tooling, and external links.

## Documentation language

Public documentation is written in English. Internal or historical notes may exist outside the published MkDocs navigation, but anything in `mkdocs.yml` should be OSS-ready and English-only.
