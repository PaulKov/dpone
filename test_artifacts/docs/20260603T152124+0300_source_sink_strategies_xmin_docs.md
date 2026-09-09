# dpone documentation coverage artifact: source/sink, load strategies, XMin

- Artifact id: 20260603T152124+0300_source_sink_strategies_xmin_docs
- Date: 2026-06-03 15:21:24 +0300
- Operator: Codex
- Repository path: <workspace>/dpone
- Scope: documentation and package metadata coverage for MSSQL extra, source -> sink guides, type mappings, load strategies, and Postgres XMin strategy.

## Implemented documentation scope

- Added public README coverage for `dpone[mssql]` and `dpone[kafka]` optional extras.
- Added source -> sink documentation matrix for 25 combinations across Postgres, MSSQL, ClickHouse, REST/API, and Kafka sources and MSSQL, Postgres, ClickHouse, BigQuery, and Kafka sinks.
- Added type mapping matrix across supported source -> sink dialect families.
- Added canonical load strategy guide covering `full_refresh`, `incremental_append`, `incremental_merge`/upsert, and `replace`.
- Added detailed Postgres XMin incremental extraction runbook with manifests, state backends, algorithm, recovery, and operational caveats.
- Added per-flow strategy examples and links from combo docs back to the canonical strategy guide.
- Linked XMin guidance from Postgres source flow docs, state docs, CDC docs, production readiness docs, README, and docs index.

## Files added or expanded

- `docs/SOURCE_SINK_MATRIX.md`
- `docs/TYPE_MAPPING_MATRIX.md`
- `docs/LOAD_STRATEGIES.md`
- `docs/POSTGRES_XMIN.md`
- `docs/source_sink/*.md` with 25 per-flow guides
- `README.md`
- `docs/README.md`
- `docs/MSSQL.md`
- `docs/STATE.md`
- `docs/CDC.md`
- `docs/PRODUCTION_READINESS.md`
- `pyproject.toml`

## Verification results

| Check | Command | Result |
| --- | --- | --- |
| Source/sink guide count | `find docs/source_sink -maxdepth 1 -type f -name '*.md' \| wc -l` | 25 docs |
| Documentation placeholder scan | `rg -n 'undefined|\\[object Object\\]|FIXME|TODO|source -> sync|sync docs' ...` | no matches |
| Package extras metadata | `uv run python - <<'PY' ...` | `mssql` and `kafka` extras present; `full` includes `pyodbc` and `confluent-kafka` |
| Ruff lint | `uv run ruff check .` | passed |
| Ruff format check | `uv run ruff format --check .` | passed, 728 files already formatted |
| Type check | `uv run mypy --config-file mypy.ini` | passed, no issues in 161 source files |
| Non-live tests | `uv run pytest -m "not integration_live" -q` | passed, exit code 0 |
| Build | `uv build` | passed, built `dist/dpone-0.1.0.tar.gz` and `dist/dpone-0.1.0-py3-none-any.whl` |
| Twine validation | `uvx twine check dist/*` | passed for wheel and sdist |

## Result

The public documentation now explicitly covers MSSQL extras, MSSQL source/sink usage, all documented source -> sink combinations, per-flow strategy examples, cross-system type mapping policy, and the Postgres XMin strategy as a self-service operational runbook.
