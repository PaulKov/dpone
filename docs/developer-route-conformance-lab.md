# Developer guide: Route Conformance Lab

This guide explains how to extend Route Conformance Lab without coupling it to
specific database connectors or turning CLI handlers into route logic.

## Architecture Boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.route_conformance.RouteConformanceService` | Facade that composes route profile lookup, deterministic dataset generation, exact verification, policy scoring, and report writing. |
| `dpone.ops.routes.conformance_models` | Stable JSON/Markdown contracts for dataset profiles, snapshots, verification results, conformance reports, summaries, and release gates. |
| `dpone.ops.routes.conformance_dataset` | `SyntheticRouteDatasetFactory` and deterministic wide/nested fixture generation. |
| `dpone.ops.routes.conformance_verifier` | `RouteConformanceVerifier` for row counts, chunk typed hashes, whole-dataset typed hashes, physical contract drift, and mismatch samples. |
| `dpone.ops.routes.conformance_policy` | `RouteConformancePolicy` for fail-closed blockers, warnings, score, status, and next actions. |
| `dpone.ops.routes.conformance_rendering` | Shared JSON/Markdown file rendering helpers. |
| `dpone.ops.route_conformance_live.RouteConformanceLiveService` | Live runner facade that composes dataset generation, source seeding, route execution, snapshot reading, exact verification, and live report writing. |
| `dpone.ops.routes.conformance_live_ports` | `RouteConformanceSourceSeeder`, `RouteConformanceSchemaEvolutionApplier`, `RouteConformanceRouteExecutor`, `RouteConformanceSnapshotReader`, and the combined live adapter protocol. |
| `dpone.ops.routes.conformance_live_adapters` | Credential-free `in_memory` live adapter and default adapter registry for `in_memory`, `vendor_live`, and `docker`. |
| `dpone.ops.routes.conformance_vendor_live` | `VendorRouteConformanceLiveAdapter`, `VendorLiveRouteBinding`, `RouteConformanceLiveStore`, and `MemoryRouteConformanceLiveStore` for route-binding-driven vendor-live tests. |
| `dpone.ops.routes.conformance_vendor_sql` | Thin Docker/vendor-live factory that wires env-driven connectors to route bindings. |
| `dpone.ops.routes.conformance_vendor_sql_store` | Lazy SQL store plus Postgres, MSSQL, and ClickHouse dialects. It imports no runtime connectors directly. |
| `dpone.ops.catalog_route_conformance` | Dependency injection factory for default conformance services. |
| `dpone.commands.ops_parsers_route_conformance` | Argument parsing only. |
| `dpone.services.ops.command_handlers_route_conformance` | Handler glue only: build inputs, call a service, emit JSON/Markdown, return exit code. |

`RouteProfileCatalog` is the source for route existence. Do not duplicate
`source -> sink -> strategy` support rules in the service, policy, or CLI.

## Extension Rules

- Do not add route-specific logic to CLI handlers.
- Do not open database connections from `RouteConformanceService`.
- Do not mutate runtime state, source offsets, sink tables, or manifests.
- Keep synthetic dataset generation deterministic. A profile must always produce
  the same rows, columns, physical contracts, and fingerprint.
- Keep verifier logic generic: compare snapshots and contracts, not connector
  internals.
- Keep live adapters behind `RouteConformanceSourceSeeder`,
  `RouteConformanceSchemaEvolutionApplier`, `RouteConformanceRouteExecutor`,
  and `RouteConformanceSnapshotReader`.
- Do not add Postgres, MSSQL, ClickHouse, or future vendor code directly to CLI
  handlers or policy modules.
- Put vendor endpoint behavior behind `RouteConformanceLiveStore`. A store may
  create tables, insert deterministic rows, apply a route-generic schema
  evolution column, and read snapshots, but it should not decide route support.
- Put route support in `VendorLiveRouteBinding`. Future routes should add a
  binding and a store/dialect when needed, not new branches in
  `RouteConformanceLiveService`.
- Put route-specific expectations in `RouteProfileCatalog`, strategy metadata,
  source-sink docs, or explicit evidence artifacts.
- Add small injected collaborators when a new evidence source is needed.

## Contract Shape

`route_conformance.json` includes:

- `schema_version = dpone.route_conformance.v1`
- canonical route identity
- route profile metadata when the route is supported
- dataset profile: row count, column count, chunk size, nested flag, schema
  evolution flag, and seed
- verification: source rows, sink rows, chunks, typed hashes, physical contract
  mismatches, mismatch samples, blockers, and warnings
- schema evolution status
- artifact paths for `source_snapshot.json`, `sink_snapshot.json`, and the
  report itself

`route_conformance_summary.json` and
`route_conformance_release_gate.json` normalize multiple route artifacts into
one aggregate decision.

`route_conformance_live.json` adds:

- `schema_version = dpone.route_conformance_live.v1`
- adapter name and live-run configuration
- live steps such as `seed_source`, `execute_route`, and `verify_exact`
- top-level schema-evolution status for operator review
- paths to `live_source_snapshot.json`, `live_sink_snapshot.json`, and the
  embedded `route_conformance.json`
- the same exact verification block used by offline conformance

## Testing Rules

When changing this layer, update or run:

```bash
uv run pytest tests/test_route_conformance_lab.py \
  tests/test_route_conformance_live.py \
  tests/test_cli_route_conformance_commands.py \
  tests/test_route_conformance_docs_contract.py -q
```

Then run the quality gates:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -q
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
```

## Live Docker Evidence

Live MSSQL, Postgres, and ClickHouse jobs should not be embedded in CLI
handlers or the verifier. They belong in small adapter implementations that
implement the live ports:

- `RouteConformanceSourceSeeder` creates or truncates the disposable source
  table and inserts deterministic rows.
- `RouteConformanceRouteExecutor` invokes the real route path or a reviewed
  route execution command.
- `RouteConformanceSnapshotReader` reads source and sink rows back into the
  generic snapshot contract.

The default `in_memory` adapter keeps OSS CI credential-free. Protected
Docker-live or vendor-live workflows can register real adapters and run
`DPONE_LIVE_ROUTE_CONFORMANCE=1 uv run dpone ops route-conformance live-run`.

The built-in `VendorRouteConformanceLiveAdapter` uses `VendorLiveRouteBinding`
entries for the first industrial routes:

- `postgres -> mssql -> incremental_merge`
- `mssql -> clickhouse -> incremental_merge`

The adapter is also exposed as `--adapter vendor_live` and `--adapter docker`.
It requires `DPONE_VENDOR_LIVE=1` by default. Without that opt-in it writes a
blocked report with `vendor_live.opt_in_missing:DPONE_VENDOR_LIVE`, which keeps
ordinary OSS CI deterministic and credential-free.

## Adding New Dataset Coverage

Add a new profile flag or generated column family only when it is route-generic.
Examples:

- timestamp precision and timezone strings;
- high-precision decimals;
- binary payload checksums;
- nullable edge cases;
- nested `id` and `parent_id` hierarchies;
- schema evolution operations that can be represented independently of a
  specific database vendor.

Every new profile field needs model tests, generator tests, verifier tests when
it affects hashes, CLI docs, and docs contract coverage.

## Related Docs

- [Route Conformance Lab](route-conformance-lab.md)
- [Route bootstrap and doctor](route-bootstrap-doctor.md)
- [Developer route readiness](developer-route-readiness.md)
- [Developer CI/CD guide](developer-ci-cd.md)
- [Architecture](architecture.md)
