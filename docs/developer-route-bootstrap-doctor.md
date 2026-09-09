# Developer guide: route bootstrap and doctor

This guide explains how to extend the route onboarding layer without turning it
into route-specific command logic.

## Architecture boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.bootstrap_models` | Public JSON/Markdown contracts for connection doctor, source discovery, route bootstrap, and route doctor reports. |
| `dpone.ops.routes.bootstrap_policy` | Pure scoring, status, blocker, warning, and next-action helpers. |
| `dpone.ops.connection_doctor.ConnectionDoctorService` | Composes route profile lookup, tool/env/import checks, and report writing. |
| `dpone.ops.source_discovery.SourceDiscoveryService` | Normalizes exported schema JSON into table and column profiles. |
| `dpone.ops.route_bootstrap.RouteBootstrapService` | Builds manifest drafts, type-risk summaries, and next-command plans from route profile plus discovery evidence. |
| `dpone.ops.route_doctor.RouteDoctorService` | Aggregates onboarding artifacts into one route go/no-go report. |
| `dpone.commands.ops_parsers_routes` | Argument parsing only. |
| `dpone.services.ops.command_handlers_routes` | Handler glue only: parse args, call a service, emit JSON/Markdown, return exit code. |
| `dpone.ops.catalog_readiness` | Dependency injection factory for default services. |

`RouteProfileCatalog` remains the source of route existence and route metadata.
Do not duplicate source/sink/strategy rules in onboarding services.

## Extension rules

- Do not add route-specific logic to CLI handlers.
- Do not add database IO to `ConnectionDoctorService`; it checks explicit tools,
  environment variables, imports, and matrix metadata only.
- Do not add connector imports to `SourceDiscoveryService`; it reads exported
  schema JSON. Live schema readers should produce that JSON through a separate
  adapter or integration test fixture.
- Do not make `RouteBootstrapService` execute manifests. It only writes a draft
  and command plan.
- Do not make `RouteDoctorService` mutate artifacts. It aggregates immutable
  upstream evidence and fails closed when required artifacts are missing or red.
- Keep route-specific behavior in `RouteProfile`, matrix metadata, or small
  injected adapters. Future integrations should require metadata and docs, not
  new command branches.

## Report contracts

All reports include:

- `schema_version`
- status fields: `passed`, `status`, `score`
- machine-readable `blockers`, `warnings`, and `next_actions`
- `json_path`, `markdown_path`, and `output_dir`

Route-scoped reports also include the canonical `RouteKey` dictionary with
`source`, `sink`, `strategy`, `pair_id`, `case_id`, and `colon_id`.

## Testing rules

Add or update all of these when changing onboarding behavior:

```bash
uv run pytest tests/test_route_bootstrap_doctor.py \
  tests/test_cli_route_bootstrap_doctor_commands.py \
  tests/test_route_bootstrap_doctor_docs_contract.py -q
```

Then run generated docs and architecture checks:

```bash
uv run dpone docs update-cli-reference --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
```

## Adding a new source schema shape

Prefer expanding `SourceDiscoveryService` normalization helpers over adding a
new command. Keep parsing rules generic:

- accept common `tables`, `columns`, `schema`, `name`, `type`, `nullable`, and
  `row_count` aliases;
- return warnings for ambiguous metadata instead of silently assuming keys or
  cursors;
- preserve fail-closed blockers for malformed JSON and missing tables;
- cover every new alias with service tests and CLI JSON tests.

## Adding a new connection check

Add a new `OnboardingCheck` producer in `ConnectionDoctorService` only when the
check is credential-free and route-generic. Checks that connect to a database,
create tables, inspect vendor permissions, or read live CDC state belong in
route live certification or an injected integration adapter, not in connection
doctor.

## Related docs

- [Route bootstrap and doctor](route-bootstrap-doctor.md)
- [Developer route readiness](developer-route-readiness.md)
- [Developer CI/CD guide](developer-ci-cd.md)
- [Architecture](architecture.md)
