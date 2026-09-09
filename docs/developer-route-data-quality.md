# Developer route data quality

This developer guide explains how route data quality scorecards are organized
for any `source -> sink -> strategy` route.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.data_quality_models` | Public dataclasses and JSON/Markdown report contracts. |
| `dpone.ops.routes.data_quality_evidence` | Thin artifact reader and normalizer. It has no route-specific policy logic. |
| `dpone.ops.routes.data_quality_policy` | Pure score, status, blocker, warning, and next-action decisions. |
| `dpone.ops.route_data_quality` | Facade service that composes catalog lookup, evidence reader, policy, and report writing. |
| `dpone.services.ops.command_handlers_routes` | Thin CLI handler only. |
| `dpone.commands.ops_parsers_routes` | Argument parsing only. |

The service does not execute live databases, replay quarantine rows, repair
records, mutate sinks, or promote state. Runtime connectors stay focused on
extract/load/apply. Route data quality stays focused on certification evidence,
exception UX, and go/no-go decisions.

## Commands

- `dpone ops route-data-quality` builds a scorecard and exception backlog
  receipt for one `source -> sink -> strategy` route.

## Public contracts

`route_data_quality.json` uses schema `dpone.route_data_quality.v1` and includes:

- route identity from `RouteKey`;
- static route metadata from `RouteProfileCatalog`;
- threshold configuration;
- weighted route score;
- normalized dimensions;
- exception backlog totals;
- normalized evidence with checksums and route-match state;
- blockers, warnings, next actions, and output paths.

`route_data_quality.md` mirrors the same information for release reviews and
operator Runbook usage.

## Extension rules

1. Add new source -> sink routes to the integration matrix first.
2. Keep route-specific behavior in route profiles or optional small policies,
   not in CLI handlers.
3. Keep parsing generic in `RouteDataQualityEvidenceReader`.
4. Keep scoring and blocker decisions in `RouteDataQualityPolicy`.
5. Do not import runtime connector implementations into route DQ modules.
6. Add new evidence domains by documenting artifact shape and adding service,
   CLI, docs, and release-gate tests.
7. Prefer a new small collaborator over expanding an existing class when a file
   starts mixing parsing, policy, rendering, and orchestration.

## Status taxonomy

| Status | Developer rule |
| --- | --- |
| `passed` | Profile exists, every required artifact exists, route matches, score meets threshold, and no critical blockers exist. |
| `warning` | The route is release-usable but score or optional evidence should be reviewed. |
| `blocked` | Missing, malformed, unsupported, failed, or route-mismatched required evidence. |
| `waiver_required` | Evidence signals a critical DQ exception that needs steward approval. |
| `quarantine_sla_breached` | Exception count, ratio, or age exceeds policy thresholds. |

## Test strategy

Use TDD for new behavior:

- service tests for ready, missing evidence, route mismatch, malformed JSON,
  warning score, quarantine SLA breach, waiver behavior, and first supported
  routes;
- CLI tests for JSON output, non-zero exits, output files, and threshold
  arguments;
- docs contract tests for user docs, developer docs, architecture docs, CI/CD,
  release gate docs, and MkDocs navigation;
- release-gate regression tests when `route_data_quality` is required evidence;
- generated CLI reference and quality metrics checks before PR.

## Runbook

When changing route data quality modules:

1. Add or update a failing test first.
2. Keep dataclasses immutable and focused on public contracts.
3. Keep evidence readers side-effect free except reading files.
4. Keep policy deterministic and independent from the filesystem.
5. Run focused route DQ tests.
6. Run neighboring route run supervisor and route release gate tests.
7. Stage new Python files before running `uv run dpone docs update-dev-metrics`.
8. Run `uv run dpone docs update-cli-reference --check`,
   `uv run dpone docs check-docs`, and `uv run mkdocs build --strict`.
