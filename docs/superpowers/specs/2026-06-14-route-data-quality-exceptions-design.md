# Route Data Quality Exceptions Design

## Context

`route-run-supervisor` proves that a route run has a coherent lifecycle receipt, but it does not answer the enterprise trust question: did the route produce data that is good enough to promote, and are bad rows or events under control?

This design introduces a generic route-level data quality and exception management capability for every `source -> sink -> strategy` pair. It is a control-plane abstraction. It reads existing evidence artifacts, scores route quality, highlights exception backlog risk, and writes stable release artifacts. It does not execute source queries, mutate sinks, replay quarantine rows, or repair records.

## Goals

- Provide a reusable data quality scorecard for any supported route.
- Normalize evidence from data contracts, quarantine, reconciliation, CDC observability, SLO, and route run receipts.
- Block release when critical data quality domains fail, route identity mismatches, malformed evidence appears, or quarantine SLA is breached.
- Keep route-specific behavior out of services and CLI handlers.
- Produce self-service JSON and Markdown artifacts for CI, release gates, operators, and docs.

## Non-Goals

- No live database execution.
- No data repair, CDC replay, or schema apply.
- No route-specific hard-coding for `postgres -> mssql` or `mssql -> clickhouse`.
- No UI layer.

## Public Contract

The service writes:

- `route_data_quality.json`
- `route_data_quality.md`

The JSON schema version is `dpone.route_data_quality.v1`.

Top-level fields include:

- `route`
- `profile`
- `passed`
- `status`
- `score`
- `thresholds`
- `dimensions`
- `exceptions`
- `blockers`
- `warnings`
- `next_actions`
- `evidence`
- `evidence_index`
- `json_path`
- `markdown_path`

## Architecture

The feature follows the existing route ops taxonomy:

- `dpone.ops.routes.data_quality_models`
  - immutable report, evidence, dimension, exception, threshold, and decision contracts.
- `dpone.ops.routes.data_quality_evidence`
  - thin artifact reader and normalizer; no route policy logic.
- `dpone.ops.routes.data_quality_policy`
  - pure score, status, blocker, warning, and next-action decision.
- `dpone.ops.route_data_quality`
  - facade service that composes catalog, evidence reader, and policy.
- CLI parser and handler
  - argument parsing and service invocation only.

## Status Taxonomy

- `passed`: score meets threshold, no critical blockers, route is supported.
- `warning`: score is below warning threshold or optional evidence is weak, but no release blocker exists.
- `blocked`: critical required evidence failed, missing evidence failed, route mismatch, unsupported route, or malformed artifact.
- `waiver_required`: a critical DQ blocker exists but waiver evidence is present and explicit approval is required before release.
- `quarantine_sla_breached`: quarantine or exception backlog exceeds configured maximum count, ratio, or age.

## Evidence Domains

Default required domains:

- `data_contract`
- `quarantine`
- `reconciliation`

Common optional domains:

- `cdc_observability`
- `slo`
- `route_run_supervisor`
- `route_reconciliation_repair`
- `route_schema_evolution`

Operators can add required domains via repeated `--require`.

## Data Flow

1. CLI receives `source`, `sink`, `strategy`, artifacts, thresholds, and required evidence.
2. `RouteDataQualityService` resolves a `RouteKey` and `RouteProfile`.
3. `RouteDataQualityEvidenceReader` normalizes each artifact into `RouteDataQualityEvidence`.
4. `RouteDataQualityPolicy` evaluates supported route, evidence pass/fail, route matching, DQ score, quarantine backlog, and waiver state.
5. `RouteDataQualityReport` writes JSON and Markdown artifacts.
6. `route-release-gate` can require `route_data_quality` evidence as a release domain.

## Testing Strategy

- Unit tests for report rendering, route matching, malformed artifacts, scoring, warnings, blockers, quarantine SLA, and waiver behavior.
- CLI tests for JSON output and blocked exit code.
- Matrix-driven test proving `postgres -> mssql` and `mssql -> clickhouse` can produce route DQ reports without route-specific service logic.
- Docs contract tests requiring user docs, developer docs, CLI docs, CI/CD docs, control-plane docs, release-gate docs, and MkDocs nav.

## Documentation Strategy

All OSS docs are English:

- `docs/route-data-quality.md`
- `docs/developer-route-data-quality.md`
- Updates to ops CLI, control plane, CI/CD, release gate, route run supervisor, and MkDocs navigation.
- Generated CLI reference and quality metrics are refreshed after staging new Python files.
