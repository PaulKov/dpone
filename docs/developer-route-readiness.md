# Developer guide: route readiness

Route readiness is a control-plane taxonomy for `source -> sink -> strategy`
certification. It is not a runtime connector interface and it does not replace
`AbstractSource`, `AbstractSink`, or load strategies. Runtime code extracts and
loads data; route readiness decides whether the evidence for a route is complete
enough for CI, release review, and operator self-service.

## Architecture boundaries

The implementation is intentionally split into small modules:

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.models` | Public immutable value objects and report contracts. |
| `dpone.ops.routes.catalog` | Matrix-driven route profile lookup. |
| `dpone.ops.routes.evidence` | Artifact parsing and evidence normalization. |
| `dpone.ops.routes.policy` | Generic score, level, blockers, warnings, and next actions. |
| `dpone.ops.routes.certification` | Route certification pack generation and safe evidence probes. |
| `dpone.ops.route_readiness` | Facade service that composes catalog, reader, and policy. |
| `dpone.commands.*` and `dpone.services.ops.*` | CLI parsing, formatting, and service invocation only. |

Class taxonomy:

| Class | Role |
| --- | --- |
| `RouteKey` | Immutable route identity. It normalizes aliases and renders `pair_id`, `case_id`, and `colon_id`. |
| `RouteProfile` | Static metadata for one supported route: docs, extras, profiles, certification status, native fast path, SLO hints, and required evidence. |
| `RouteEvidenceItem` | Normalized artifact record reused for matrix, benchmark, performance, type, docs, governance, and reliability evidence. |
| `RouteEvidenceReader` | Thin parser/normalizer. It has no route-specific logic. |
| `RouteReadinessPolicy` | Pure readiness decision from a profile and evidence. It owns score, level, blockers, warnings, and next actions. |
| `RouteReadinessReport` | Stable JSON/Markdown contract for CLI, CI, docs, and release gates. |
| `RouteReadinessService` | Orchestrator for catalog lookup, evidence loading, policy evaluation, and report writing. |
| `RouteEvidenceProbe` | Optional safe extension point for generating one readiness-compatible evidence artifact. |
| `RouteEvidenceProbeResult` | Probe result payload with pass/fail, summary, blockers, and source path metadata. |
| `RouteCertificationPackService` | Generates `evidence/*.json`, calls route readiness over those artifacts, and writes a pack report. |

The route readiness service must stay side-effect light. It writes reports, but
does not run Docker, execute live database tests, or mutate runtime state.
The same rule applies to `RouteCertificationPackService` and `RouteEvidenceProbe`.
Do not execute heavy certification tests inside probes; probes may only inspect
local files, metadata, injected lightweight facts, or already-produced artifacts.

## Adding a new route

1. Add or update the route in `DEFAULT_INTEGRATION_MATRIX`.
2. Add or update the strategy certification entry in
   `StrategyCertificationMatrixService`.
3. Add the source -> sink guide under `docs/source-sink/` and link it from
   [Source -> sink matrix](source-sink-matrix.md).
4. Add required route-specific evidence in `dpone.ops.routes.catalog` only when
   the generic evidence domains are insufficient.
5. Add focused tests that prove the new route can build a `RouteProfile`.
6. Add user examples and runbook steps to [Route readiness](route-readiness.md)
   when the route needs special operator handling.
7. Run docs, architecture, and quality checks before opening a PR.

Do not add route-specific logic to the CLI. CLI handlers should parse arguments,
call `RouteReadinessService`, render the selected format, and return the report
exit code.

When a new route needs an operator-friendly evidence bundle, extend
[`Route certification pack`](route-certification-pack.md) examples and add any
safe `RouteEvidenceProbe` implementation behind dependency injection. The CLI
command `dpone ops route-certification-pack` must stay a thin adapter over
`RouteCertificationPackService`.

## Policy extension rules

Prefer data over branching:

- Put route metadata in `RouteProfile`.
- Put artifact facts in `RouteEvidenceItem`.
- Keep pass/fail semantics generic in `RouteEvidenceReader`.
- Keep scoring generic in `RouteReadinessPolicy`.
- Add a tiny policy extension only when a route has a genuinely different
  go/no-go rule that cannot be expressed as required evidence.
- Add a tiny probe only when evidence can be generated without live heavy work.

Per-route policy extensions must be injected through the service constructor or
catalog metadata. They must not import concrete source or sink runtime classes.

## Tests and CI

Required local checks for route readiness changes:

```bash
uv run pytest \
  tests/test_route_readiness.py \
  tests/test_cli_route_readiness_command.py \
  tests/test_route_readiness_docs_contract.py \
  tests/test_route_certification_pack.py \
  tests/test_cli_route_certification_pack_command.py \
  tests/test_route_certification_pack_docs_contract.py

uv run pytest tests/test_industrial_readiness_suite.py tests/test_cli_industrial_readiness_command.py
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run mkdocs build --strict
uv run dpone docs update-cli-reference --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-architecture-fitness
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
```

Use matrix-driven tests for generic guarantees:

- every `IntegrationMatrixCase` can produce a `RouteProfile`;
- `RouteEvidenceReader` normalizes pass, fail, malformed, and missing
  artifacts consistently;
- `RouteReadinessPolicy` produces deterministic scores and blockers;
- `RouteCertificationPackService` writes readiness-compatible generated evidence;
- `RouteEvidenceProbe` injection can satisfy one evidence domain without CLI branching;
- CLI tests cover JSON output, exit codes, and report paths;
- docs contract tests require user docs, developer docs, examples, runbooks,
  architecture links, and source-sink matrix references.

## Dependency rules

- `dpone.ops.routes.*` may depend on integration matrix metadata, strategy
  certification metadata, checksums, standard library parsers, and value models.
- `dpone.ops.routes.*` must not depend on runtime connector implementations,
  database clients, Docker helpers, or CLI renderers.
- `dpone.services.ops.*` may depend on the route readiness service and output
  helpers.
- `dpone.commands.*` must not contain scoring, evidence parsing, or route
  metadata.

This keeps route readiness reusable by CLI, CI, docs generation, future Studio
surfaces, and release gates without creating another god module.

## Report compatibility

`RouteReadinessReport` currently publishes schema version
`dpone.route_readiness.v1`. If the JSON shape changes:

1. Add backward-compatible fields when possible.
2. Update user docs and developer docs in the same PR.
3. Update CLI reference through `dpone docs update-cli-reference`.
4. Add migration notes when a field is removed or renamed.
5. Keep Markdown report changes readable for release reviewers.
