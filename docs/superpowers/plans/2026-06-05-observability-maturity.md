# dpone observability maturity plan

## Goal

Build the runtime observability maturity layer as a self-service, dependency-light
contract that works in local CI and can be promoted to Prometheus/OpenTelemetry
pipelines without changing dpone runtime behavior.

## Scope

- Extend canonical runtime metrics with retry, error, warning, throughput, and
  success/failure evidence from `dpone run` reports.
- Add OpenTelemetry resource attributes separately from metric labels.
- Add a local metrics artifact index with SHA-256 checksums and file sizes.
- Add a manual/scheduled GitHub Actions observability maturity gate.
- Document user commands, developer extension points, CI/CD workflow, and
  failure runbooks.

## Design constraints

- Keep `dpone.observability.*` dependency-light and collector-free.
- Keep command modules as argparse adapters only.
- Do not mix observability business logic into `dpone.ops`.
- Preserve existing Prometheus and OTLP-shaped artifact names.
- Keep all new logic small, testable, and injectable.

## TDD checklist

- Service test for extended metrics and metrics index.
- CLI test for `--resource-attr`.
- Renderer test for safe Prometheus label names.
- Docs contract test for the new observability workflow and runbook.

## Quality gates

- `uv run pytest tests/test_observability.py tests/test_cicd_docs_contracts.py -q`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy --config-file mypy.ini`
- `uv run dpone docs check-docs`
- `uv run mkdocs build --strict`
- `uv run pytest -m "not integration_live" -q`
- `uv build`
- `uv tool run twine check dist/*`
