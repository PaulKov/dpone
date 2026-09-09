# dpone managed UX verification

- Date: 2026-06-02
- Tester: Codex
- Scope: managed-like CLI UX, diagnostics, init, plan, run artifacts, quality contracts, state UX, connector certification/scaffold, performance advisor, local Studio payload.

## Implemented features

- `dpone doctor --profile local|ci|production --format text|json|md`
- `dpone init`
- `dpone plan`
- `dpone run-report`
- `dpone state inspect/reset/export/replay-from/compare`
- `dpone connectors list/certify/scaffold`
- `dpone perf advise`
- `dpone studio`
- Optional manifest sections: `quality`, `observability`, `performance`, `certification`
- Run artifact writer for JSON, Markdown, and HTML
- Data quality checks: `not_null`, `unique`, `accepted_values`, `min_rows`, `max_null_ratio`, `checksum`, plus plan-only live checks
- Safe state UX with preview mode unless `--yes`
- Connector SDK scaffold

## Verification commands

```text
uv run ruff check .
Result: passed
```

```text
uv run ruff format --check .
Result: passed, 726 files already formatted
```

```text
uv run mypy --config-file mypy.ini
Result: passed, no issues in 161 source files
```

```text
uv run pytest -m "not integration_live" -q
Result: passed, expected live-gated skips only
```

```text
uv sync --all-extras
Result: passed
```

```text
uv build && uvx twine check dist/*
Result: passed for wheel and sdist
```

```text
Fresh venv install: dist/dpone-0.1.0-py3-none-any.whl[full]
Result: passed, managed UX imports and CLI parser commands available
```

```text
Managed CLI smoke:
- dpone doctor --profile ci --format json
- dpone init ... --format json
- dpone plan ... --format json
- dpone perf advise ... --format json
- dpone state reset ... --format json
- dpone connectors list --format json
- dpone studio --format json
Result: passed
```

## Notes

- `dpone plan` remains dry-run only.
- Destructive state commands preview unless `--yes` is supplied.
- `dpone studio` prints local Studio metadata by default; `--serve` starts a blocking local HTTP server.
- BigQuery live validation remains external-credential gated.
