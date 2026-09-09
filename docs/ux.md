# UX principles

`dpone` is designed as a self-service production framework, not just a library.

## Product principles

- Manifests should be readable without opening runtime code.
- Every dangerous operation should have a dry-run or plan mode.
- Error messages should name the failing connector, table, strategy, and fix path.
- Secrets must be redacted in all logs, reports, plans, and artifacts.
- Generated artifacts should be useful in pull requests and incident reviews.

## CLI UX

The CLI defaults to human-readable text and supports machine-readable `json` or repository-friendly `md` output for automation.

Key commands:

- `dpone doctor`
- `dpone init`
- `dpone plan`
- `dpone run-report`
- `dpone state inspect/reset/export/replay-from/compare`
- `dpone connectors list/certify/scaffold`
- `dpone perf advise`
- `dpone studio`

## Low-code and GitOps

A low/no-code workflow should still produce versioned manifests, `.env.example` files, smoke commands, quality gates, and pull-request friendly artifacts. Studio and CLI features must reuse the same service APIs so the UI never becomes a second runtime.
