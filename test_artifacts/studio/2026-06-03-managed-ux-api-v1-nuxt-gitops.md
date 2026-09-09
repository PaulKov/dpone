# dpone Managed UX / Studio API v1 Test Artifact

- Artifact date: 2026-06-03
- Tested by: Codex
- Scope: dpone headless Studio API v1, separate Nuxt/Vue `dpone-studio` UI, low/no-code GitOps builder, ClickHouse marker gate, package build metadata
- Framework repo: `<workspace>/dpone`
- Studio repo: `<workspace>/dpone-studio`
- API bridge under test: `http://127.0.0.1:8766`
- Nuxt preview under test: `http://127.0.0.1:3033`

## What was implemented

- `dpone` remains a headless production framework; Studio UI is kept outside the framework repository.
- Added Studio API v1 facade and HTTP bridge endpoints:
  - `GET /healthz`
  - `GET /openapi.json`
  - `GET /api/connections/capabilities`
  - `POST /api/manifests/draft`
  - `POST /api/plan`
  - `POST /api/quality/check`
  - `POST /api/gitops/prepare`
  - `GET /api/runs`
  - `GET /api/certification/matrix`
  - `GET /api/state/inspect`
  - `GET /api/perf`
- Added contract tests for the API bridge.
- Kept local Studio HTML inside `dpone` as an API landing/status shell only.
- Built separate `dpone-studio` Nuxt 3 + Vue 3 UI with light ChatGPT-like styling.
- Added low/no-code GitOps builder using the framework API for manifest draft, dry-run plan, quality check, and GitOps command preparation.
- Added responsive CSS hardening after mobile overflow was detected.
- Added executable `integration_clickhouse` marker gate and deterministic ClickHouse integration credentials.

## Core framework verification

| Gate | Result |
| --- | --- |
| `uv run ruff check .` | PASS |
| `uv run ruff format --check .` | PASS, `728 files already formatted` |
| `uv run mypy --config-file mypy.ini` | PASS, `Success: no issues found in 161 source files` |
| `uv run pytest tests/test_studio_api_v1_contracts.py tests/test_ci_pipeline_t5_clickhouse.py tests/test_managed_ux_contracts.py -q` | PASS, `18 passed` |
| `uv run pytest -m "not integration_live" -q` | PASS |
| `uv build` | PASS, built `dist/dpone-0.1.0.tar.gz` and `dist/dpone-0.1.0-py3-none-any.whl` |
| `uvx twine check dist/*` | PASS for wheel and sdist |

## Studio repo verification

| Gate | Result |
| --- | --- |
| `npm run test` | PASS, 2 files / 4 tests |
| `npm run typecheck` | PASS |
| `npm run build` | PASS, Nuxt/Nitro production build complete |
| `npm audit --omit=optional` | PASS, 0 vulnerabilities |

## Browser smoke verification

Playwright tested the production Nuxt preview against the local dpone API bridge.

Assertions:

- Desktop title contains `Monitor dpone pipelines`.
- Six feature cards render.
- Low/no-code GitOps builder renders.
- GitOps review flow renders.
- Quality API result renders as `Quality: passed`.
- Plan API result renders as `Plan: dry-run ready`.
- Manifest draft contains `kind: dpone.batch.v1`.
- API bridge indicator renders.
- No API connection error is visible.
- Mobile viewport has no horizontal overflow.

Observed result:

```json
{
  "desktop": {
    "title": "Monitor dpone pipelines from one calm workspace.",
    "cards": 6,
    "lowCode": 1,
    "gitOpsText": true,
    "qualityPassed": true,
    "planReady": true,
    "yamlDraft": true,
    "apiBridge": true,
    "errorText": false
  },
  "mobile": {
    "overflow": false,
    "docWidth": 390,
    "viewportWidth": 390,
    "lowCode": 1,
    "gitOpsText": true
  }
}
```

Screenshots captured during verification:

- `/tmp/dpone-studio-api-v1-desktop.png`
- `/tmp/dpone-studio-api-v1-mobile.png`

## Result

PASS.

This closes the current managed-like UX increment at MVP production quality:

- The framework exposes stable headless APIs for local/control-plane UX.
- The Nuxt/Vue UI is isolated in a separate repository and can evolve independently.
- The low/no-code builder is API-backed rather than purely static.
- GitOps is represented as reviewable commands and PR-ready metadata.
- Quality checks and dry-run planning are part of the default UI flow.
- Responsive visual smoke is passing on desktop and mobile.

## Remaining production maturity work

- Add authenticated direct Git provider integration for opening pull requests from Studio, while keeping command-based GitOps as the safe default.
- Add persistent Studio project/workspace storage if multiple operators need shared drafts.
- Add polished SaaS-grade frontend flows only in the separate `dpone-studio` repository.
- Add full browser E2E tests around editing every builder field and copying generated commands.
