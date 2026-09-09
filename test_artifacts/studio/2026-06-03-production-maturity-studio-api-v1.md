# dpone Production Maturity / Studio API v1 Final Test Artifact

- Artifact date: 2026-06-03
- Tested by: Codex
- Framework repo: `<workspace>/dpone`
- Studio repo: `<workspace>/dpone-studio`
- Framework API bridge under test: `http://127.0.0.1:8767`
- Nuxt/Vue preview under test: `http://127.0.0.1:3034`
- Scope: self-service production maturity UX, headless API bridge, separate Nuxt/Vue UI, low/no-code GitOps, security/audit, SLO/observability, deploy guide, schema explorer, reconciliation preview

## Implementation summary

### Framework/API

Implemented or verified these local Studio API v1 contracts in `dpone`:

- `GET /healthz`
- `GET /openapi.json`
- `GET /api/studio`
- `GET /api/doctor`
- `GET /api/connections/capabilities`
- `POST /api/manifests/draft`
- `POST /api/plan`
- `POST /api/quality/check`
- `POST /api/gitops/prepare`
- `GET /api/runs`
- `GET /api/certification/matrix`
- `GET /api/state/inspect`
- `GET /api/perf`
- `GET /api/security/policy`
- `GET /api/audit/events`
- `GET /api/observability/slo`
- `GET /api/deploy/guide`
- `GET /api/schema/explorer`
- `POST /api/reconciliation/preview`

Security and governance behavior:

- `DPONE_STUDIO_TOKEN` enables bearer/header token protection for working API endpoints.
- Public endpoints remain `/`, `/healthz`, and `/openapi.json`.
- API responses expose local RBAC policy, guardrails and recent in-memory audit events.
- GitOps remains safe-by-default: generated branch/plan/test/commit/PR commands are reviewable and no automatic push is performed by Studio v1.

### Separate Nuxt/Vue Studio

Implemented or verified these UI capabilities in `<workspace>/dpone-studio`:

- Light ChatGPT-like flat UI with calm neutral tones and no raised/embossed visual treatment.
- Low/no-code manifest builder backed by framework API calls.
- Mandatory quality gate selection and API-backed validation preview.
- API-backed dry-run plan preview.
- GitOps review command generation.
- Production maturity center with security, audit, SLO, deploy topology, schema compatibility and reconciliation preview.
- Optional frontend token forwarding via `NUXT_PUBLIC_DPONE_API_TOKEN`.
- Mobile responsive hardening with no horizontal overflow.

## Framework verification

| Gate | Result |
| --- | --- |
| `uv run ruff check .` | PASS |
| `uv run ruff format --check .` | PASS, `728 files already formatted` |
| `uv run mypy --config-file mypy.ini` | PASS, `Success: no issues found in 161 source files` |
| `uv run pytest tests/test_studio_api_v1_contracts.py tests/test_managed_ux_contracts.py -q` | PASS, `17 passed` |
| `uv run pytest -m "not integration_live" -q` | PASS |
| `uv build` | PASS, built `dist/dpone-0.1.0.tar.gz` and `dist/dpone-0.1.0-py3-none-any.whl` |
| `uvx twine check dist/*` | PASS for wheel and sdist |

## Studio verification

| Gate | Result |
| --- | --- |
| `npm run test` | PASS, 2 test files / 4 tests |
| `npm run typecheck` | PASS |
| `npm run build` | PASS, Nuxt/Nitro production build complete |
| `npm audit --omit=optional` | PASS, 0 vulnerabilities |

## Browser smoke verification

Playwright tested the production Nuxt preview against the fresh local dpone API bridge.

Observed result:

```json
{
  "desktop": {
    "title": "Monitor dpone pipelines from one calm workspace.",
    "cards": 9,
    "lowCode": 1,
    "maturity": 1,
    "gitOpsText": true,
    "qualityPassed": true,
    "planReady": true,
    "yamlDraft": true,
    "security": true,
    "slo": true,
    "schemaPrefix": true,
    "reconciliation": true,
    "apiBridge": true,
    "errorText": false
  },
  "mobile": {
    "overflow": false,
    "docWidth": 390,
    "viewportWidth": 390,
    "maturity": 1,
    "security": true,
    "schemaPrefix": true
  }
}
```

Screenshots captured during final verification:

- `/tmp/dpone-studio-maturity-desktop.png`
- `/tmp/dpone-studio-maturity-mobile.png`

## Result

PASS.

The current increment reaches the intended production maturity UX baseline:

- Framework stays clean/headless and does not embed SaaS UI code.
- Studio UI is a separate Nuxt/Vue service and uses framework-owned APIs.
- Low/no-code operation is tied to quality gates, dry-run plan and GitOps review.
- Security, RBAC policy, audit, SLO, deploy guide, schema evolution and reconciliation are first-class operator surfaces.
- Desktop and mobile visual smoke is passing.
- Package build metadata remains valid after the changes.

## Follow-up candidates

- Direct authenticated GitHub/GitLab PR creation from Studio after OAuth/token governance is designed.
- Persistent workspace storage for shared Studio drafts and run history.
- Browser E2E suite for every builder field and every maturity center action.
- Multi-user SaaS deployment hardening outside the framework repo.
