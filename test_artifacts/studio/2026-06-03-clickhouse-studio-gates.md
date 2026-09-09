# dpone ClickHouse + Studio Gate Artifact

- Artifact date: 2026-06-03
- Executed by: Codex local agent
- Framework repo: `<workspace>/dpone`
- Studio repo: `<workspace>/dpone-studio`

## Framework results

| Gate | Result | Notes |
| --- | --- | --- |
| `uv run pytest tests/test_ci_pipeline_t5_clickhouse.py tests/test_managed_ux_contracts.py -q` | PASSED | ClickHouse marker/CI contract and Studio API bridge contract. |
| `uv run pytest -m integration_clickhouse tests/integration/clickhouse -q` | PASSED | 4/4 against local ClickHouse 24.8 on `127.0.0.1:59000`. |
| `uv run ruff check .` | PASSED | All checks passed. |
| `uv run ruff format --check .` | PASSED | 726 files already formatted after formatting touched files. |
| `uv run mypy --config-file mypy.ini` | PASSED | No issues found in 161 source files. |
| `uv run pytest -m "not integration_live" -q` | PASSED | Completed at 100%. |

## ClickHouse local service

| Setting | Value |
| --- | --- |
| Image | `clickhouse/clickhouse-server:24.8` |
| Container | `dpone-it-clickhouse` |
| Native endpoint | `127.0.0.1:59000` |
| HTTP endpoint | `127.0.0.1:58123` |
| Database | `dpone_it` |
| User | `default` |
| Password | `dpone` |

Root cause fixed: ClickHouse Docker image disables network access for `default` when neither `CLICKHOUSE_USER` nor non-empty `CLICKHOUSE_PASSWORD` is set. The integration compose, CI variables, docs and pytest defaults now use explicit `CLICKHOUSE_PASSWORD=dpone`.

## Studio architecture

| Layer | Result | Notes |
| --- | --- | --- |
| Framework | Headless API bridge | `dpone studio --serve` exposes `/api/studio`, `/api/doctor`, `/api/connectors`, `/api/perf`. |
| Frontend | Separate Nuxt/Vue repo | Created at `<workspace>/dpone-studio`; intended GitHub repo `PaulKov/dpone-studio`. |
| Boundary | Preserved | No SaaS frontend embedded in `dpone` framework package. |

## Studio frontend results

| Gate | Result | Notes |
| --- | --- | --- |
| `npm install` | PASSED | Dependency tree installed. |
| `npm audit --omit=optional` | PASSED | 0 vulnerabilities after removing unused `@nuxt/test-utils`/`happy-dom` and using Vitest 4.x. |
| `npm run test` | PASSED | 4/4 unit tests. |
| `npm run typecheck` | PASSED | Nuxt/Vue typecheck. |
| `npm run build` | PASSED | Production Nuxt/Nitro build. |
| Playwright desktop smoke | PASSED | Title, six panels, low/no-code builder and GitOps flow present. Screenshot: `/tmp/dpone-studio-light-desktop.png`. |
| Playwright mobile smoke | PASSED | No horizontal overflow at 390px. Screenshot: `/tmp/dpone-studio-light-mobile.png`. |

## UX changes

- Reworked Studio into a calm light UI inspired by ChatGPT-like surfaces: flat panels, light tones, no heavy shadows, no raised buttons.
- Added low/no-code visual manifest builder.
- Added quality gate selector with generated YAML preview.
- Added GitOps review flow commands: branch, plan artifact, tests, commit, draft PR.
- Kept the UI as a separate Nuxt/Vue repo consuming the framework API bridge.
