# Studio capability self-service v1 validation

- Date: 2026-07-23
- Base commit: `77f2b530b730`
- Branch: `codex/studio-capability-v1`
- Exact-head evidence: PR `#442` required checks and workflow artifacts.

## Result

Core capability discovery, CLI, Studio API v1, compatibility, security, and
documentation gates are `PASS`. The separate public Studio UI, remote
production deployment, live connector certification, and human usability gate
remain `UNVERIFIED` and are not release claims of this change.

## Focused evidence

| Gate | Status | Result |
|---|---|---|
| Capability, CLI, Studio API/HTTP, pipeline summary, quality, certification, Airflow explain | PASS | 227 tests passed |
| CORS response-splitting regression | PASS | request origins are never reflected; 26 HTTP security tests passed |
| Task contract validator | PASS | 0 errors, 0 warnings |
| Fresh-context architecture/UX review | PASS | no P0/P1 findings after closure review; `READY TO MERGE` |
| Full mypy | PASS | 658 source files |
| Ruff lint | PASS | all checks passed |
| Ruff format | PASS | 3652 files formatted |
| Import rules | PASS | no violations |
| Architecture fitness | PASS | average clustering 0.179877; hard budget 0.180 |
| Layer metrics | PASS | cross-layer ratio 0.299; no regression issue |
| Module size | PASS | no hard-limit issue |
| Airflow public contracts | PASS | CLI 15/15, packages 1/1, Python 11/11, schemas 54/54 |
| Documentation links | PASS | 599 Markdown files, 2168 local links |
| Generated references | PASS | 3/3 synchronized |
| Generated quality metrics | PASS | `dpone docs update-dev-metrics --check` |
| Documentation language tests | PASS | 25 tests passed |
| MkDocs strict build | PASS | site built successfully |
| Compatibility policy | PASS | 19 registry entries |
| Package build and metadata | PASS | 4 wheels + 4 source distributions; all passed `twine check` |
| Full non-live suite | PASS | 7347 passed, 557 skipped, 1 warning |

The focused count is the final combined command:

```bash
uv run pytest \
  tests/test_capability_discovery_v1.py \
  tests/test_self_service_capability_cli.py \
  tests/test_studio_api_v1_contracts.py \
  tests/test_studio_http_security_v1.py \
  tests/test_studio_pipeline_summary_v1.py \
  tests/test_managed_ux_contracts.py \
  tests/test_airflow_explain_snapshot_consistency.py \
  tests/test_native_transfer_capability_certification.py \
  tests/test_cli_connector_sdk_commands.py \
  tests/test_operations_maturity_services.py -q
```

## Packaging evidence

The following `0.73.17` wheel and source distributions were built and passed
`uv tool run twine check` from the final source tree:

- `dpone`;
- `dpone-airflow-pack`;
- `apache-airflow-providers-dpone`;
- `dpone-native-accel`.

Artifact directory:
`/tmp/dpone-studio-v1-final.96y3cb`.

## Review closure

The fresh-context reviewer initially found two P1 issues:

- no-flag/default pipeline initialization could bypass malformed recipe
  authority;
- `StudioApplicationService` could perform hidden UI package discovery.

Both were corrected. Default CLI and direct Python built-in scaffolding now
resolve through the same authoring authority before writes, while external
recipe compatibility remains unchanged. UI asset discovery is confined to the
Studio composition root and the application facade requires the projection as
an injected dependency. The closure review reported `NO BLOCKERS` and
`READY TO MERGE`.

The exact-head CodeQL review also identified request-origin reflection in the
CORS response header. The adapter now emits only a configured trusted origin or
a canonical same-origin value derived from the server authority. Control
characters are rejected in configured origins, and regression coverage verifies
that untrusted request input is never reflected into response headers.

## Explicitly unverified

- No approved live connector, Vault, Airflow cluster, or remote Studio
  environment was used. Live checks are `SKIP/UNVERIFIED`, not `PASS`.
- The five-user Studio/self-service study has not run. See
  `test_artifacts/studio-usability/README.md`.
- `dpone-studio` UI and `dpone-studio-assets` are not published by this change.
- SSO, RBAC, production ASGI deployment, sample execution from Studio, and
  authoring v0.2 remain outside this release.
