# Studio local adapter operations

Operate the bundled Studio HTTP bridge as a bounded local-development adapter.
This page is for platform engineers; the first-time journey remains in the
[Studio guide](studio.md).

## Security contract

The stdlib adapter enforces:

- request body at most 1 MiB;
- response deadline 10 seconds;
- at most 32 concurrent requests;
- at most 1000 in-memory audit events;
- page size 1 through 200;
- same-origin CORS by default;
- project-root confinement including symlink checks;
- redacted `dpone.error.v1` responses.

The response deadline does not forcibly terminate a Python thread. A timed-out
operation retains its concurrency slot until it returns, so Studio v1 exposes
only bounded local operations and no vendor/network calls.

Non-loopback development binding requires every control:

```bash
export DPONE_STUDIO_TOKEN='<injected-secret>'
dpone studio --serve \
  --host 0.0.0.0 \
  --allow-remote \
  --cors-origin https://studio.example.internal
```

Clients send `Authorization: Bearer ...`. Shared-token mode is not SSO, RBAC,
or a production hosting claim. Never pass the token on the command line.

## Compatibility

Legacy `/api/*` routes emit `Deprecation: true` and `Sunset`. They remain for
at least two minor releases and 12 months; the earliest planned removal date
is 2027-07-23.

| Legacy operation | New-client path | Migration status |
| --- | --- | --- |
| `/api/studio` | `/api/v1/meta` | migrate; legacy shape remains |
| `/api/connectors` | `/api/v1/capabilities` | migrate |
| `/api/connections/capabilities` | `/api/v1/capabilities` | migrate |
| `/api/certification/matrix` | `/api/v1/capabilities` | migrate; axes are separate |
| `/api/manifests/draft` | `/api/v1/manifests/draft` | direct alias |
| `/api/plan` | `/api/v1/plans` | direct alias |
| `/api/doctor`, `/api/perf`, `/api/state/inspect`, `/api/runs` | none | retirement only |
| `/api/security/policy`, `/api/audit/events`, `/api/observability/slo`, `/api/deploy/guide` | none | retirement only |
| `/api/schema/explorer`, `/api/quality/check`, `/api/gitops/prepare`, `/api/reconciliation/preview` | none | retirement only |

New clients must not depend on retirement-only operations. Empty, plan-only,
or all-skipped quality results remain fail-closed.

## Troubleshooting

| Code | Meaning | Recovery |
| --- | --- | --- |
| `DPONE_STUDIO_REMOTE_NOT_ALLOWED` | remote bind not enabled | use loopback or add `--allow-remote` |
| `DPONE_STUDIO_REMOTE_TOKEN_REQUIRED` | shared token absent | inject `DPONE_STUDIO_TOKEN` |
| `DPONE_STUDIO_REMOTE_CORS_REQUIRED` | exact origin absent | add `--cors-origin` |
| `DPONE_STUDIO_PATH_UNSAFE` | path escaped root or crossed symlink | use a regular project-relative path |
| `DPONE_ROUTE_NOT_SUPPORTED` | route absent | inspect capabilities or `dpone recipe list` |
| `DPONE_STUDIO_REQUEST_SCHEMA_INVALID` | request shape/type invalid | compare with OpenAPI |
| `DPONE_STUDIO_CONCURRENCY_LIMIT` | 32 operations active | retry with bounded concurrency |
| `DPONE_STUDIO_REQUEST_TIMEOUT` | deadline exceeded | use the corresponding CLI |

Invalid CLI usage exits `2`, unsafe remote startup exits `4`, and bind failure
exits `3`. JSON failures are written to stderr without tracebacks or secret
values.

## UI release gate

The core API can be reviewed independently. The separate
`dpone-studio v0.1.0` UI and v0.2 authoring milestone are **NO-GO** until an
exact-commit human study includes at least five first-time users and at least
80 percent complete the journey without help. Evidence belongs under
`test_artifacts/studio-usability/`.
