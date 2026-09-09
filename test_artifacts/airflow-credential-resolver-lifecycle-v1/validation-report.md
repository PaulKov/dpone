# Airflow credential resolver lifecycle v1 validation report

- Validation date: 2026-07-17
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `26105f83`
- Specification:
  `docs/feature-design-airflow-credential-resolver-lifecycle-v1.md`
- Task contract:
  `test_artifacts/agent-policy/airflow-credential-resolver-lifecycle-v1.yml`
- Local result: PASS
- Live Vault/Kubernetes/Airflow result: UNVERIFIED

## Scope proved locally

- One successful resolution per logical alias and workload.
- Workload A retains Vault KV v2 version 17 after rotation.
- New workload B resolves version 18.
- New workload C fails safely during an outage and caches no failure.
- Already resolved workload B remains usable during the outage without another
  backend read.
- Workload C retries after recovery and resolves version 19.
- Concurrent uses of one alias perform one backend read.
- `pinned` and `dag_run_start` fail before backend I/O with stable codes.
- Explicit KV v2 responses require positive version metadata.
- Ordinary secret fields named `version` or `metadata` cannot become version
  evidence.
- Every declared env, Vault, projected-file, and Kubernetes API field must be
  present and non-empty.
- Backend exception text, secret values and restricted paths do not enter the
  public error or safe evidence.
- Safe-sample copy and temporary-target lifecycle preserve typed credential
  codes instead of replacing them with generic runtime errors.
- Airflow provider/base CLI parsing does not import `vault-kv-client` or perform
  credential I/O.

## Validation results

| Check | Status | Observed result |
| --- | --- | --- |
| Task-contract validator | PASS | 0 errors, 0 warnings |
| Focused lifecycle and workload-scope tests | PASS | 20 tests |
| Existing Phase 1B runtime and self-service CLI baseline | PASS | All selected tests passed |
| Airflow parse SLO/provider packaging/lazy imports | PASS | 19 tests |
| `ruff check .` | PASS | No findings |
| `ruff format --check .` | PASS | 3,236 files formatted |
| `mypy --config-file mypy.ini` | PASS | 586 source files |
| Import rules | PASS | No architectural import violations |
| Layer metrics | PASS | exact cross-layer ratio 0.299796747967; within budget |
| Module-size gate | PASS | No failures; changed resolver is 363 LOC |
| Architecture fitness | PASS | clustering 0.178; no findings |
| Full non-live pytest suite | PASS | 5,032 passed, 476 skipped in 218.40 seconds |
| Documentation link check | PASS | 513 Markdown files, 1,845 links |
| Generated references | PASS | 3/3 in sync |
| Documentation language contracts | PASS | 5 tests |
| Strict MkDocs build | PASS | Site built successfully |
| Compatibility registry | PASS | 19 entries, docs block in sync |
| Workflow security | PASS | 0 errors, 0 warnings |
| Core `dpone` build and twine | PASS | wheel and sdist passed |
| `dpone-native-accel` build and twine | PASS | wheel and sdist passed |
| `dpone-airflow-pack` build and twine | PASS | wheel and sdist passed |
| Live Vault Kubernetes Auth rotation/outage/recovery | UNVERIFIED | No approved cluster or credentials used |
| Live Kubernetes Secret/CSI rotation | UNVERIFIED | No approved cluster used |
| Live Airflow external Secrets Backend bridge rotation | UNVERIFIED | No approved Airflow environment used |
| Fresh-context independent review | UNVERIFIED | Agent thread capacity was unavailable |

Final build artifacts were produced in the isolated temporary directory
`/tmp/dpone-credential-lifecycle-final-dist.U20Ufc`; they are validation outputs, not
release artifacts.

An intermediate full-suite run failed the exact architecture fitness budget at
`0.3004265691651432 > 0.300` after typed errors were imported directly by
multiple service/readiness adapters. The implementation was refactored to one
safe service-boundary projection and one cohesive credential contract. The
final exact ratio is `0.299796747967`; no threshold or baseline was weakened.

## Security review

The implementation uses injected minimal ports and keeps the vendor import lazy
inside the runtime composition. Static readiness and Airflow parsing remain
network- and secret-free. `CredentialResolutionError` carries only a stable code,
fixed message, and resolver name. Backend exception chaining is suppressed at
the public boundary. The resolver never hashes secret values or accepts ordinary
secret fields as version evidence.

## Compatibility impact

- Valid `latest + workload_start` registries remain supported.
- Vault KV v1 remains readable with `resolved_version: null`, without a rotation
  certification claim.
- Registry schema v1 remains parse-compatible with planned `pinned` and
  `dag_run_start` values, but readiness/runtime now reject them instead of
  silently executing different semantics.
- `CredentialResolutionError` subclasses `ValueError`.
- No release-set, deployment-set, pack, provider signature, or beginner CLI
  migration is required.

## Remaining risk and required follow-up

`vault-kv-client 0.1.0` strips KV v2 response metadata from its public
`get_secret()` result. dpone therefore cannot live-certify the primary Vault
path through that adapter and correctly returns
`DPONE_CREDENTIAL_VERSION_METADATA_MISSING` for explicit KV v2 resolution. The
client needs a public atomic data-plus-version snapshot API; private `hvac`
access and secret-derived hashes are explicitly rejected.

Real production certification still requires current evidence from an approved
Vault/Kubernetes/Airflow environment for the exact dpone and client commits.
Until then the resolver is locally contract-complete but not production-live
certified.
