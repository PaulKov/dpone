# v0.72.9 Airflow Connection Secret GC validation

- Status: LOCAL PASS - REVIEWED
- Base commit: `0cb951f2`
- Branch: `codex/v0.72.9-airflow-secret-retention-gc`
- Verified: 2026-07-16
- Approved specification:
  `docs/feature-design-airflow-connection-secret-gc-v0729.md`

## Scope

This evidence covers the local contract for digest-only lifecycle metadata,
metadata-only Kubernetes inventory, active-Pod and minimum-age protection,
plan-first bounded deletion, UID/resourceVersion preconditions, structured
CLI/schema output, documentation, architecture fitness, and packaging.

It does not certify a real Airflow/Kubernetes cluster. No live credentials or
approved cluster were available during this validation.

## Results

| Gate | Status | Evidence |
| --- | --- | --- |
| Focused lifecycle/GC/provider/runtime/schema/lazy-import suite | PASS | `131` tests passed in `3.67s` |
| Full non-live regression suite | PASS | Complete `not integration_live` suite passed after the final dependency and KPO precedence changes |
| Real Airflow KPO lifecycle precedence | PASS | `6` real-Airflow loader/provider tests passed; final Pod keeps validated lifecycle metadata above conflicting user arguments |
| Optional Kubernetes SDK install | PASS | Isolated Python 3.12 wheel install resolved `dpone[kubernetes]` with Kubernetes client `36.0.3`; provider lifecycle and GC imports passed |
| 10,000-item deterministic inventory | PASS | Two opposite-order classifications completed in `0.21s`; identical sorted candidates |
| Ruff lint | PASS | `All checks passed` |
| Ruff format | PASS | `3086 files already formatted` |
| Mypy | PASS | `502` source files, zero issues |
| Import rules | PASS | No architectural import violations |
| Architecture fitness | PASS | Average clustering `0.179`; cross-layer ratio `0.299848123` below hard `0.300` budget; max fan-out `20` |
| Layer metrics | PASS | `4609` edges; max cross-layer flow `94`; no gate issue |
| Module size | PASS | No new root or provider warning; lifecycle runtime was split before integration |
| Workflow security | PASS | `0` errors, `0` warnings |
| Documentation links/contracts | PASS | `423` Markdown files and `1750` local links checked |
| Generated CLI/schema references | PASS | Both generated references in sync |
| Compatibility policy | PASS | `19` registry entries, generated block in sync |
| Docs language contracts | PASS | `4` tests passed |
| MkDocs strict build | PASS | Strict site build completed |
| Root/provider/native builds | PASS | Wheel and sdist built for each distribution in isolated `/tmp` outputs |
| Twine metadata | PASS | All six wheel/sdist artifacts passed |
| Live Airflow/Kubernetes certification | UNVERIFIED | No approved cluster or credentials; not represented as PASS |
| Fresh-context correctness/security review | PASS | Initial KPO precedence finding was reproduced, fixed, covered by unit and real-Airflow tests, and re-review found no remaining blocker |

The full suite was run twice. The first run found the architecture-fitness
cross-layer ratio at `0.30091`; implementation dependencies were reduced rather
than relaxing the threshold. The focused architecture test and the complete
suite passed after that correction.

## Security proof

- Inventory accepts only `PartialObjectMetadataList` and rejects full Secret or
  Pod object fields without a fallback.
- Provider/help import paths do not load the Kubernetes SDK.
- Fixed selectors are not user-controlled and cleanup is namespace-scoped.
- Existing managed Pods and young Secrets protect matching credential objects.
- Lifecycle metadata is pinned on both pod specs and higher-precedence KPO
  `labels`/`annotations`; caller metadata cannot erase the active-use lease.
- Malformed managed Pod metadata blocks all deletion; malformed Secrets are
  quarantined individually.
- Apply validates confirmation and exact actor allowlist before infrastructure
  construction, re-plans, and deletes a deterministic bounded prefix.
- Every delete supplies both observed UID and resourceVersion.
- HTTP `404` is an absent skip; HTTP `409` returns partial; RBAC and dependency
  failures use distinct exit codes.
- Reports and exceptions contain digest refs and safe status only. Tests reject
  raw tokens, URLs, physical object names, credential values, and response
  bodies.
- Pre-v0.72.9 unlabelled objects remain outside automated adoption/deletion.

## Packaging proof

Final artifacts were built outside the repository to avoid overwriting existing
release files:

```text
/tmp/dpone-v0729-final-root/dpone-0.72.2-py3-none-any.whl
/tmp/dpone-v0729-final-root/dpone-0.72.2.tar.gz
/tmp/dpone-v0729-final-provider/dpone_airflow_pack-0.72.2-py3-none-any.whl
/tmp/dpone-v0729-final-provider/dpone_airflow_pack-0.72.2.tar.gz
/tmp/dpone-v0729-final-native/dpone_native_accel-0.72.2-py3-none-any.whl
/tmp/dpone-v0729-final-native/dpone_native_accel-0.72.2.tar.gz
```

Wheel inspection confirmed the lifecycle modules in the provider artifact and
the adapter, ports, service, policy, readiness, CLI, and schema modules in the
root artifact.

The root wheel advertises `Provides-Extra: kubernetes` and
`kubernetes>=32.0.1,!=36.0.0,<37`. An isolated install imported the real
`V1Preconditions`, application service, and provider lifecycle contract, while
base/help paths remain lazy.

## Residual risk and release status

- Live retained/active/orphan lifecycle, real metadata content negotiation,
  real UID/resourceVersion conflict, and namespace RBAC remain `UNVERIFIED`.
- The repository distribution version remains `0.72.2`. The v0.72.9 name is the
  implementation slice; version bump and release publication have not occurred.
- `pyproject.toml`, `CHANGELOG.md`, and release workflow files are protected
  shared paths in this task contract and were not changed.
- Existing module-size warnings listed by the gate predate this slice; no new
  warning was introduced.

The change is review/merge ready: focused and broad gates pass and the
fresh-context re-review has no unresolved finding. It is not release-ready or
production-certified until the protected release files and approved live
certification are completed.
