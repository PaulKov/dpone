# v0.72.6 Route-Attestation Integrity Validation

- Date: 2026-07-15
- Branch: `codex/v0.72.6-route-attestation-integrity`
- Base: `078ff2bc`
- Specification: `docs/feature-design-route-attestation-integrity-v0726.md`
- Task contract: `test_artifacts/agent-policy/2026-07-15-v0726-route-attestation-integrity.yml`
- Live certification: **UNVERIFIED**

## Result

The local implementation, compatibility, packaging, documentation, security,
and non-live regression gates pass. Production keyless Sigstore plus
KPO/Vault/MSSQL/ClickHouse certification was not executed because `cosign` and
an approved live environment are unavailable. This is not reported as PASS.

## Checks

| Check | Status | Observed result |
|---|---|---|
| Focused route-attestation, safe-sample, schema, CLI, and architecture tests | PASS | All selected tests passed |
| `pytest -m "not integration_live" -n auto --dist loadfile` | PASS | 4335 passed, 472 skipped in 186.55 s |
| `ruff check .` | PASS | No findings |
| `ruff format --check .` | PASS | 3063 files formatted |
| `mypy --config-file mypy.ini` | PASS | 493 source files checked |
| Import rules | PASS | No architecture import violations |
| Layer metrics | PASS | cross-layer ratio 0.300; baseline delta +0.002 within budget |
| Architecture fitness | PASS | clustering 0.179; cross-layer ratio 0.300; no findings |
| Module size | PASS | No new warnings; 10 pre-existing warnings remain |
| Workflow security | PASS | 0 errors, 0 warnings |
| Task contract | PASS | 0 errors, 0 warnings |
| Docs links/contracts | PASS | 420 Markdown files and 1743 links checked |
| Generated references | PASS | 2/2 synchronized |
| Documentation language tests | PASS | 4 passed |
| Compatibility registry | PASS | 19 entries; docs synchronized |
| `mkdocs build --strict` | PASS | Built in 8.25 s |
| `git diff --check` | PASS | No whitespace errors |

## Packaging

`uv build` passed for the root distribution, `dpone-native-accel`, and
`dpone-airflow-pack`. `twine check` passed for all six wheel/sdist artifacts in
`test_artifacts/airflow-self-service-v0726-route-attestation/dist-final/`.
Package metadata remains `0.72.2`; this feature branch does not perform a
release-version bump.

A fresh Python 3.11 environment installed the root wheel and passed:

- route-attestation contract/readiness/service import smoke;
- `dpone ops route-attestation-build --help`;
- `dpone ops route-attestation-verify --help`.

## Security Review

The fresh-context architecture review initially found parent-directory
symlink/TOCTOU gaps in trust-material reads and create-only writes. The final
implementation now traverses paths with descriptor-relative `O_NOFOLLOW`,
rejects traversal, pins parent descriptors, revalidates parent/file inodes,
publishes by create-only hard link, fsyncs, and removes partial output on
failure. Adversarial parent/final symlink, parent-swap, traversal, duplicate,
oversize, duplicate-key, and non-finite JSON tests pass.

The reviewer rechecked the fixes and reported no blocking findings. The
bounded-validity replay model, `not_before`, expiry, attestation revocation, and
signer revocation branches are implemented and tested. Invalid trust is proven
to fail before credential resolver construction.

## Remaining Risk

- Production verification intentionally fails closed where `O_NOFOLLOW` and
  descriptor-relative file APIs are unavailable; this is an availability, not
  authorization, risk.
- Real cosign identity/trusted-root behavior and the complete KPO/Vault/database
  route remain **UNVERIFIED** until an approved live certification run.
- The repository retains 10 pre-existing module-size warnings; this change did
  not add to that debt.

## Readiness

Ready for code review and stacked integration. Not yet release-certified for
the production live route until the explicit live gate produces current
evidence from the release candidate commit.
