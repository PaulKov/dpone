# Signed catalog and extension conformance v1 validation

- Date: `2026-07-17`
- Branch: `codex/airflow-self-service-roadmap`
- Base: `origin/master`
- Specification:
  `docs/feature-design-signed-catalog-extension-conformance-v1.md`
- ADR: `docs/adr/0021-signed-catalog-bundle-conformance-boundary.md`

## Result

Local implementation and non-live validation: **PASS**.

Real keyless Sigstore verification: **UNVERIFIED**. No approved CI identity,
trusted root, or restricted production artifact store was used.

Fresh-context review: **UNVERIFIED**. The reviewer orchestrator returned
`agent thread limit reached`. Automated gates and integrator review are not
misrepresented as independent review.

## Public contract and compatibility

- Added deterministic `dpone.catalog-bundle.v1` for recipe catalogs and
  connection registries, plus trust-policy and verification receipts.
- Added four closed extension-conformance profiles. Missing, `SKIP`, `N/A`, or
  unavailable required evidence cannot produce `PASS`.
- Added three platform-only `dpone supply-chain` commands. The five-command
  beginner Airflow journey is unchanged.
- Route attestation keeps its stable imports and error codes while delegating
  process execution to the generic blob verifier.
- Existing unsigned local catalogs remain compatible; signed promotion is an
  additive platform path.

## Validation

| Check | Status | Observed evidence |
|---|---|---|
| Focused bundle/conformance/route/recipe tests | PASS | all selected tests reached 100% |
| Mutation during verifier and duplicate JSON keys | PASS | fail closed before a verified receipt |
| 1,000-entry catalog benchmark | PASS | p95 `1929.496 ms`; budget `2000 ms`; 20 repetitions |
| Mutation corpus | PASS | 20/20 substitutions detected; 0 false successes |
| Benchmark side effects | PASS | network `0`; subprocess `0` |
| Airflow 100 DAG / 500 workload parse SLO | PASS | `tests/test_airflow_provider_parse_slo.py` |
| Full non-live suite | PASS | exit `0`; 100%; `317.26s` |
| Ruff and format | PASS | 3,270 files formatted; no findings |
| Mypy | PASS | 605 source files |
| Import rules and architecture fitness | PASS | no violations |
| Layer budget | PASS | cross-layer ratio `0.300`; max flow `99` within baseline allowance |
| Module-size budget | PASS | no new hard-limit debt |
| Airflow v1 public-contract freeze | PASS | CLI 14/14; Python 11/11; schemas 51/51 |
| Compatibility registry | PASS | 19 entries; generated block synchronized |
| Generated references | PASS | 3/3 synchronized |
| Documentation and links | PASS | 522 Markdown files; 1,860 local links |
| MkDocs strict | PASS | site built successfully |
| Root/native/pack/provider builds | PASS | wheel and sdist for version `0.72.3` |
| Twine metadata | PASS | all artifacts in `dist/` passed |
| Governance gate | PASS | producer-generated receipt in this evidence directory |
| Real keyless Sigstore identity/root | UNVERIFIED | approved environment unavailable |
| Live Vault/Kubernetes/MSSQL/ClickHouse | N/A | no runtime or route behavior changed in this slice |
| Fresh-context reviewer | UNVERIFIED | agent thread limit reached |

## Security and failure semantics

- Every listed payload is confined, size-bounded, checksummed before signature
  verification, and rechecked after signature verification to close the
  mutation window.
- Canonical bundle JSON rejects duplicate keys, non-finite constants, excess
  depth and excess nodes. YAML catalog data retains its bounded parser.
- Connection-registry payloads reject inline secret-value keys. Error and CLI
  output contain no secret values or registry bodies.
- Receipts and conformance outputs are create-only. A differing existing output
  is a conflict, never an overwrite.
- Catalog controls represent the full 1,001-payload contract with finite
  32,000-token and 16,000-node bounds.

## Documentation and CJM

Platform docs now cover build, external signing, verification, promotion,
recovery, limits, conformance profiles and stable error codes. The beginner
journey and Airflow parse path do not gain a command, signer dependency,
network call, template execution, or secret lookup.

## Remaining risk

The change is ready for code review and CI. Production designation remains
blocked on a real keyless Sigstore run against the exact release commit and an
approved trusted root. Resolver and route conformance still require their own
approved live evidence. Independent reviewer evidence must be rerun when an
agent slot is available.
