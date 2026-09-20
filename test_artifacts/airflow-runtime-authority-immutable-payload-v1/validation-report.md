# Immutable runtime-authority payload validation report

- Date: 2026-09-20
- Base commit: `bb1c6ea75e0355f9d9cd2de636d1e5eb160b6984`
- Validated implementation commit: `93f90708ce28b54992ce23ec707240a6e56837a9`
- Target release: `0.83.0`
- Scope: bounded non-secret immutable runtime-authority payload for protected Airflow KPO development deployments

No payload bytes, credentials, private endpoint names, or tenant-specific values are present in this report.

## Results

| Area | Status | Evidence |
|---|---|---|
| Focused runtime, provider, CLI, dbt wire, schema, output-redaction, and side-effect-ordering tests | PASS | All affected suites passed after the independent-review fixes |
| Full non-live regression suite | PASS | `25690 passed, 571 skipped, 10 warnings in 882.26s` using `uv run pytest -m "not integration_live" -n auto --dist loadfile` |
| Ruff lint and formatting | PASS | `uv run ruff check .`; `uv run ruff format --check .` |
| Static typing | PASS | `uv run mypy --config-file mypy.ini`; 1,220 source files checked |
| Import rules | PASS | `uv run dpone docs check-import-rules` |
| Layer metrics | PASS | 58 layers, 9,677 edges, max cross-flow 214, within the +5 ratchet |
| Architecture fitness | PASS | average clustering 0.181892, below the 0.182 hard limit |
| Module-size ratchet | PASS | exact base/head comparison; debt entries reduced from 50 to 49 |
| Documentation checks | PASS | docs check, generated-reference check, language contracts, and strict MkDocs build |
| Independent fresh-context review | PASS | Follow-up review of `b4cab1aa88a5b39382f70a512f35f361e51de8ad`; `independent-review.md` |
| Governance and workflow security | PASS | `test_artifacts/agent-policy/agent_governance_gate.json` records `head_commit=93f90708ce28b54992ce23ec707240a6e56837a9`; branch-protection and workflow-security validators passed |
| Package build and metadata | PASS | wheel and sdist for all four packages; Twine accepted all eight artifacts |
| Clean wheel smoke install | PASS | all four `0.83.0` wheels installed together and exposed the expected versions |
| Live Kubernetes projection | UNVERIFIED | no explicitly approved live cluster or credentials were supplied; no live claim is made |

## Failure and retry disclosure

The first broad run used an incomplete local environment and failed on missing optional dependencies. After the documented `uv sync --locked --all-extras`, all affected groups passed. A subsequent parallel run exposed two resource-sensitive timeout/benchmark failures; both passed in isolation, and the final unchanged exact-command run passed in full. These intermediate failures are not counted as passing evidence.

The first independent review returned `BLOCK`: public JSON could expose
`payload_b64`, malformed v5 input could create the durable development-evidence
spool before rejection, and this report/governance evidence was stale. Commit
`93f90708ce28b54992ce23ec707240a6e56837a9` redacts the field at the shared
public-output boundary, validates/materializes the payload before spool
creation, adds regression tests for both JSON/text output and side-effect
ordering, and regenerates the governance receipt. The follow-up independent
review returned `PASS` with no remaining blocking findings.

## Contract and compatibility conclusion

The legacy Kubernetes Secret mode remains on frozen v4 contracts. Immutable
mode is opt-in on closed v5 deployment, index, and runtime-plan contracts. Its
bytes are explicitly safe-to-persist configuration, not credentials; SHA-256
supplies integrity, not confidentiality.

Malformed, oversized, non-canonical, path-substituted, or digest-mismatched
inputs fail before durable spool creation or runtime authority access. The
immutable bytes remain in hash-bound on-disk artifacts but are redacted from
public JSON and omitted from text output.

The implementation passed follow-up independent review and is ready for normal
pull-request CI. Release readiness and publication remain separate decisions
until the reviewed commit is merged and the release-controller receipts
succeed.
