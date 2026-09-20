# Immutable runtime-authority payload validation report

- Date: 2026-09-20
- Base commit: `bb1c6ea75e0355f9d9cd2de636d1e5eb160b6984`
- Validated implementation commit: `e1e4f7421c9d083b80f049083c7a854965a2f54a`
- Target release: `0.83.0`
- Scope: bounded non-secret immutable runtime-authority payload for protected Airflow KPO development deployments

No payload bytes, credentials, private endpoint names, or tenant-specific values are present in this report.

## Results

| Area | Status | Evidence |
|---|---|---|
| Focused runtime, provider, CLI, dbt wire, and schema tests | PASS | 295 selected tests passed after the final architecture refactor |
| Full non-live regression suite | PASS | `25688 passed, 571 skipped, 10 warnings` using `uv run pytest -m "not integration_live" -n auto --dist loadfile` |
| Ruff lint and formatting | PASS | `uv run ruff check .`; `uv run ruff format --check .` |
| Static typing | PASS | `uv run mypy --config-file mypy.ini`; 1,220 source files checked |
| Import rules | PASS | `uv run dpone docs check-import-rules` |
| Layer metrics | PASS | 58 layers, 9,677 edges, max cross-flow 214, within the +5 ratchet |
| Architecture fitness | PASS | average clustering 0.181892, below the 0.182 hard limit |
| Module-size ratchet | PASS | exact base/head comparison; debt entries reduced from 50 to 49 |
| Documentation checks | PASS | docs check, generated-reference check, language contracts, and strict MkDocs build |
| Governance and workflow security | PASS | `test_artifacts/agent-policy/agent_governance_gate.json`; branch-protection and workflow-security validators |
| Package build and metadata | PASS | wheel and sdist for all four packages; Twine accepted all eight artifacts |
| Clean wheel smoke install | PASS | all four `0.83.0` wheels installed together and exposed the expected versions |
| Live Kubernetes projection | UNVERIFIED | no explicitly approved live cluster or credentials were supplied; no live claim is made |

## Failure and retry disclosure

The first broad run used an incomplete local environment and failed on missing optional dependencies. After the documented `uv sync --locked --all-extras`, all affected groups passed. A subsequent parallel run exposed two resource-sensitive timeout/benchmark failures; both passed in isolation, and the final unchanged exact-command run passed in full. These intermediate failures are not counted as passing evidence.

## Contract and compatibility conclusion

The legacy Kubernetes Secret mode remains on frozen v4 contracts. Immutable mode is opt-in on closed v5 deployment, index, and runtime-plan contracts. Its bytes are explicitly safe-to-persist configuration, not credentials; SHA-256 supplies integrity, not confidentiality. Malformed, oversized, non-canonical, path-substituted, or digest-mismatched inputs fail before runtime authority access.

The implementation is ready for independent review and normal pull-request CI. Release readiness and publication remain separate decisions until the reviewed commit is merged and the release-controller receipts succeed.
