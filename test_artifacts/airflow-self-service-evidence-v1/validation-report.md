# Airflow self-service evidence kit v1 validation

- Scope: Phase 4 five-user and production reference evidence projection
- Baseline commit: `6fc1289070eac73171a683656e5b99cdda56c042`
- Branch: `codex/airflow-self-service-roadmap`
- Validated: `2026-07-16`

## Result

The local implementation is ready for review and integration. It does not make
the external roadmap acceptance gates pass. Five real first-time users and two
approved independent production deployments remain `UNVERIFIED`.

## Checks

| Check | Status | Observed result |
|---|---|---|
| Focused self-service and schema contracts | PASS | 53 focused scenarios after the final cross-identity case |
| Full non-live suite | PASS | 5127 passed, 476 skipped |
| Ruff lint | PASS | all files |
| Ruff format | PASS | 3295 files formatted |
| Mypy | PASS | 612 source files |
| Import rules | PASS | no architecture import violations |
| Layer metrics | PASS | 58 layers, 5053 edges, cross ratio 0.300, no issues |
| Module size | PASS | no hard-limit violations |
| Architecture fitness | PASS | hard cross-layer ratio remains at or below 0.300 |
| Agent policy setup | PASS | 0 errors, 0 warnings |
| Branch protection policy | PASS | valid |
| Workflow security | PASS | valid |
| Agent policy tests | PASS | complete focused suite |
| Generated references | PASS | 3/3 in sync |
| Compatibility policy | PASS | 19 entries; docs in sync |
| Airflow v1 public contract | PASS | CLI 15/15, packages 1/1, Python 11/11, schemas 53/53 |
| Documentation links | PASS | 538 Markdown files, 1892 local links |
| Documentation tests | PASS | language and Airflow docs contracts |
| MkDocs strict build | PASS | site built without warnings |
| Core package build | PASS | sdist and wheel |
| Native accelerator build | PASS | sdist and wheel |
| Lightweight Airflow reader build | PASS | sdist and wheel |
| Formal Airflow provider build | PASS | sdist and wheel |
| Twine metadata | PASS | all artifacts in `dist/` |
| Five first-time users | UNVERIFIED | no approved human study supplied |
| Two production reference deployments | UNVERIFIED | no approved deployment proof supplied |
| Fresh-context independent review | UNVERIFIED | agent thread limit prevented reviewer spawn |

## Evidence

- Machine-readable readiness report:
  `test_artifacts/airflow-self-service-evidence-v1/publication/self-service-certification.json`
- Human-readable readiness report:
  `test_artifacts/airflow-self-service-evidence-v1/publication/self-service-certification.md`
- Approved feature specification:
  `docs/feature-design-airflow-self-service-evidence-v1.md`
- Agent task contract:
  `test_artifacts/agent-policy/airflow-self-service-evidence-v1.yml`

The readiness report intentionally says `UNVERIFIED`. Synthetic fixtures in
unit tests prove only deterministic policy behavior and cannot be used as
human-research or production certification evidence.

## Public contract and compatibility

The change is additive. Existing authoring, runtime, pack, provider, release,
deployment, route, and Airflow evidence contracts are unchanged. The new
command is a platform tier facade; the beginner golden path remains five
commands. Existing immutable identity and evidence parsers remain authoritative.

## Documentation and CJM

The facilitator protocol, privacy rules, operator checklist, status semantics,
troubleshooting, schema references, CLI reference, frozen v1 reference, roadmap
and first-DAG navigation are updated. A new user is not asked to run the
certification command.

## Remaining risk

- Real participant recruitment, consent and timing are external activities.
- Production proof requires approved Airflow, Kubernetes, Vault and route
  evidence from two independent deployments.
- No live profile is reported as PASS.
- A fresh-context reviewer must still inspect the exact integration commit.
