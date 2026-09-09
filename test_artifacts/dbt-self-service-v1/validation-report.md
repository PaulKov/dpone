# dbt to Airflow self-service v1 validation report

- Date: 2026-07-28
- Base release: `0.73.21`
- Base commit: `704cac9b40923f7ac0f52a84209e83e6cde03fd2`
- Branch: `codex/dbt-airflow-self-service-v1`
- Frozen implementation commit: `edcb1a797dddf037c82af57e06b696f94e1dc078`
- Frozen implementation tree: `4ab3417f11d53b44ad777646f3d6f5a707ab86dc`
- Specification: `docs/feature-design-dbt-inline-self-service-v1.md`
- Architecture decision: `docs/adr/0034-native-dbt-self-service-multi-repo.md`

## Result

`PASS` for the local, credential-free implementation contract.

The frozen implementation commit above is the exact code, schema, workflow, and
documentation subject validated by this report. This report is committed
separately so its own provenance does not create a self-referential subject
identity.

The implementation compiles one authoritative dbt project snapshot into a
content-addressed project bundle, strict dbt execution and transfer packs,
native DAG specs, and `release-set.v2`. It preserves one dpone topology,
promotion, retry, and evidence authority. Production promotion verifies a
byte-identical source mirror and dev evidence before binding the same release
to an environment-specific deployment.

The beginner path remains two author commands:

```bash
dbt parse
dpone dbt check
```

No Airflow Python or generated dpone manifest is edited by the model author.

## Verification

| Check | Status | Evidence |
| --- | --- | --- |
| Focused dbt compiler/runtime/promotion tests | PASS | selected suites covering CLI, schemas, selection, release, runtime, evidence, promotion, atomicity, and workflow outcomes |
| Full non-live suite | PASS | `8271 passed, 558 skipped, 1 warning in 113.48s` on the frozen implementation commit |
| Ruff lint and formatting | PASS | `ruff check .` and `ruff format --check .`; 3,955 files formatted |
| Mypy | PASS | 762 source files, no issues |
| Import rules | PASS | no architecture import-rule violations |
| Architecture fitness | PASS | average clustering `0.17993101066364292` with limit `0.18`; cross-layer ratio `0.28729922172545125` with limit `0.300` |
| Layer metrics | PASS | 6,039 edges; intra-layer `0.713`; cross-layer `0.287`; max cross flow 98 |
| Module size | PASS | no hard-limit violations and no increase in allowlisted debt |
| Documentation | PASS | 653 Markdown files and 2,441 local links checked |
| Documentation language/contracts | PASS | 36 focused tests |
| Generated references | PASS | 3/3 generated reference families in sync |
| MkDocs strict build | PASS | documentation site built successfully |
| Workflow security | PASS | repository workflow security policy reported zero errors and warnings |
| Agent governance | PASS | `test_artifacts/agent-policy/agent_governance_gate.json` |
| Airflow public contracts | PASS | 21 CLI, 1 package, 11 Python, and 59 schema contracts |
| Main/native/reader/provider builds | PASS | four version `0.73.21` sdist/wheel pairs in `/tmp/dpone-final-dist-edcb1a79` |
| Twine metadata | PASS | all eight candidate distributions passed |
| Native package metadata contract | PASS | editable package and wheel metadata match `0.73.21` |
| Fresh-context architecture/security/UX review | PASS with residual preview BLOCKED | Correctness and security P0s closed after independent review (see Remediation); UX MERGE; production activation remains intentionally BLOCKED |
| XCom/evidence duplicate-key fail-closed | PASS | Mapping XCom re-encoded via strict JSON; evidence→XCom rejects duplicate keys; `COMMIT_UNKNOWN` cannot green via passed hint |
| Workflow `workflow_call` input allowlist | PASS | reusable dbt workflows validate version/path/digest inputs before shell expansion; `workflow_security` PASS |
| Live MSSQL/ClickHouse/Airflow/Kubernetes/Vault | UNVERIFIED | no approved live environment was used |
| Cosmos coexistence matrix | UNVERIFIED | no isolated Airflow/Cosmos environment was executed in this local run |
| Five-user usability study | UNVERIFIED | no human-session evidence was supplied |

`558 skipped` non-live tests are not interpreted as live certification.

## Public Contract And Compatibility

- `dpone dbt check`, `explain`, `compile`, and shell-free `execute-pack` use
  stable JSON schemas and `dpone.error.v1` failures.
- New dbt release, project bundle, selection, execution, evidence, promotion,
  and report schemas are additive.
- `release-set.v2` is used for bounded dbt runtime payloads;
  `release-set.v1` remains supported for non-dbt releases.
- `dpone.dbt_publish` remains a compatibility facade. Deprecation starts in
  `0.73.21`; removal is no earlier than `0.75.0` and no earlier than twelve
  months after that release, whichever is later.
- Cosmos is neither installed nor used as a topology/runtime dependency.

## Security And Failure Closure

- Airflow parsing performs no network, database, secret, Vault, or dbt calls.
- Project archives reject traversal, symlinks, unsafe members, source races,
  and configured size/count limit violations.
- Runtime uses pinned release/deployment identities and verifies artifact
  checksums before extraction or execution.
- dbt exit codes and invalid/missing `run_results.json` fail the workload.
- Stateful transfer manifests require checkpoint configuration.
- Missing, malformed, cross-run, stale, or secret-bearing dev evidence blocks
  promotion.
- The protected promoter allowlist and asserted caller identity are independent
  inputs.
- Partial compilation is never installed; changed existing output is a CAS
  conflict.

## Documentation And CJM

The five-minute guide, dbt metadata and policy reference, complete stable error
catalog, threat model, multi-repo promotion guide, reusable workflow examples,
operator runbook, rollback procedure, compatibility policy, schema reference,
and executable demo are synchronized.

## Independent review remediation (2026-07-27)

Fresh-context reviews initially returned **NO-MERGE** on two axes. Closed before
commit:

1. **Architecture / correctness** — XCom outcome now prefers
   `/airflow/xcom/return.json` and re-validates Mapping payloads with duplicate-key
   rejection; evidence→XCom uses duplicate-free JSON (`allow_nonfinite` only for
   metric sanitization); unset `--status` no longer injects synthetic `failed`;
   `COMMIT_UNKNOWN` / failed signals beat last-wins `passed`.
2. **Security / CI** — reusable dbt self-service workflows allowlist
   caller-controlled version, path, and digest inputs before expanding them in
   shell; `tools/dbt_self_service/validate_workflow_inputs.py` covers the same
   contract in unit tests.
3. **UX** — MERGE; prod-mirror admin vars documented adjacent to the platform
   workflow section.
4. **Runtime image** — the pinned GitHub CLI Debian package now has its
   mandatory `git` dependency installed before `dpkg`; the release smoke
   contract enforces the ordering.
5. **Provider quality budget** — run-identity validation and init-fetch
   delivery policy were split into cohesive internal modules; the Airflow pack
   hard SLOC gate now passes without allowlisting new debt.

Focused regression after remediation: XCom/outcome/workflow allowlist suites
PASS; init-fetch/provider/dbt end-to-end suites PASS; release image contract
PASS; `workflow_security` PASS.

## Residual Risk

This report proves local contracts and packaged behavior only. Production route
certification, Kubernetes/Vault identity, Airflow version rows, Cosmos
coexistence, failure injection against live databases, rollout timing, and the
five-user usability target remain `UNVERIFIED` until evidence is produced from
the exact release and approved environments. Production workflow activation stays
fail-closed with `DPONE_ARTIFACT_ATTESTATION_REQUIRED` until that gate is lifted
with attested evidence.
