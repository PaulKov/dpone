# Route certification matrix v1 validation report

- Candidate base commit: `17e61ed053c9853b8aec9ac5fe936c3bba46bb50`
- Branch: `codex/airflow-self-service-roadmap`
- Scope: six-dimensional route matrix, content-bound matrix claim, schemas,
  CLI, documentation, and immutable publication behavior
- Local validation date: `2026-07-16`

## Result

| Gate | Status | Evidence |
|---|---|---|
| Approved design and task contract | PASS | `docs/feature-design-route-certification-matrix-v1.md`; `test_artifacts/agent-policy/route-certification-matrix-v1.yml` |
| Focused route/schema/CLI tests | PASS | Route matrix, route-certify, route-certify CLI, and GitOps schema tests completed without failure |
| Full non-live suite | PASS | `5113 passed, 476 skipped`; skipped tests are not treated as live proof |
| Ruff lint and format | PASS | Repository-wide checks completed without findings |
| Mypy | PASS | `612` source files checked without findings |
| Architecture/import budgets | PASS | Import rules, layer metrics, and module-size gates passed |
| Documentation | PASS | Generated references current; `536` Markdown files and `1883` local links checked; strict MkDocs build passed |
| Agent/workflow policy | PASS | Task contract, setup, governance, workflow security, branch policy, and policy tests passed |
| Packaging | PASS | Main, native accelerator, Airflow reader, and formal provider sdists/wheels built; Twine checks passed |
| Airflow public contracts | PASS | `14/14` CLI, `1/1` package, `11/11` Python, and `51/51` schema compatibility checks passed |
| Fresh-context independent review | UNVERIFIED | Subagent capacity was unavailable after repeated attempts; the draft PR must receive an independent review before merge |
| Approved vendor-live route proof | UNVERIFIED | No approved MSSQL/ClickHouse live environment or credentials were supplied |
| Production/enterprise route proof | UNVERIFIED | No verified deployment-bound production attestation sets were supplied |

## Security and failure semantics

- A route cannot advance from `experimental` without a content-valid release,
  complete source commit, fresh in-content timestamp, and exact six-dimensional
  `dpone.route-matrix-claim.v1`.
- Copying a stale bundle does not refresh proof age because filesystem `mtime`
  is not authoritative.
- Production promotion requires the existing verified attestation receipt bound
  to exact bundle bytes, release ID, deployment ID, environment, validity, and
  signer identity.
- Evidence and output readers reject symlinks, unsafe path components,
  oversized files, duplicate JSON keys, and byte conflicts.
- Repeating a semantically identical publication preserves the original
  immutable bytes even when the process clock has advanced.

## Generated artifacts

- `test_artifacts/route-certification-matrix-v1/publication/route-certification-matrix.json`
- `test_artifacts/route-certification-matrix-v1/publication/route-certification-matrix.md`
- `test_artifacts/route-certification-matrix-v1/agent_governance_gate.json`
- `test_artifacts/route-certification-matrix-v1/dist-validation-final/`
- `test_artifacts/route-certification-matrix-v1/dist-validation-exact/main/`

The catalog-only matrix is intentionally `experimental`; it is not a live or
production certification receipt. Exact-commit CI and external proof sets must
be attached to the draft PR before any stronger release claim.
