# B02 recommended v1: independent design review receipt

- Date: 2026-09-14
- Verdict: **PASS — ready for maintainer contract approval**
- Reviewed artifact: [b02-codec-design-draft.md](b02-codec-design-draft.md)
- Specification SHA256: `47a926b8925c973587a0501b1d000ffc36f4bd1078b0b9621d1e750d47d860d4`
- Reviewed [approval appendix](b02-contract-appendix.md) SHA256:
  `cfa3434f51049338f3fa96314296eb4f5ed2c91931fcca9b98ea5ac93acd1433`
- Assessed source / frozen B01 commit: `dad0f8c5781320933ee93ddd849bc81dbda30ae0`
- Design status: **RESEARCHED — no implementation authority**
- Review method: three independent read-only role reviews and follow-up inspection
  of the same final specification/appendix hash pair. The roles did not author
  these artifacts. RESEARCHED follows the coordinator's final instruction after
  design blockers were resolved; it is not maintainer APPROVED status.

## Scope and resulting proposal

The artifact specifies a new explicit `ClickHouseSink.stage_validated_file` Python
API with mandatory bounded storage policy, a finite type/mode matrix, canonical
source decoding, separately identified RowBinary preparation, controlled client
and HTTP adapters, durable attempt events and manual recovery for unknown remote
execution. Ordinary CLI/runtime dispatch and schema evolution remain outside v1.
This is an additive feature proposal requiring approval, not an existing-contract
repair or evidence of working transport support.

Only the design, proposed ADR/path appendix and this receipt were written for this
follow-up. No production
code, tests, dependencies, tracked B01 artifacts or PR53 commit were changed.

## Independent verdicts

| Role / agent | Final verdict on the artifact hash pair | Scope and limits |
| --- | --- | --- |
| Architecture / `wrapper_contract` | PASS — ready for contract approval | Binding, preparation, empty input, summary/evidence ordering, cancellation and retained resources; no blocking contradiction remains in the changed scope |
| Test/certification / `binary_test_matrix` | PASS — test/certification contract-approval readiness | Exact value oracles and false-positive controls; adapter, journal, resource, deadline and unknown-execution test gates |
| Documentation/UX / `b02_design_ux` | PASS — docs/UX approval readiness | Python-only journey, bounded policy, failure/recovery boundaries and genuine remaining maintainer decisions |

These verdicts concern design readiness. They do not approve implementation,
establish live transport fidelity or authorize a release.

## Findings and disposition

| Finding | Disposition | Follow-up evidence |
| --- | --- | --- |
| A1: opaque binding cannot itself recheck the privately retained mutable contract before spool sealing | RESOLVED: preparer accepts the active typed attempt, reads its binding and calls verify_unchanged before sealing | Architecture PASS on final hash |
| A2: zero-row INSERT omission conflicted with unconditional acknowledgment requirements | RESOLVED: no INSERT/query ID/sender receipt, local zero-byte/empty-hash proof and exactly one COUNT=0; transport acknowledgment is conditional on nonempty input | Architecture and test PASS on final hash |
| T1: durable journal, controlled adapter and resource guarantees needed explicit fault gates | RESOLVED: added real local adapter fixtures, per-mutation intent/fsync failures, final-event failure, immutable hash-chain/no-replace/tamper, exact/+1 limits, deadlines/capacity and remote-unknown retention tests | Test/certification PASS on final hash |
| T2: proposed writer contract omitted paths for required generated evidence and review receipts | RESOLVED: explicit new local non-live output ownership; protected historical evidence and live authorization remain separate | Test/certification PASS on final pair |
| Coordinator workflow completeness: required generated quality metrics and approved-artifact transfer | RESOLVED: quality-metrics path producer-only; exact approved files copied and hash-verified before future worktree contract activation | All three roles PASS on final pair |

Earlier review verdicts were REVISE until these changes were made. No blocking
finding is waived. These are specification corrections; the specified tests have
not been implemented or executed by this artifact-only task.

## Checks and evidence

- **PASS:** final specification and appendix SHA256 inspection by all three roles.
- **PASS:** template major sections, RESEARCHED marker, no D1–D7 technical placeholders,
  Markdown fences/table widths, local links and trailing-whitespace inspection.
  Proposed YAML has exactly the task-template keys, one shared writer, unique owned
  paths, no forbidden-path overlap and existing read-only paths.
  Machine-readable result:
  `/tmp/dpone-binary-audit-b285-rkmEEC/b02-design-structure.json`.
- **PASS:** tracked worktree/index diff empty; source HEAD and remote PR53 both
  remain `dad0f8c5781320933ee93ddd849bc81dbda30ae0`.
- **N/A:** implementation tests, packaging and site build for this untracked
  design-only artifact. Earlier B01 evidence is separate and is not reused as B02
  capability proof.
- **UNVERIFIED:** B02 implementation, real adapter behavior and resource bounds.
- **SKIP:** external/live value reconciliation; no environment authorization.
- **N/A:** merge, publication and release certification in this task.

## Documentation, risks and next decisions

The draft defines a Python developer journey and a future tutorial/reference,
architecture/ADR and operator recovery plan. It explicitly avoids representing
ordinary PostgreSQL→ClickHouse CLI examples as acceptance evidence.

The maintainer must accept the narrow API/type/transport scope and its full-file
preparation, mandatory local journal and manual unknown-attempt recovery cost,
then grant implementation through APPROVED status and acceptance of the concrete
ADR0064 decisions and single-integrator path contract in the appendix.
Target release and any live environment remain separate future decisions.

Current source receipt provenance is trusted but unauthenticated. External DDL in
the private attempt namespace is excluded; UUID/comment check then DROP is not an
atomic compare-and-swap. No automatic retry or general crash cleanup is promised.
Transport acknowledgment/cancellation must be proven through the specified local
adapter tests and later authorized readback before capability claims.

Ready for **contract approval review**. Not an implementation, merge or release
approval. The frozen B01 candidate remains separately reviewable in
[PR53](https://github.com/PaulKov/dpone/pull/53).
