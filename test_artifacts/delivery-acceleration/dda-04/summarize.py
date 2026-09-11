"""Assemble the DDA-04 handoff from retained, source-bound command records."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent
SUBJECT = "d8b09de644ab3bf72f9eee7b51cc8b8e38ca8a3c"
PLANNING = "f3682940f8864563cde0e6b6ecee60f746b49020"
NAMES = "producer focused contract selection ruff format mypy mypy-component imports layers architecture module-size pytest docs references docs-language mkdocs".split()


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main() -> None:
    checks = [json.loads((OUTPUT / f"{name}.json").read_text()) for name in NAMES]
    for check in checks:
        assert check["head"] == SUBJECT
        assert check["source_unchanged"] and check["source_before"] == check["source_after"]
    contract = yaml.safe_load((OUTPUT.parent / "agent-task-contracts/dda-04-switch.yml").read_text())
    paths = set(git("diff", "--name-only", PLANNING).splitlines())
    paths.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    paths.update(
        str((OUTPUT / name).relative_to(ROOT)) for name in ("completion.md", "owned-paths.txt", "ownership.json")
    )
    conflicts = [
        path for path in paths if not any(path == own or path.startswith(own + "/") for own in contract["owned_paths"])
    ]
    assert not conflicts, conflicts
    (OUTPUT / "owned-paths.txt").write_text("\n".join(sorted(paths)) + "\n")
    (OUTPUT / "ownership.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "subject": SUBJECT,
                "planning_dependency_excluded": PLANNING,
                "contract": str((OUTPUT.parent / "agent-task-contracts/dda-04-switch.yml").relative_to(ROOT)),
                "owned_changed_paths": sorted(paths),
                "conflicts": conflicts,
            },
            indent=2,
        )
        + "\n"
    )
    table = "\n".join(
        f"| {c['name']} | {c['status']} | {c['duration_seconds']} | [{c['log']}]({c['log']}) |" for c in checks
    )
    report = f"""# DDA-04 completion and integration handoff

PR: [DDA-04 isolated SQL Server SWITCH](https://github.com/PaulKov/dpone/pull/29), draft.
Reviewed production implementation: `9795f01cb9a7ad01f404ce69e1356fa2aa32a399`.
Reviewed producer correction and exact validation subject: `{SUBJECT}`.
Planning dependency (unchanged): `{PLANNING}`.
Audited upstream baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
Final artifact commit contains this report/logs only; it does not change the
reviewed production source. Resolve its SHA from the PR head/history.

## Result and compatibility

Implemented immutable feature-local models and SQL/transaction ports, a pure
one-partition planner, a strict SQL Server 2022 catalog adapter, and old-out/new-in
executor. The authored finite temporal RANGE RIGHT interval controls scope even
for empty input. Catalog/object/owner/storage drift, unsupported dependencies,
foreign or nonempty switch-out and outside-window prepared rows fail closed.
The executor holds deterministic table locks, invokes injected full typed
prepared verification under those locks and returns uncommitted row counts.
It never creates/settles a transaction, receipt, retry, fallback or cleanup.

Public native SWITCH rejection before I/O remains unchanged. No schema, registry,
policy, composition, version, provider or release changes. No migration required.
All own changed paths are listed in [owned-paths.txt](owned-paths.txt); the
producer-checked ownership result is [ownership.json](ownership.json). Inherited
planning paths are excluded from this audit. Shared semantic files remain DDA-06
owned. The branch was created from a clean worktree and imported the immutable
planning dependency by ordinary fast-forward merge; pushes never forced history.

## Validation evidence

Every command record binds source and producer identities before/after execution.
All records below have unchanged identities on `{SUBJECT}`. `dirty: true`
records generated artifacts/review prose; production source stayed frozen.
The original pre-freeze suite was interrupted and is not counted as a pass.

| Check | Status | Seconds | Log |
| --- | --- | ---: | --- |
{table}

Focused: **103 PASS**, zero skips/errors/failures, comprising 98 component cases
plus five existing public-contract regressions. Producer: **4 PASS**. See
[focused JUnit](focused-junit.xml), per-command JSON and [environment](environment.json).
Red-to-green evidence is retained in [red.log](red.log).

Full non-live suite: **FAIL**, 20,583 passed, 815 skipped, 28 failed, 2 collection
errors, 1,563.27 pytest seconds; two xdist workers. None of the failed test names
belongs to DDA-04's new test files. Missing psycopg/pyarrow/Google SDK/dbt/native
accelerator dependencies are explicit in multiple traces. Other accelerated and
runtime-factory expectations are unresolved in this base-only environment;
dependency causation for every such assertion is not independently established.
No claim is made that these are passing baseline tests. Two architecture tests
also fail at the exact clustering value reported below. Full tracebacks and
failed names remain in [pytest.log](pytest.log); no tests were weakened/skipped
to manufacture a green suite. Required integrated validation remains outstanding.

Layer metrics: **FAIL**, runtime-to-contracts 217 > 214 (baseline 209 plus allowed
5). Architecture fitness: **FAIL**, average clustering 0.1823846551601551 > tool
threshold 0.182. The four new contracts edges construct actual models/errors;
removing them as annotation-only would be false. No budget/baseline was changed.
DDA-06/root have been given this precise shared integration conflict.

Live SQL: **SKIP**, no explicitly approved disposable environment or credentials.
Live SWITCH syntax/atomicity/locking and performance: **UNVERIFIED**. Synthetic
SQL/finalizer/receipt tests do not establish live certification. Packaging,
publication, provider changes and release readiness: **N/A** for this task.

## Review, documentation and user journey

Two independent fresh-context reviewers inspected the component. Corrected first
findings: DDL trigger visibility, bound rules/defaults/fulltext metadata and
same-count prepared tampering. The second reviewer found no further component
blocker and identified the evidence source-identity race, then independently
re-reviewed its correction and four tests. See [review.md](review.md).

The English component guide is `docs/delivery-acceleration/partition-switch.md`:
purpose/profile, owned preparation, API/authority requirements, execution,
stable reasons, receipt-first recovery, retention, upgrade and fixture journey.
It includes a sequence diagram and points to executed synthetic examples.
Strict MkDocs/docs/reference/language checks pass. Navigation/architecture/
changelog/shared operations updates belong to DDA-06. The [ADR decision note](adr-decision-note.md)
is ready for the next unused ADR number; no forbidden ADR/index file was edited.

## Exact integrator instructions and remaining limits

1. With planning dependency present, integrate reviewed commits in order using
   `cherry-pick -x`: `9795f01cb9a7ad01f404ce69e1356fa2aa32a399`, then `{SUBJECT}`,
   then this report's artifact commit if desired. Preserve FAIL/SKIP/UNVERIFIED.
2. Keep SWITCH unregistered; do not add shared runtime wiring or change public
   rejection. Frozen helpers remain `plan_native_switch(snapshot, *, interval,
   owner_binding)` and `execute_native_switch(plan, *, transaction)`.
3. A future approved transaction bridge must provide same-session query/execute,
   assert_authority(binding), and verify_prepared(plan) under executor locks.
   Bind the verifier to durable full typed payload/framework metadata and the
   exact invocation/generation/mutation, including caller transaction-clock
   projection. Resolve exact receipts before former prepared-content reads.
4. Freeze privileged database/server DDL-trigger configuration through the caller
   transaction; table locks do not guard global administration. Retain prepared
   and switch-out resources until outcome/evidence/retention requirements settle.
5. Apply the independently authorized shared dependency correction; rerun layer,
   architecture and the full non-live suite with the required test extras on the
   exact integrated commit. Do not launder imports or weaken budgets.
6. Allocate/register the ADR and navigation; give DDA-05 the
   [disposable fixture requirements](dda-05-fixture-requirements.md). Live
   activation additionally needs aligned provisioning, durable ownership,
   retention, public admission approval and exact-environment proof.

Ready for **review and scoped integration**. **Not merge-ready** while required
shared gates/full-suite validation remain unresolved; **not release/production
certified**. There is no outstanding implementation edit inside DDA-04 ownership.
"""
    (OUTPUT / "completion.md").write_text(report)
    print("Wrote completion.md, owned-paths.txt and ownership.json; no ownership conflicts.")


if __name__ == "__main__":
    main()
