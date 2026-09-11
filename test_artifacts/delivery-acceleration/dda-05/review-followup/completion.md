# DDA-05 final completion after independent review

The certification harness correction and required local validation are complete.
The exact final source is `33b7ad30cdb12a0f1804bb1b72570504133f4342`.
The commit recording this report changes retained artifacts only; it is distinct
from the reviewed and executed source. PR: [DDA-05 / #30](https://github.com/PaulKov/dpone/pull/30).

## Changes and compatibility

Independent reviews exposed false acceptance of partial publication, premature
pipeline completion, pre-recovery corruption repaired before observation,
repeated source/publication counts, and an operation receipt appearing without
atomic target publication. The final correction rejects each invalid boundary
and retains metadata/receipt mismatches in structured diagnostic values.
Known-commit recovery may complete pending evidence/checkpoint work; an unknown
outcome still blocks replay and preserves resources.

The 49 new boundary cases complement the existing 70 producer tests. Frozen v1
outer schemas, check IDs, methods, status/evidence fields and opt-in requirements
remain compatible. Production runtime, CLI/API, manifests, wire/state/journal
and checkpoint contracts are unchanged. No migration or new ADR is required.
The certification guide documents the strengthened assertions and diagnostics.

The approved planning dependency is
`f3682940f8864563cde0e6b6ecee60f746b49020`; the production base is
`d5ad9aaecc900c24df421b160ed36b4cfc726e45`. The supplemental
[task contract](task-contract.yml) assigns the new boundary-test module.
The final [ownership audit](completion-ownership-audit.json) covers the complete
DDA-05 diff after that planning dependency, including this retained evidence.

## Validation and evidence

The full suite ran exclusively on the shared host from
2026-09-10 17:40:34 UTC to 18:01:48 UTC. Its start and end HEAD were the exact
source above; both worktree checks were clean. The two workers used the existing
locked all-extras Python 3.12.11 environment on macOS arm64. All live approval
flags were disabled. A new platform temporary parent had effective GID 20 and
verified setgid mode `02777`; no test, assertion, timeout or budget was changed.

| Check | Status | Command and retained evidence |
| --- | --- | --- |
| Full non-live suite | **PASS: 20,918 passed, 570 skipped; exit 0** | `uv run --locked --no-sync pytest -m 'not integration_live' -n auto --dist loadfile --basetemp <verified-parent>/pytest`; [raw log](final-broad.log), [machine run record](final-broad-run.json). Pytest: 1,270.80 seconds; wrapper including startup/identity checks: 1,274.27 seconds. |
| Final focused producer/boundary tests | **PASS: 119 tests** | Both test modules, independently rerun on exact clean `33b7ad3`; [review record](independent-reviews.md), existing [focused log](final-focused.log) and [final refinement coverage](final-targeted.log). |
| Independent adversarial probes | **PASS: 24 checks plus frozen v1 fixture** | [Observed results](independent-probes.json). Expected negative outcomes remain FAIL/UNVERIFIED; zero eligible live samples. |
| Ruff / format / mypy | **PASS** | Required repository checks; [Ruff](final-ruff.log), [format](final-format.log), [mypy: 1,163 files](final-mypy.log). |
| Import/layer/module gates | **PASS** | [Import rules](final-import-rules.log), [layer metrics](final-layer-metrics.log), [support module budget](final-module-size.log), [exact base/head source module gate](final-source-module-size.log). |
| Documentation and generated references | **PASS** | [Links](final-docs.log), [generated references](final-generated-references.log), [32 language cases](final-docs-language.log), [strict MkDocs](final-mkdocs.log). Final report checks are retained separately below. |
| Independent documentation/CLI journey | **PASS** | Five help paths, absence run, inspect, overwrite refusal, parsed examples, rendered sections and links; [audit](independent-docs-audit.json). |
| Current integrated producer/consumer suite | **PASS: 229 tests** | Exact separate integrated source `003d7d5073306f3cb445d9179acc420cd3dcc952`; [execution and independent review references](independent-reviews.md#integrated-interoperability). |
| Live fixture absence | **SKIP: 19 cases** | Approval disabled; [raw reasons](final-live-absence.log). No approved disposable environment. |
| Live SQL/BCP, recovery, SWITCH, performance | **UNVERIFIED** | No approved live execution or application-supplied real factory. |
| Packaging, publication and release | **N/A** | No packaging or release change; no release authority requested. |

The [change-aware plan](final-validation-plan.json) and
[validated supplemental contract](task-contract.log) remain applicable. The
source checks preceded evidence retention; the final report validation logs are
[documentation links](completion-docs.log), [language contracts](completion-language.log)
and [strict build](completion-mkdocs.log). Hashes of the newly retained execution
and review outputs are listed in [the artifact index](completion-artifacts.json).

The 570 skipped tests are reported separately and are not treated as passed or
as live certification. The dedicated 19-case absence run independently confirms
the documented environment gate.

## Historical failures and review outcome

The earlier full run remains **FAIL: 2 failed, 20,863 passed, 570 skipped**, as
recorded in the [historical completion](../completion.md). It started before the
final corrections. Both Airflow permission failures were independently traced
to a temporary parent with GID 0 that could not retain setgid. The unchanged
tests passed in the controlled diagnosis and now pass in the complete frozen
run. The earlier missing-dependency/interrupted run also remains retained as a
failure. No historical result was edited into a pass.

The two REQUEST CHANGES reviews and their red regressions remain in the earlier
records. A third fresh-context architect approved exact `33b7ad3` after its own
119 tests and 24 probes. A separate fresh-context docs/UX reviewer reported no
actionable findings. Their [recorded scopes and limitations](independent-reviews.md)
support component review readiness. The subsequent artifact-only commit is
subject to a further independent review, whose verdict is reported in the PR
and task without changing these executed-source identities.

## User journey, remaining limits and handoff

Preparation, safe absence, execution, observation, diagnosis, recovery and
cleanup are described in the [certification guide](../../../../docs/delivery-acceleration/certification.md).
The reviewed application/environment supplies real route composition and an
authoritative visibility probe. Unknown outcomes retain recoverable resources;
the harness does not authorize cleanup from a diagnostic report alone.

DDA-06 already imported both corrective source commits and exercised the current
consumer boundary. The exclusive test slot was released immediately after this
component's full process exited. Shared integration gates, current hosted CI
and merge decisions remain with their owners; this record does not assert that
those separate gates passed. Native SWITCH public admission remains unchanged.
The component is ready for review after the final artifact review; live and
release readiness remain unverified or out of scope as stated above.
