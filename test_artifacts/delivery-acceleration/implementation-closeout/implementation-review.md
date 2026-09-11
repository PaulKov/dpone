# Fresh independent review of benchmark corrections

Verdict: **APPROVE the bounded correction for integration. No actionable findings.** Both previously reproduced P2 defects are resolved. Final merge readiness remains conditional on the parent's broader gates and exact final-head CI; this review grants no merge, release or live-execution authority.

Reviewed repository: `/Users/paulkov007/.codex/worktrees/b364/dpone-public`.
Reviewed commit: `81a7f4813dc9a4a4d41f81401cf4490d284b51d2`.
Reviewed tree: `72e0b2e12826ebca2941e9155dfede2c5f710727`.
Correction base: `770c3fea9bd24fa004ad3a300f59f9586673e257`.
Original reviewed PR34: `fa16c23c137c76fc4b218cd729820b987b698418`.
Pinned imported upstream: `a1aadcaf6beaa2e82abcac27f2aed0b5113986f7`.

This reviewer did not implement the correction. I read applicable ancestor/repository/source/test/docs AGENTS rules, the approved `dda-06-benchmark-review-fixes.md` supplement and YAML task contract, relevant testing/compatibility/coordination guidance, the complete correction diff, actual producer/consumer/error/output execution paths, and the original independent reproduction scripts and report. I wrote only this assigned external evidence directory. No source, dependencies, branches or historical evidence were changed.

## Finding disposition and source reasoning

- **P2 stale configuration/environment identity: RESOLVED.** At `src/dpone/runtime/native_delivery_benchmark.py:149`, exact limits still pass through `normalize_delivery_limits`; invalid values retain `invalid_limits`. Lines 154–158 compare the canonical normalized configuration and complete environment body against their recorded SHA-256 values before any receipt or sample reads at line 164. The existing full receipt/observation bindings and retained-byte checks remain unchanged. Reading the original negative reproduction files now raises precisely `configuration_digest_mismatch` or `environment_digest_mismatch`, even with equal changes on both subjects. Independently rehashing copied negative bodies still fails `receipt_identity_mismatch`.
- **P2 cross-version rejection: RESOLVED.** `src/dpone/contracts/native_delivery_observations.py:204` introduces one pure projection. It excludes only the outer `sha256` and replaces only the value of an existing `versions.dpone` entry. Key presence, every other version, layout, resources and their JSON types remain significant. `src/dpone/runtime/native_delivery_benchmark.py:285` and `tools/native_delivery_live_support/validation.py:185` share this policy. The helper independently checks each full environment digest before projection and preserves `invalid_run_contract` on mismatch. The original, unchanged 0.76.0/0.77.0 producer pair now compares PASS with ratio 0.8. The new producer regression independently executed here also proves the current 0.76.0/0.77.1 pair.
- **Compatibility and failure behavior: preserved in reviewed scope.** The helper import remains inside `require_comparable` at line 191. `run`, `inspect` and help import `validate_run` without loading the candidate-only contract. `runner.configuration` continues adapting the actually loaded old model. I inspected the real-baseline subprocess test and the parent's exact-source 269-test result, which includes that test; I did not repeat its baseline environment setup. Atomic report publication, evidence alias protection, failure cleanup, concurrent no-clobber behavior, status aggregation and within-run environment checks are unchanged. The independently executed atomic-output tests and replay probes passed.

The digest checks establish content consistency only. Neither the source nor this review treats synthetic `execution="live"` fixture labels as live authenticity or certification.

## Risk-based validation matrix

| Dimension | Layer | Result and evidence |
| --- | --- | --- |
| Positive / canonical ordering | Unit and producer-consumer contract with row/session doubles | PASS: same-version and original cross-version reports; new current version-pair producer regression; reordered mapping keys; exact nonmutating projection |
| Negative identity / stale evidence | Contract and CLI subprocess | PASS: original stale configuration/environment files reject before output; both-side stale helper hashes reject; checksum-only repair cannot rebind receipts |
| Boundary / types / fields | Unit and contract | PASS: invalid-limit errors; Python/dependency/server/BCP/layout drift; integer versus float/bool resources; extra nested key; present versus absent dpone key; two absent keys remain comparable |
| Backward compatibility | Source/import-path review; parent's actual baseline subprocess evidence | PASS for reviewed lazy-import behavior and retained v1 producer reports; parent's focused-green includes actual old run/inspect/help. Independent execution of the baseline fixture: N/A, not repeated |
| Retry / replay / idempotency | Offline file integration | PASS: repeated comparison and overwrite produce identical bytes; portable retained bundle can be reread; original input bytes remain unchanged |
| Failure / concurrency / output atomicity | Contract and mocked failure integration | PASS: stable CLI exit 2 and sanitized stderr with empty stdout; invalid inputs preserve existing output and create no extra files; failed atomic replace cleans temporary file; concurrent no-clobber behavior remains intact |
| Delivery transaction / checkpoint changes | Source diff review | N/A: no delivery, transaction, journal, checkpoint, manifest or state change in this correction |
| Live integration / certification / performance | Live and certification | SKIP live execution: no approved environment. Live behavior and acceleration claims UNVERIFIED; mocked/synthetic outcomes are not live passes |

## Commands, environment and artifacts

Existing `.venv`, Python 3.12.11, macOS 26.3.2 arm64. All independent Python invocations used `-B` and `PYTHONDONTWRITEBYTECODE=1`; pytest used `-p no:cacheprovider`, with temporary output in `fresh-review/pytest-tmp`. Actual imported paths for all three corrected modules resolve to this reviewed checkout (`accounting.json`). Both retained command receipts record the clean reviewed source before and after, unchanged tree and source SHA-256 `f3ffc29a79c0dc465efa6ede1a80707f1e237d45c542864cc412bf1136593acb`. The unchanged repository `dda-06/run_check.py` producer was imported with only its output directory redirected.

| Command | Observed result | Artifacts |
| --- | --- | --- |
| `.venv/bin/python -B fresh-review/independent_probes.py` with checkout `PYTHONPATH` | PASS, exit 0, 19 assertion-based probe cases, 1.902s | `independent-probes.json`, `independent-probes.log`, `independent_probes.py` |
| `.venv/bin/python -B -m pytest tests/test_native_delivery_benchmark_identity.py` plus invalid-limits and three atomic-output test nodes, `-q -p no:cacheprovider` with external basetemp/JUnit | PASS, exit 0, 26 tests, zero failures/errors/skips, 4.113s | `focused-independent.json` contains exact argv; `focused-independent.log`, `focused-independent-junit.xml`, `accounting.json` |
| Parent's six-module focused-green command, inspected rather than rerun | PASS recorded on this same clean source, 269 tests, zero failures/errors/skips, 39.186s | `../focused-green.json`, `../focused-green-junit.xml`, `../focused-green.log` |
| Broad static/types/architecture/docs/metrics and upstream preservation | N/A within this bounded review; independently owned by parent | Parent records; no duplicate claim here |

The 103 historical artifact files were hashed before and after the independent probes and remained unchanged. New `rehashed-configuration/` and `rehashed-environment/` directories are deliberately invalid copied fixtures for negative receipt-binding tests. `portable-comparison.json` and its retained bundles are synthetic diagnostic output, never live evidence. The original red fixture failure and later corrected red reproduction were not overwritten or reclassified.

## Documentation, impact and readiness

Certification, observation and operations guidance now consistently explains both full per-run hash binding and the narrow cross-subject version exception. Error documentation gives the new stable digest-mismatch reasons and producer-based recovery; the changelog records the correction. This repairs the retained-report inspection and pinned-baseline comparison journeys. Valid v1 producer evidence requires no migration; diagnostic format, public delivery behavior and transaction contracts remain unchanged.

No remaining correction blocker was established. Review is complete for the exact commit above and ready for integration subject to the parent's required final gates. Any later production/test/doc changes require a bounded follow-up review. Generator-only metrics or exact-tree transfer changes need identity/freshness confirmation before this verdict is applied to a different final commit.
