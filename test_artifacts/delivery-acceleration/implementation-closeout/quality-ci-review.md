# Independent final-head quality population follow-up

Verdict: **APPROVE the scoped quality CI evidence. No findings.** Successful shard execution and complete deterministic population coverage are independently verified for both supported interpreters on `c1a56b055f079bcc6dd9957300c5494f00b6080c`.

Provider run: [34573417026, attempt 1](https://github.com/PaulKov/dpone/actions/runs/34573417026), completed SUCCESS. Fresh read-only provider run/job/step and artifact-metadata responses match the retained records. Source review and transfer conclusions remain in `report.md` and `followup-source-h-and-transfer.md`.

| Check | Result |
| --- | --- |
| Actual jobs and steps | PASS: all 16 unique `Quality tests` jobs, their collection/test/upload steps, preflight, and both `Quality checks` jobs succeeded |
| Original provider ZIPs | PASS: all 17 archives match provider ID, run, head, SHA-256 and size; every archived member matches retained extracted bytes and member hashes |
| Population and manifest | PASS: 22,149 unique canonical node IDs; all eight manifest partitions equal independently recomputed SHA-256 first-eight-byte big-endian modulo-eight assignments |
| Python 3.11 receipts | PASS: eight distinct shards; each count, selected hash, membership, interpreter/head identity and population hash matches; unique union equals all 22,149 nodes |
| Python 3.12 receipts | PASS: eight distinct shards with the same complete checks and 22,149-node union |
| Missing, duplicate or extra membership | PASS: zero for both interpreters |

The population digest is `657a8e6c41e19ca4ef5ac9f5fa1082fa137fa18d103476aa2609e7f6506c82a9`. The audit verifies the full population independently of the aggregate validator's conclusion. The unchanged workflow exits with the actual pytest status after writing the membership receipt, so successful provider test steps establish successful shard execution. Python 3.11's two coverage-combination/upload steps are correctly SKIP because those steps apply only to Python 3.12; every shard test step succeeded.

Command: `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B fresh-review/verify_ci_population.py`. Result: PASS, exit 0, 8.914 seconds. Environment: existing Python 3.12.11 on macOS 26.3.2 arm64. The audit calls only read-only `gh run view` and `gh api`, then independently checks retained JSON, ZIPs and hashes using the standard library. Exact provider commands, job IDs/steps, artifact/member hashes, shard counts/digests, and the audit-script hash are retained in `ci-population-followup.json`. Fresh raw provider responses are `ci-followup-run.json` and `ci-followup-artifacts.json`; the executable audit is `verify_ci_population.py`.

No production, test, documentation or raw provider evidence was changed. Public contracts, compatibility and user journeys are unaffected. Test/producer reruns are N/A for this read-only follow-up. These receipts contain membership rather than individual test outcomes: exact per-test PASS/SKIP counts are UNVERIFIED, and no zero-skip claim is made. This is the non-live population; live execution remains SKIP and live certification UNVERIFIED.

The quality gate evidence is ready for the parent's final required-context assessment. Other required contexts, PR readiness, merge and release authority are outside this bounded review; no all-context or release approval is implied.
