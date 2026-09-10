# PostgreSQL strategy preservation evidence

Production source: `1ff83879fc7640d358cad15402672eafddcabf41`.
Baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
MR: <https://github.com/PaulKov/dpone/pull/31>.

**Independent review follow-up:** a new reviewer found two P2 regressions in
`648237b6`: ignored legacy-loader dependencies and incorrect partition row
metrics. Both were fixed in `48c123d`, independently reviewed with **APPROVE**,
and validated with 65 focused tests and a new 40-case PostgreSQL campaign.
The findings and validation status are tracked in
[independent-review.md](independent-review.md). They change production bytes;
the original live/CI receipts below remain evidence for their recorded commits.

The next independent review found an additional snapshot-diff metric issue.
Its correction and the maintainer-requested large-partition Docker measurements
are tracked in [performance-review.md](performance-review.md), including the
final candidate's review and validation status.

## Result and scope

Internal-query, memory and file artifacts now reach the configured PostgreSQL
load strategy through the shared staging lifecycle. Default full refresh uses
TRUNCATE + INSERT and preserves an existing target's identity and structure.
Explicit exchange remains available with sink-owned transaction handling.
The change also prevents out-of-scope native partition replacement, makes NULL
partition replay deterministic, preserves every LoadResult field and retains
primary SQL errors when cleanup or rollback also fails.

There are no new manifest fields or public signature changes. Historical loader
imports remain compatibility facades. Those loaders now honor the configured
strategy. Upgrading does not reconstruct constraints lost in earlier runs;
the route guide describes inspection and recovery.

The design and task contracts are recorded in [design.md](design.md),
[task-contract.yml](task-contract.yml), and
[live-test-contract.yml](live-test-contract.yml).
Independent code and proof reviews are recorded in [review.md](review.md).
The later independent review and finding disposition are recorded separately in
[independent-review.md](independent-review.md).

## Validation

| Check | Status | Evidence and limits |
| --- | --- | --- |
| Focused strategy, native-partition and documentation tests | PASS | 74 cases; `precommit-focused.log` |
| Direct real PostgreSQL campaign | PASS | [40 frozen-source cases](verification-direct.json); PostgreSQL 16.15 |
| Existing PostgreSQL file append/merge routes | PASS | 2 live cases; `file-strategy-live.log` |
| Strict Airflow/Kubernetes journey | PASS | [5 cases](verification-kubernetes.json); exact installed source and admitted image |
| Unsupported internal-query snapshot_diff/scd2 | PASS | Existing pre-sink TypeError preserved; `unsupported-query-contracts.log` |
| Ruff and format | PASS | `ruff-1ff8387.log`, `format-1ff8387.log`; proof producers also checked |
| Mypy | PASS | 1163 source files; `mypy-1ff8387.log` |
| Import, layer and module-size gates | PASS | `imports-1ff8387.log`, `layers-1ff8387.log`, `module-size-1ff8387.log` |
| Architecture fitness | PASS | `architecture-fitness.log`; advisory clustering 0.18095 exceeds green target 0.180 but remains within the canonical 0.182 budget |
| Generated references and compatibility registry | PASS | `generated-references.log`, `compatibility.log` |
| Documentation and strict MkDocs | PASS | `docs-final-staged.log`, `docs-language-final.log`, `mkdocs-final.log` |
| Four package builds and Twine | PASS | `build-core.log`, `build-native.log`, `build-pack.log`, `build-provider.log`, `twine.log`; no publication |
| Installed wheel smoke and Airflow compatibility CI | PASS | Python 3.11/3.12 runtime wheel smoke and Airflow 2.10.5, 2.11.0, 3.2.0 and 3.3.0 on both interpreters |
| Preliminary local full non-live suite | FAIL | Interrupted under host memory pressure: 10 failed, 10695 passed, 730 skipped; not final-source certification |
| Missing optional test dependencies | PASS | Installed declared accel/dbt extras; 25 cases passed in `optional-dependency-rerun.log` |
| FIFO, backfill bootstrap and safe-path timeout replays | PASS | Their focused cases passed in `timing-failure-rerun.log`; its separate doctor timeout is addressed below |
| Doctor timing failure replay after host pressure cleared | PASS | Same depth-5 test and 20s timeout; `doctor-tracemalloc-quiet-host.log`. Earlier timeout also reproduced before this change in [baseline receipt](doctor-tracemalloc-baseline.json) |
| Full candidate CI on Python 3.11 and 3.12 | PASS | All 16 shards and both aggregate quality gates; 21357 collected cases per interpreter with matching population digest; [3.11 matrix](ci-matrix-3.11.json), [3.12 matrix](ci-matrix-3.12.json) |
| Known fixture credential scan | PASS | [secret-scan.json](secret-scan.json); no matching values in retained artifacts |
| Hooks, manifest resources, other routes, publication | N/A | Outside this PostgreSQL correction and synthetic campaign |

Raw logs, Pod events, images and database snapshots remain in this local campaign
directory and are ignored by Git. The tracked verification receipts contain
their checksums. A PASS applies only to its stated source, environment and
scenario. It does not establish general production or release readiness.

## What the live campaigns establish

The direct campaign covers target OID, PK, NOT NULL, CHECK, indexes, defaults,
grants, triggers and view dependencies; changed snapshots, empty and missing
targets; constraint failures, cancellation and lock timeout with retry;
explicit exchange in the same/different schema; safe native partitions and
fallback/refusal for wider or NULL partitions; and actual file/memory/legacy
entry points. An absent target does not claim to clone source constraints.

The Kubernetes campaign executes public init, reconcile, release-materialize,
build, exact publish to the local MinIO fixture, cache-sync, installed DAG loading,
and actual Airflow DAG.test. Four runs succeed. The fourth input violates the
existing target CHECK after TRUNCATE; the DAG fails, old rows remain visible,
and the next valid run succeeds. The outcome cleanup task deliberately
rematerializes the failed outcome so a successful cleanup cannot hide failure.

The host verifier joins each run to its stored launch pin, Pod UID, admitted
specification, observed image digest, original runtime evidence/stderr/XCom,
Airflow-persisted XCom and outcome gate, deployment index and independent
database catalog/rows. It verifies the controller's installed source, wheel
claims and embedded provenance. Controller UID:
`1826409a-ecdc-488c-9f58-6e1c38542224`.

Source/test bytes were frozen throughout the direct run:
`595c6072d46b08497500eeb9cb2007681128fe02a4282e8bc62027470d5d5a7d`.
The Kubernetes runtime image is pinned to
`sha256:a15298761cd85fa8dee100d705af6f2a35179b2622e8abba6c715993495c892e`.
All 3776 installed Python files from the four wheels were checked against the
frozen source. Subsequent documentation/proof-only commits must preserve these
production and test bytes; they do not retroactively change the live source ID.

## Failed and intermediate attempts

Initial unit failures and the real NULL-partition replay failure remain as RED
evidence. A 28-case live pass and a later run overlapping edits are intermediate
evidence only. The latter is not a single-source certification.

The first Kubernetes fixture omitted the configured staging schema. Its
controller attempt `26fd8b9b-e69f-4a2e-968f-2e20c12ad5ec` and original service
files are retained. Its coarse rejected-case PASS did not establish a CHECK
failure; the whole campaign failed. Preparation now creates staging, and the
final verifier checks the exact structured CHECK error after target truncation.

The first image push lacked the Docker credential-helper directory in PATH;
the corrected local push succeeded. Initial CI preflight rejected outdated
generated developer metrics. They were refreshed with their producer, without
changing quality budgets. None of these failed attempts is reported as a pass.

The downloaded CI receipts were verified with the repository's canonical
`tools/ci_quality_shards.py` consumer, separately for each interpreter. Its
complete-population checks passed for commit `30e9221`, whose production,
package, test and dependency bytes match the live source. Initial mixed-version
input was correctly rejected; its diagnostic logs are retained.

## Reproduction

Use an explicitly approved disposable local environment. These producers are
bound to namespace `dpone-pg-preserve-ec30`, minikube profile
`dpone-airflow-live-b0912cc`, and their dedicated kubeconfig. They never target
the original Airflow namespace or stop the cluster. A fresh Kubernetes run
requires clean campaign-owned fixture tables; do not alter unrelated objects.

From the repository root:

```bash
uv sync --frozen --all-extras --group dev
uv run --no-sync python test_artifacts/postgres-strategy-preservation/run_direct_live.py live-direct-new
uv run --no-sync python test_artifacts/postgres-strategy-preservation/verify_direct.py live-direct-new
```

Build all four wheels before running `build_live_image.py`. Put
`/Applications/Docker.app/Contents/Resources/bin` on PATH for the local Docker
credential helper. The builder refuses modified source/package files and checks
wheel Python bytes against the frozen Git commit before building/pushing to the
approved local registry.

The Kubernetes producer sequence is `cluster.py bootstrap`, then the namespace
watcher and collector, then `cluster.py controller`. Start `node-capture.sh`
inside the minikube node via Docker stdin before the controller; it reads only
service files for namespace-observed UIDs. Its stop marker is campaign-owned.
Run `cluster.py verify-installed` while the controller is Running. After the
completion marker, collect the final controller files, export service files,
stop only the campaign collectors, and run `harness/verify.py CONTROLLER_UID`.
The controller stays alive for 120 seconds after completion for collection.
Credentials are generated/read in memory and are not printed or exported.

## Documentation and review readiness

User guidance was updated in PostgreSQL and route docs, load strategies,
extraction lifecycle, compatibility and architecture docs, schema descriptions
and CHANGELOG. The journey explains preservation, exchange tradeoffs, lock/FK
failures, retry, commit uncertainty and inspection of previously affected tables.

The scoped implementation and live proof are ready for review. Full source CI
passed on `30e9221`; the final documentation/proof follow-up preserves those
production and test bytes. GitHub checks on the eventual MR head remain the
merge gate. No merge, release or PyPI publication was performed by this task.
