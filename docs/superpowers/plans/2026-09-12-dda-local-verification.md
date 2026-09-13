# Verify DDA delivery in an isolated local Docker environment

Status: APPROVED implementation follow-up under the existing
[bounded DDA specification](../../feature-design-data-delivery-acceleration-v1.md).
On 2026-09-12 the maintainer restricted this task to DDA and explicitly authorized
provisioning local Docker Desktop services for synthetic ClickHouse, SQL Server
and BCP tests. dbt and release-composition work remain in their separate task.

## Goal and scope

Complete the missing environment adapter for the existing DDA benchmark protocol,
execute real bounded-native delivery and recovery, and measure the published
candidate against the audited baseline. This is test infrastructure under the
approved DDA-05/DDA-06 contract; it does not add a public connector mode, change
production defaults or activate public native SWITCH.

Candidate source: `a54281906040f7e8d21e52071820c8b5f9d9608d` (0.79.0).
Baseline source: `d5ad9aaecc900c24df421b160ed36b4cfc726e45` (0.76.0).
The factory/harness producer identity must be recorded separately from both
runtime subjects. No source checkout may be relabeled or dirtied by run output.

## Independent work packages and DoD

| Package | Responsibility | Definition of Done |
|---|---|---|
| DDA-L1 environment | Dedicated Docker network, synthetic CH/SQL services, BCP runner, bounded resources | Actual image IDs, versions, platform/emulation and resource caps retained; existing containers unchanged; healthy private-network services; secrets absent from reports/files |
| DDA-L2 factory | Existing NativeMssqlRuntime bindings, real source/BCP, SQL transaction admission and durable fixture inventory | Actual native path executes; restart can attach exact owned inventory; no fake receipts or no-op authority checks; foreign ownership and unsupported strategy rejected |
| DDA-L3 correctness/recovery | Independent exact target/metadata/receipt probes and fault adapters | All supported type profiles, empty input, duplicate multiplicity and outside-window sentinels preserved; post-EOF recovery never opens source; known/unknown commit branches remain distinct; cleanup refuses unknown outcome |
| DDA-L4 comparison | Baseline/candidate imports, dependency alignment, frozen experiment inputs | One warmup plus at least three eligible trials per declared workload; same non-subject dependencies/layout/resources; raw samples retained; no p95 claim; unavailable measurements remain explicit |
| DDA-L5 integration and review | Documentation, checks, independent review, scoped PR | Focused/broad applicable checks pass; review findings resolved; live evidence linked with exact limitations; no release or public SWITCH activation implied |

One integrator owns shared factory assembly, docs, configuration and task status.
Parallel source writers require separate worktrees and validated disjoint path
contracts. Environment work writes only its external artifact directory.

## Execution algorithm and evidence boundaries

1. Inspect available Docker resources without stopping or changing existing
   containers. Use an isolated DDA namespace and network with no public host
   ports. Generate credentials for the test services in memory and expose them
   only to their owned runtime processes.
2. Provision synthetic databases and fixture-owned tables through the existing
   SQL catalog/authority mechanisms. Bind physical database identity and durable
   ownership before any mutation; an output filename cannot authorize cleanup.
3. Supply the benchmark RouteFactory using actual native runtime, source,
   independent BCP importer connections, SQLite window store and native journal.
   Restore stable invocation/bindings before extraction; preserve atomic target
   mutation and operation receipt in the existing finalizer.
4. Read target rows, metadata and exact operation receipts independently outside
   the timed span. Start timing at actual source acquisition; stop visibility
   timing only after committed state is independently visible. Evidence and
   checkpoint completion remain a separate duration.
5. Validate small exact-multiset fixtures before timed datasets. Inject faults
   at existing boundaries, observe rollback/receipt results, and never manufacture
   a journal state from absence of a receipt. Retain unknown outcomes and resources.
6. Run baseline and candidate sequentially under identical declared limits and
   dependencies. Record the actual imported source and actual producer separately.
   Differences in bundled package versions must be resolved honestly, never by
   suppressing version keys or weakening the comparator.
7. Emit reports through the existing producer and validate comparison inputs.
   Preserve raw failures/skips and keep all output outside runtime checkouts.
8. Independently review the adapter and results. Document the measured local
   environment; ARM-host SQL Server emulation is not production x86-64 evidence.

## Local experiment profile

Docker Desktop uses an ARM64 Linux VM. SQL Server 2022 Developer runs as the
repository-pinned amd64 image under emulation; ClickHouse 24.8 and the BCP/Python
runner are native ARM64. SQL Server is limited to two CPUs and 3 GiB RAM (2 GiB
SQL memory); ClickHouse to 1.5 CPUs and 1 GiB; the runner to two CPUs and 1 GiB.
The private network has no published ports. Dependency preparation may temporarily
attach only the owned runner to an egress network, then disconnect before tests.
Existing unrelated containers remain active and their contention is a limitation.

Compare native runtime source trees using one isolated Python environment with
identical locked core, ClickHouse and MSSQL third-party requirements (31 entries
in both audited exports). Import dpone from the selected clean subject's src
path. The candidate native import path additionally requires the dpone-airflow-pack
helper distribution. Install the same retained 0.79.0 helper wheel for both
subjects and record it honestly with every installed distribution; do not hide
its version or interpret this as Airflow execution. Optional native-acceleration
extras remain absent. The baseline source declares a 0.76.0 helper dependency,
so this uniform source-component experiment is not a supported full baseline
wheel installation. Independent review must limit its performance claim and may
classify certification UNVERIFIED even when comparable component timings exist.

## Acceptance and limitations

Zero type-fidelity failures or partial publications is mandatory. The existing
performance targets remain a candidate median at most 0.85 of baseline on at
least one predeclared workload and at most 1.05 on every declared workload.
Targets are not results. Host contention, emulation or incompatible dependencies
may make performance UNVERIFIED even when live correctness passes.

Public native SWITCH remains rejected. Its separate isolated component campaign
may follow only after ordinary bounded-native delivery works; it is not a
prerequisite for the initial ordinary-route comparison. No dbt/composition fixes,
PyPI checks, version changes or republication are part of this plan.

## Status

- [x] DDA-only scope and local disposable environment authorized.
- [x] Baseline/candidate commits identified; existing DDA implementation preserved.
- [x] Environment healthy with retained identity/resource evidence (rechecked 2026-09-13).
- [x] Real factory and independent correctness/recovery adapters implemented.
- [x] Candidate development live correctness and recovery pass (frozen-commit rerun pending).
- [ ] Comparable baseline/candidate measurements retained.
- [ ] Independent review resolved and scoped PR available.

### 2026-09-13 restart and first live smoke

Docker Desktop and the three retained DDA containers were restarted after the
host process-creation failure. The disposable ClickHouse database did not survive
the stop; only the authorized `dda_synthetic` databases were provisioned again.
Actual query probes returned ClickHouse 24.8.14.39 and SQL Server 16.0.4265.3.
The first route attempt reached real SQL schema admission and failed because
the initial fixture DDL made required source columns nullable. This is a factory
construction failure, not a passed delivery test. Correct the fixture DDL and
rerun before producing performance envelopes. The external evidence directory
`dpone-dda-followup-20260912` retains `smoke-initial.log`, `smoke-restored.log`
and the credential-free `environment/restore_databases.py` producer.

### Native window correction and development evidence

The real native-source route exposed a production query defect: ClickHouse
substituted the temporal SELECT alias into the unqualified window predicate,
comparing integer microseconds with DateTime64. Qualifying the original relation
column restores the existing half-open UTC contract. This is an isolated bug fix,
with a failing regression before the change, passing unit checks and four live
DateTime/DateTime64 nullable/nonnullable boundary cases afterward. No manifest,
state format or public option changes.

Development execution passed all six supported profiles under both strategies
(12 combinations), exact metadata/receipt checks and the existing recovery
cases. The full narrow 10,000-row harness also passed fidelity, recovery, warmup
and three trials; its envelope correctly remains UNVERIFIED because code was
dirty. Raw producers/results are retained in the local external evidence root.
Independent reviews resolved stale rollback proof, terminal rollback handling,
partial cleanup, descriptor I/O and public invocation ID issues. Final comparison
and the remaining broad checks are still required before claiming completion.
