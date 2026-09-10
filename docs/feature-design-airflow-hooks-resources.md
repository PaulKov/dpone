# Feature design: Reliable Airflow hooks and workload resources

- Status: IMPLEMENTED (live Kubernetes acceptance remains UNVERIFIED)
- Owner: dpone maintainers; implementation integrator: Codex
- Issue: maintainer supplied specification, “Reliable Airflow hooks and declarative Kubernetes resources”
- Target release: next coordinated dpone / dpone-airflow-pack release
- Approval: maintainer explicitly requested implementation of the supplied specification on 2026-09-10.
- Base: `3977ca2d04ca5dbcf3338d7c31faff8a1199549e` (0.74.36)
- Integration base: `d5ad9aa` (0.76.0); preserve the new release-composition boundary.
- Last verified: 2026-09-10

## Executive summary

Separate hooks currently disable the Airflow XCom sidecar but the verified
runtime wrapper still creates its directory before starting the child. Resource
settings supplied through arbitrary Pod overrides can disappear during strict
projection. Make publication explicit and provide a bounded resource contract.
The measurable outcome is preservation of declared resources and truthful,
diagnosable child execution without writes to the verified workload tree.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Configure one workload | Pod overrides silently disappear | Preview and Pod contain declared resources |
| Platform engineer | Run a non-root image | Hook requires an unmounted XCom directory | Hook runs using the existing run volume |
| Operator | Recover a failed start | Generic OSError message | Stage, exception class and errno identify the failure |

Discover the resource guide from the Airflow provider overview; configure
`airflow.resources` in workload configuration, build and inspect the compact
pack, load the strict deployment, inspect effective Pod resources, and run.
Diagnose startup failures from the safe log record and run-volume artifacts.
Fix permissions, capacity or the image before retrying. Upgrade matched package
versions and the runtime image before building new delivery artifacts.

## Scope

Runtime service files, separate pre-hooks, bounded workload resources, safe
startup diagnostics, contract tests and self-service documentation are in scope.
Data-transfer algorithms, PVC provisioning, cluster configuration, arbitrary Pod
overrides and per-hook resource overrides are non-goals. No live environment or
credentials have been approved; Kubernetes certification remains UNVERIFIED
until an approved live test is recorded.

## Public contract

### CLI and Python API

Existing build/load commands remain the entry points. Build errors identify the
invalid field and supported replacement. `VerifiedPackCommand` gains an explicit
publication flag defaulting to true for direct-call compatibility. The verified
launcher sets it false for separate hooks and true for runtime, including dbt.
Publication and child exit policies are independent. Hooks return the child exit
status; ordinary runtime keeps its existing XCom outcome gate; dbt keeps child
status propagation. Startup failure returns nonzero even if artifact writing
also fails. No full argv, environment or raw OSError text is logged.

### Manifest/schema

Workload configuration accepts optional `airflow.resources.requests` and
`airflow.resources.limits`, each a nonempty mapping of `cpu`, `memory` and
`ephemeral-storage` to quoted Kubernetes quantities. At least one section is
required when resources is present. Null, unknown keys, malformed/negative
quantities and request greater than limit are rejected. CPU precision is at
least one millicore. Quantities are compared exactly, without floating point.
The same resources apply to the base container of runtime and every separate
hook; init-fetch and the XCom sidecar retain their existing resource behavior.
Missing resources produces the existing Pod defaults. Kubernetes admission may
apply namespace defaults. Unsupported resource-bearing Pod/operator overrides
fail with a field path and guidance to use workload `airflow.resources`.

The ordinary release-composition verifier added in 0.75.0 admits this validated
resource-only manifest metadata during source reconstruction. Other `gitops`
fields, hooks and custom runner authority remain outside its supported scope.

### Artifacts, identity and compatibility

Resources appear in compact pack `provider_execution.pod_spec.spec.containers[0]`
and are covered by existing canonical pack identity and downstream receipt
digests. There is no second scheduler override authority. Both execution kinds
capture stdout/stderr under `/var/lib/dpone/run`. Runtime publishes its summary
to `/airflow/xcom/return.json`; hooks require no XCom path operations and retain
a local summary. Existing workload/artifact mounts stay read-only. The existing
run emptyDir is writable without root; KPO provides its XCom emptyDir when
publication is enabled. No security-context override is added.

Existing resource-free packs keep their wire shape. Existing direct wrapper
callers retain publication by default. Unsupported resource configuration that
was silently discarded now fails: move it to the supported field and rebuild.
Do not claim old/new mixed package versions are certified.

## Detailed algorithm

1. Resolve hierarchical workload settings using existing precedence.
2. Validate the bounded resource mapping and exact request/limit relation.
3. Insert resources into the base Pod container before strict projection.
4. Bind the resulting provider projection to existing pack identity.
5. Provider validates the wire and copies base resources into the final Pod.
6. Launcher derives publication and exit policy from the verified execution.
7. Prepare the run directory and, only when required, XCom directory.
8. Capture child output from the read-only workload working directory.
9. Write the summary, emit a safe outcome and apply the verified exit policy.
10. On OS failure emit stable code, stage, exception type, errno and a bounded
    service-path role; attempt local diagnostics without masking the failure.

```text
validate author resources -> compile Pod -> project -> fingerprint -> verify
prepare verified command -> prepare service files -> start child -> summarize
OS failure -> safe diagnostic -> best-effort artifact -> nonzero exit
```

Retries use fresh task Pods and the existing Airflow dependency graph.
`all_success` prevents runtime after failed hooks. This change adds no state
commit, transport retry, concurrency policy or replay authority. Failed writes
never become success. Empty input does not bypass directory or resource
validation. Timeouts/cancellation retain the existing operator behavior.

## Architecture and alternatives

The existing provider wire contract remains the distribution-independent
authority for Kubernetes resource syntax; core authoring composes that bounded
contract without importing Kubernetes or Airflow SDKs. Canonical runtime helpers
own execution, service-file and safe OS-diagnostic policy. Resource authoring
policy lives under `dpone.manifest`; the legacy readiness executor re-exports
the canonical implementation without adding policy. Existing compiler and command
facades call these helpers. One integrator owns schemas, navigation, changelog
and cross-layer integration. New modules must satisfy the existing quality
budgets; no new framework or plugin registry is needed.

Disabling hook XCom reuses the existing writable run volume and avoids an idle
sidecar; adding mandatory hook XCom was rejected as unnecessary overhead.
Bounded `airflow.resources` reuses the trusted provider projection; arbitrary Pod
templates were rejected because they can replace commands and execution identity.
This extends the existing strict delivery boundary; ADR 0024 records the shared
resource authority, v1 compatibility and independent publication/exit policies.

## Market comparison and measurable differentiation

Official sources retrieved 2026-09-10:

| System/version | Observed pattern | Adopt / reject |
|---|---|---|
| [Cosmos, current docs through 1.15.1](https://astronomer.github.io/astronomer-cosmos/getting_started/kubernetes.html) | Kubernetes execution forwards operator arguments and documents a command override regression/fix | Adopt explicit launch/version contracts; reject broad command overrides |
| [Airbyte 2.2.0, 2026-08-24](https://airbyte.com/blog/airbyte-2.2) | Restores resource fallback previously ignored by Helm V2; scoped main-container values take precedence | Adopt explicit precedence and effective-resource upgrade inspection; reject silent discard |
| [gusty, current official README](https://github.com/pipeline-tools/gusty) | YAML selects operators, parameters and dependencies | Adopt short declarative configuration; reject unrestricted operator kwargs for strict resources |
| dlt, Informatica, Fivetran, Pentaho, SSIS | N/A: data integration products outside the bounded verified-pack/KPO contract | No product-wide comparison |
| Apache Beam | N/A: separate runner resource abstraction | No performance comparison |

These are source observations; applying them to dpone is a design choice, not
measured evidence of superiority. The
[Kubernetes resource contract](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
defines quantities/CPU precision, while the
[Airflow KPO contract](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
defines XCom sidecar publication. The existing run emptyDir supports non-root
writes through kubelet's directory mode; see the
[Kubernetes v1.34 implementation](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.34.0/pkg/volume/emptydir/empty_dir.go).

Target: every accepted resource field is present unchanged in the final base
container and every injected startup OS failure has a safe distinguishable
diagnostic. Baseline: dpone 0.74.36. Procedure: author/build/load/Pod contract
tests plus real local child execution. Evidence lives in
`test_artifacts/airflow-hooks-resources/`. Offline tests do not establish
Kubernetes scheduling, disk availability or live production certification.

## Security and operations

Keep command, environment, service-account and volume authority closed.
Diagnostics use trusted service-path roles and numeric errno rather than raw
exception messages or filenames. Ephemeral-storage requests inform Kubernetes
scheduling and limits inform enforcement; container free-space checks measure
available bytes at one time. Neither guarantees a large unload will complete.

## Test, documentation and rollout plan

Unit/contract coverage: quantity formats and bounds, unsupported fields,
inheritance, unchanged defaults, identity changes, strict Pod preservation,
hook dependency failure, explicit publication and EACCES/ENOENT/ENOSPC.
Integration: real non-root local subprocess with a read-only workload directory
and unusable XCom path; full offline strict pack construction/loading/Pod route.
Live Kubernetes: UNVERIFIED without approved environment. No transport or
performance certification is implied. Run focused tests before project Python,
architecture, documentation and package gates, then independent review.

Documentation includes a minimal configuration, exact defaults, inspectable
output, startup recovery, disk semantics, package update order and links from
the provider overview. Roll back by restoring the previous matched image and
packages and rebuilding prior configuration; retain failed-Pod diagnostics.

## Agent execution plan

Four read-only roles map execution, architecture, tests and docs. A scoped writer
may implement runtime publication/diagnostics in a separate worktree. Codex is
the integrator and owns resource authoring, shared schemas, docs, changelog,
validation evidence and final review. Concrete contracts are stored with the
task evidence before handing off write access.

## Implementation evidence

The implementation is available in [PR #22](https://github.com/PaulKov/dpone/pull/22).
It includes ordinary release-composition resource preservation, canonical
manifest/runtime policy ownership and the readiness compatibility facade.
Independent review found no remaining actionable issues after the integration
corrections. Focused runtime/resource/composition coverage passed 231 tests;
the composition, schema and identity integration group passed 163 tests.

Ruff, formatting, mypy, import rules, layer/module budgets, generated references,
strict documentation rendering and all four distribution builds passed.
Installed-wheel checks reproduced a failing hook from a read-only workload with
an unusable XCom destination and preserved its child exit code. The cross-layer
ratio is 0.2999345835, within the unchanged 0.300 limit.

Exact revisions, full-suite results and logs are recorded in the PR and
`test_artifacts/airflow-hooks-resources/completion.md`. The first integrated full
run exposed stale local native-acceleration distribution metadata after the
0.76.0 update; reinstalling that editable package from current source resolved
all 23 tests in its contract module without code or test changes.

These are implementation and offline compatibility results. They do not certify
live scheduling, writable Kubernetes volumes, disk capacity or a published
package/image combination. Live acceptance requires an approved environment and
an exact runtime image digest; no publication is authorized by this status.
