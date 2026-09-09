# Developer guide: GitOps control plane

The GitOps control plane is the scheduler-neutral layer above manifest sparse
paths. It gives runners a stable plan/verify contract without teaching Airflow,
`KubernetesPodOperator`, GitHub Actions, or Dagster about dpone manifest schema
details.

## Taxonomy

- `GitOpsPlanBuilder` consumes a manifest sparse-path report and builds a
  stable `gitops.plan` contract.
- `GitOpsImpactAnalyzer` consumes changed repo-relative files plus sparse-path
  reports and builds a stable `gitops.affected` contract.
- `GitOpsBundlePolicyEvaluator` applies scheduler handoff policy gates such as
  `--fail-on-empty-impact`, `--fail-on-warnings`, and `--require-lock`.
- `GitOpsBundleAttestationBuilder` computes SHA-256 artifact digests and a
  deterministic `bundle_digest` without importing Git, schedulers, or runtime
  connectors.
- `GitOpsBundleVerifier` recomputes attested artifact digests and bundle
  digests for `dpone gitops bundle verify`.
- `GitOpsAirflowArtifactRenderer`, `GitOpsAirflowDoctor`,
  `GitOpsAirflowEvidenceBundleCollector`, `GitOpsAirflowRenderService`,
  `GitOpsAirflowDoctorService`, `GitOpsAirflowEvidenceBundleService`, and
  `GitOpsAirflowImageContractService` build and validate Airflow
  `KubernetesExecutor` / `KubernetesPodOperator` runner handoff artifacts for a
  custom dpone image without importing Airflow.
- `GitOpsSchemaContract` and `GitOpsSchemaValidator` publish and enforce
  lightweight JSON Schema contracts for GitOps artifacts.
- `GitOpsPlanVerifier` checks a sparse worktree against a `gitops.plan`
  artifact and writes a `gitops.verify` contract.
- `GitOpsIssue`, `GitOpsSparsePath`, `GitOpsPlanReport`,
  `GitOpsAffectedReport`, `GitOpsVerifyReport`, and `GitOpsBundleReport` are
  stable DTOs for CLI, CI, docs, and scheduler wrappers.
- `GitOpsPlanService` composes the YAML/filesystem ports with the manifest
  sparse-path planner.
- `GitOpsAffectedService` discovers manifest entrypoints, delegates dependency
  expansion to the manifest sparse-path planner, and can emit per-manifest
  `gitops.plan` artifacts.
- `GitOpsVerifyService` reads a plan through the filesystem port and delegates
  worktree checks to the verifier.
- `GitOpsBundleService` composes affected planning, emitted plan creation,
  verify report creation, and bundle artifact writing without embedding
  scheduler or manifest-schema logic.
- `GitOpsLockBuilder` records provenance-friendly file digests without calling
  Git or runtime connectors.
- `GitOpsLockVerifier` compares `gitops.plan.lock` file digests against a
  sparse worktree when `dpone gitops verify --verify-lock` is enabled.
- `GitChangedFilesResolver` normalizes `--changed-files`,
  `--changed-files-file`, and `--from-ref` / `--to-ref` inputs before impact
  analysis.

Do not import Airflow, Kubernetes clients, Git clients, runtime connectors, or
database SDKs into this slice. Runner-specific integration belongs outside
dpone or in tiny renderers that consume the stable plan JSON.

## Module boundaries

- `dpone.commands.gitops.*` contains argparse, service invocation, rendering,
  and optional artifact writing only.
- `dpone.services.gitops.*` owns use-case orchestration and dependency
  injection through ports.
- `dpone.gitops.models` owns JSON contracts, including `gitops.bundle`.
- `dpone.gitops.affected` owns impact matching and emitted-plan DTO updates.
- `dpone.gitops.changed_files` owns Git diff and changed-files file resolution.
- `dpone.gitops.bundle_attestation` owns bundle artifact digest construction.
- `dpone.gitops.bundle_policy` owns bundle policy blockers.
- `dpone.gitops.bundle_profiles` owns named policy profile defaults.
- `dpone.gitops.bundle_verify` owns offline bundle artifact and attestation
  verification.
- `dpone.gitops.airflow_models` owns `gitops.airflow_render`,
  `gitops.airflow_doctor`, and `gitops.airflow_image_contract` DTOs.
- `dpone.gitops.airflow_artifacts` owns pure rendering for
  `pod_template_file`, `executor_config.json`, `airflow_task.py`, and
  `entrypoint.sh`.
- `dpone.gitops.airflow_doctor` owns reusable Airflow/Kubernetes runner
  validation, including bundle attestation, `base` container, image, and image
  contract checks.
- `dpone.gitops.airflow_rendering` owns Markdown rendering for the Airflow
  runner pack.
- `dpone.gitops.airflow_evidence_bundle` owns pure artifact digest,
  child-evidence, and Airflow attempt to Kubernetes pod correlation rules for
  `gitops.airflow_evidence_bundle`.
- `dpone.gitops.lock` owns path digest lock construction.
- `dpone.gitops.lock_verify` owns lock digest verification.
- `dpone.gitops.paths` owns shared GitOps path-safety helpers.
- `dpone.gitops.plan` owns plan construction from sparse-path reports.
- `dpone.gitops.schema_contracts` owns public JSON Schema dictionaries for
  `gitops.affected`, `gitops.plan`, `gitops.verify`, `gitops.bundle`, bundle
  attestation, and Airflow runner artifacts such as `gitops.airflow_pod_contract`
  and `gitops.airflow_xcom_summary`.
- `dpone.gitops.schema_validation` owns small structural validation for GitOps
  payloads without adding a mandatory `jsonschema` runtime dependency.
- `dpone.gitops.verify` owns plan path validation and sparse worktree checks.
- `dpone.gitops.rendering` owns Markdown rendering only, including
  `gitops.bundle` and `gitops.bundle_verify` summaries.

`dpone manifest sparse-paths` remains the low-level dependency discovery
primitive. `dpone gitops plan` should compose it rather than reimplementing
manifest traversal.

## Extension rules

Add a new runner family by extending allowed CLI values and documenting the
runner semantics. Do not branch on runner kind inside manifest discovery.

Add a new manifest-owned file dependency by extending the sparse-path dependency
rule catalog first. GitOps planning should inherit the new path through the
sparse-path report.

Add a new impact source by extending `GitOpsImpactAnalyzer` input adapters, not
CLI branching. The analyzer should continue to operate on normalized
repo-relative path strings and `SparsePathReport` objects.

Add a new changed-file input source by extending `GitChangedFilesResolver`.
Keep Git execution isolated in that resolver, inject runners for tests when
needed, and keep public blockers sanitized so local absolute paths are not
emitted.

Add a new lock field only when it remains deterministic without credentials or
network access. The lock/provenance contract must not expose local absolute
paths.

Add a new artifact field only when it is stable enough for CI and scheduler
automation. Prefer additive JSON fields and keep existing fields backward
compatible.

Add a new bundle policy by extending `GitOpsBundlePolicy` and
`GitOpsBundlePolicyEvaluator`. The command layer should only expose the flag
and pass it to the service; policy decisions must remain testable without
argparse, Git, Airflow, Kubernetes, or runtime connectors.

Add a new policy profile by extending `dpone.gitops.bundle_profiles`. Profiles
may enable stricter defaults, but the service must keep the final
`GitOpsBundlePolicy` explicit in JSON so CI users can audit which gates were
active.

Add a new attestation artifact by extending the service-owned artifact list
before `GitOpsBundleAttestationBuilder` runs. The builder should continue to
operate on repo-relative paths and file bytes only. Do not include
`bundle.json` in `bundle_digest`; the bundle contains its own attestation and
would otherwise require circular hashing.

Add a new JSON Schema contract by extending `dpone.gitops.schema_contracts` and
copying the generated schema to `docs/schemas/gitops/`. Keep JSON Schema files
additive and backward compatible; external consumers should tolerate unknown
fields, so required fields must be stable public automation fields only.

Add a new `dpone gitops bundle verify` check in `dpone.gitops.bundle_verify`
when it concerns artifact integrity. Keep policy and filesystem orchestration
in `GitOpsBundleVerifyService`; do not add digest verification to
`GitOpsBundleService`, because bundle construction and bundle verification are
separate use cases.

Add a new Airflow runner artifact by extending
[Developer GitOps Airflow runner pack](developer-gitops-airflow-runner-pack.md)
rules first. The command layer must remain argparse-only; reusable validation
belongs in `GitOpsAirflowDoctor`, and generated examples may reference Airflow
only as output text.

`dpone gitops bundle` should keep composing `dpone gitops affected` and
`dpone gitops verify` semantics. Do not duplicate changed-file resolution,
manifest dependency discovery, lock verification, or sparse-path validation in
the bundle service.

## Runbook

1. Write failing tests in `tests/test_gitops_plan_verify.py`,
   `tests/test_gitops_affected.py`, `tests/test_gitops_bundle.py`,
   `tests/test_gitops_bundle_verify.py`,
   `tests/test_cli_gitops_bundle_command.py`,
   `tests/test_cli_gitops_bundle_verify_command.py`,
   `tests/test_gitops_schema_contracts.py`,
   `tests/test_cli_gitops_affected_command.py`, and
   `tests/test_cli_gitops_plan_verify_commands.py`.
2. Validate user-facing command behavior for `dpone gitops affected`,
   `dpone gitops bundle`, `dpone gitops bundle verify`,
   `dpone gitops plan`, `dpone gitops verify`,
   `--verify-lock`, `--changed-files-file`, `--from-ref` / `--to-ref`,
   `--fail-on-empty-impact`, `--fail-on-warnings`, `--require-lock`,
   `--policy-profile`, `--attest`, and `--require-attestation`.
3. Update [GitOps control plane](gitops-control-plane.md), this developer
   guide, [Developer GitOps Airflow runner pack](developer-gitops-airflow-runner-pack.md)
   when Airflow artifacts change, architecture docs, and generated CLI
   reference.
4. Keep command handlers thin and verify `dpone docs check-import-rules`.
5. Run architecture fitness and module-size gates before publishing.

```bash
uv run pytest tests/test_gitops_plan_verify.py tests/test_cli_gitops_plan_verify_commands.py tests/test_gitops_docs_contract.py -q
uv run pytest tests/test_gitops_affected.py tests/test_cli_gitops_affected_command.py -q
uv run pytest tests/test_gitops_bundle.py tests/test_cli_gitops_bundle_command.py -q
uv run pytest tests/test_gitops_bundle_verify.py tests/test_cli_gitops_bundle_verify_command.py tests/test_gitops_schema_contracts.py -q
uv run dpone docs update-cli-reference --check
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run mkdocs build --strict
```

## Quality guardrails

The GitOps slice must stay credential-free, connector-free, and scheduler-free.
If plan construction grows beyond sparse-path composition, split new policy or
artifact-normalization modules instead of expanding command handlers or
services.
