# Developer guide: GitOps Airflow runner pack

The GitOps Airflow runner pack is a control-plane slice on top of
`gitops.bundle`. It helps Airflow `KubernetesExecutor`,
`KubernetesPodOperator`, and KubernetesPodExecutor-style wrappers run a custom
dpone image without teaching scheduler code about manifests, sparse paths,
bundle attestation, or runtime connector internals.

Do not import Airflow, Kubernetes clients, source/sink connectors, or database
SDKs into this slice. Generated `airflow_task.py` may contain Airflow imports
and generated `airflow_dag_factory.py` may contain `KubernetesPodOperator`
and `PythonOperator` imports because they are output artifacts, not dpone
runtime code. The generated DAG factory is an artifact-loading helper:
`build_dpone_gitops_task_from_artifacts` consumes `kpo-kwargs.json` and
`pod-spec.yaml`. The direct KPO mode is not the sparse git-sync runtime contract.
When a DAG repository has dpone installed in the scheduler image, use
`dpone.airflow.runtime_adapter` for the same reusable helper surface:
`load_dpone_kpo_kwargs`, `build_dpone_gitops_task_from_artifacts`, and
`validate_dpone_artifacts`; import `build_dpone_gitops_step_tasks_from_artifacts`
from `dpone.airflow.step_tasks`.
The module is intentionally DAG-side only and must not discover manifests,
compute sparse paths, choose git-sync auth, or rebuild Kubernetes PodSpecs.
Step-task rendering reads only `run-spec.json`; source-refresh dependencies are
already compiled there by `GitOpsAirflowRunSpecBuilder`.
`artifact-index.json` and `dpone gitops airflow preflight` are the release
gate around that artifact directory: index owns inventory/digests, preflight
owns schema validation, detection for stale artifacts, pod-doctor composition, and
runtime-only Airflow connection bridge plan checks, and user-facing
`next_actions`.
The only command that executes manifests is
`dpone gitops airflow run-spec-exec`; keep it behind the `CommandRunner`
interface so tests and docs can exercise runtime evidence without launching
real connectors. Cluster smoke execution is separate, opt-in, and isolated
behind `AirflowK8sSmokeRunner`. Pod launch evidence collection is also
separate, opt-in, and isolated behind `AirflowPodLaunchEvidenceRunner`.
Cluster readiness checks are separate from both smoke execution and pod-watch:
`dpone gitops airflow cluster-doctor` validates namespace, service account,
RBAC, Secret keys, ExternalSecret readiness, and quota/LimitRange visibility
without launching a pod or reading secret values.

## Taxonomy

- `GitOpsAirflowArtifactRenderer` renders `pod_template.yaml`,
  `executor_config.json`, `airflow_task.py`, `entrypoint.sh`, and delegates the
  artifact-loading `airflow_dag_factory.py` to the focused factory renderer
  from an already-produced bundle or runtime profile.
- `dpone.airflow.runtime_adapter` is the optional importable DAG-side adapter
  for Airflow scheduler images that already install dpone. It exposes
  `load_dpone_kpo_kwargs`, `build_dpone_gitops_task_from_artifacts`, and
  `validate_dpone_artifacts`; it only consumes `kpo-kwargs.json`,
  `pod-spec.yaml`, and `pod-contract.json`.
- `GitOpsAirflowDoctor` validates bundle JSON, bundle attestation presence,
  `pod_template_file` shape, `base` container requirements, image presence, and
  image contract content.
- `GitOpsAirflowRunnerPolicyEvaluator` applies `advisory`, `pr`, and
  `release` runner policy profiles over already-loaded bundle, pod template,
  and image contract payloads.
- `GitOpsAirflowRunSpecBuilder` turns an existing `gitops.bundle` into ordered
  `bundle_verify`, `gitops_verify`, optional `source_refresh`, and `dpone_run`
  runtime steps without executing commands. It reads manifest hook metadata only
  to render Airflow-visible hook steps requested by `execution.airflow:
  separate_task`.
- `GitOpsAirflowRuntimeEvidenceVerifier` validates `runtime-evidence.json`
  against a run-spec without executing commands.
- `GitOpsAirflowRuntimeProfileBuilder` turns an existing bundle/run-spec pair
  plus runtime placement input into a stable `runtime-profile.json` contract.
- `GitOpsAirflowRuntimeProfilePolicy` evaluates `advisory`, `pr`, and
  `release` profile checks over runtime placement. Release mode requires
  image digest, non-default service account, CPU and memory requests and
  limits, and an explicit ArtifactSink path.
- `GitOpsAirflowGitSyncContract`, `GitOpsAirflowGitSyncAuth`, and
  `GitOpsAirflowGitSyncSparsePath` are DTOs for the optional sparse git-sync
  runtime contract. They serialize repository ref, git-sync image, safe auth
  references, worktree paths, sparse checkout file, and deterministic sparse
  allowlist entries without secret values or local absolute paths.
- `GitOpsAirflowGitSyncArtifactCollector` aggregates sparse checkout entries
  from already-rendered bundle entries and `gitops.plan.sparse_paths`. It does
  not inspect manifest schema and does not rerun manifest discovery.
- `GitOpsAirflowRuntimeProfileGitSyncBuilder` maps CLI/runtime-profile options
  into a `GitOpsAirflowGitSyncContract` and owns auth-mode blockers before the
  runtime profile is written. It depends on the `GitOpsAirflowGitSyncPathCollector`
  protocol, not on a concrete artifact reader.
- `GitOpsAirflowConnectionBridgeBuilder` discovers `connection_type: airflow`
  refs from generated bundle/plan manifest paths and builds the runtime-only
  `connection_bridge` contract. It reads raw YAML through `YamlCodec`; it does
  not compile manifests, import Airflow, or serialize connection URI values.
  The public runtime-profile flags are `--airflow-connection-bridge`,
  `--airflow-connection-secret`, and `--airflow-runtime-mode`.
  The bridge emits `AIRFLOW_CONN_` env var references only; secret values stay
  in Kubernetes Secrets or scheduler-provided env.
- `GitOpsAirflowConnectionBridgePlanBuilder` consumes the already-rendered
  `connection_bridge` contract and builds `connection-bridge-plan.json`,
  `airflow-connections-secret.yaml`,
  `airflow-connections-externalsecret.yaml`, and
  `airflow-connections.env.example` skeletons. It never reads manifests,
  imports Airflow, calls Kubernetes, or serializes URI values.
- `GitOpsAirflowConnectionBridgePlanService` owns repo-relative path
  validation, loads `runtime-profile.json` and `pod-contract.json`, writes the
  report and skeleton files, and stays behind the small
  `dpone gitops airflow connection-bridge-plan` CLI facade.
- `evaluate_connection_bridge_plan_preflight` is the release gate helper for
  `GitOpsAirflowPreflightService`. It blocks release preflight when required
  `AIRFLOW_CONN_*` refs exist but `connection-bridge-plan.json` or required
  skeleton artifacts are missing or stale.
- `GitOpsAirflowGitSyncPodPatchBuilder` builds the reusable PodSpec patch:
  `dpone-worktree` volume, `dpone-sparse-checkout` initContainer,
  `dpone-git-sync` initContainer, auth secret mounts/env, base worktree mount,
  and base working directory.
- `GitOpsAirflowPodContractBuilder` turns a bundle, run-spec, runtime profile,
  and pod options into `pod-contract.json`, `pod-spec.yaml`, and
  `kpo-kwargs.json` without importing Airflow or Kubernetes clients.
- `GitOpsAirflowPodDoctor` validates generated pod contract artifacts,
  including `base` container drift, PodSpec image drift, `pod_template_file`
  drift, `do_xcom_push` drift, git-sync drift, and Airflow connection bridge
  drift for runtime-only pods.
- `GitOpsAirflowArtifactIndexBuilder` builds the deterministic
  `artifact-index.json` inventory from known Airflow artifact specs. It records
  expected kind, actual kind, schema version, producer, byte size, and SHA-256
  without reading manifests or executing runtime commands.
- `GitOpsAirflowPackPlanner` builds the public `gitops.airflow_pack` report for
  `airflow-runtime-pack.json`. It consumes the shared Airflow artifact catalog,
  normalizes artifact existence/kind/digest evidence, emits deterministic
  golden-path commands, and separates ordinary plan warnings from verify-mode
  blockers. It does not parse manifests, mutate PodSpecs, call Airflow, call
  Kubernetes, or execute subprocesses. The public JSON contract is documented
  by `airflow-pack.schema.json`.
- `GitOpsAirflowPreflightService` composes the current artifact index, the
  saved `artifact-index.json`, registered GitOps schema validation, and
  `GitOpsAirflowPodDoctorService`. It owns stale artifact blockers and
  `next_actions`; it must not duplicate pod-doctor checks or mutate PodSpecs.
- `GitOpsAirflowXComOutcomeBuilder` converts runtime evidence into the final
  XCom summary with status, failed step, evidence SHA-256, step counts, and
  artifact paths.
- `GitOpsAirflowOutcomeGateEvaluator` evaluates a final
  `gitops.airflow_xcom_summary` payload for downstream Airflow gates and CI
  release collectors.
- `GitOpsAirflowArtifactSink`, `GitOpsAirflowRuntimeResources`,
  `GitOpsAirflowRuntimeProfile`, and `GitOpsAirflowXComSummary` are DTOs for
  placement, resources, XCom handoff, and release evidence.
- `GitOpsAirflowRunSpecExecutor` executes run-spec steps through the
  `CommandRunner` protocol and writes runtime evidence. Use
  `StaticAirflowCommandRunner` in tests; use `SubprocessAirflowCommandRunner`
  only in the custom dpone image.
- `GitOpsAirflowK8sSmokePlanner` builds an opt-in live smoke contract from
  `run-spec.json`, `runtime-profile.json`, `pod-contract.json`,
  `image-contract.json`, and `xcom-summary.json` without importing Airflow or
  Kubernetes clients.
- `GitOpsAirflowClusterDoctorPlanner` builds the opt-in cluster readiness
  contract from `runtime-profile.json`, `pod-contract.json`, and optional
  `connection-bridge-plan.json`. It owns static artifact checks, safe Secret
  key expectations, ExternalSecret references, and live result policy.
- `AirflowClusterDoctorRunner` is the execution port for generated cluster
  checks. Use `StaticAirflowClusterDoctorRunner` in tests and
  `SubprocessAirflowClusterDoctorRunner` only when `--mode live` is explicitly
  selected in a credentialed Airflow/Kubernetes environment.
- `AirflowK8sSmokeRunner` is the execution port for generated smoke commands.
  Use `StaticAirflowK8sSmokeRunner` in tests and
  `SubprocessAirflowK8sSmokeRunner` only when `--mode live` is explicitly
  selected in a credentialed Airflow/Kubernetes environment.
- `GitOpsAirflowPodLaunchEvidencePlanner` builds the reusable pod-watch
  contract from `runtime-profile.json`, `pod-contract.json`,
  `image-contract.json`, `runtime-evidence.json`, and `xcom-summary.json`.
  It normalizes Kubernetes pod, event, and log evidence without importing
  Airflow or Kubernetes clients.
- `GitOpsAirflowEvidenceBundleCollector` builds one release-grade
  `gitops.airflow_evidence_bundle` from already-emitted JSON artifacts. It
  owns artifact kind checks, SHA-256 digests, byte sizes, child blocker
  summaries, and Airflow attempt to Kubernetes pod correlation. It does not
  read the filesystem and does not execute commands.
- `AirflowPodLaunchEvidenceRunner` is the execution port for pod-watch
  commands. Use `StaticAirflowPodLaunchEvidenceRunner` in tests and
  `SubprocessAirflowPodLaunchEvidenceRunner` only when `--mode live` is
  explicitly selected in a credentialed Airflow/Kubernetes environment.
- `GitOpsAirflowRenderService` owns dependency injection, repo-relative path
  validation, bundle loading, artifact writing, and `gitops.airflow_render`
  view construction.
- `GitOpsAirflowRunSpecService` owns runtime contract writing and
  `gitops.airflow_run_spec` view construction.
- `GitOpsAirflowRuntimeProfileService` owns runtime profile writing,
  `xcom-summary.json` writing, DAG factory writing, and
  `gitops.airflow_runtime_profile` view construction.
- `GitOpsAirflowPackService` owns repo-relative path validation, loading the
  Airflow artifact directory, writing `airflow-runtime-pack.json`, and
  `gitops.airflow_pack` view construction. Keep `dpone gitops airflow pack` as
  a thin UX orchestrator over existing artifacts; do not add runtime execution,
  PodSpec reconstruction, manifest discovery, or git-sync auth decisions here.
- `GitOpsAirflowRuntimeProfileOptions` owns parsed runtime-profile CLI options
  so the runtime-profile service remains orchestration-only.
- `GitOpsAirflowRuntimeProfileGitSyncBuilder` composes git-sync auth options,
  bundle-derived sparse paths, and required Airflow control artifacts into the
  optional runtime `git_sync` contract.
- `GitOpsAirflowConnectionBridgeBuilder` composes Airflow connection discovery,
  bridge mode, runtime mode, and Secret/env refs into the optional
  `connection_bridge` contract.
  `--airflow-connection-bridge k8s_secret` is the default runtime-only path;
  `--airflow-runtime-mode airflow_image` is the explicit custom-image path.
  Both paths keep Airflow connection values out of generated artifacts by
  serializing only `AIRFLOW_CONN_` env names and Secret refs.
- `GitOpsAirflowPodContractService` owns pod contract path validation,
  artifact loading, PodSpec writing, KPO kwargs writing, and
  `gitops.airflow_pod_contract` view construction.
- `GitOpsAirflowPodContractOptions` owns parsed pod-contract CLI options and
  reserved-name blockers for git-sync volumes and mounts.
- `GitOpsAirflowPodDoctorService` owns pod contract artifact loading,
  doctor orchestration, and `gitops.airflow_pod_doctor` view construction.
- `GitOpsAirflowOutcomeGateService` owns XCom summary loading, outcome gate
  orchestration, and `gitops.airflow_outcome_gate` view construction.
- `GitOpsAirflowEvidenceVerifyService` owns runtime evidence loading,
  run-spec loading, verifier orchestration, and `gitops.airflow_runtime_evidence`
  view construction.
- `GitOpsAirflowDoctorService` owns dependency injection, artifact loading,
  doctor-policy orchestration, and `gitops.airflow_doctor` view construction.
- `GitOpsAirflowImageContractService` owns image contract construction,
  optional artifact writing, and `gitops.airflow_image_contract` view
  construction.
- `GitOpsAirflowK8sSmokeService` owns artifact loading, path validation,
  planner orchestration, optional live execution through `AirflowK8sSmokeRunner`,
  and `gitops.airflow_k8s_smoke` view construction.
- `GitOpsAirflowPodLaunchEvidenceService` owns artifact loading, path
  validation, pod-watch planner orchestration, optional live execution through
  `AirflowPodLaunchEvidenceRunner`, and
  `gitops.airflow_pod_launch_evidence` view construction.
- `GitOpsAirflowEvidenceBundleService` owns repo-relative artifact loading,
  path validation, the evidence artifact catalog, collector orchestration, and
  `gitops.airflow_evidence_bundle` view construction.
- `GitOpsAirflowImageContract`, `GitOpsAirflowRenderReport`,
  `GitOpsAirflowDoctorReport`, `GitOpsAirflowImageContractReport`,
  `GitOpsAirflowRunSpec`, `GitOpsAirflowRuntimeProfile`,
  `GitOpsAirflowRuntimeEvidence`, `GitOpsAirflowPodContract`, and
  `GitOpsAirflowPodDoctorReport` are the stable JSON DTOs for CLI, CI, docs,
  Airflow artifacts, and release gates. `GitOpsAirflowOutcomeGateReport` is
  the stable report for downstream XCom gating.
  `GitOpsAirflowPodLaunchEvidenceReport` is the stable report for Kubernetes
  pod phase, imageID, restart, event, and log-tail evidence.
  `GitOpsAirflowEvidenceBundleReport` is the stable report for Airflow attempt
  correlation, Kubernetes pod correlation, and evidence artifact digests.
- `dpone.commands.gitops.airflow_cmd` owns the Airflow command group and shared
  argparse/output rendering for core Airflow commands.
- `dpone.commands.gitops.airflow_k8s_smoke_cmd` owns only `k8s-smoke` argparse
  and rendering so the main Airflow command adapter stays below module-size
  warning thresholds.
- `dpone.commands.gitops.airflow_pod_launch_evidence_cmd` owns only
  `pod-watch` argparse and rendering so the main Airflow command adapter stays
  below module-size warning thresholds.
- `dpone.commands.gitops.airflow_evidence_bundle_cmd` owns only
  `evidence-bundle` argparse and rendering so attempt evidence collection stays
  reusable and command handlers remain thin.

## Module boundaries

- `dpone.commands.gitops.airflow_cmd` and
  `dpone.commands.gitops.airflow_k8s_smoke_cmd` and
  `dpone.commands.gitops.airflow_pod_launch_evidence_cmd` and
  `dpone.commands.gitops.airflow_pod_cmd` and
  `dpone.commands.gitops.airflow_outcome_cmd` must stay thin adapters: parse
  arguments, call a service, render JSON or Markdown, and write optional
  output.
- `dpone.services.gitops.airflow_*` composes filesystem and YAML ports with
  domain renderers and validators.
- `dpone.readiness.airflow_compact_pack_release` orchestrates immutable compact
  release construction. `dpone.readiness.airflow_compact_pack_runtime_payloads`
  owns the closed runtime-payload ID grammar, bounded file materialization, and
  release-relative descriptor projection; keep those filesystem and payload
  rules out of the orchestration module.
- `dpone.gitops.airflow_runtime_profile_options` parses
  runtime-profile CLI options into a small DTO. It owns key/value syntax and
  repo-relative artifact sink validation so the service stays orchestration
  only.
- `dpone.gitops.airflow_runtime_profile_paths` normalizes and validates
  runtime-profile artifact paths before the service loads or writes files.
- `dpone.gitops.airflow_runtime_profile_git_sync` composes git-sync
  auth options, bundle-derived sparse paths, and control artifact paths into
  the optional runtime `git_sync` contract.
- `dpone.gitops.airflow_git_sync_artifacts` consumes bundle and plan
  artifacts to aggregate sparse checkout paths. Missing plan files become
  blockers only when the profile is being written; Airflow helpers never call
  this collector directly.
- `dpone.gitops.airflow_pod_contract_options` parses pod-contract
  CLI options and owns reserved-name blockers for git-sync volumes and mounts.
- `dpone.gitops.airflow_pod_contract_paths` normalizes and validates
  pod-contract artifact paths before the service loads or writes files.
- `dpone.gitops.airflow_json_artifacts` owns the small JSON artifact loading
  and user-facing parse/missing-artifact issues shared by Airflow services.
- `dpone.gitops.airflow_models` owns public DTOs and JSON contracts.
- `dpone.gitops.airflow_artifacts` owns pure rendering with no filesystem,
  argparse, or scheduler dependency.
- `dpone.gitops.airflow_outcome_gate` owns outcome mode normalization and final
  XCom gate evaluation.
- `dpone.gitops.airflow_runtime_models` owns run-spec and runtime evidence
  DTOs.
- `dpone.gitops.airflow_runtime_profile_models` owns runtime placement,
  ArtifactSink, resource, and XCom summary DTOs.
- `dpone.gitops.airflow_runtime_profile` owns pure runtime-profile building.
- `dpone.gitops.airflow_runtime_profile_policy` owns release/advisory
  runtime-profile decisions.
- `dpone.gitops.airflow_git_sync_models` owns the optional git-sync DTOs used
  by runtime-profile, pod-contract, schemas, CLI, docs, and CI gates.
- `dpone.gitops.airflow_connection_bridge_models` owns the runtime-only
  Airflow connection bridge DTOs used by runtime-profile, pod-contract,
  schemas, pod-doctor, docs, and CI gates.
- `dpone.gitops.airflow_connection_bridge` owns manifest-scoped discovery of
  `connection_type: airflow` ids and the bridge policy inputs. It must stay
  dependency-free with respect to Airflow and Kubernetes clients.
- `dpone.gitops.airflow_connection_bridge_plan` owns deploy-time Secret,
  ExternalSecret, and env-example skeleton rendering from a normalized
  `connection_bridge` contract. It must not discover manifests or inspect
  Airflow Connections.
- `dpone.gitops.airflow_connection_bridge_plan_rendering` owns Markdown
  rendering for the bridge plan report only; keep it out of
  `airflow_rendering` to avoid module bloat.
- `dpone.gitops.airflow_git_sync` owns pure sparse checkout rendering and
  PodSpec patch construction for git-sync initContainers. It must not load
  files, parse argparse, import Airflow, or know manifest dependency rules.
- `dpone.gitops.airflow_pod_contract_models` owns public pod contract and pod
  doctor DTOs.
- `dpone.gitops.airflow_pod_contract` owns pure PodSpec/KPO kwargs
  construction.
- `dpone.gitops.airflow_pod_doctor` owns reusable pod contract validation.
- `dpone.gitops.airflow_xcom_outcome` owns final XCom outcome construction
  from runtime evidence.
- `dpone.gitops.airflow_run_spec` owns pure run-spec construction from a
  bundle payload.
- `dpone.gitops.airflow_runtime_evidence` owns runtime evidence parsing and
  verification policy.
- `dpone.gitops.airflow_runtime_executor` owns the `CommandRunner` protocol,
  subprocess-backed runtime execution, and static test runner.
- `dpone.gitops.airflow_k8s_smoke_models` owns public Airflow Kubernetes smoke
  DTOs and JSON rendering.
- `dpone.gitops.airflow_k8s_smoke_runner` owns the `AirflowK8sSmokeRunner`
  protocol, static test runner, and subprocess-backed opt-in live runner.
- `dpone.gitops.airflow_k8s_smoke` owns release checks and generated commands,
  while re-exporting the public smoke names for compatibility. It must stay
  generic to `KubernetesExecutor`, `KubernetesPodOperator`, and
  KubernetesPodExecutor-style wrappers.
- `dpone.gitops.airflow_pod_launch_evidence_models` owns public pod-watch
  command, result, and report DTOs plus JSON rendering.
- `dpone.gitops.airflow_pod_launch_evidence_checks` owns the reusable
  pod-watch check DTO and source taxonomy.
- `dpone.gitops.airflow_pod_launch_evidence_parser` owns normalization for
  Kubernetes pod JSON, event JSON, base container log tail evidence, and
  observed-pod DTOs.
- `dpone.gitops.airflow_pod_launch_evidence_policy` owns artifact checks,
  release blockers, expected pod phase checks, image digest checks, restart
  checks, and event blocker checks.
- `dpone.gitops.airflow_pod_launch_evidence_runner` owns the
  `AirflowPodLaunchEvidenceRunner` protocol, static test runner, and
  subprocess-backed opt-in live runner.
- `dpone.gitops.airflow_pod_launch_evidence` owns pod-watch planning,
  command construction, result parsing orchestration, and policy composition.
  It must stay generic to Airflow `KubernetesExecutor`,
  `KubernetesPodOperator`, KubernetesPodExecutor-style wrappers, and future
  source -> sink route types.
- `dpone.gitops.airflow_cluster_doctor_models`,
  `dpone.gitops.airflow_cluster_doctor_commands`,
  `dpone.gitops.airflow_cluster_doctor_runner`, and
  `dpone.gitops.airflow_cluster_doctor` split cluster readiness into DTOs,
  safe command construction, runner port, and planner policy. Secret checks
  must list keys only and redact live stdout to expected key names before
  writing JSON.
- `dpone.services.gitops.airflow_cluster_doctor_service` loads repo-relative
  artifacts, extracts ExternalSecret names from generated skeleton files, and
  composes optional live execution. It must not import Airflow, Kubernetes
  clients, source/sink connectors, or database SDKs.
- `dpone.gitops.airflow_evidence_bundle_models` owns public evidence bundle
  DTOs and JSON rendering for attempt correlation, pod correlation, artifact
  digests, warnings, and blockers.
- `dpone.gitops.airflow_evidence_bundle` owns pure artifact normalization,
  child evidence status checks, and correlation derivation. It must stay
  generic to Airflow `KubernetesExecutor`, `KubernetesPodOperator`,
  KubernetesPodExecutor-style wrappers, and future evidence producers.
- `dpone.gitops.schema_airflow_evidence_bundle_contracts` owns the public JSON
  Schema builder for `airflow-evidence-bundle.schema.json`.
- `dpone.gitops.schema_airflow_git_sync_contracts` owns the shared JSON Schema
  fragment for the runtime-profile and pod-contract `git_sync` field.
- `dpone.gitops.airflow_doctor` owns reusable validation policy and emits
  `GitOpsIssue` blockers or warnings.
- `dpone.gitops.airflow_policy` owns runner profile names, `runner_policy`
  normalization, and profile-specific checks. `--runner-policy release` must
  stay generic to Airflow/Kubernetes/image-contract posture and must not encode
  source -> sink route behavior.
- `dpone.gitops.airflow_rendering` owns Markdown rendering only.

The pack consumes the existing `gitops.bundle` contract. It must not duplicate
changed-file resolution, sparse-path discovery, lock verification, or bundle
attestation digest logic. Those rules remain in `dpone gitops affected`,
`dpone gitops plan`, `dpone gitops verify`, and `dpone gitops bundle verify`.

## Extension rules

Add a new Airflow artifact by extending `GitOpsAirflowArtifactRenderer`, adding
a `GitOpsAirflowArtifact` entry in `GitOpsAirflowRenderService`, documenting
the file in [GitOps Airflow runner pack](gitops-airflow-runner-pack.md), and
covering it in `tests/test_gitops_airflow_runner_pack.py`.

Add a new runtime placement field by extending
`GitOpsAirflowRuntimeProfile`, `GitOpsAirflowRuntimeProfileBuilder`, the
`dpone gitops airflow runtime-profile` parser, `airflow-runtime-profile.schema.json`,
and both user and developer docs. Keep file paths repo-relative and keep
validation in `GitOpsAirflowRuntimeProfilePolicy`; do not branch inside CLI
handlers.

Add a new git-sync auth mode by extending `GitOpsAirflowGitSyncAuth`,
`GitOpsAirflowRuntimeProfileGitSyncBuilder`,
`GitOpsAirflowGitSyncPodPatchBuilder`, `airflow_git_sync_schema`, and both
docs. Serialize only Kubernetes Secret names and key names; never serialize
tokens, private keys, passwords, or local credential file paths. Keep auth
validation in the runtime-profile git-sync builder and reserved pod name
validation in pod-contract options.

Add a new git-sync clone optimization by extending
`GitOpsAirflowGitSyncCloneOptions`, `GitOpsAirflowRuntimeProfileGitSyncBuilder`,
`GitOpsAirflowGitSyncPodPatchBuilder`, `airflow_git_sync_schema`, and
`GitOpsAirflowRuntimeProfilePolicy`. Keep version-specific safety in
`dpone.gitops.airflow_git_sync_capabilities`; do not branch on image tags inside
CLI handlers or Airflow helper code. `git-sync:v4.4.0` is sparse-checkout only,
while `git-sync:v4.7.0+` supports `--filter=blob:none` and `--filter=tree:0`.

The public CLI surface for this contract is `--git-sync-repo`,
`--git-sync-ref`, `--git-sync-image`, `--git-sync-auth-mode`,
`--git-sync-depth`, `--git-sync-filter`, `--git-sync-ssh-secret`, and
`--git-sync-https-secret` on `dpone gitops airflow runtime-profile`. The
supported auth mode values are `image`, `ssh_secret`, and `https_secret`; the
supported partial clone filters are `blob:none` and `tree:0`.

Add a new Airflow connection bridge mode by extending
`GitOpsAirflowConnectionBridge`, `GitOpsAirflowConnectionBridgeBuilder`,
`GitOpsAirflowConnectionBridgePlanBuilder`, `GitOpsAirflowPodContractBuilder`,
`airflow_pod_doctor_connection_bridge`, the runtime-profile and pod-contract
schemas, `airflow-connection-bridge-plan.schema.json`, CLI docs, and
user/developer docs. Keep secret values outside all JSON, YAML, Markdown, and
logs. If the mode changes manifest discovery rules, add the rule to
`dpone.gitops.airflow_connection_bridge`; do not parse manifests inside
generated Airflow DAG helpers. If the deploy-time skeleton changes, update
`dpone.gitops.airflow_connection_bridge_plan` and the preflight helper instead
of branching in command handlers.

Add a new sparse checkout source by extending
`GitOpsAirflowGitSyncArtifactCollector` only when the source is already a
rendered dpone control-plane artifact. Do not parse manifest schema in Airflow
helpers; manifest dependency discovery belongs to `dpone manifest sparse-paths`
and `gitops.plan.sparse_paths`.

Add a new pod contract option by extending `GitOpsAirflowPodContractInput`,
`GitOpsAirflowPodContractBuilder`, the `dpone gitops airflow pod-contract`
parser, `airflow-pod-contract.schema.json`, and both docs. Keep repo-owned
paths under `safe_relative_path`; keep container paths explicit and JSON-safe.
Do not add Airflow SDK imports to the service.

Add a new pod doctor check in `GitOpsAirflowPodDoctor` when the rule concerns
the generated `pod-contract.json`, `pod-spec.yaml`, or `kpo-kwargs.json`.
Return a `GitOpsAirflowCheck` plus a blocker or warning. Keep
`GitOpsAirflowPodDoctorService` limited to loading files and building the view.
Git-sync-specific pod checks belong in `dpone.gitops.airflow_pod_doctor_git_sync`
so sparse checkout, clone depth, partial clone, worktree mounts, and
initContainer order stay reusable without turning the main doctor into a large
module.

Add a new image contract field by extending `GitOpsAirflowImageContract` and
`GitOpsAirflowImageContractService`. Keep the field optional unless every
custom dpone image can populate it. Public outputs must stay repo-relative and
must not expose local absolute paths.

Add a new doctor check in `GitOpsAirflowDoctor` when it is a reusable
Airflow/Kubernetes contract. Return a check plus a blocker or warning. Do not
raise exceptions for user-facing validation failures, and do not add CLI
branches for validation policy.

Add a new release gate in `GitOpsAirflowRunnerPolicyEvaluator` when it depends
on the combined runner context or on the selected `runner_policy`. `pr` should
prefer warnings; `release` should fail closed with blockers. Keep
`runner_policy` in the public `gitops.airflow_doctor` report.

Add native route tool requirements through the image contract, not through
Airflow-specific code. For example, MSSQL -> ClickHouse can require
`clickhouse-client`, while Postgres -> MSSQL can require `bcp`; the runner pack
should still treat both as generic `tools` entries.

Add a run-spec step by extending `GitOpsAirflowRunSpecBuilder`. Keep each step
as data: `name`, `kind`, `command`, `required`, and optional `manifest`. Do not
add route-specific branches to `dpone.commands.gitops.airflow_cmd`.

Add runtime evidence checks in `GitOpsAirflowRuntimeEvidenceVerifier`.
`run-spec-exec` should write evidence for what happened; `evidence-verify`
should decide whether the artifact is acceptable for CI or release gates.

Add final XCom outcome fields in `GitOpsAirflowXComOutcomeBuilder` and
`GitOpsAirflowXComSummary`. Keep fields additive because Airflow XCom
consumers often parse a subset of the payload. The pod contract keeps the
Airflow return file fixed at `/airflow/xcom/return.json`; scheduler wrappers
should copy `xcom-summary.json` there after `run-spec-exec` exits.

Add outcome gate behavior in `GitOpsAirflowOutcomeGateEvaluator` and
`GitOpsAirflowOutcomeGateService`. Keep `strict_fail` and `xcom_then_gate`
semantics generic to Airflow XCom delivery, not to a specific source -> sink
route. `xcom_then_gate` must be documented with a downstream gate because the
pod task exits `0` after writing XCom so Airflow can push the payload.

Add a new Airflow Kubernetes smoke check in `GitOpsAirflowK8sSmokePlanner` when
the rule can be evaluated from existing artifacts. Add a new live command only
when it is generic to the custom dpone image contract and can be represented as
data in `GitOpsAirflowK8sSmokeCommand`. Do not add command branching to
`dpone.commands.gitops.airflow_cmd`; use the `AirflowK8sSmokeRunner` port for
test doubles and subprocess execution.

Add a new pod launch evidence check in
`GitOpsAirflowPodLaunchEvidencePlanner` when the rule can be evaluated from
runtime profile, pod contract, image contract, final XCom summary, runtime
evidence, Kubernetes pod JSON, Kubernetes events, or base container logs. Add
new Kubernetes JSON fields in `dpone.gitops.airflow_pod_launch_evidence_parser`
first, then add policy in the planner. Do not branch in
`dpone.commands.gitops.airflow_cmd`; use `AirflowPodLaunchEvidenceRunner` for
test doubles and subprocess execution.

Add a new evidence bundle artifact by extending the artifact catalog in
`GitOpsAirflowEvidenceBundleService`, then update
`GitOpsAirflowEvidenceBundleCollector` only when the artifact changes
correlation or pass/fail semantics. Keep artifact paths repo-relative, keep
missing optional artifacts as warnings, and keep release-required artifacts
behind explicit `--require-*` flags.

Add scheduler-specific examples as generated artifacts or docs. Do not add an
Airflow runtime dependency to dpone itself.

## JSON contracts

`dpone gitops airflow render` emits `gitops.airflow_render` with:

- `bundle_path`
- `output_dir`
- `image`
- `artifacts`
- `commands`
- `warnings`
- `blockers`

When `--require-attestation` is set, `GitOpsAirflowRenderService` and
`GitOpsAirflowDoctorService` require bundle attestation before Airflow handoff.
This is the release-gate mode for a custom dpone image running under
`KubernetesExecutor` or `KubernetesPodOperator`.

`dpone gitops airflow doctor` emits `gitops.airflow_doctor` with:

- `bundle_path`
- `pod_template`
- `image_contract`
- `image`
- `runner_policy`
- `checks`
- `warnings`
- `blockers`

`dpone gitops airflow image-contract` emits
`gitops.airflow_image_contract` with:

- `output_path`
- `contract`
- `warnings`
- `blockers`

The `contract` object is written to `image-contract.json`; the report wrapper
is printed to stdout or optional `--output`.

`dpone gitops airflow run-spec` emits `gitops.airflow_run_spec` with:

- `bundle_path`
- `bundle_digest`
- `image`
- `image_digest`
- `worktree`
- `evidence_output`
- `airflow_context_env`
- `entries`
- `steps`
- `warnings`
- `blockers`

`dpone gitops airflow runtime-profile` emits
`gitops.airflow_runtime_profile` with:

- `bundle_path`
- `bundle_digest`
- `run_spec_path`
- `runtime_evidence_path`
- `xcom_summary_path`
- `dag_factory_path`
- `outcome_gate_path`
- `image`
- `image_digest`
- `namespace`
- `service_account`
- `resources`
- `artifact_sink`
- `env`
- `labels`
- `annotations`
- `git_sync`
- `runner_policy`
- `outcome_mode`
- `warnings`
- `blockers`

When the profile passes, `GitOpsAirflowRuntimeProfileService` writes
`runtime-profile.json`, `xcom-summary.json`, `airflow_dag_factory.py`, and
`outcome_gate.py`. `xcom-summary.json` uses `gitops.airflow_xcom_summary` and
is intentionally small so an Airflow `KubernetesPodOperator` can push it as
task metadata.

`dpone gitops airflow pod-contract` emits `gitops.airflow_pod_contract` with:

- `bundle_path`
- `run_spec_path`
- `runtime_profile_path`
- `pod_spec_path`
- `kpo_kwargs_path`
- `image`
- `namespace`
- `service_account`
- `xcom`
- `pod_spec`
- `kpo_kwargs`
- `git_sync`
- `warnings`
- `blockers`

When the contract passes, `GitOpsAirflowPodContractService` writes
`pod-contract.json`, `pod-spec.yaml`, and `kpo-kwargs.json`.

`dpone gitops airflow pod-doctor` emits `gitops.airflow_pod_doctor` with:

- `artifact_dir`
- `pod_contract_path`
- `pod_spec_path`
- `kpo_kwargs_path`
- `runner_policy`
- `checks`
- `warnings`
- `blockers`

The preferred CLI shape is `dpone gitops airflow pod-doctor --artifact-dir
.dpone/gitops/airflow --runner-policy release`; explicit
`--pod-contract-path`, `--pod-spec-path`, and `--kpo-kwargs-path` flags remain
available for targeted drift debugging.

`dpone gitops airflow artifact-index` emits and writes
`gitops.airflow_artifact_index` with:

- `artifact_dir`
- `output_path`
- `created_at`
- `entries`
- `warnings`
- `blockers`

`dpone gitops airflow preflight` emits `gitops.airflow_preflight` with:

- `artifact_dir`
- `artifact_index_path`
- `runner_policy`
- `artifact_index`
- `pod_doctor`
- `checks`
- `next_actions`
- `warnings`
- `blockers`

`dpone gitops airflow connection-bridge-plan` emits
`gitops.airflow_connection_bridge_plan` with:

- `artifact_dir`
- `output_path`
- `runtime_profile_path`
- `pod_contract_path`
- `mode`
- `runtime_mode`
- `secret_name`
- `required_connection_ids`
- `env`
- `artifacts`
- `warnings`
- `blockers`

The report never includes Airflow Connection URI values. For the default
`k8s_secret` mode, the service writes a Kubernetes Secret skeleton,
ExternalSecret skeleton, and `.env.example` file with placeholder values only.
Release preflight requires this plan when `connection_type: airflow` ids are
present in a runtime-only pod contract.

`dpone gitops airflow run-spec-exec` executes `steps` and emits
`gitops.airflow_runtime_evidence` with:

- `run_spec_path`
- `bundle_path`
- `image`
- `status`
- `started_at`
- `finished_at`
- `duration_seconds`
- `steps`
- `warnings`
- `blockers`

It also writes `gitops.airflow_xcom_summary` to `--xcom-output` with:

- `status`
- `runtime_profile_path`
- `run_spec_path`
- `runtime_evidence_path`
- `runtime_evidence_sha256`
- `failed_step`
- `step_counts`
- `artifact_paths`
- `blockers`

`dpone gitops airflow outcome-gate` emits `gitops.airflow_outcome_gate` with:

- `xcom_summary_path`
- `required_status`
- `status`
- `passed`
- `run_spec_path`
- `runtime_evidence_path`
- `runtime_evidence_sha256`
- `failed_step`
- `step_counts`
- `warnings`
- `blockers`

`dpone gitops airflow evidence-verify` returns the same
`gitops.airflow_runtime_evidence` shape after adding verification blockers such
as `runtime_step_failed`, `runtime_step_missing`, and
`runtime_run_spec_path_mismatch`.

`dpone gitops airflow k8s-smoke` emits `gitops.airflow_k8s_smoke` with:

- `mode`
- `runner_kind`
- `runner_policy`
- `run_spec_path`
- `runtime_profile_path`
- `pod_contract_path`
- `image_contract_path`
- `xcom_summary_path`
- `namespace`
- `service_account`
- `image`
- `image_digest`
- `image_ref`
- `smoke_name`
- `timeout_seconds`
- `checks`
- `commands`
- `results`
- `warnings`
- `blockers`

`mode=plan` returns the deterministic command contract without credentials.
`mode=live` is opt-in live execution through `AirflowK8sSmokeRunner`; failed
required commands become `airflow_k8s_command_failed` blockers.

`dpone gitops airflow cluster-doctor` emits
`gitops.airflow_cluster_doctor` with:

- `mode`
- `runner_policy`
- `artifact_dir`
- `runtime_profile_path`
- `pod_contract_path`
- `connection_bridge_plan_path`
- `namespace`
- `service_account`
- `timeout_seconds`
- `secret_refs`
- `external_secret_refs`
- `checks`
- `commands`
- `results`
- `warnings`
- `blockers`

`mode=plan` returns deterministic `kubectl get`, `kubectl auth can-i`, Secret
key-listing, and ExternalSecret status commands without credentials.
`mode=live` is opt-in execution through `AirflowClusterDoctorRunner`.
Required command failures, RBAC denial, missing Secret keys, and required
ExternalSecret `Ready=True` drift become release blockers. Secret values must
never appear in `results.stdout`: secret-key command output is normalized to
the expected key names before serialization.

`dpone gitops airflow k8s-manifests` emits
`gitops.airflow_k8s_manifests` with:

- `artifact_dir`
- `manifest_path`
- `runtime_profile_path`
- `pod_contract_path`
- `connection_bridge_plan_path`
- `gitops_controller`
- `namespace`
- `service_account`
- `objects`
- `controller_hints`
- `warnings`
- `blockers`

`GitOpsAirflowK8sManifestBuilder` is the only owner of Kubernetes object
taxonomy for this feature. It builds deterministic `ServiceAccount`, `Role`,
`RoleBinding`, Secret skeleton, optional ExternalSecret, and optional
NetworkPolicy objects from generated Airflow artifacts.
`airflow_k8s_controller_metadata` owns the `--gitops-controller` Argo CD/Flux
metadata policy: `plain` preserves the historical YAML shape, `argocd` adds
stable dpone labels and `argocd.argoproj.io/sync-wave` annotations, and `flux`
adds stable dpone labels while leaving prune/wait policy to the Flux
`Kustomization` CR.
`GitOpsAirflowK8sManifestsService` owns artifact loading, path validation, and
YAML/report writing. Services may serialize the resulting objects to YAML, but
must not mutate Kubernetes object semantics. Secret values are never part of
this contract; generated Secret manifests may name required keys and leave
`stringData` empty.

`dpone gitops airflow admission-check` emits
`gitops.airflow_admission_check` with:

- `mode`
- `runner_policy`
- `artifact_dir`
- `manifest_path`
- `pod_spec_path`
- `timeout_seconds`
- `commands`
- `results`
- `warnings`
- `blockers`

`mode=plan` returns deterministic `kubectl apply --dry-run=server` commands
without credentials. `mode=live` is opt-in execution through
`AirflowAdmissionCheckRunner`; failed required dry-run commands become
`airflow_admission_command_failed` blockers.
`GitOpsAirflowAdmissionCheckService` owns path validation and runner
injection. Keep this layer Kubernetes client-free: do not import Kubernetes
Python SDKs or Airflow providers.

`dpone gitops airflow pod-watch` emits
`gitops.airflow_pod_launch_evidence` with:

- `mode`
- `runner_policy`
- `runtime_profile_path`
- `pod_contract_path`
- `image_contract_path`
- `runtime_evidence_path`
- `xcom_summary_path`
- `pod_name`
- `namespace`
- `service_account`
- `image`
- `image_digest`
- `expected_phase`
- `timeout_seconds`
- `log_tail_lines`
- `checks`
- `commands`
- `results`
- `observed_pod`
- `warnings`
- `blockers`

`mode=plan` returns deterministic `kubectl get pod`, `kubectl get events`, and
`kubectl logs` commands without credentials. `mode=live` is opt-in execution
through `AirflowPodLaunchEvidenceRunner`; observed pod phase drift, image
digest drift, non-zero base container exits, restarts, and Kubernetes warning
events become release blockers.

`dpone gitops airflow evidence-bundle` emits
`gitops.airflow_evidence_bundle` with:

- `runner_policy`
- `attempt`
- `pod`
- `artifacts`
- `warnings`
- `blockers`

The `attempt` object contains `dag_id`, `task_id`, `run_id`, `try_number`, and
`map_index`. The `pod` object contains `pod_name`, optional `pod_uid`,
namespace, service account, image, and image digest. Each artifact records the
repo-relative path, expected kind, actual kind, required flag, existence,
SHA-256 digest, byte size, pass/fail state, and reason. Missing required
artifacts, kind drift, failed child statuses, and child blockers fail the
bundle; missing optional artifacts are warnings unless the matching
`--require-*` flag is set.

Public JSON Schema files live under `docs/schemas/gitops/`:

- `airflow-render.schema.json`
- `airflow-doctor.schema.json`
- `airflow-image-contract.schema.json`
- `airflow-run-spec.schema.json`
- `airflow-runtime-evidence.schema.json`
- `airflow-runtime-profile.schema.json`
- `airflow-xcom-summary.schema.json`
- `airflow-cluster-doctor.schema.json`
- `airflow-k8s-manifests.schema.json`
- `airflow-admission-check.schema.json`
- `airflow-outcome-gate.schema.json`
- `airflow-pod-contract.schema.json`
- `airflow-pod-doctor.schema.json`
- `airflow-connection-bridge-plan.schema.json`
- `airflow-artifact-index.schema.json`
- `airflow-preflight.schema.json`
- `airflow-k8s-smoke.schema.json`
- `airflow-pod-launch-evidence.schema.json`
- `airflow-evidence-bundle.schema.json`

When a report field changes, update `dpone.gitops.schema_contracts`, regenerate
or copy the schema file, and extend `tests/test_gitops_schema_contracts.py`.

The runtime-profile and pod-contract schemas both reuse the shared
`airflow_git_sync_schema` fragment. This keeps `git_sync.enabled`, repository
metadata, auth modes, sparse checkout entries, and worktree paths consistent
across the two public contracts.

## Runbook

1. Write failing tests in `tests/test_airflow_runtime_adapter.py`,
   `tests/test_gitops_airflow_runner_pack.py`,
   `tests/test_gitops_airflow_git_sync_contract.py`,
   `tests/test_gitops_airflow_k8s_smoke.py`,
   `tests/test_gitops_airflow_pod_launch_evidence.py`, and
   `tests/test_cli_gitops_airflow_commands.py`.
2. Keep implementation in the domain/service/command split listed above.
3. Update [GitOps Airflow runner pack](gitops-airflow-runner-pack.md), this
   developer guide, architecture docs, CI/CD docs, and generated CLI reference.
   Include `--runner-policy release`, `runner_policy`, `run-spec.json`,
   `runtime-profile.json`, `pod-contract.json`, `pod-spec.yaml`,
   `kpo-kwargs.json`, `xcom-summary.json`, `airflow_dag_factory.py`,
   `outcome_gate.py`, `runtime-evidence.json`, `airflow-k8s-smoke.json`,
   `airflow-cluster-doctor.json`, `airflow-pod-launch-evidence.json`,
   `airflow-evidence-bundle.json`,
   `airflow-k8s-manifests.json`, `airflow-k8s-manifests.yaml`,
   `airflow-admission-check.json`,
   `artifact-index.json`, `connection-bridge-plan.json`,
   `airflow-connections-secret.yaml`,
   `airflow-connections-externalsecret.yaml`,
   `airflow-connections.env.example`,
   `airflow-artifact-index.schema.json`,
   `airflow-connection-bridge-plan.schema.json`,
   `airflow-cluster-doctor.schema.json`,
   `airflow-k8s-manifests.schema.json`,
   `airflow-admission-check.schema.json`,
   `airflow-preflight.schema.json`, and the Airflow schema file names in docs
   when policy or schema behavior changes. If `git_sync` changes, include sparse git-sync initContainers,
   auth modes, `example-workloads` artifact-loading helper UX, and the rule
   that Airflow must not perform manifest discovery.
4. Run the focused gate:

```bash
uv run pytest tests/test_gitops_airflow_git_sync_contract.py tests/test_gitops_airflow_runner_pack.py tests/test_gitops_airflow_k8s_smoke.py tests/test_gitops_airflow_pod_launch_evidence.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_airflow_docs_contract.py -q
uv run pytest tests/test_gitops_airflow_k8s_manifests.py tests/test_gitops_airflow_admission_check.py -q
uv run pytest tests/test_gitops_schema_contracts.py -q
uv run dpone docs update-cli-reference --check
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
```

5. Run the full non-live gate before PR publication.

## Quality guardrails

- Do not import Airflow or Kubernetes clients into `src/dpone`.
- Do not execute pods, Docker, manifests, or database connections from
  render, doctor, runtime-profile, run-spec, image-contract, or
  evidence-verify services.
- Keep `run-spec-exec` behind `CommandRunner`; tests should use
  `StaticAirflowCommandRunner` and must not launch real source/sink work.
- Keep `k8s-smoke --mode live` behind `AirflowK8sSmokeRunner`; tests should use
  `StaticAirflowK8sSmokeRunner`, while normal OSS CI should use `--mode plan`.
- Keep `cluster-doctor --mode live` behind `AirflowClusterDoctorRunner`; tests
  should use `StaticAirflowClusterDoctorRunner`, while normal OSS CI should use
  `--mode plan`.
- Keep `admission-check --mode live` behind `AirflowAdmissionCheckRunner`;
  tests should use `StaticAirflowAdmissionCheckRunner`, while normal OSS CI
  should use `--mode plan`.
- Keep Kubernetes manifest rendering behind `GitOpsAirflowK8sManifestBuilder`;
  command and service layers should not branch on individual Kubernetes object
  internals.
- Keep `pod-watch --mode live` behind `AirflowPodLaunchEvidenceRunner`; tests
  should use `StaticAirflowPodLaunchEvidenceRunner`, while normal OSS CI should
  use `--mode plan`.
- Keep final XCom outcome construction behind `GitOpsAirflowXComOutcomeBuilder`
  so scheduler helpers do not parse runtime evidence internals.
- Keep downstream gate decisions behind `GitOpsAirflowOutcomeGateEvaluator` so
  scheduler helpers can consume final XCom summaries without understanding
  run-spec internals.
- Keep sparse git-sync construction behind `GitOpsAirflowGitSyncPodPatchBuilder`
  and `GitOpsAirflowRuntimeProfileGitSyncBuilder`. Airflow helpers should
  consume `kpo-kwargs.json` and `pod-spec.yaml`; they must not compute sparse
  paths, inspect manifests, choose auth modes, or mutate initContainers.
- Keep git-sync secret handling name/key-only in public artifacts. Secret
  values belong in Kubernetes Secrets and must never appear in JSON, YAML,
  Markdown, test fixtures, or logs.
- Do not emit local absolute paths in public JSON or Markdown.
- Keep command handlers thin; validation belongs in `GitOpsAirflowDoctor`.
- Keep generated artifacts deterministic so CI, release gates, and Airflow task
  logs can compare them across runs.
