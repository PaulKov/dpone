# Discover, check and compile a dbt workspace

For analysts and data engineers adding dbt projects to an existing repository.
These commands provide discovery, offline checks and one immutable multi-project
release. Use [compact workspace delivery](dbt-compact-delivery.md) for supported
consumer delivery. Physical-target certification and live rollout require
separate environment evidence.
Start at the [dbt integration hub](dbt.md).

## No central domain registration

A project is any regular `dbt_project.yml` below the chosen workspace root.
Directory names do not select technology or delivery policy. To use dpone
publishing, the project contains exactly one existing platform policy:

- `dpone/dbt-publish-profiles.yml`, or
- `.dpone/dbt-publish-profiles.yml`.

Reuse your platform's reviewed policy; its connection aliases and certified
runtime belong to the platform, not a second CI routing registry. The analyst
declares model publishing using the [standard model metadata](dbt-inline-publishing.md).
The compiler reads dbt's resolved manifest, so SQL `config()` and inherited YAML
metadata behave identically; dpone does not parse SQL to guess configuration.

| Local state | Discovery result | Next step |
| --- | --- | --- |
| One standard policy | `policy_present`, publishing candidate | Prepare manifest, then workspace check |
| No policy | `not_configured`, excluded from dpone publishing | Keep ordinary dbt quality/Cosmos checks in consumer CI |
| Two policies or unsafe metadata | `invalid`, entire discovery fails | Correct the reported project; it is not silently omitted |

`not_configured` means no local policy, **not** proof that no model requests
publishing. Discovery does not open manifests, SQL, policy contents or credential
profiles. A configured project with missing or invalid inputs fails check.

## First check

From the repository root:

```bash
dpone dbt workspace discover --root .
dpone dbt workspace discover --root . --format json
```

The output lists every discovered project in repository-relative path order.
No dependencies are downloaded, dbt subprocesses started or files written.
Use a complete root: selecting only changed projects is not a complete release
inventory and could omit unchanged workloads.

For each publishing project, prepare dependencies and the canonical manifest
with the existing certified toolchain and platform-provided local dbt profile:

```bash
dbt deps --project-dir dbt/my_project
dbt parse --project-dir dbt/my_project
dpone dbt workspace check --root . --format json
```

The dbt commands above retain their standard profile prerequisites; the workspace
command neither supplies credentials nor installs missing packages for you.
Check uses `target/manifest.json` by default and honors a literal project-local
`target-path`. Every publishing candidate is checked exactly once, including
unchanged projects. It retains project B's failure alongside project A's result.
Empty discovery/check is a successful no-op, not an empty release publication.

Inputs are acquired through confined, bounded reads: project/policy YAML is at
most 1 MiB; the existing graph-validation manifest limit remains 16 MiB. Duplicate
YAML keys, aliases and excessive nesting are rejected without including raw
policy values in errors. Malformed JSON object fields are project validation
failures, not a reason to abort the entire report. Model and graph validation use
the same acquired manifest bytes; replacement between these phases fails check.

## Compile every publishing project into one release

Compile requires the certified dbt Core/adapter in the active Python environment,
resolved packages and canonical manifests for every publishing project. It runs
offline `dbt parse` and `dbt ls`, not `dbt build` or database queries. Use the
same profile/target names already declared in the project's publishing policy
(`runtime.dbt_profile` and `runtime.dbt_target`; defaults `dpone_runtime` and
`runtime`).

Standard dbt connection profiles are **not** dpone publishing-policy files.
Each project's `profiles.yml` is the default parse input. Alternatively, one
shared file can hold all named profiles:

```bash
dpone dbt workspace compile --root . --output-dir .ci/release
dpone dbt workspace compile --root . --output-dir .ci/release \
  --dbt-profiles-dir /opt/ci/dbt-parse-profiles --format json
```

A relative `--dbt-profiles-dir` resolves against the command's working directory,
not separately against each project. It applies only to compile; discovery and
check do not read these profiles. There is no fallback to `~/.dbt`,
`DBT_PROFILES_DIR`, or `DBT_ENGINE_PROFILES_DIR`. The explicit directory follows
dbt's [standard profiles mechanism](https://docs.getdbt.com/docs/local/profiles.yml);
avoiding ambient fallback is dpone's reproducibility policy.

Use **non-secret parse-only credentials**, never production passwords.
The isolated invocation does not forward ambient variables, including
`DBT_ENV_SECRET_*`; required `env_var()` references without defaults will fail.
Profiles must be regular YAML mappings (at most 1 MiB), with no duplicate keys,
anchors or aliases. Every workflow's named profile and output must exist.
The compiler captures each file once, uses a private temporary snapshot for both
parse and selection, then cleans it up on success or error. Shared profiles are
captured once for the complete workspace. Neither profiles nor `.env` files
enter the project archives or release report.

Keep the standard dbt distinction between the profile's base schema and the
model's effective schema. For example, base `alpha` with `+schema: alpha`
produces `alpha_alpha`. You do not need to remove `+schema` or enter either
value again in dpone. During workspace compilation, the certified dbt toolchain
renders the captured profile under the same isolated parse context.

Workspace execution-pack v2 records this base target separately from the
manifest's effective model target. Runtime uses the base to render its private
profile, then requires actual parse/selection to reproduce the effective
relations before model SQL can execute. A changed target, wrong connection or
mixed v1/v2 pack fails closed. Profile errors are redacted; check the selected
profile/output locally using non-secret parse credentials. The v1 singleton
format and default command behavior remain unchanged.

Reader/producer support does not authorize activation on an older runtime. Deploy matching readers first; exact runtime/provider checks,
physical-target validation and environment acceptance remain required.

Canonical manifest preparation must use the same logical target, certified
toolchain, project source and invocation variables as compile. The public
`DbtInvocationContext.canonical().selection_vars_json()` provides the exact
build-plane interval placeholders. For example, when the policy uses the default
profile/target names:

```bash
DBT_PARSE_VARS="$(python -c 'from dpone.contracts.dbt_invocation import DbtInvocationContext; print(DbtInvocationContext.canonical().selection_vars_json())')"
dbt parse --project-dir dbt/my_project --profiles-dir /opt/ci/dbt-parse-profiles \
  --profile dpone_runtime --target runtime --vars "$DBT_PARSE_VARS"
```

Repeat manifest preparation for every publishing project. Compile independently
parses captured source under `isolated_v1` and rejects mismatches, including a
different logical target. Matching manifests do **not** prove matching server
endpoints or credentials; physical-target checks belong to deployment binding.

Success emits one `release-set.json`, a complete source inventory, each project's
content-addressed source/manifest/selection trio, and
`release-subjects.sha256`. SQL changes affect release artifacts, not the toolchain
image. The command does not sign, deploy or promote the release.

The `dpone.dbt-workspace-compile.v1`
[report schema](schemas/dbt/dpone.dbt-workspace-compile.v1.schema.json) retains the
complete check report, absolute local output path, release ID, source snapshot
digest and the checksum subject's digest. Failure keeps all checked project rows
but leaves all three output identities null; it never reports a partial release
as successful. Empty compile fails instead of retiring existing workloads.

### One writer per relation

Compile checks every selected materialized model, including upstream models
without publishing metadata, and every generated transfer destination across
the complete workspace. Different workflow IDs do not permit two writers to
the same literal connector/connection/database/schema/relation coordinates.
A conflict names both project/workflow/resource owners and returns exit 2 before
publication, preserving every project check and leaving release identities null.
Correct the owning model alias or transfer policy; do not edit generated packs.

The pinned SQL Server adapter also reserves temporary/backup tables and helper
views for each selected model. Unit tests create temporary relations too; their
names include the tested model version when present. Another selected resource
or transfer cannot occupy these slots, even within one workflow. The exact
[write-footprint matrix](feature-design-dbt-multi-project-release.md#source-inventory-and-ownership)
is platform-owned; analysts do not maintain a second registry of these names.

This offline check compares exact declared coordinates. Different connection
aliases, default versus named databases, or differently cased identifiers are
**not** proof of distinct physical tables. Deployment preflight must still
resolve actual service/database identity and certified identifier comparison
before activation. No additional central domain registration is required.

## Read results

Discovery emits `dpone.dbt-workspace-discovery.v1`; check emits
`dpone.dbt-workspace-check.v1`, containing the discovery result and existing
per-project compile reports. [Generated schemas](schemas/dbt/dpone.dbt-workspace-check.v1.schema.json)
describe the exact shape. Project paths and names are separate identities;
workflow, DAG and workload IDs must be globally unique. dbt model `unique_id`
remains project-scoped, so identical local node IDs alone are not a conflict.

Workspace commands return `0` on success, `2` on validation/usage failure and `5`
on an unexpected internal failure. JSON goes to stdout. In text mode, the summary
goes to stdout and actionable diagnostics to stderr. Legacy singleton check's
blocker exit `1` is unchanged. No output file is created by discovery/check.

Python callers use the same service and reports:

```python
from pathlib import Path
from dpone.app.dbt_workspace_composition import build_dbt_workspace_service

service = build_dbt_workspace_service()
report = service.check(Path("."))
print(report.to_dict())

# Optional shared parse profiles; paths resolve when composition is constructed.
service = build_dbt_workspace_service(dbt_profiles_dir=Path("/opt/ci/dbt-parse-profiles"))
compiled = service.compile(Path("."), output_dir=Path(".ci/release"))
print(compiled.to_dict())
```

Service tests may inject a discovery adapter and project compiler factory.
Production composition keeps `require_certified_routes=True`; there is no
workspace CLI flag that relaxes certification.

## Diagnose and retry

- Unset `DPONE_DBT_PUBLISH_PROFILES`, even if empty. A workspace-wide override
  could apply one project's policy to all projects and is rejected.
- Keep one policy, regular metadata files and literal generated paths. A wrong
  policy is an error, not a reason to classify a project as dbt-only.
- Regenerate a missing/mismatched manifest in its owning project. Do not copy
  another project's manifest or edit generated JSON.
- Correct duplicate project/workflow ownership in a reviewed source change.
  Existing deployed DAG IDs require the normal migration policy, not auto-renaming.
- Place dependency projects under `packages-install-path` (default
  `dbt_packages`); they are excluded along with target output. Nested source
  project roots are rejected. A root project `.` must be the only project.
- Non-ignored symlinks, even in-root aliases, fail closed. Default exclusions:
  `.git`, `.worktrees`, `.venv`, `venv`, `node_modules`, `target`, `logs`,
  `dbt_packages`, `.dpone-cache`, `.ci`.
- Bounds are 100,000 visited entries, 64 projects and directory depth 32. Reaching
  a bound does not produce a successful partial inventory. Select a narrower
  **complete** root or correct misplaced generated output, then retry.

Discovery/check are read-only. Compile publishes once, only after every project
and the complete source tree pass. An identical retry leaves existing output
unchanged; different content returns exit 2 without overwriting it. Use a new
immutable output directory for a new release.

A durability failure returns exit 5: the output may already be visible.
Verify it or retry identical inputs; do not delete it to force a green result.
Missing/unsafe parse profiles return exit 2 with the owning project's path and
the `DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID` code. See the
[error catalog](dbt-self-service-errors.md) for all stable codes.

## What this does not certify

Offline checks do not resolve physical database servers or collations, prove
cross-project target disjointness, authenticate DEV evidence, sign a release or
activate PROD. SQL must still be captured into immutable project artifacts bound
to the same release as manifests/selections, not loaded from a mutable audit
checkout or baked into a runtime image on every model change.

Next: [multi-project design and remaining rollout](feature-design-dbt-multi-project-release.md),
[source verification](dbt-workspace-source-verification.md), and
[complete audit-mirror promotion](dbt-workspace-promotion.md).

## Compact delivery after compilation

Use the [compact workspace delivery guide](dbt-compact-delivery.md) to carry the
complete compile tree through materialization, deployment/index projection,
provider init-fetch and verified launcher preflight. This does not remove the
independent workspace activation gate or establish SQL execution evidence.
