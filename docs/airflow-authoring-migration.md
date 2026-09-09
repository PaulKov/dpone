# Authoring-mode migration

Use `dpone migrate authoring` when one pipeline must move between `classic`,
`flow`, and `folder` authoring. The command rewrites the editable source only.
It does not build a release, publish Airflow artifacts, contact Vault, or run a
pipeline.

## Safe path

First review a plan:

```bash
dpone migrate authoring pipelines/orders_daily --to flow --plan
```

For automation, assert the detected source mode and request the public JSON
contract:

```bash
dpone migrate authoring orders_daily \
  --from classic \
  --to flow \
  --plan \
  --format json
```

Apply only after reviewing the source checksum, semantic fingerprint, file
actions, retained files, and unified diff:

```bash
dpone migrate authoring orders_daily --from classic --to flow --apply
dpone check pipelines/orders_daily
dpone airflow preview orders_daily
```

Apply recomputes the plan. It succeeds only when the source still compiles and
the candidate has the same canonical semantic fingerprint. Repeating the
command for the selected mode is a deterministic no-op.

## What each target creates

| Target | Primary source | Additional file |
| --- | --- | --- |
| `classic` | generated `dpone.batch.v1` authoring source | none |
| `flow` | inline `dpone.flow.v1` processes | none |
| `folder` | `dpone.flow.v1` root with one declared fragment | sibling `processes.yaml` |

### Folder fragment names: scaffold vs migrate

These names are intentionally different and must not be mixed up:

| Path | Who creates it | Role |
| --- | --- | --- |
| `steps/load.yaml` | `dpone init` / self-service scaffold for new `folder` pipelines | Canonical greenfield fragment listed by the root |
| `processes.yaml` | `dpone migrate authoring --to folder` only | Migration-owned sibling fragment created beside an existing root |

Migration never rewrites a scaffold `steps/load.yaml` into `processes.yaml`.
A different user-owned `processes.yaml` blocks migrate; greenfield authors should
keep using `steps/load.yaml` from
[Airflow authoring](airflow-authoring.md). When moving away from folder mode, old
fragments are retained and reported for manual review; dpone never deletes them
automatically.

## Safety model

The command:

- reads bounded, unique-key YAML confined to the project root;
- rejects symlink and path escapes;
- redacts secret-like values from text and JSON diffs;
- blocks recipe-authored sources rather than silently materializing a recipe;
- compares canonical semantic fingerprints before writing;
- verifies the source SHA-256 again under a confined file descriptor;
- atomically replaces the primary source and rolls back a newly created folder
  fragment if that switch fails;
- performs no network, Airflow, database, Kubernetes, Vault, or runtime I/O.

The JSON plan and receipt use `dpone.authoring-migration.v1`, published in the
[GitOps schema catalog](reference/gitops-schema-catalog.md). A blocked apply
leaves generated releases and deployments untouched. Restore the authoring
source from version control if post-write verification reports an operating
system or storage failure.

## Unsupported inputs

Recipe-authored sources must keep their immutable recipe reference. Create a
new explicit source in review if materialization is truly intended. Legacy
single-manifest migration remains `dpone manifest migrate`; legacy connection
field migration remains `dpone fix --plan|--apply`.
