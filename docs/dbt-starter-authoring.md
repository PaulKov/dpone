# Create a dbt starter from platform policy

This guide is for dbt authors creating a native project from an explicit policy
provided by their platform team. `dpone init dbt` creates source files offline.
It does not run dbt, fetch dependencies, obtain credentials, provision SQL, or
publish data.

The command is unreleased. Complete installed managed-package resources and the
installed end-to-end authoring journey remain **UNVERIFIED**. The current source
inventory is incomplete: an otherwise valid request fails closed with
`DPONE_DBT_STARTER_RESOURCES_INVALID` before scaffold apply. The commands below
describe the implemented interface; they are not evidence of a qualified
installation.

## Prepare and preview

Obtain a nonsecret `dpone.dbt-publish-policy.v4` file from the platform team, with
an explicit native profile, authoring template, and workflow. The selected
profile must admit `full_refresh`, its serialized payload budget, and
`rowstore_none`. The destination's parent directory must already exist.

In this example, `platform-policy.yml`, `analytics`, and `orders` are placeholders
for the supplied file and its exact profile/workflow names:

```bash
dpone init dbt ./orders-project \
  --profiles=./platform-policy.yml \
  --profile=analytics \
  --workflow=orders \
  --dry-run --format=json
```

All four inputs are required. Put dbt-specific options after `dbt`; the legacy
`dpone init --profile REF pipeline` option selects a recipe profile and retains
its existing meaning. Use `--profile=-local` for a name beginning with a dash,
and `./-project` for a relative destination beginning with a dash.

Dry-run creates no destination files or directories. The existing authoring
lock may maintain bounded state outside the destination. The result describes
the intended changes; it does not claim that the project exists.

## Apply and inspect

Remove `--dry-run` to apply. Apply is the default; there is no `--apply`,
`--force`, or overwrite option:

```bash
dpone init dbt ./orders-project \
  --profiles=./platform-policy.yml \
  --profile=analytics \
  --workflow=orders --format=json
```

A complete successful plan covers 23 files: six rendered starter files,
`packages.yml`, `package-lock.yml`, fourteen files under
`dbt_packages/dbt_dpone`, and the captured policy at
`dpone/dbt-publish-profiles.yml`. The supplied policy bytes are retained. Existing
identical files are no-ops; a differing file rejects the whole planned write
set. Apply uses per-file ownership-aware writes and compensation, not an atomic
whole-directory swap.

Application results go to stdout in `text` (default), `json`, or `md` format,
with empty stderr. Argument errors use the existing CLI parser and stderr;
with JSON format selected, the usage error is JSON on stderr.

| Result field | Meaning |
| --- | --- |
| `passed` | Whether this authoring operation succeeded. This is not runtime qualification. |
| `changes` | File paths and actions. File contents, diffs, and free-form change messages are suppressed. |
| `errors` | Structured diagnostic codes with sanitized messages. A file conflict can instead be represented by its change action. |
| `policy_sha256`, `dry_run`, `file_count` | Present after policy and resource validation, alongside the scaffold result. |
| `rollback_journal` | Scaffold write/recovery receipt; inspect it after a failed apply. |
| `recovery_required`, `rollback_issues`, `recovery_artifacts` | Additional failure fields describing compensation and retained recovery material. |
| `next_command` | On successful dry-run, the safely quoted apply command; on successful apply, the parse/check suggestion. |
| `rerun_command` | A safely quoted invocation included for an actual file conflict. It does not overwrite the conflict. |

| Exit | Meaning and next action |
| --- | --- |
| `0` | Preview or apply succeeded, including identical-file no-ops. Check `dry_run` before assuming files were created. |
| `1` | Invalid/unreadable policy, unavailable profile/workflow, or conflicting files. Correct the selected policy or reconcile the reported paths. |
| `2` | Invalid arguments/project path or incomplete installed resources. Repair the inputs or approved installation first. |
| `4` | Authoring lock or scaffold apply failure. Inspect the structured error and recovery receipt before retrying. |

After a successful apply, the suggested command enters the project and runs
`dbt parse --profiles-dir profiles --no-partial-parse`, followed by
`dpone dbt check .`. These are separate user actions: init does not execute them.
They require the platform-provided runtime setup, and their success does not
establish managed build or publication qualification.

## Diagnose and recover

For `DPONE_DBT_STARTER_RESOURCES_INVALID`, ask the platform team to restore the
complete approved installed distribution. Init has no source-checkout fallback
and does not repair resources by downloading dependencies. Maintainer preparation
is documented separately in [Starter resource generation](dbt-starter-resource-generation.md).

For a conflict, inspect the named path and preserve your work. Use a different
destination or reconcile the conflicting file deliberately before rerunning.
For exit `4`, retain the returned journal and recovery paths; automatic retry or
deleting an entire project is not a recovery procedure. The error catalog covers
[scaffold and authoring failures](dbt-self-service-errors.md).

Existing init targets and recipe profile syntax remain compatible. Return to
the [dbt integration overview](dbt.md) for authoring and operational references.
