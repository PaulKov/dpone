# GitHub Pages documentation site

This repository ships a professional static documentation site powered by MkDocs Material and GitHub Pages.

## Local preview

Install the documentation toolchain:

```bash
python -m pip install -r docs/requirements.txt
```

Run the local server:

```bash
mkdocs serve
```

Build the production site exactly like CI:

```bash
uv sync --locked
uv run dpone docs check-generated-references
uv run mkdocs build --strict
```

## GitHub Pages deployment

The workflow lives in [.github/workflows/pages.yml](https://github.com/PaulKov/dpone/blob/master/.github/workflows/pages.yml).

It runs for:

- every pull request, providing an always-present required docs result;
- pushes to `master` that change documentation files;
- manual `workflow_dispatch` runs.

Pull requests run a read-only build and may rerun it, but never upload,
configure, or deploy Pages. Non-PR build/upload, current-master verification,
and deploy are eligible only for provider run attempt `1`.

The workflow has no top-level authority. `build` and
`verify_current_master` receive only `contents: read`; the verifier requires the
current 40-hex `refs/heads/master` SHA to equal the workflow subject and emits
that SHA plus its exact run attempt. Only `deploy` receives `pages: write` and
`id-token: write`, and only after successful build plus same-attempt freshness
verification. The protected `github-pages` environment remains attached to
that deploy job.

PR runs group by PR number and cancel only a superseded head of that PR.
Non-PR runs group by ref and serialize the entire build/verify/deploy path. This
keeps a newer run pending until the older run finishes, after which freshness
verification prevents an obsolete subject from deploying. No Pages job uses
`queue`.

## Repository settings

In GitHub, enable Pages once:

1. Open `Settings -> Pages`.
2. Set `Build and deployment` to `GitHub Actions`.
3. Save the setting.

No PyPI token, GitHub token, database credential, or external service secret is required for the docs deploy.

## Navigation policy

The public site uses curated navigation:

- onboarding and CLI docs first;
- core concepts next;
- connector guides;
- source -> sink implementation guides;
- operations and production readiness;
- developer docs.

Older rollout notes and provider-specific implementation history remain in the repository, but they are intentionally not prominent in the public navigation.

## Style policy

The site uses a light, quiet interface inspired by modern documentation products:

- flat surfaces instead of heavy cards;
- high-contrast text on white/off-white backgrounds;
- subdued borders;
- responsive grids that avoid narrow, tall panels;
- copyable code blocks and searchable navigation.

## Troubleshooting

### `mkdocs build --strict` fails with a missing nav page

Confirm the file exists under `docs/` and the nav path in `mkdocs.yml` is relative to `docs/`, not the repository root.

### GitHub Pages deploy is skipped on a pull request

That is expected. Pull requests build the site only. Deployment happens after merge or push to `master`.

The skipped deploy job may appear as check success. This means deployment
`NOT_RUN` with `UNVERIFIED` deployment evidence; the read-only docs build is the
required PR result.

### A non-PR Pages run fails or becomes stale

Do not rerun all jobs, only `verify_current_master`, or only `deploy`. Every
non-PR upload, verification, and deploy is attempt-1 only, and a rerun cannot
reuse earlier verification or the immutable `github-pages` artifact. Preserve
the run, fix the cause, and dispatch a new run on current `master` with the
copyable [Pages recovery procedure](cicd/runbooks.md#docs-and-github-pages-failures).

Deployment PASS requires authenticated provider evidence for the exact
repository, active workflow path and blob, returned run ID, attempt `1`, event,
`head_branch == "master"`, SHA, unique `Deploy GitHub Pages documentation` job,
and its pinned `Deploy Pages` step. A workflow conclusion, environment state,
older deployment record, same-SHA run, or skipped job is insufficient.

### GitHub Pages shows a 404

Check that repository Pages settings use `GitHub Actions` as the source and that the latest `docs` workflow run completed successfully on `master`.
