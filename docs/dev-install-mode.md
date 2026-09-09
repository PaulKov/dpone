# Development install mode

Development install mode allows an environment with a baked `dpone` version to test a newer snapshot package without rebuilding the base image.

This mode is intended for development and review environments, not normal production releases.

## Concept

1. CI builds a snapshot package.
2. The runtime sets package overlay environment variables.
3. The process starts through `dpone-runtime-exec`.
4. The wrapper installs the snapshot into a writable target directory and prepends it to `PYTHONPATH`.

Production should normally use immutable images and release tags.

## Environment variables

| Variable | Purpose |
|---|---|
| `DPONE_INSTALL_MODE` | `baked` or `snapshot`. |
| `DPONE_PACKAGE_SPEC` | Package spec such as `dpone==X.Y.Z.devN`. |
| `DPONE_PACKAGE_INDEX_URL` | Package index containing the snapshot. |
| `DPONE_PACKAGE_EXTRA_INDEX_URL` | Optional extra index URL. |
| `DPONE_PACKAGE_EXTRA_INDEX_URLS` | Optional shell-style list of additional indexes. |
| `DPONE_PACKAGE_TARGET` | Writable directory added to `PYTHONPATH`. |
| `DPONE_PACKAGE_PIP_ARGS` | Optional extra `pip install` flags. |

## Dry run

```bash
DPONE_INSTALL_MODE=snapshot \
DPONE_PACKAGE_SPEC='dpone==X.Y.Z.devN' \
DPONE_PACKAGE_TARGET=/tmp/dpone-overlay \
dpone-runtime-exec --dry-run python -c 'import dpone; print(dpone.__version__)'
```

## Real run

```bash
DPONE_INSTALL_MODE=snapshot \
DPONE_PACKAGE_SPEC='dpone==X.Y.Z.devN' \
DPONE_PACKAGE_TARGET=/tmp/dpone-overlay \
dpone-runtime-exec python -m dpone --help
```

## Practical notes

- `DPONE_PACKAGE_TARGET` must be writable.
- The target directory is recreated on every start to avoid stale files.
- This mode assumes a baseline `dpone` already exists in the image so `dpone-runtime-exec` is available.
- Use public PyPI or a private package index according to your environment policy.
