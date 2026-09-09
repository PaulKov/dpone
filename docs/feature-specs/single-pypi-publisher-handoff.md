# Single PyPI publisher handoff

## Status

**APPROVED — PaulKov, 2026-08-26.**

The original source-workflow handoff/GitHub Release sequence below is
superseded for ordinary PyPI publication by the controller's
[approved OIDC publisher specification](https://github.com/PaulKov/dpone-release-controller/blob/3b6ff638e2dbefc092447c584084a75ac72fd117/docs/feature-specs/oidc-pypi-release-controller.md).
This 2026-08-28 documentation reconciliation preserves the single-publisher
decision; it does not change workflow code or provider permissions.

## Decision

`PaulKov/dpone-release-controller/.github/workflows/pypi-release.yml` is the
sole ordinary PyPI publisher for dpone's four release distributions. The
candidate repository's tag workflow no longer mints a PyPI OIDC token and
contains no PyPI publishing action.

## Release path

1. Prepare the exact source/version/tag and required source-readiness evidence.
   Tagging can still start source workflows; they are not an automatic
   controller handoff or a source of controller archive bytes.
2. With separate publication authorization, manually dispatch the controller's
   `pypi-release.yml` with the identical `X.Y.Z` version as its only input.
3. The controller builds all four distributions from that tag, publishes its
   retained archives using OIDC, and verifies the public archive hashes.
4. Record the exact controller run and manifest. The ordinary controller does
   not create a GitHub Release. Use its read-only retrospective verifier for
   an already published version; never dispatch publication to obtain proof.

The old sequence linked publication to the source workflow's handoff check and
subsequent GitHub Release creation. That is not the current ordinary operator
journey. Follow [Release](../release.md) for commands and evidence boundaries.

## Failure and recovery

The handoff job performs no upload and cannot compensate for a failed or
partial controller run. Reconcile the controller's immutable artifact manifest
against PyPI first. Never restore a second publisher, add `skip-existing`, or
substitute candidate artifacts to make the candidate workflow pass.
