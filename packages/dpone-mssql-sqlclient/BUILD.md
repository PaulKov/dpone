# Offline companion build and deployment admission

These developer tools stage the optional `mssql_sqlclient` companion for the
fixed Linux Arm64 profile: SDK 8.0.425, runtime 8.0.31, Microsoft.Data.SqlClient
7.0.2 and Apache.Arrow 23.0.0. BCP remains the default. A successful build is not
SQL connectivity, bulk-load, recovery, performance, or route certification.
Neither tool installs a package or enables a backend in a pipeline.

## Approved inputs

Use an independently reviewed SDK image with network disabled. The external
build controller must verify image identity; a process inside a container cannot
prove its own image provenance. The build tool additionally compares the exact
SDK file inventory supplied by that controller. The current image pin is encoded
in `tools/build_companion.py`. Build-only Python tooling may be provided by the
controller without changing the admitted SDK tree or deployment runtime.

Prepare an immutable source directory containing exactly `Worker.csproj`,
`Worker.packages.lock.json`, and the approved `worker/**/*.cs` files. Do not copy
tests, caches, generated outputs, or other project files into it. Prepare a flat,
read-only offline NuGet feed containing the approved `.nupkg` files.

The separately retained build pins JSON has exactly these keys:

- `sdk_image`: the approved image identity;
- `sdk_version`: `8.0.425`;
- `source`, `sdk`, `feed`: maps from relative file names to approved SHA-256
  hashes for their respective roots.

The producer does not create these trusted inputs by inspecting candidates.
Empty, missing, extra, changed, or linked files fail validation. The build SDK
inventory permits up to 8192 files; deployment manifest limits remain unchanged.

## Build a new staging directory

Run with the repository source available on `PYTHONPATH`, Python 3.12, and the
approved .NET SDK in the build-only Linux environment:

```bash
python packages/dpone-mssql-sqlclient/tools/build_companion.py \
  --source /approved/source \
  --sdk /usr/share/dotnet \
  --feed /approved/feed \
  --pins /approved/build-pins.json \
  --output /staging/build-one
```

All roots and output paths must be absolute, canonical paths. The output must
not exist or be inside an input root. The producer selects SDK 8.0.425 with
roll-forward disabled, restores in locked mode from only the offline feed, then
publishes Release/linux-arm64 without restoring. Shared compilation, MSBuild
servers and node reuse are explicitly disabled in the fixed invocation and
environment. The receipt records these command and environment settings. Each build uses its own source
copy, intermediate directory, package cache and published output. Forced lock
reevaluation and arbitrary MSBuild properties are not exposed.

The exact forty deployment files are copied to `companion/`: one worker DLL,
one runtime configuration, one dependency description and 37 managed dependency
or satellite DLLs. Only the worker PDB and XML documentation are separated into
`diagnostics/`. An unexpected asset fails the build; no asset or dependency
record is silently removed to make a validator pass. Existing Python profile
and dependency validators are used unchanged.

`build-receipt.json` is written only after success. It records input inventories,
commands, configuration hashes, measured outputs and package license declarations.
License declarations are review evidence, not a completed legal review. Failed
builds retain diagnostics and no success receipt. Child execution has a shared
240-second deadline; timeout kills the process group and reaps its direct child. These tools
assume immutable admitted input roots and controlled staging custody.

Build a second time into another new directory. Compare the exact companion
name set and every SHA-256 digest, including the generated JSON files. Setting
`Deterministic=true` alone does not demonstrate reproducibility. A negative
qualification must show that a project/lock disagreement actually fails locked
restore. Do not repair or rewrite the lock as part of that test.

The checked-in double-build producer performs that comparison with two isolated
package caches, intermediate trees and publish directories:

```bash
python packages/dpone-mssql-sqlclient/tools/qualify_reproducible_build.py \
  --source /approved/source \
  --sdk /usr/share/dotnet \
  --feed /approved/feed \
  --pins /approved/build-pins.json \
  --output /staging/reproducible-build
```

The output retains both complete build directories and a
`reproducibility-receipt.json`. The receipt is emitted only when the complete
companion inventories are byte-identical. It is reproducibility evidence, not
approval, signing, installation or certification.

## Produce existing deployment admission inputs

A separate approval step retains the successful build receipt digest, exact
companion inventory, and exact approved runtime inventory. The admission pins
JSON contains exactly `companion`, `runtime`, and `build_receipt_sha256`.
Runtime entries cover `dotnet`, `host/fxr/8.0.31/**`, and
`shared/Microsoft.NETCore.App/8.0.31/**`. A second host/fxr version is rejected.

```bash
python packages/dpone-mssql-sqlclient/tools/produce_admission.py \
  --companion /staging/build-one/companion \
  --runtime /approved/runtime \
  --pins /approved/admission-pins.json \
  --build-receipt /staging/build-one/build-receipt.json \
  --output /staging/admission-one
```

The new output directory must be outside both inventoried roots. It contains
canonical `deployment.json` and `admission-receipt.json`. The manifest uses the
existing deployment schema and digest domain with one NUL separator. The receipt
provides `build_sha256` and `companion_inventory` for the existing internal
`AdmittedSqlClientInstallation` constructor; the trusted composition root also
supplies independently admitted Python/source identity and absolute paths.

Hashing a candidate manifest does not authorize it. Keep the approved expected
digest and inventories separately and protect the installed files from mutation.
No new runtime factory, user-selectable executable, installer, manifest role,
platform, or SQL permission is introduced. Existing managed startup verification,
full worker integration, distribution packaging and route qualification remain
separate gates.

## Assemble the unsigned release bundle

After independent admission, prepare a reviewed `dpone.licenses.v1` JSON file.
Its `packages` array must cover every exact dependency coordinate from the build
receipt and `Microsoft.NETCore.App` `8.0.31` once. Each row has the closed keys
`name`, `version`, `spdx_expression` and `license_text`; supply the reviewed
license text, not a URL or an unreviewed package declaration. An empty, missing
or additional dependency fails packaging.

```bash
python packages/dpone-mssql-sqlclient/tools/package_release.py \
  --version 0.83.0 \
  --build /staging/reproducible-build/build-one \
  --reproducibility /staging/reproducible-build/reproducibility-receipt.json \
  --admission /staging/admission-one \
  --runtime /approved/runtime \
  --licenses /approved/licenses.json \
  --output /staging/release-0.83.0
```

The runtime root must match every admitted `origin=dotnet` deployment row by
path and SHA-256, with no missing, changed, linked, or extra files. Packaging is
bounded to 8192 runtime files, 512 MiB per file, and 2 GiB total. Runtime inputs
are copied into `runtime/`; the copied inventory is measured again before the
manifest is written. No download or reconstruction is performed.

Only `0.83.x` companion versions are accepted by this producer. The result has
a deterministic uncompressed tar, a read-only version/build-digest install tree,
the complete companion and runtime trees, the existing closed `deployment.json`,
a closed release manifest, exact checksums,
dependency inventory, SPDX 2.3 SBOM, reviewed license records, compatibility
metadata, sanitized provenance and an unsigned signature-input subject. The
producer never invents a signature and never publishes or installs the artifact.

Extract each new version beside existing roots. Verify the external signature,
admit the exact deployment digest and inventories, then atomically select that
complete root through trusted composition. Rollback selects a previously admitted
complete root. Never merge files between roots or mutate an installed version.


The runtime-only deployment requires the native OS dependencies of .NET 8 as
well as the admitted runtime files. In particular, the normal-globalization
profile needs ICU. See Microsoft's [OS package requirements](https://github.com/dotnet/core/blob/main/release-notes/8.0/os-packages.md)
and the companion README. SDK-free execution does not mean OS-dependency-free
execution. Keep OS-platform provenance separate from the runtime and companion
inventories; do not rewrite runtime configuration to hide a missing dependency.
