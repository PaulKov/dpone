# Runtime delivery of the dbt-dpone package

The governed runtime image installs the exact four-file package read-only at
`/opt/dpone/runtime/dbt-dpone`. Platform composition passes that directory as
`package_source_root`; authors do not install or override it.

Semantic refresh V2 does not rely on an author-owned `macro-paths` entry or on
copying a repository-local package declaration. The platform runtime image must
contain this exact four-file dbt package as a protected deployment input. The
canonical lifecycle binds its content digest into the plan bundle and protected
activated pack. Airflow passes that authenticated digest to the dbt gate; the
application composition binds only the platform image's local package directory.

At task execution, `execute_semantic_refresh_dbt_pack` verifies the closed
package inventory and digest, copies it to the attempt-local project's resolved
`packages-install-path`, applies the authenticated project overlay, and only
then enters the pinned dbt preflight/build path. A missing, stale, or modified
package blocks before dbt starts.

The former example `packages.yml` and `package-lock.yml` were removed because a
revision copied from a mutable development branch is not reproducible release
authority. Release automation may publish a package declaration only after it
can name a real immutable source revision whose bytes match the lifecycle
policy digest.
