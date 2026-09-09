"""Deprecated re-exports for canonical dbt compiler assembly."""

from dpone.app.dbt_publish_composition import (
    DbtPublishProfileLoader as DbtPublishProfileLoader,
)
from dpone.app.dbt_publish_composition import (
    __all__ as __all__,
)
from dpone.app.dbt_publish_composition import (
    build_dbt_artifact_writer as build_dbt_artifact_writer,
)
from dpone.app.dbt_publish_composition import (
    build_dbt_dpone_compiler as build_dbt_dpone_compiler,
)
from dpone.app.dbt_publish_composition import (
    build_dbt_release_materializer as build_dbt_release_materializer,
)
from dpone.app.dbt_publish_composition import (
    build_legacy_preview_execution_pack as build_legacy_preview_execution_pack,
)
from dpone.app.dbt_publish_composition import (
    legacy_preview_artifact_writer_defaults as legacy_preview_artifact_writer_defaults,
)
from dpone.app.dbt_publish_composition import (
    legacy_preview_compiler_dependencies as legacy_preview_compiler_dependencies,
)
