# DPONE_AUTHORING_FOLDER_LOADER_UNAVAILABLE

Folder compilation was invoked without the bounded filesystem adapter. Normal
CLI, manifest-loader, preview, and sample composition roots install it
automatically. Application integrators should use `default_authoring_compiler()`
or inject a `FolderFragmentLoader`; pipeline authors should report this as an
installation/integration error.
