# DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID

Folder authoring requires a non-empty `fragments` list and cannot also contain
an inline `processes` list. Keep `pipeline.yaml` as the primary source and move
processes into explicitly listed `dpone.flow-fragment.v1` files.

Rerun `dpone check <pipeline>` after editing the root source.
