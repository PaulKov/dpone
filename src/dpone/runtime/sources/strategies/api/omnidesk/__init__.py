from dpone.runtime.sources.strategies.api.omnidesk.dictionary_extract import (
    OmnideskDictionaryExtractStrategy,
)
from dpone.runtime.sources.strategies.api.omnidesk.full_extract import OmnideskFullExtractStrategy
from dpone.runtime.sources.strategies.api.omnidesk.incremental_append_extract import (
    OmnideskIncrementalAppendExtractStrategy,
)
from dpone.runtime.sources.strategies.api.omnidesk.incremental_merge_extract import (
    OmnideskIncrementalMergeExtractStrategy,
)

__all__ = [
    "OmnideskFullExtractStrategy",
    "OmnideskIncrementalMergeExtractStrategy",
    "OmnideskIncrementalAppendExtractStrategy",
    "OmnideskDictionaryExtractStrategy",
]
