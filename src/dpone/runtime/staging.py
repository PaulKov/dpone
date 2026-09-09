from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.cloud_artifacts import GCSExportArtifact


class StagingManager(ABC):
    """Контракт для менеджеров staging на стороне приёмника."""

    @abstractmethod
    def create(self, load_config, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        """Создаёт staging таблицу и возвращает артефакт."""

    @abstractmethod
    def insert_rows(
        self,
        artifact: StagingTableArtifact,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        """Вставляет набор строк в staging таблицу."""

    @abstractmethod
    def insert_from_query(
        self,
        artifact: StagingTableArtifact,
        query: str,
        schema: Sequence[tuple[str, str]],
        params: Sequence[Any] | None = None,
    ) -> int:
        """Заполняет staging результатом запроса к внешнему источнику."""

    @abstractmethod
    def load_from_gcs_artifact(
        self,
        artifact: StagingTableArtifact,
        gcs_artifact: GCSExportArtifact,
    ) -> int:
        """Загружает данные из GCS в staging таблицу (для BigQuery)."""

    @abstractmethod
    def drop(self, artifact: StagingTableArtifact) -> None:
        """Удаляет staging таблицу."""


@contextmanager
def owned_staging_handle(
    staging_manager: StagingManager,
    load_config: Any,
    schema: Sequence[tuple[str, str]],
) -> Iterator[StagingTableArtifact]:
    """Transfer a new staging handle only after construction succeeds.

    A materializer owns the handle between ``create`` and its return to the
    caller.  If population or validation fails during that interval, this
    guard drops the otherwise unreachable staging table.  Cleanup is
    best-effort and can annotate, but never replace, the primary failure.
    """

    handle = staging_manager.create(load_config, schema)
    try:
        yield handle
    except BaseException as primary_error:
        try:
            handle.cleanup()
        except BaseException as cleanup_error:
            add_note = getattr(primary_error, "add_note", None)
            if callable(add_note):
                add_note(f"staging handle cleanup failed: {type(cleanup_error).__name__}")
        raise


__all__ = ["StagingManager", "owned_staging_handle"]
