"""
GCS URI utilities.

Утилиты для работы с Google Cloud Storage URI форматами.
Конвертация между gs://, https://, парсинг путей и bucket'ов.
"""

from __future__ import annotations


def gs_to_https(uri: str) -> str:
    """
    Конвертирует GCS URI из gs:// формата в HTTPS URL.

    Args:
        uri: GCS URI в формате gs://bucket/path или уже https://storage.googleapis.com/bucket/path

    Returns:
        HTTPS URL в формате https://storage.googleapis.com/bucket/path
    """
    if uri.startswith("gs://"):
        # Убираем gs:// (5 символов) и добавляем https:// префикс
        return f"https://storage.googleapis.com/{uri[5:]}"
    return uri


def https_to_gs(uri: str) -> str:
    """
    Конвертирует HTTPS URL в GCS URI формат.

    Args:
        uri: HTTPS URL или уже gs:// URI

    Returns:
        GCS URI в формате gs://bucket/path
    """
    if uri.startswith("https://storage.googleapis.com/"):
        # Убираем https://storage.googleapis.com/ (31 символ)
        return f"gs://{uri[31:]}"
    return uri


def normalize_gcs_path(path: str) -> str:
    """
    Нормализует GCS путь: убирает trailing slash.

    Args:
        path: GCS URI или путь

    Returns:
        Нормализованный путь без trailing slash

    Example:
        >>> normalize_gcs_path("gs://my-bucket/path/")
        'gs://my-bucket/path'

        >>> normalize_gcs_path("https://storage.googleapis.com/bucket/")
        'https://storage.googleapis.com/bucket'
    """
    return path.rstrip("/")


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    """
    Парсит GCS URI на bucket и path компоненты.

    Args:
        gs_uri: GCS URI в формате gs://bucket/path

    Returns:
        Tuple[bucket, path]: Имя bucket и путь внутри bucket

    Raises:
        ValueError: Если URI не в формате gs://
    """
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI format: {gs_uri}. Expected format: gs://bucket/path")

    path_part = gs_uri[5:]

    # Разделяем на bucket и путь
    parts = path_part.split("/", 1)
    bucket = parts[0]
    path = parts[1] if len(parts) > 1 else ""

    return bucket, path


def build_gcs_file_path(
    base_uri: str,
    file_prefix: str,
    extension: str,
    *,
    partition_id_placeholder: bool = False,
) -> str:
    """
    Строит полный путь к файлу в GCS с учётом чанкования.

    Args:
        base_uri: Базовый GCS URI (gs:// или https://)
        file_prefix: Префикс имени файла
        extension: Расширение файла (без точки)
        partition_id_placeholder: Добавить {_partition_id} для чанкования ClickHouse

    Returns:
        Полный путь к файлу
    """
    # Нормализуем base URI (убираем trailing slash)
    base = normalize_gcs_path(base_uri)

    # Формируем имя файла
    if partition_id_placeholder:
        filename = f"{file_prefix}_{{_partition_id}}.{extension}"
    else:
        filename = f"{file_prefix}.{extension}"

    return f"{base}/{filename}"
