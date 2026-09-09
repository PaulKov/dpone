"""Formatting helpers for ETL logger output."""

from __future__ import annotations

from typing import Any, Protocol


class LogLevelToken(Protocol):
    value: str


try:
    from prettytable import PrettyTable  # type: ignore
except Exception:  # pragma: no cover

    class PrettyTable:  # type: ignore
        def __init__(self):
            self.field_names: list[str] = []
            self.align = "l"
            self._rows: list[list[str]] = []

        def add_row(self, row):
            self._rows.append(["" if v is None else str(v) for v in row])

        def __str__(self) -> str:
            cols = [self.field_names] + self._rows
            widths: list[int] = [0] * (len(self.field_names) or 0)
            for r in cols:
                for i, v in enumerate(r):
                    if i >= len(widths):
                        widths.append(0)
                    widths[i] = max(widths[i], len(str(v)))

            def fmt_row(r):
                out = []
                for i, v in enumerate(r):
                    s = str(v)
                    w = widths[i] if i < len(widths) else len(s)
                    out.append(s.ljust(w))
                return " | ".join(out)

            if not self.field_names:
                return "\n".join(" | ".join(r) for r in self._rows)

            sep = "-+-".join("-" * w for w in widths)
            table_lines = [fmt_row(self.field_names), sep]
            for r in self._rows:
                table_lines.append(fmt_row(r))
            return "\n".join(table_lines)


def format_table(rows: list[dict[str, Any]], title: str | None = None) -> list[str]:
    """
    Форматирует данные в красивую таблицу используя prettytable.

    Args:
        rows: Список словарей, где ключи - это названия колонок
        title: Опциональный заголовок таблицы

    Returns:
        Список строк для вывода
    """
    if not rows:
        return []

    # Определяем все колонки, сохраняя порядок из первого словаря
    # Python 3.7+ гарантирует порядок вставки в dict
    all_columns = set()
    first_row_keys = list(rows[0].keys()) if rows else []

    for row in rows:
        all_columns.update(row.keys())

    # Сохраняем порядок из первого словаря, затем добавляем остальные (если есть)
    columns = list(first_row_keys)
    for key in sorted(all_columns - set(first_row_keys)):
        columns.append(key)

    # Создаем таблицу с prettytable
    table = PrettyTable()
    table.field_names = columns

    # Добавляем строки
    for row in rows:
        table.add_row([row.get(col, "") for col in columns])

    # Настройка стиля таблицы - используем Box-drawing characters
    table.align = "l"  # Выравнивание по левому краю
    table.padding_width = 1
    table.junction_char = "┼"
    table.horizontal_char = "─"
    table.vertical_char = "│"
    # Настройка corner и junction символов для красивой таблицы
    table.left_junction_char = "├"
    table.right_junction_char = "┤"
    table.top_junction_char = "┬"
    table.bottom_junction_char = "┴"
    table.top_left_junction_char = "┌"
    table.top_right_junction_char = "┐"
    table.bottom_left_junction_char = "└"
    table.bottom_right_junction_char = "┘"

    # Получаем строки таблицы
    table_string = table.get_string()
    lines = table_string.split("\n")

    # Добавляем отступ в 3 пробела для каждой строки
    lines = [f"   {line}" for line in lines]

    # Если есть заголовок, добавляем его перед таблицей
    if title:
        # Получаем ширину таблицы из первой строки
        if lines:
            table_width = len(lines[0]) - 3  # минус 3 пробела отступа
        else:
            table_width = len(title) + 4

        title_lines = []
        title_lines.append("   ┌" + "─" * (table_width - 2) + "┐")
        title_padding = (table_width - len(title) - 2) // 2
        title_lines.append(f"   │{' ' * title_padding}{title}{' ' * (table_width - len(title) - 2 - title_padding)}│")
        title_lines.append("   └" + "─" * (table_width - 2) + "┘")
        title_lines.append("")  # Пустая строка между заголовком и таблицей
        lines = title_lines + lines

    return lines


def format_message_lines(level: LogLevelToken, message: str, details: dict[str, Any] | None = None) -> list[str]:
    border = "=" * 80

    lines = ["", border, f"{level.value} {message}", border]

    if details:
        # Обрабатываем ключи в порядке вставки (Python 3.7+ гарантирует порядок в dict)
        # Только TABLE_LINE_ нужно сортировать по номеру
        table_lines = {}
        other_keys = []

        for key in details.keys():
            if key.startswith("TABLE_LINE_"):
                # Извлекаем номер строки для сортировки
                parts = key.split("_")
                if parts[-1].isdigit():
                    table_lines[int(parts[-1])] = key
                else:
                    other_keys.append(key)
            else:
                other_keys.append(key)

        # Формируем финальный список ключей: сначала не-таблицы в исходном порядке,
        # затем таблицы в отсортированном порядке
        sorted_keys = []
        for key in other_keys:
            sorted_keys.append(key)
        for line_num in sorted(table_lines.keys()):
            sorted_keys.append(table_lines[line_num])

        for key in sorted_keys:
            value = details[key]
            if key.startswith("EMPTY_LINE"):
                lines.append("")
            elif key.startswith("TABLE_LINE_"):
                # Табличные строки - выводим как есть
                lines.append(value)
            elif key in {
                "⚙️ CONFIGURATION",
                "📈 DATA FLOW",
                "⚡ PERFORMANCE",
                "🔍 CONTEXT",
                "📋 SAMPLE DATA",
                "⚡ PERFORMANCE METRICS",
                "🔍 SQL QUERY",
                "📋 PARAMETERS",
                "📊 QUALITY CHECK RESULTS",
                "📊 STATE CHANGE",
                "🚨 ERRORS",
                "🔍 VALIDATION SUMMARY",
                "🔍 VALIDATION SUMMARY BY DATE",
            }:
                lines.append(f"   {key}")
            elif isinstance(value, dict):
                lines.append(f"   {key}")
                for sub_key, sub_value in value.items():
                    lines.append(f"      • {sub_key}: {sub_value}")
            elif isinstance(value, list):
                lines.append(f"   {key}: {', '.join(map(str, value))}")
            else:
                lines.append(f"   {key}: {value}")

    return lines


__all__ = ["format_message_lines", "format_table"]
