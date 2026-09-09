"""Validated Kubernetes CronJob schedule subset without runtime dependencies."""

from __future__ import annotations

import re

_DESCRIPTORS = frozenset({"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"})
_INTEGER = re.compile(r"^[0-9]+$")
_MONTH_NAMES = {name: index for index, name in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}
_DAY_NAMES = {name: index for index, name in enumerate("SUN MON TUE WED THU FRI SAT".split())}
_FIELD_BOUNDS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))


def require_kubernetes_cron_schedule(value: str) -> str:
    """Return one supported schedule or raise a stable input error."""

    schedule = value.strip()
    if schedule in _DESCRIPTORS:
        return schedule
    fields = schedule.split()
    if len(fields) != 5:
        raise ValueError("schedule must be a five-field Kubernetes CronJob expression or standard descriptor")
    for index, (field, bounds) in enumerate(zip(fields, _FIELD_BOUNDS, strict=True)):
        names = _MONTH_NAMES if index == 3 else _DAY_NAMES if index == 4 else {}
        _validate_field(field, lower=bounds[0], upper=bounds[1], names=names)
    return schedule


def _validate_field(field: str, *, lower: int, upper: int, names: dict[str, int]) -> None:
    if not field or any(character.isspace() for character in field):
        raise ValueError("schedule contains an empty cron field")
    for item in field.split(","):
        if not item:
            raise ValueError("schedule contains an empty cron list item")
        expression, step = _split_step(item)
        if step is not None and not 1 <= step <= upper - lower + 1:
            raise ValueError("schedule cron step is outside the field range")
        if expression in {"*", "?"}:
            continue
        if "-" in expression:
            parts = expression.split("-")
            if len(parts) != 2:
                raise ValueError("schedule contains an invalid cron range")
            start = _field_value(parts[0], lower=lower, upper=upper, names=names)
            end = _field_value(parts[1], lower=lower, upper=upper, names=names)
            if start > end:
                raise ValueError("schedule cron range must be ascending")
            continue
        _field_value(expression, lower=lower, upper=upper, names=names)


def _split_step(value: str) -> tuple[str, int | None]:
    parts = value.split("/")
    if len(parts) == 1:
        return value, None
    if len(parts) != 2 or not parts[0] or not _INTEGER.fullmatch(parts[1]):
        raise ValueError("schedule contains an invalid cron step")
    return parts[0], int(parts[1])


def _field_value(value: str, *, lower: int, upper: int, names: dict[str, int]) -> int:
    normalized = value.upper()
    if normalized in names:
        parsed = names[normalized]
    elif _INTEGER.fullmatch(value):
        parsed = int(value)
    else:
        raise ValueError("schedule contains an unsupported cron token")
    if not lower <= parsed <= upper:
        raise ValueError("schedule cron value is outside the field range")
    return parsed


__all__ = ["require_kubernetes_cron_schedule"]
