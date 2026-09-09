from __future__ import annotations

from dataclasses import dataclass

from dpone.runtime.sources.strategies.api.yandex_webmaster.dates import MOSCOW_TZ

YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES: tuple[str, ...] = ("DESKTOP", "MOBILE", "TABLET")


@dataclass(frozen=True, slots=True)
class YandexWebmasterRegion:
    region_id: int
    region_name: str


YANDEX_WEBMASTER_DEFAULT_REGIONS: tuple[YandexWebmasterRegion, ...] = (
    YandexWebmasterRegion(225, "Россия"),
    YandexWebmasterRegion(11235, "Алтайский край"),
    YandexWebmasterRegion(11375, "Амурская область"),
    YandexWebmasterRegion(10842, "Архангельская область"),
    YandexWebmasterRegion(10946, "Астраханская область"),
    YandexWebmasterRegion(10645, "Белгородская область"),
    YandexWebmasterRegion(10650, "Брянская область"),
    YandexWebmasterRegion(10658, "Владимирская область"),
    YandexWebmasterRegion(10950, "Волгоградская область"),
    YandexWebmasterRegion(10853, "Вологодская область"),
    YandexWebmasterRegion(10672, "Воронежская область"),
    YandexWebmasterRegion(10687, "Ивановская область"),
    YandexWebmasterRegion(11266, "Иркутская область"),
    YandexWebmasterRegion(11013, "Кабардино-Балкарская Республика"),
    YandexWebmasterRegion(10857, "Калининградская область"),
    YandexWebmasterRegion(11020, "Карачаево-Черкесская Республика"),
    YandexWebmasterRegion(11282, "Кемеровская область (Кузбасс)"),
    YandexWebmasterRegion(10699, "Костромская область"),
    YandexWebmasterRegion(10995, "Краснодарский край"),
    YandexWebmasterRegion(11309, "Красноярский край"),
    YandexWebmasterRegion(11158, "Курганская область"),
    YandexWebmasterRegion(10705, "Курская область"),
    YandexWebmasterRegion(10712, "Липецкая область"),
    YandexWebmasterRegion(1, "Москва и Московская область"),
    YandexWebmasterRegion(10897, "Мурманская область"),
    YandexWebmasterRegion(11079, "Нижегородская область"),
    YandexWebmasterRegion(10904, "Новгородская область"),
    YandexWebmasterRegion(11316, "Новосибирская область"),
    YandexWebmasterRegion(11318, "Омская область"),
    YandexWebmasterRegion(11084, "Оренбургская область"),
    YandexWebmasterRegion(10772, "Орловская область"),
    YandexWebmasterRegion(11095, "Пензенская область"),
    YandexWebmasterRegion(11108, "Пермский край"),
    YandexWebmasterRegion(11409, "Приморский край"),
    YandexWebmasterRegion(10926, "Псковская область"),
    YandexWebmasterRegion(11111, "Республика Башкортостан"),
    YandexWebmasterRegion(11010, "Республика Дагестан"),
    YandexWebmasterRegion(11012, "Республика Ингушетия"),
    YandexWebmasterRegion(11077, "Республика Марий Эл"),
    YandexWebmasterRegion(11117, "Республика Мордовия"),
    YandexWebmasterRegion(11021, "Республика Северная Осетия — Алания"),
    YandexWebmasterRegion(11119, "Республика Татарстан"),
    YandexWebmasterRegion(11029, "Ростовская область"),
    YandexWebmasterRegion(10776, "Рязанская область"),
    YandexWebmasterRegion(11131, "Самарская область"),
    YandexWebmasterRegion(10174, "Санкт-Петербург и Ленинградская область"),
    YandexWebmasterRegion(11162, "Свердловская область"),
    YandexWebmasterRegion(10795, "Смоленская область"),
    YandexWebmasterRegion(11069, "Ставропольский край"),
    YandexWebmasterRegion(10802, "Тамбовская область"),
    YandexWebmasterRegion(10819, "Тверская область"),
    YandexWebmasterRegion(11353, "Томская область"),
    YandexWebmasterRegion(10832, "Тульская область"),
    YandexWebmasterRegion(11153, "Ульяновская область"),
    YandexWebmasterRegion(11457, "Хабаровский край"),
    YandexWebmasterRegion(11193, "Ханты-Мансийский автономный округ - Югра"),
    YandexWebmasterRegion(11225, "Челябинская область"),
    YandexWebmasterRegion(11024, "Чеченская Республика"),
    YandexWebmasterRegion(11156, "Чувашская Республика"),
    YandexWebmasterRegion(10841, "Ярославская область"),
)
YANDEX_WEBMASTER_REGION_NAME_BY_ID = {item.region_id: item.region_name for item in YANDEX_WEBMASTER_DEFAULT_REGIONS}

__all__ = [
    "MOSCOW_TZ",
    "YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES",
    "YANDEX_WEBMASTER_DEFAULT_REGIONS",
    "YANDEX_WEBMASTER_REGION_NAME_BY_ID",
    "YandexWebmasterRegion",
]
