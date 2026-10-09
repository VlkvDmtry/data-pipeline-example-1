"""Transform: парсинг сырых ответов Open-Meteo, очистка и запись Parquet в staging."""

from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from p1 import s3
from p1.config import HOURLY_VARIABLES, City, Settings, load_cities

log = logging.getLogger(__name__)

# Порядок колонок в staging: сначала ключи и атрибуты города, затем метрики
KEY_COLUMNS = ["city", "ts"]
CITY_COLUMNS = ["city_name", "country", "latitude", "longitude"]
METRIC_COLUMNS = list(HOURLY_VARIABLES.values())
STAGING_COLUMNS = KEY_COLUMNS + CITY_COLUMNS + METRIC_COLUMNS
# Метрики в процентах: физически допустимы только значения 0..100
PERCENT_COLUMNS = ["humidity_pct", "cloud_cover_pct"]


def parse_response(payload: dict, city: City) -> pd.DataFrame:
    """Превращает JSON-ответ API в плоскую таблицу: одна строка = один город за один час.

    В ответе данные лежат «по колонкам»:
        {"hourly": {"time": [...], "temperature_2m": [...], ...}}
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    n = len(times)
    # Время в ответе в UTC без суффикса зоны, явно помечаем его как UTC
    df = pd.DataFrame({"ts": pd.to_datetime(times, utc=True)})
    for api_name, column in HOURLY_VARIABLES.items():
        # Если переменной нет в ответе, колонка заполняется пропусками.
        # to_numeric(errors="coerce") превращает null и мусор в NaN
        values = hourly.get(api_name, [None] * n)
        df[column] = pd.to_numeric(pd.Series(values, dtype="object"), errors="coerce").astype(
            "float64"
        )
    # Атрибуты города берём из нашего справочника, а не из ответа:
    # API возвращает координаты ближайшего узла сетки, а не исходные
    df["city"] = city.slug
    df["city_name"] = city.name
    df["country"] = city.country
    df["latitude"] = city.lat
    df["longitude"] = city.lon
    return df[STAGING_COLUMNS]


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка: убираем строки без ключей и дубли, исправляем известные артефакты, сортируем.

    Правим только задокументированные особенности источника (см. PERCENT_COLUMNS).
    Остальные выходы за физические диапазоны ловят DQ-проверки, а не тихо исправляет transform.
    """
    # Строки без города или времени бесполезны: их нельзя ни агрегировать, ни связать
    df = df.dropna(subset=KEY_COLUMNS)
    # Дубликат (город, час) может появиться при пересечении запросов; оставляем последний
    df = df.drop_duplicates(subset=KEY_COLUMNS, keep="last")
    # Известный артефакт модели: в тумане влажность бывает 101–102% (перенасыщение).
    # Обрезаем проценты до [0, 100] и пишем в лог, сколько значений поправили
    df = df.copy()
    for col in PERCENT_COLUMNS:
        fixed = int(((df[col] < 0) | (df[col] > 100)).sum())
        if fixed:
            log.info("clean: %s обрезано до [0, 100] у %d значений", col, fixed)
            df[col] = df[col].clip(lower=0, upper=100)
    # Стабильный порядок строк: удобно для чтения и сравнения файлов
    return df.sort_values(KEY_COLUMNS).reset_index(drop=True)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Сериализует DataFrame в Parquet (в памяти)."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    # Spark не читает метки времени с наносекундами, поэтому приводим их к микросекундам
    pq.write_table(table, buf, compression="snappy", coerce_timestamps="us")
    return buf.getvalue()


def split_by_day(df: pd.DataFrame, start: date, end: date) -> dict[date, pd.DataFrame]:
    """Режет таблицу на дни (UTC) в пределах [start, end]."""
    days = df["ts"].dt.date
    # Отбрасываем часы вне запрошенного периода (на всякий случай)
    df = df[(days >= start) & (days <= end)]
    return {day: part.reset_index(drop=True) for day, part in df.groupby(df["ts"].dt.date)}


def transform_range(
    start: date,
    end: date,
    settings: Settings | None = None,
    cities: list[City] | None = None,
) -> list[str]:
    """raw -> staging за период: читает JSON всех городов, чистит и пишет Parquet по дням.

    Каждый день перезаписывается целиком, поэтому повторный запуск идемпотентен.
    """
    settings = settings or Settings.from_env()
    cities = cities or load_cities(settings.cities_file)
    client = s3.get_client(settings)

    # 1. Читаем сырой ответ каждого города и парсим его в таблицу
    frames = []
    for city in cities:
        payload = s3.get_json(client, settings.s3_bucket, s3.raw_key(start, end, city.slug))
        frames.append(parse_response(payload, city))

    # 2. Объединяем города и чистим
    df = clean(pd.concat(frames, ignore_index=True))

    # 3. Пишем по одному Parquet-файлу на день: staging/weather_hourly/dt=YYYY-MM-DD/
    keys = []
    for day, part in split_by_day(df, start, end).items():
        key = s3.staging_key(day)
        s3.put_bytes(client, settings.s3_bucket, key, to_parquet_bytes(part))
        keys.append(key)
    log.info("staging: %d дней, %d строк за %s..%s", len(keys), len(df), start, end)
    return keys
