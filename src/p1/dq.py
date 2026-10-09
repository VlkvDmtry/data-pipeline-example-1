"""Проверки качества данных (Data Quality).

Две точки контроля:
    staging - после Python transform, до Spark: полнота и корректность почасовых данных
    mart    - после загрузки в ClickHouse: полнота и согласованность витрины
Если хоть одна проверка провалена, функция run_* бросает DataQualityError,
задача Airflow падает, а следующие шаги пайплайна не выполняются.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd

from p1 import s3
from p1.config import City, Settings, load_cities
from p1.dates import date_range

log = logging.getLogger(__name__)

# Допустимые физические диапазоны метрик (включительно)
RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-90.0, 60.0),
    "humidity_pct": (0.0, 100.0),
    "precipitation_mm": (0.0, 500.0),
    "wind_speed_kmh": (0.0, 500.0),
    "pressure_hpa": (850.0, 1100.0),
    "cloud_cover_pct": (0.0, 100.0),
}
# Максимально допустимая доля пропусков в каждой метрике за день
MAX_NULL_SHARE = 0.05
# Ожидаемое число почасовых замеров на город в сутки
HOURS_PER_DAY = 24


class DataQualityError(Exception):
    """Данные не прошли проверки качества."""


def check_staging_frame(df: pd.DataFrame, expected_cities: set[str], day: date) -> list[str]:
    """Проверяет почасовые данные за один день. Возвращает список ошибок (пустой = всё ок)."""
    errors: list[str] = []
    prefix = f"[{day}]"

    # 1. Данные вообще есть
    if df.empty:
        return [f"{prefix} нет данных"]

    # 2. В ключевых колонках нет пропусков
    for col in ("city", "ts"):
        nulls = int(df[col].isna().sum())
        if nulls:
            errors.append(f"{prefix} {nulls} пропусков в ключевой колонке {col}")

    # 3. Нет дублей (город, час)
    dups = int(df.duplicated(subset=["city", "ts"]).sum())
    if dups:
        errors.append(f"{prefix} {dups} дублей по (city, ts)")

    # 4. Присутствуют все города из справочника
    missing = expected_cities - set(df["city"].dropna())
    if missing:
        errors.append(f"{prefix} нет городов: {sorted(missing)}")

    # 5. У каждого города ровно 24 часа
    hours = df.groupby("city")["ts"].nunique()
    incomplete = hours[hours != HOURS_PER_DAY]
    if not incomplete.empty:
        errors.append(f"{prefix} не 24 часа у городов: {incomplete.to_dict()}")

    # 6. Пропусков в метриках не больше порога, значения в физических диапазонах
    for col, (low, high) in RANGES.items():
        if col not in df.columns:
            errors.append(f"{prefix} нет колонки {col}")
            continue
        null_share = float(df[col].isna().mean())
        if null_share > MAX_NULL_SHARE:
            errors.append(f"{prefix} {col}: доля пропусков {null_share:.1%} > {MAX_NULL_SHARE:.0%}")
        # NaN в сравнении даёт False, поэтому пропуски здесь не считаются нарушениями
        out_of_range = int(((df[col] < low) | (df[col] > high)).sum())
        if out_of_range:
            errors.append(f"{prefix} {col}: {out_of_range} значений вне [{low}, {high}]")

    return errors


def run_staging_checks(
    start: date,
    end: date,
    settings: Settings | None = None,
    cities: list[City] | None = None,
) -> int:
    """Проверяет staging за каждый день периода. Возвращает число проверенных строк."""
    settings = settings or Settings.from_env()
    cities = cities or load_cities(settings.cities_file)
    client = s3.get_client(settings)
    expected = {c.slug for c in cities}

    errors: list[str] = []
    total_rows = 0
    for day in date_range(start, end):
        key = s3.staging_key(day)
        try:
            data = s3.get_bytes(client, settings.s3_bucket, key)
        except client.exceptions.NoSuchKey:
            errors.append(f"[{day}] нет файла {key}")
            continue
        df = pd.read_parquet(io.BytesIO(data))
        total_rows += len(df)
        errors.extend(check_staging_frame(df, expected, day))

    if errors:
        # Показываем первые 50 ошибок, чтобы лог оставался читаемым
        raise DataQualityError(
            f"staging DQ: {len(errors)} ошибок\n" + "\n".join(errors[:50])
        )
    log.info("staging DQ OK: %s..%s, %d строк", start, end, total_rows)
    return total_rows


# Агрегаты витрины за период одним запросом.
# FINAL заставляет ClickHouse схлопнуть дубли ReplacingMergeTree на лету.
MART_STATS_SQL = """
SELECT
    count()                                        AS rows,
    uniqExact(city)                                AS cities,
    uniqExact(dt)                                  AS days,
    countIf(hours_count != 24)                     AS incomplete_days,
    countIf(NOT (temp_min <= temp_avg AND temp_avg <= temp_max)) AS bad_temp_order,
    countIf(isNaN(temp_avg))                       AS nan_temp
FROM weather.daily_city FINAL
WHERE dt BETWEEN {start:Date} AND {end:Date}
"""


def check_mart_stats(stats: dict, n_cities: int, n_days: int) -> list[str]:
    """Проверяет агрегаты витрины. Возвращает список ошибок."""
    errors = []
    expected_rows = n_cities * n_days
    # Полнота: каждая пара (город, день) присутствует ровно один раз
    if stats["rows"] != expected_rows:
        errors.append(f"строк {stats['rows']}, ожидалось {expected_rows} ({n_cities} × {n_days})")
    if stats["cities"] != n_cities:
        errors.append(f"городов {stats['cities']}, ожидалось {n_cities}")
    if stats["days"] != n_days:
        errors.append(f"дней {stats['days']}, ожидалось {n_days}")
    # Согласованность: агрегаты посчитаны по полным суткам, min <= avg <= max
    if stats["incomplete_days"]:
        errors.append(f"{stats['incomplete_days']} строк с hours_count != 24")
    if stats["bad_temp_order"]:
        errors.append(f"{stats['bad_temp_order']} строк, где не выполняется temp_min <= temp_avg <= temp_max")
    if stats["nan_temp"]:
        errors.append(f"{stats['nan_temp']} строк с NaN в temp_avg")
    return errors


def run_mart_checks(
    start: date,
    end: date,
    settings: Settings | None = None,
    cities: list[City] | None = None,
) -> dict:
    """Проверяет витрину ClickHouse за период. Возвращает собранную статистику."""
    # Импорт внутри функции: модуль dq можно использовать и без clickhouse-connect
    from p1.clickhouse import get_client

    settings = settings or Settings.from_env()
    cities = cities or load_cities(settings.cities_file)
    client = get_client(settings)
    # Серверные параметры {start:Date} подставляет сам ClickHouse, без склейки строк
    result = client.query(MART_STATS_SQL, parameters={"start": start, "end": end})
    stats = dict(zip(result.column_names, result.first_row))

    errors = check_mart_stats(stats, len(cities), len(date_range(start, end)))
    if errors:
        raise DataQualityError("mart DQ: " + "; ".join(errors))
    log.info("mart DQ OK: %s..%s, %s", start, end, stats)
    return stats
