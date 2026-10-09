"""Load: загрузка витрины из S3 (слой mart) в ClickHouse."""

from __future__ import annotations

import logging
from datetime import date

import clickhouse_connect

from p1 import s3
from p1.config import Settings
from p1.dates import month_chunks

log = logging.getLogger(__name__)

# ClickHouse сам читает Parquet из MinIO табличной функцией s3() и вставляет строки.
# Данные идут напрямую MinIO -> ClickHouse, минуя Airflow.
#  - use_hive_partitioning=1 превращает папку dt=YYYY-MM-DD в колонку dt
#  - явные CAST приводят типы Spark к типам таблицы
#  - loaded_at заполняется DEFAULT now64(), это версия для ReplacingMergeTree
LOAD_SQL = """
INSERT INTO weather.daily_city
    (dt, city, city_name, country, latitude, longitude, hours_count,
     temp_min, temp_max, temp_avg, precip_sum, wind_max, humidity_avg,
     pressure_avg, cloud_cover_avg, temp_norm_30d, temp_anomaly, processed_at)
SELECT
    toDate(dt), city, city_name, country, latitude, longitude,
    toUInt8(hours_count),
    temp_min, temp_max, temp_avg, precip_sum, wind_max, humidity_avg,
    pressure_avg, cloud_cover_avg, temp_norm_30d, temp_anomaly,
    toDateTime64(processed_at, 3, 'UTC')
FROM s3({url:String}, {access_key:String}, {secret_key:String}, 'Parquet')
WHERE toDate(dt) BETWEEN {start:Date} AND {end:Date}
SETTINGS use_hive_partitioning = 1
"""


def get_client(settings: Settings):
    """HTTP-клиент ClickHouse."""
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def mart_glob_url(settings: Settings, month_start: date) -> str:
    """URL с маской на все дневные партиции месяца: .../dt=2025-01-*/*.parquet."""
    return (
        f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{s3.MART_PREFIX}"
        f"/dt={month_start:%Y-%m}-*/*.parquet"
    )


def load_mart_range(start: date, end: date, settings: Settings | None = None) -> int:
    """Загружает mart за период [start, end] в weather.daily_city. Возвращает число вставленных строк.

    Грузим по месяцам: маска dt=YYYY-MM-* ограничивает, какие файлы читает ClickHouse,
    а WHERE отсекает дни месяца за пределами периода.
    """
    settings = settings or Settings.from_env()
    client = get_client(settings)
    total = 0
    for chunk_start, chunk_end in month_chunks(start, end):
        summary = client.command(
            LOAD_SQL,
            parameters={
                "url": mart_glob_url(settings, chunk_start),
                "access_key": settings.s3_access_key,
                "secret_key": settings.s3_secret_key,
                "start": chunk_start,
                "end": chunk_end,
            },
        )
        # command() для INSERT возвращает QuerySummary со счётчиком записанных строк
        written = int(getattr(summary, "written_rows", 0) or 0)
        log.info("clickhouse: %s..%s вставлено %d строк", chunk_start, chunk_end, written)
        total += written
    return total
