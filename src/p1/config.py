"""Конфигурация проекта: настройки из переменных окружения и список городов."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

# Почасовые переменные Open-Meteo и имена колонок, под которыми они лежат у нас.
# Ключ - имя в API, значение - имя колонки в staging (с единицей измерения в названии).
HOURLY_VARIABLES: dict[str, str] = {
    "temperature_2m": "temperature_c",        # температура на высоте 2 м, °C
    "relative_humidity_2m": "humidity_pct",   # относительная влажность, %
    "precipitation": "precipitation_mm",      # осадки за час, мм
    "wind_speed_10m": "wind_speed_kmh",       # скорость ветра на высоте 10 м, км/ч
    "pressure_msl": "pressure_hpa",           # давление на уровне моря, гПа
    "cloud_cover": "cloud_cover_pct",         # облачность, %
}

# Historical Forecast API: архив прогнозов с 2022 года почти до сегодняшнего дня.
# Один и тот же источник подходит и для ежедневной загрузки, и для backfill.
OPEN_METEO_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"


@dataclass(frozen=True)
class City:
    """Город из config/cities.yaml."""

    slug: str      # ключ города в данных и путях S3
    name: str      # человекочитаемое название
    country: str   # код страны ISO-2
    lat: float     # широта
    lon: float     # долгота


@dataclass(frozen=True)
class Settings:
    """Все внешние настройки пайплайна (адреса сервисов, креды, пути)."""

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    cities_file: str
    spark_jobs_dir: str
    open_meteo_url: str = OPEN_METEO_URL

    @classmethod
    def from_env(cls) -> "Settings":
        """Собирает настройки из переменных окружения (их задаёт docker-compose и .env)."""
        env = os.environ.get
        return cls(
            s3_endpoint_url=env("S3_ENDPOINT_URL", "http://minio:9000"),
            # Для MinIO ключи доступа совпадают с логином и паролем root-пользователя
            s3_access_key=env("S3_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
            s3_secret_key=env("S3_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin123")),
            s3_bucket=env("S3_BUCKET", "datalake"),
            clickhouse_host=env("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(env("CLICKHOUSE_PORT", "8123")),
            clickhouse_user=env("CLICKHOUSE_USER", "default"),
            clickhouse_password=env("CLICKHOUSE_PASSWORD", ""),
            cities_file=env("P1_CITIES_FILE", "/opt/airflow/config/cities.yaml"),
            spark_jobs_dir=env("P1_SPARK_JOBS_DIR", "/opt/airflow/spark_jobs"),
            open_meteo_url=env("OPEN_METEO_URL", OPEN_METEO_URL),
        )


def load_cities(path: str | Path) -> list[City]:
    """Читает список городов из YAML-файла."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    # Каждая запись YAML превращается в неизменяемый объект City
    return [
        City(
            slug=item["slug"],
            name=item["name"],
            country=item["country"],
            lat=float(item["lat"]),
            lon=float(item["lon"]),
        )
        for item in data["cities"]
    ]
