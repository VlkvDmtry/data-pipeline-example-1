"""Работа с S3 (MinIO): клиент, чтение/запись объектов и раскладка ключей по слоям.

Слои Data Lake внутри bucket:
    raw/      - сырые ответы API как есть (JSON), источник истины для перезапуска
    staging/  - очищенные почасовые данные (Parquet), по файлу на день
    mart/     - дневные агрегаты от Spark (Parquet), источник для ClickHouse
"""

from __future__ import annotations

import json
from datetime import date

import boto3
from botocore.client import Config

from p1.config import Settings

# Префиксы слоёв (папки внутри bucket)
RAW_PREFIX = "raw/open_meteo"
STAGING_PREFIX = "staging/weather_hourly"
MART_PREFIX = "mart/weather_daily"


def raw_key(start: date, end: date, city_slug: str) -> str:
    """Ключ сырого ответа API: один файл = один город за период запроса."""
    return f"{RAW_PREFIX}/range={start:%Y-%m-%d}_{end:%Y-%m-%d}/city={city_slug}.json"


def staging_key(day: date) -> str:
    """Ключ почасовых данных за день (все города в одном файле).

    Папка dt=YYYY-MM-DD в стиле Hive: Spark распознаёт dt как колонку-партицию.
    """
    return f"{STAGING_PREFIX}/dt={day:%Y-%m-%d}/part-0.parquet"


def get_client(settings: Settings):
    """Создаёт boto3-клиент для MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        # MinIO не проверяет регион, но boto3 требует его указать
        region_name="us-east-1",
        # path-style адресация: http://minio:9000/bucket/key (а не bucket.minio:9000)
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def put_json(client, bucket: str, key: str, payload: dict) -> None:
    """Сохраняет dict как JSON-объект."""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def get_json(client, bucket: str, key: str) -> dict:
    """Читает JSON-объект и возвращает dict."""
    obj = client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def put_bytes(client, bucket: str, key: str, data: bytes) -> None:
    """Сохраняет произвольные байты (например, Parquet-файл)."""
    client.put_object(Bucket=bucket, Key=key, Body=data)


def get_bytes(client, bucket: str, key: str) -> bytes:
    """Читает объект целиком в память."""
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()
