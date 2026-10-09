"""Тесты парсинга и очистки ответа Open-Meteo."""

import io
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

from p1.transform import (
    STAGING_COLUMNS,
    clean,
    parse_response,
    split_by_day,
    to_parquet_bytes,
)


def test_parse_response_shape_and_columns(sample_payload, moscow):
    df = parse_response(sample_payload, moscow)
    # 24 часа -> 24 строки, колонки в ожидаемом порядке
    assert len(df) == 24
    assert list(df.columns) == STAGING_COLUMNS
    # Атрибуты города берутся из справочника, а не из ответа API
    assert (df["city"] == "moscow").all()
    assert df["latitude"].iloc[0] == moscow.lat


def test_parse_response_types(sample_payload, moscow):
    df = parse_response(sample_payload, moscow)
    # Время размечено как UTC, метрики имеют тип float
    assert str(df["ts"].dt.tz) == "UTC"
    assert df["ts"].iloc[0] == pd.Timestamp("2026-10-07T00:00", tz="UTC")
    assert df["temperature_c"].dtype == "float64"
    assert df["temperature_c"].iloc[0] == 11.0


def test_parse_response_nulls_and_missing_variable(sample_payload, moscow):
    payload = sample_payload
    # null в данных -> NaN
    payload["hourly"]["temperature_2m"][0] = None
    # Переменной нет в ответе -> колонка целиком из NaN
    del payload["hourly"]["cloud_cover"]
    df = parse_response(payload, moscow)
    assert pd.isna(df["temperature_c"].iloc[0])
    assert df["cloud_cover_pct"].isna().all()


def test_parse_response_empty(moscow):
    df = parse_response({"hourly": {"time": []}}, moscow)
    assert df.empty
    assert list(df.columns) == STAGING_COLUMNS


def test_clean_removes_duplicates_and_null_keys(sample_payload, moscow, london):
    msk = parse_response(sample_payload, moscow)
    lon = parse_response(sample_payload, london)
    # Дублируем первые 3 часа Москвы и добавляем строку без времени
    broken = msk.head(1).copy()
    broken["ts"] = pd.NaT
    df = pd.concat([msk, msk.head(3), lon, broken], ignore_index=True)

    cleaned = clean(df)
    assert len(cleaned) == 48
    assert not cleaned.duplicated(subset=["city", "ts"]).any()
    assert cleaned["ts"].notna().all()
    # Строки отсортированы по (city, ts)
    assert cleaned["city"].iloc[0] == "london"


def test_clean_clips_percent_artifacts(sample_payload, moscow):
    df = parse_response(sample_payload, moscow)
    # Реальный артефакт Open-Meteo: влажность 102% в тумане (Лондон, январь 2025)
    df.loc[0, "humidity_pct"] = 102.0
    df.loc[1, "cloud_cover_pct"] = -1.0
    df.loc[2, "temperature_c"] = 75.0   # а это не артефакт: transform не трогает, ловит DQ
    cleaned = clean(df)
    assert cleaned["humidity_pct"].max() <= 100
    assert cleaned["cloud_cover_pct"].min() >= 0
    assert cleaned["temperature_c"].max() == 75.0


def test_split_by_day_filters_period(sample_payload, moscow):
    df = parse_response(sample_payload, moscow)
    parts = split_by_day(df, date(2026, 10, 7), date(2026, 10, 7))
    assert list(parts) == [date(2026, 10, 7)]
    # Пустой результат, если период не пересекается с данными
    assert split_by_day(df, date(2026, 10, 8), date(2026, 10, 9)) == {}


def test_parquet_roundtrip_uses_microseconds(sample_payload, moscow):
    df = parse_response(sample_payload, moscow)
    table = pq.read_table(io.BytesIO(to_parquet_bytes(df)))
    # Spark не читает наносекундные timestamp'ы, поэтому в файле должны быть микросекунды
    assert str(table.schema.field("ts").type) == "timestamp[us, tz=UTC]"
    assert table.num_rows == 24
