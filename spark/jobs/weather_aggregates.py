"""Spark job: дневные агрегаты погоды и температурные аномалии.

staging (почасовые данные, Parquet)  ->  mart (дневные агрегаты, Parquet)

Запуск (это делает Airflow через SparkSubmitOperator):
    spark-submit --master spark://spark-master:7077 weather_aggregates.py \
        --start 2025-01-01 --end 2025-01-31 --bucket datalake

Логика:
  1. Читаем staging за [start - 30 дней, end]. 30 дней истории нужны,
     чтобы посчитать «норму» для первых дней периода.
  2. Агрегируем часы в дни по каждому городу.
  3. Оконной функцией считаем норму: среднюю температуру за 30 предыдущих дней.
  4. Пишем в mart только дни из [start, end] с динамической перезаписью партиций:
     перезаписываются только эти дни, остальные остаются нетронутыми.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Глубина истории для расчёта нормы, в днях
NORM_WINDOW_DAYS = 30
# Минимум дней с данными в окне: по более короткой истории норма ненадёжна (будет NULL)
NORM_MIN_DAYS = 25


def parse_args() -> argparse.Namespace:
    """Аргументы командной строки job'а."""
    parser = argparse.ArgumentParser(description="Дневные агрегаты погоды")
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="datalake")
    parser.add_argument("--staging-prefix", default="staging/weather_hourly")
    parser.add_argument("--mart-prefix", default="mart/weather_daily")
    return parser.parse_args()


def read_staging(spark: SparkSession, path: str, start: date, end: date) -> DataFrame:
    """Читает почасовые данные за период вместе с историей для нормы."""
    history_start = start - timedelta(days=NORM_WINDOW_DAYS)
    return (
        spark.read
        # basePath говорит Spark, что dt=... в пути - это колонка-партиция
        .option("basePath", path)
        .parquet(path)
        # Фильтр по партиции: Spark читает только нужные папки dt=... (partition pruning)
        .where(F.col("dt").between(F.lit(history_start), F.lit(end)))
    )


def daily_aggregates(hourly: DataFrame) -> DataFrame:
    """Часы -> дни: одна строка на (город, день)."""
    return (
        hourly
        # Группируем по дню и городу; атрибуты города одинаковы внутри группы
        .groupBy("dt", "city", "city_name", "country", "latitude", "longitude")
        .agg(
            F.count("*").alias("hours_count"),                       # сколько часов в дне
            F.min("temperature_c").alias("temp_min"),                # минимум температуры
            F.max("temperature_c").alias("temp_max"),                # максимум
            F.avg("temperature_c").alias("temp_avg"),                # среднее
            F.sum("precipitation_mm").alias("precip_sum"),           # сумма осадков
            F.max("wind_speed_kmh").alias("wind_max"),               # самый сильный ветер
            F.avg("humidity_pct").alias("humidity_avg"),             # средняя влажность
            F.avg("pressure_hpa").alias("pressure_avg"),             # среднее давление
            F.avg("cloud_cover_pct").alias("cloud_cover_avg"),       # средняя облачность
        )
    )


def add_anomalies(daily: DataFrame) -> DataFrame:
    """Добавляет норму за 30 предыдущих дней и отклонение от неё."""
    # Окно по каждому городу, упорядоченное по номеру дня (unix_date = дни с 1970-01-01).
    # rangeBetween(-30, -1): 30 предыдущих дней по календарю, без текущего.
    # Если каких-то дней нет, окно не «уезжает», в отличие от rowsBetween.
    norm_window = (
        Window.partitionBy("city")
        .orderBy(F.unix_date("dt"))
        .rangeBetween(-NORM_WINDOW_DAYS, -1)
    )
    return (
        daily
        # Сколько дней с данными реально попало в окно (в начале истории их меньше 30)
        .withColumn("norm_days", F.count("temp_avg").over(norm_window))
        # Норма считается, только если истории достаточно, иначе NULL
        .withColumn(
            "temp_norm_30d",
            F.when(F.col("norm_days") >= NORM_MIN_DAYS, F.avg("temp_avg").over(norm_window)),
        )
        # NULL в норме даёт NULL и в аномалии
        .withColumn("temp_anomaly", F.col("temp_avg") - F.col("temp_norm_30d"))
        # Вспомогательная колонка в витрину не идёт
        .drop("norm_days")
    )


def round_metrics(df: DataFrame, columns: list[str], scale: int = 2) -> DataFrame:
    """Округляет дробные метрики, чтобы в витрине не было хвостов вроде 12.300000001."""
    for col in columns:
        df = df.withColumn(col, F.round(col, scale))
    return df


def main() -> None:
    args = parse_args()
    staging_path = f"s3a://{args.bucket}/{args.staging_prefix}"
    mart_path = f"s3a://{args.bucket}/{args.mart_prefix}"

    # SparkSession: точка входа. Мастер и ресурсы задаёт spark-submit
    spark = SparkSession.builder.appName(
        f"weather_aggregates {args.start}..{args.end}"
    ).getOrCreate()
    # Перезаписывать только те партиции dt, которые есть в результате
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Метки времени пишем как TIMESTAMP_MICROS (а не устаревший INT96), чтобы их понимал ClickHouse
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    # 1-3. Чтение, дневные агрегаты, нормы и аномалии
    hourly = read_staging(spark, staging_path, args.start, args.end)
    daily = add_anomalies(daily_aggregates(hourly))

    # 4. Оставляем только целевой период (история нужна была лишь для окна)
    result = (
        daily
        .where(F.col("dt").between(F.lit(args.start), F.lit(args.end)))
        .transform(lambda df: round_metrics(df, [
            "temp_min", "temp_max", "temp_avg", "precip_sum", "wind_max",
            "humidity_avg", "pressure_avg", "cloud_cover_avg",
            "temp_norm_30d", "temp_anomaly",
        ]))
        # Техническая метка: когда строка была посчитана
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        result
        # Одна shuffle-партиция на день -> один файл на папку dt=...
        .repartition("dt")
        .write
        .mode("overwrite")         # при dynamic-режиме перезаписываются только затронутые dt
        .partitionBy("dt")         # раскладка mart/weather_daily/dt=YYYY-MM-DD/
        .parquet(mart_path)
    )

    # Короткая сводка в лог задачи Airflow
    rows = spark.read.parquet(mart_path).where(
        F.col("dt").between(F.lit(args.start), F.lit(args.end))
    ).count()
    print(f"mart: записано {rows} строк за {args.start}..{args.end} в {mart_path}")

    spark.stop()


if __name__ == "__main__":
    main()
