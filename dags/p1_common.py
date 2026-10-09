"""Общие задачи и цепочка пайплайна для DAG'ов weather_daily и weather_backfill.

Оба DAG'а выполняют одну и ту же цепочку, отличается только период:
    plan_chunks -> extract (по месяцам) -> transform (по месяцам) -> dq_staging
               -> spark_aggregate -> load_clickhouse -> dq_mart

Сама логика лежит в пакете p1 (src/p1). Здесь только «склейка» с Airflow,
поэтому DAG'и остаются тонкими, а логику легко покрыть pytest.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import task

from p1.config import Settings
from p1.dates import month_chunks, parse_date
from p1.spark import spark_submit_conf

# Настройки по умолчанию для всех задач
DEFAULT_ARGS = {
    "owner": "p1",
    "retries": 2,                          # упавшую задачу Airflow перезапустит до 2 раз
    "retry_delay": timedelta(minutes=1),   # пауза перед повтором
}


@task
def plan_chunks(start: str, end: str) -> list[dict]:
    """Разбивает период на месячные куски, на каждый кусок будет своя mapped-задача."""
    return [
        {"start": s.isoformat(), "end": e.isoformat()}
        for s, e in month_chunks(parse_date(start), parse_date(end))
    ]


# max_active_tis_per_dagrun=2: не больше двух кусков одновременно, бережём лимиты API
@task(max_active_tis_per_dagrun=2)
def extract(chunk: dict) -> dict:
    """Extract: API -> s3://datalake/raw/. Возвращает тот же кусок для следующей задачи."""
    from p1.open_meteo import extract_range

    extract_range(parse_date(chunk["start"]), parse_date(chunk["end"]))
    return chunk


@task
def transform(chunk: dict) -> dict:
    """Transform: raw (JSON) -> staging (Parquet по дням)."""
    from p1.transform import transform_range

    transform_range(parse_date(chunk["start"]), parse_date(chunk["end"]))
    return chunk


@task
def dq_staging(start: str, end: str) -> int:
    """DQ-проверки почасовых данных в staging; падение задачи останавливает пайплайн."""
    from p1.dq import run_staging_checks

    return run_staging_checks(parse_date(start), parse_date(end))


@task
def load_clickhouse(start: str, end: str) -> int:
    """Load: mart (Parquet в S3) -> ClickHouse weather.daily_city."""
    from p1.clickhouse import load_mart_range

    return load_mart_range(parse_date(start), parse_date(end))


@task
def dq_mart(start: str, end: str) -> dict:
    """DQ-проверки витрины в ClickHouse."""
    from p1.dq import run_mart_checks

    return run_mart_checks(parse_date(start), parse_date(end))


def build_pipeline(start, end) -> None:
    """Собирает цепочку задач внутри DAG'а.

    start/end - XComArg'и задачи, которая вычисляет период (у каждого DAG'а своя).
    """
    settings = Settings.from_env()

    # Dynamic task mapping: .expand() создаёт по задаче на каждый месячный кусок
    chunks = plan_chunks(start, end)
    extracted = extract.expand(chunk=chunks)
    transformed = transform.expand(chunk=extracted)

    checked = dq_staging(start, end)

    # Spark job запускается на кластере spark://spark-master:7077 (Connection spark_default).
    # Driver работает в контейнере airflow-scheduler (deploy-mode client),
    # executor'ы работают на spark-worker-1 и spark-worker-2.
    spark_aggregate = SparkSubmitOperator(
        task_id="spark_aggregate",
        conn_id="spark_default",
        application=os.path.join(settings.spark_jobs_dir, "weather_aggregates.py"),
        name="weather_aggregates",
        conf=spark_submit_conf(settings),
        # XComArg'и внутри списка Airflow подставит при запуске задачи
        application_args=["--start", start, "--end", end, "--bucket", settings.s3_bucket],
    )

    loaded = load_clickhouse(start, end)
    mart_checked = dq_mart(start, end)

    # Порядок выполнения: transform по всем кускам -> DQ -> Spark -> загрузка -> DQ витрины
    transformed >> checked >> spark_aggregate >> loaded >> mart_checked
