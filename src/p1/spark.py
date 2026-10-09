"""Настройки spark-submit: подключение к кластеру и к MinIO через S3A."""

from __future__ import annotations

from p1.config import Settings

# Папка с jar'ами hadoop-aws и AWS SDK (общий том spark_jars во всех контейнерах)
EXTRA_JARS_DIR = "/opt/extra-jars"
# Имя хоста, где работает Spark driver (контейнер, в котором LocalExecutor выполняет задачи)
DRIVER_HOST = "airflow-scheduler"


def spark_submit_conf(settings: Settings) -> dict[str, str]:
    """Параметры --conf для spark-submit."""
    return {
        # --- Classpath: S3A-jar'ы уже лежат локально у driver'а и у executor'ов ---
        # (это быстрее, чем --jars: 640 МБ не гоняются по сети при каждом запуске)
        "spark.driver.extraClassPath": f"{EXTRA_JARS_DIR}/*",
        "spark.executor.extraClassPath": f"{EXTRA_JARS_DIR}/*",
        # --- Сеть: executor'ы на worker'ах должны достучаться до driver'а ---
        "spark.driver.host": DRIVER_HOST,
        "spark.driver.bindAddress": "0.0.0.0",
        # --- Ресурсы: по одному executor'у на worker (2 ядра, 1 ГБ), всего 4 ядра ---
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "2",
        "spark.cores.max": "4",
        # Данных немного: 16 shuffle-партиций вместо 200 по умолчанию
        "spark.sql.shuffle.partitions": "16",
        # --- S3A: файловая система s3a:// поверх MinIO ---
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint_url,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
        # MinIO работает с path-style адресами и без TLS
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }
