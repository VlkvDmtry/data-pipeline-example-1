# Образ Airflow для проекта P1.
# Базовый образ на Python 3.10: та же версия Python, что в образе Spark
# (иначе PySpark откажется запускать Python-код на executor'ах).
FROM apache/airflow:3.3.2-python3.10

# Системные пакеты ставим под root
USER root

# Java нужна для Spark driver: spark-submit запускает JVM внутри этого контейнера.
# --no-install-recommends не тянет лишнее, а очистка списков apt уменьшает размер слоя
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Путь к Java для spark-submit
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python-пакеты ставим под пользователем airflow (так требует образ)
USER airflow

# Копируем список зависимостей отдельным слоем, чтобы кэш сборки работал
COPY requirements.txt /requirements.txt

# Ставим зависимости. Явно фиксируем apache-airflow той же версии,
# чтобы pip случайно не обновил или не откатил сам Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
