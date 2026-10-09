#!/usr/bin/env bash
# Запуск DAG'а изнутри контейнера Airflow.
# Отдельный shell-скрипт нужен, чтобы JSON с параметрами собирался внутри
# Linux-контейнера: PowerShell 5.1 портит кавычки при передаче JSON во внешние программы.
#
# Использование:
#   trigger_dag.sh weather_daily
#   trigger_dag.sh weather_backfill 2025-01-01 2025-01-31
set -euo pipefail   # падать при любой ошибке, неизвестной переменной или ошибке в пайпе

dag_id="$1"

if [ $# -ge 3 ]; then
  # Собираем conf для DAG'а с параметрами start_date / end_date
  conf=$(printf '{"start_date": "%s", "end_date": "%s"}' "$2" "$3")
  airflow dags trigger "$dag_id" --conf "$conf"
else
  # Запуск без параметров (для weather_daily)
  airflow dags trigger "$dag_id"
fi
