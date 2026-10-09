<#
.SYNOPSIS
    Команды управления проектом P1 (замена Makefile для Windows).

.EXAMPLE
    .\scripts\p1.ps1 up                              # поднять весь стек
    .\scripts\p1.ps1 status                          # состояние контейнеров и последние запуски DAG'ов
    .\scripts\p1.ps1 test                            # pytest в контейнере Airflow
    .\scripts\p1.ps1 backfill 2025-01-01 2025-01-31  # загрузить историю за период
    .\scripts\p1.ps1 daily                           # внеплановый запуск ежедневного DAG'а
    .\scripts\p1.ps1 runs weather_backfill           # список запусков DAG'а
    .\scripts\p1.ps1 ch "SELECT count() FROM weather.daily_city FINAL"
    .\scripts\p1.ps1 chfile clickhouse\queries\01_overview.sql
    .\scripts\p1.ps1 s3 staging                      # содержимое слоя в MinIO
    .\scripts\p1.ps1 logs airflow-scheduler          # логи сервиса
    .\scripts\p1.ps1 stop [сервис]                   # приостановить стек или сервис (контейнеры остаются)
    .\scripts\p1.ps1 start [сервис]                  # запустить ранее остановленные контейнеры
    .\scripts\p1.ps1 down                            # остановить стек и удалить контейнеры (тома сохраняются)
    .\scripts\p1.ps1 reset                           # остановить и УДАЛИТЬ все данные (тома)
#>
param(
    # Имя команды (первый аргумент)
    [Parameter(Position = 0)]
    [ValidateSet("up", "stop", "start", "down", "reset", "build", "status", "logs", "test",
                 "backfill", "daily", "runs", "ch", "chfile", "s3", "help")]
    [string]$Command = "help",

    # Остальные аргументы команды
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

# Любая ошибка PowerShell останавливает скрипт
$ErrorActionPreference = "Stop"
# Работаем из корня проекта, откуда бы ни запустили скрипт
Set-Location (Split-Path -Parent $PSScriptRoot)
# Вывод в UTF-8, чтобы кириллица из контейнеров отображалась корректно
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Читает значение переменной из .env (нужно для кредов ClickHouse и MinIO)
function Get-EnvValue([string]$Name) {
    $line = Get-Content .env | Where-Object { $_ -match "^$Name=" } | Select-Object -First 1
    return ($line -split "=", 2)[1]
}

# Выполняет команду внутри контейнера scheduler (там есть CLI Airflow и наш код)
function Invoke-Airflow([string[]]$Arguments) {
    docker compose exec -T airflow-scheduler @Arguments
}

# Передаёт SQL в clickhouse-client через stdin (так кавычки внутри SQL не ломаются)
function Invoke-ClickHouse([string]$Sql) {
    $user = Get-EnvValue "CLICKHOUSE_USER"
    $password = Get-EnvValue "CLICKHOUSE_PASSWORD"
    # --multiquery выполняет несколько запросов через ';', PrettyCompact даёт читаемые таблицы
    $Sql | docker compose exec -T clickhouse clickhouse-client --user $user --password $password `
        --multiquery --format PrettyCompact
}

switch ($Command) {
    "up" {
        # При первом запуске создаём .env из шаблона
        if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host "Создан .env из .env.example" }
        # Собираем образ Airflow (если изменился) и поднимаем все сервисы в фоне
        docker compose up -d --build
        Write-Host ""
        Write-Host "Airflow     http://localhost:8080"
        Write-Host "Spark UI    http://localhost:8081  (workers: 8082, 8083)"
        Write-Host "MinIO       http://localhost:9001"
        Write-Host "ClickHouse  http://localhost:8123/play"
    }
    "stop" {
        # Останавливаем контейнеры, не удаляя их; без аргументов - весь стек, иначе только указанные сервисы
        docker compose stop @Rest
    }
    "start" {
        # Запускаем ранее остановленные контейнеры (без пересборки образов)
        docker compose start @Rest
    }
    "down" {
        # Останавливаем и удаляем контейнеры; тома с данными остаются
        docker compose down
    }
    "reset" {
        # Удаляем контейнеры И тома: Postgres, MinIO, ClickHouse и скачанные jar'ы
        $answer = Read-Host "Удалить ВСЕ данные проекта (тома)? [y/N]"
        if ($answer -eq "y") { docker compose down -v } else { Write-Host "Отменено" }
    }
    "build" {
        # Пересобрать образ Airflow (после правки docker/airflow/*)
        docker compose build
    }
    "status" {
        # Состояние контейнеров (healthy / exited ...)
        docker compose ps --format "table {{.Service}}\t{{.State}}\t{{.Status}}"
        Write-Host "`n--- Последние запуски DAG'ов ---"
        foreach ($dag in @("weather_daily", "weather_backfill")) {
            # Вывод не обрезаем через Select-Object -First: это обрывает docker exec с кодом 255
            Invoke-Airflow @("airflow", "dags", "list-runs", $dag, "-o", "table")
        }
    }
    "logs" {
        # Логи сервиса (или всех сервисов), последние 200 строк, с продолжением в реальном времени
        docker compose logs -f --tail 200 @Rest
    }
    "test" {
        # pytest внутри контейнера Airflow: там те же версии библиотек, что в пайплайне
        Invoke-Airflow @("python", "-m", "pytest", "/opt/airflow/tests", "-q", "-p", "no:cacheprovider")
    }
    "backfill" {
        if ($Rest.Count -lt 2) { throw "Использование: p1.ps1 backfill <start YYYY-MM-DD> <end YYYY-MM-DD>" }
        # JSON с параметрами собирается внутри контейнера скриптом trigger_dag.sh
        Invoke-Airflow @("bash", "/opt/airflow/scripts/trigger_dag.sh", "weather_backfill", $Rest[0], $Rest[1])
    }
    "daily" {
        # Внеплановый запуск: обработает вчерашний день
        Invoke-Airflow @("bash", "/opt/airflow/scripts/trigger_dag.sh", "weather_daily")
    }
    "runs" {
        # Список запусков DAG'а с их статусами
        $dag = if ($Rest) { $Rest[0] } else { "weather_backfill" }
        Invoke-Airflow @("airflow", "dags", "list-runs", $dag, "-o", "table")
    }
    "ch" {
        # Произвольный SQL-запрос к ClickHouse
        if (-not $Rest) { throw "Использование: p1.ps1 ch ""SELECT ...""" }
        Invoke-ClickHouse ($Rest -join " ")
    }
    "chfile" {
        # SQL-файл (например, из clickhouse/queries) целиком
        if (-not $Rest) { throw "Использование: p1.ps1 chfile <путь к .sql>" }
        Invoke-ClickHouse (Get-Content -Raw -Encoding UTF8 $Rest[0])
    }
    "s3" {
        # Содержимое bucket (или его слоя) в MinIO: размер и число объектов, затем последние ключи
        $user = Get-EnvValue "MINIO_ROOT_USER"
        $password = Get-EnvValue "MINIO_ROOT_PASSWORD"
        $bucket = Get-EnvValue "S3_BUCKET"
        $prefix = if ($Rest) { $Rest[0] } else { "" }
        $script = "mc alias set local http://localhost:9000 $user $password >/dev/null && " +
                  "mc du --depth 2 local/$bucket/$prefix && mc ls -r local/$bucket/$prefix | tail -n 10"
        docker compose exec -T minio sh -c $script
    }
    default {
        # Справка: блок комментариев в начале файла
        Get-Help $PSCommandPath -Examples
    }
}
