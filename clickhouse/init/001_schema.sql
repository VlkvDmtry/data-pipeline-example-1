-- =====================================================================
-- Схема аналитической витрины. Выполняется один раз при первом старте
-- контейнера ClickHouse (пока том clickhouse_data пустой).
-- =====================================================================

-- База для всех таблиц проекта
CREATE DATABASE IF NOT EXISTS weather;

-- Витрина: дневные погодные показатели по городам.
-- Одна строка = один город за один день.
CREATE TABLE IF NOT EXISTS weather.daily_city
(
    dt               Date,                    -- дата (UTC)
    city             LowCardinality(String),  -- slug города, например moscow
    city_name        LowCardinality(String),  -- человекочитаемое название
    country          LowCardinality(String),  -- код страны ISO-2
    latitude         Float64,
    longitude        Float64,
    hours_count      UInt8,                   -- сколько почасовых замеров попало в агрегат (норма 24)
    temp_min         Float64,                 -- минимальная температура за день, °C
    temp_max         Float64,                 -- максимальная температура, °C
    temp_avg         Float64,                 -- средняя температура, °C
    precip_sum       Float64,                 -- сумма осадков, мм
    wind_max         Float64,                 -- максимальная скорость ветра, км/ч
    humidity_avg     Float64,                 -- средняя относительная влажность, %
    pressure_avg     Float64,                 -- среднее давление на уровне моря, гПа
    cloud_cover_avg  Float64,                 -- средняя облачность, %
    temp_norm_30d    Nullable(Float64),       -- «норма»: средняя temp_avg за предыдущие 30 дней
    temp_anomaly     Nullable(Float64),       -- отклонение temp_avg от нормы, °C
    processed_at     DateTime64(3, 'UTC'),    -- когда строку посчитал Spark
    loaded_at        DateTime64(3, 'UTC') DEFAULT now64(3)  -- когда строку загрузили в ClickHouse
)
-- ReplacingMergeTree схлопывает строки с одинаковым ключом сортировки (city, dt),
-- оставляя версию с максимальным loaded_at. Поэтому повторная загрузка того же
-- периода безопасна (идемпотентна). Схлопывание идёт фоново, при чтении используем FINAL.
ENGINE = ReplacingMergeTree(loaded_at)
-- Партиция = месяц: старые месяцы можно удалять или перезаливать целиком
PARTITION BY toYYYYMM(dt)
-- Ключ сортировки: он же ключ дедупликации и первичный индекс
ORDER BY (city, dt);
