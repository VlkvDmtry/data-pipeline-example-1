"""P1: учебный data pipeline Open-Meteo -> S3 -> Spark -> ClickHouse.

Модули пакета:
    config      - настройки из переменных окружения и список городов
    dates       - работа с датами и разбиение периода на месячные куски
    s3          - чтение/запись объектов в MinIO и раскладка ключей по слоям
    open_meteo  - Extract: запросы к API и сохранение сырых ответов (raw)
    transform   - Transform: парсинг, очистка, Parquet в staging
    dq          - проверки качества данных (staging и витрина)
    clickhouse  - Load: загрузка витрины из S3 в ClickHouse
    spark       - настройки spark-submit для работы с MinIO через S3A
"""
