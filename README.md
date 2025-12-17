# BigDataFlink

## Лабораторная работа №3

**Анализ больших данных — потоковая обработка данных с использованием Apache Flink, Apache Kafka и PostgreSQL**

Выполнил студент группы М8О-214СВ-24

Красавин М.А.

---

## Исходные данные

В качестве источника данных использовались CSV-файлы `MOCK_DATA*.csv`:

- всего **10 файлов**;
- по **1000 строк** в каждом;
- итоговый объём — **10000 записей**.

Каждая запись содержит информацию о продаже:
- покупатель;
- продавец;
- товар;
- магазин;
- поставщик;
- дата продажи;
- количество, цена, рейтинг и отзывы.

---

## Архитектура решения

Решение реализовано в виде потокового ETL-пайплайна и развернуто с использованием **Docker Compose**.

### Используемые компоненты

- **Apache Kafka** — брокер сообщений, источник streaming-данных;
- **Apache Flink** — потоковая обработка данных и трансформация в модель «звезда»;
- **PostgreSQL** — хранение аналитической модели данных;
- **Python (PyFlink)** — реализация producer и Flink-job.

### Общая схема

```
CSV files → Kafka → Apache Flink → PostgreSQL (Star Schema)
```

---

## Модель данных в PostgreSQL

Данные хранятся в схеме **`dw`** и реализованы в виде аналитической модели **«звезда»**.

### Таблица фактов

- `dw.fact_sales`
  - fact_key;
  - source_sale_id;
  - customer_key;
  - seller_key;
  - product_key;
  - store_key;
  - supplier_key;
  - date_key;
  - sale_quantity;
  - sale_total_price.

### Таблицы измерений

- `dw.dim_customer`;
- `dw.dim_seller`;
- `dw.dim_product`;
- `dw.dim_store`;
- `dw.dim_supplier`;
- `dw.dim_date`.

DDL-скрипт создания схемы и таблиц:
```
postgres/00_ddl_dw.sql
```

---

## Streaming ETL-пайплайн

### 1. Producer: CSV → Kafka

Python-приложение читает CSV-файлы, преобразует каждую строку в JSON и отправляет сообщения в Kafka-topic `sales`.

Файл:
```
producer/csv_to_kafka.py
```

Особенности:
- каждое сообщение Kafka соответствует одной строке CSV;
- используется формат JSON;
- всего отправляется **10000 сообщений**.

---

### 2. Streaming ETL: Kafka → PostgreSQL (Apache Flink)

Flink-job читает сообщения из Kafka в режиме streaming, выполняет трансформацию данных и сохраняет результат в модель «звезда».

Файл:
```
flink/stream_to_star.py
```

Основные этапы:
1. чтение JSON-сообщений из Kafka;
2. парсинг данных;
3. формирование таблиц измерений;
4. заполнение таблицы фактов `dw.fact_sales`;
5. идемпотентная загрузка по `source_*_id`.

---

## Запуск и тестирование

### 1. Запуск инфраструктуры
```bash
docker compose up -d --build
```

---

### 2. Инициализация PostgreSQL
```bash
Get-Content postgres/00_ddl_dw.sql | docker exec -i bigdataflink-postgres-1 psql -U bigdata -d bigdataFlink
```

Проверка таблиц:
```sql
\dt dw.*
```

---

### 3. Создание Kafka-topic
```bash
docker exec -it bigdataflink-kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic sales --partitions 1 --replication-factor 1
```

---

### 4. Запуск producer (CSV → Kafka)
```bash
docker compose up -d producer
docker logs -f bigdataflink-producer-1
```

Проверка количества сообщений:
```bash
docker exec -it bigdataflink-kafka-1 kafka-run-class \
  kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic sales --time -1
```

---

### 5. Запуск Flink-job
```bash
docker exec -it bigdataflink-flink-jobmanager-1 flink run \
  --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.2.0-1.18.jar \
  -py /flink/stream_to_star.py
```

Проверка состояния:
```bash
docker exec -it bigdataflink-flink-jobmanager-1 flink list
```

---

## Проверка результатов

Количество строк:
```sql
SELECT COUNT(*) FROM dw.fact_sales;
SELECT COUNT(*) FROM dw.dim_customer;
SELECT COUNT(*) FROM dw.dim_seller;
SELECT COUNT(*) FROM dw.dim_product;
SELECT COUNT(*) FROM dw.dim_store;
SELECT COUNT(*) FROM dw.dim_supplier;
SELECT COUNT(*) FROM dw.dim_date;
```

Проверка уникальности фактов:
```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT source_sale_id) AS distinct_ids
FROM dw.fact_sales;
```

Проверка ссылочной целостности:
```sql
SELECT
  SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) AS missing_customers,
  SUM(CASE WHEN s.seller_key IS NULL THEN 1 ELSE 0 END) AS missing_sellers,
  SUM(CASE WHEN p.product_key IS NULL THEN 1 ELSE 0 END) AS missing_products,
  SUM(CASE WHEN st.store_key IS NULL THEN 1 ELSE 0 END) AS missing_stores,
  SUM(CASE WHEN su.supplier_key IS NULL THEN 1 ELSE 0 END) AS missing_suppliers,
  SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS missing_dates
FROM dw.fact_sales f
LEFT JOIN dw.dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN dw.dim_seller   s ON s.seller_key   = f.seller_key
LEFT JOIN dw.dim_product  p ON p.product_key  = f.product_key
LEFT JOIN dw.dim_store    st ON st.store_key  = f.store_key
LEFT JOIN dw.dim_supplier su ON su.supplier_key = f.supplier_key
LEFT JOIN dw.dim_date     d ON d.date_key     = f.date_key;
```

Ожидаемый результат — все значения равны **0**.

---

## Используемые технологии

- Apache Flink 1.18;
- Apache Kafka;
- PostgreSQL;
- Python (PyFlink);
- Docker / Docker Compose.

---

## Итог

В ходе лабораторной работы был реализован потоковый ETL-пайплайн.  
Данные в режиме streaming передаются через Apache Kafka, обрабатываются Apache Flink и сохраняются в аналитическую модель «звезда» в PostgreSQL.  
Решение полностью соответствует требованиям лабораторной работы.
