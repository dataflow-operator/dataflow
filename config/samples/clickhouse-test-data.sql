-- 1. Очистка (на случай повторного запуска) и создание таблицы
DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id UInt64,
    name String,
    category String,
    price Decimal(10, 2),
    stock_quantity Int32 DEFAULT 0,
    sku String,
    is_available UInt8 DEFAULT 1,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, created_at);

-- 2. Вставка 100 записей
-- В ClickHouse numbers(n) возвращает колонку number (0..n-1), используем подзапрос для 1..100
INSERT INTO products (name, category, price, stock_quantity, sku, created_at, updated_at)
SELECT
    'Товар ' || toString(i),
    multiIf(i % 5 = 0, 'Электроника', i % 5 = 1, 'Одежда', i % 5 = 2, 'Дом', i % 5 = 3, 'Спорт', 'Книги'),
    round(rand() * 5000 + 100, 2),
    toInt32(floor(rand() * 1000)),
    'ART-' || toString(round(rand() * 5000 + 100)),
    now(),
    now()
FROM (SELECT number + 1 AS i FROM numbers(100));

-- 3. Обновление 17 случайных записей (ClickHouse mutations)
-- Сначала получаем ID через подзапрос (mutations выполняются асинхронно)
ALTER TABLE products UPDATE
    price = round(price * 1.3, 2),
    stock_quantity = greatest(0, stock_quantity - toInt32(floor(rand() * 10))),
    updated_at = now()
WHERE id IN (
    SELECT id FROM products ORDER BY rand() LIMIT 17
);

-- Дождаться завершения мутации (опционально, для проверки)
-- SELECT * FROM system.mutations WHERE table = 'products' AND is_done = 0;

-- 4. Проверка результатов (обновлённые записи)
SELECT id, name, price, stock_quantity, created_at, updated_at
FROM products
WHERE updated_at > created_at
ORDER BY price DESC
LIMIT 50;

-- 4. Проверка products_clone (если dataflow уже скопировал данные)
-- Примечание: dataflow по умолчанию создаёт таблицу с колонками (data String, created_at).
-- Для такой же схемы, как products, создайте products_clone вручную или настройте трансформации.
SELECT id, name, price, stock_quantity, created_at, updated_at
FROM products_clone
WHERE updated_at > created_at
ORDER BY price DESC
LIMIT 50;

-- 4. Проверка products_raw_clone (raw-режим с _metadata)
SELECT id, value, _metadata, created_at, updated_at, deleted_at
FROM products_raw_clone
WHERE updated_at > created_at
ORDER BY id DESC
LIMIT 50;

-- Подсчёт записей
SELECT count(*) FROM products;
SELECT count(*) FROM products_clone;
SELECT count(*) FROM products_raw_clone;

-- 5. Удаление дубликатов в products_clone, оставляя последнюю запись по id
-- В ClickHouse нет ctid; используем ReplacingMergeTree или пересоздание таблицы

-- Вариант A: Если products_clone — ReplacingMergeTree(updated_at), достаточно:
-- OPTIMIZE TABLE products_clone FINAL;

-- Вариант B: Пересоздание таблицы с дедупликацией через argMax
DROP TABLE IF EXISTS products_clone_dedup;
CREATE TABLE products_clone_dedup AS products_clone;

INSERT INTO products_clone_dedup
SELECT
    id,
    argMax(name, updated_at) AS name,
    argMax(category, updated_at) AS category,
    argMax(price, updated_at) AS price,
    argMax(stock_quantity, updated_at) AS stock_quantity,
    argMax(sku, updated_at) AS sku,
    argMax(is_available, updated_at) AS is_available,
    argMax(created_at, updated_at) AS created_at,
    argMax(updated_at, updated_at) AS updated_at
FROM products_clone
GROUP BY id;

RENAME TABLE products_clone TO products_clone_old;
RENAME TABLE products_clone_dedup TO products_clone;
DROP TABLE products_clone_old;
