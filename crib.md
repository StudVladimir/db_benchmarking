# Docker

## Запуск и остановка
docker compose up -d

# 2 Проверить, что контейнеры живые
docker compose ps

# 3 Посмотреть логи (например Postgres)
docker compose logs -f postgres

# 4 Остановить контейнеры (данные в volumes сохранятся)
docker compose down

# 5 Остановить контейнеры (данные в volumes не сохранятся)
docker compose down -v

## Запуск только одного сервиса
docker compose up -d postgres
docker compose up -d mysql
docker compose up -d redis
docker compose up -d sqlite

# MySQL

## Проверка баз и таблиц
docker exec -it dbb-mysql mysql -uroot -proot -e "SHOW DATABASES;"
docker exec -it dbb-mysql mysql -uroot -proot -e "USE mydb; SHOW TABLES;"

## Удалить старую базу
docker exec -i dbb-mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS mydb;"

## Развернуть схему
Get-Content .\schema_myslq_new.sql | docker exec -i dbb-mysql mysql -uroot -proot

## Проверить, что схема создалась
docker exec -it dbb-mysql mysql -uroot -proot -e "SHOW DATABASES;"
docker exec -it dbb-mysql mysql -uroot -proot -e "USE mydb; SHOW TABLES;"

# PostgreSQL

## Проверка баз, схем и таблиц
docker exec -it dbb-postgres psql -U benchmark -d postgres -c "\l"
docker exec -it dbb-postgres psql -U benchmark -d benchmark -c "\dn"
docker exec -it dbb-postgres psql -U benchmark -d benchmark -c "\dt mydb.*"

## Удалить старую схему
docker exec -i dbb-postgres psql -U benchmark -d benchmark -c "DROP SCHEMA IF EXISTS mydb CASCADE;"

## Развернуть схему
Get-Content .\schema_postgres_new.sql | docker exec -i dbb-postgres psql -U benchmark -d benchmark

## Проверить, что схема создалась
docker exec -it dbb-postgres psql -U benchmark -d benchmark -c "\dn"
docker exec -it dbb-postgres psql -U benchmark -d benchmark -c "\dt mydb.*"

## Count по одной таблице
$q = @'
SELECT COUNT(*) AS row_count FROM mydb."User_Addresses";
'@
$q | docker exec -i dbb-postgres psql -U benchmark -d benchmark

# Redis

## Проверка keyspace
docker exec -it dbb-redis redis-cli INFO keyspace
docker exec -it dbb-redis redis-cli DBSIZE

## Полная очистка Redis
docker exec -i dbb-redis redis-cli FLUSHALL

# SQLite

## Развернуть схему
Get-Content .\shema_sqlite.sql | docker exec -i dbb-sqlite sqlite3 /data/benchmark.db

## Проверить таблицы
docker exec -i dbb-sqlite sqlite3 /data/benchmark.db ".tables"
docker exec -i dbb-sqlite sqlite3 /data/benchmark.db "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"

## Count по одной таблице
docker exec -i dbb-sqlite sqlite3 /data/benchmark.db "SELECT COUNT(*) FROM \"User_Addresses\";"

# Импорт CSV через скрипт

## По умолчанию: импорт в MySQL + PostgreSQL + SQLite из data_csv(third)
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1

## Использовать другую папку CSV
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1 -CsvFolder "data_csv(second)"

## Только MySQL
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1 -SkipPostgres -SkipSqlite

## Только PostgreSQL
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1 -SkipMySql -SkipSqlite

## Только SQLite
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1 -OnlySqlite -CsvFolder "data_csv(third)"

## Не очищать таблицы перед импортом
powershell -ExecutionPolicy Bypass -File .\import_csv_to_dbs.ps1 -ResetData:$false