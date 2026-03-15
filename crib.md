# 1 Запуск всех сервисов в фоне
docker compose up -d

# 2 Проверить, что контейнеры живые
docker compose ps

# 3 Посмотреть логи (например Postgres)
docker compose logs -f postgres

# 4 Остановить контейнеры (данные в volumes сохранятся)
docker compose down

# 5 Остановить контейнеры (данные в volumes не сохранятся)
docker compose down -v