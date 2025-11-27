#!/bin/bash

# Выход из скрипта при любой ошибке
set -e

# 1. Запускаем сервер Prefect в фоновом режиме
echo "Starting Prefect server..."
prefect server start --host 0.0.0.0 &

# 2. Ждем, пока сервер не только запустится, но и станет "здоровым"
# Мы будем проверять API сервера, пока не получим успешный ответ
echo "Waiting for Prefect API to be ready..."
pip install httpx
until httpx get "http://localhost:4200/api/health" -s | grep "true"; do
  >&2 echo "Prefect server is unavailable - sleeping"
  sleep 1
done
>&2 echo "Prefect server is up - continuing..."

# 3. Теперь, когда сервер 100% готов, регистрируем наш пайплайн
echo "Building and applying deployment..."
prefect deployment build /opt/prefect/flows/etl_flow.py:big_data_etl_flow -n "Big Data ETL" --apply

# 4. Запускаем агента. Эта команда будет выполняться вечно, не давая контейнеру завершиться.
echo "Starting agent..."
prefect agent start -q 'default'