# 1. Берем официальный образ Prefect за основу
FROM prefecthq/prefect:2-latest

# 2. Копируем файл с нашими зависимостями внутрь образа
COPY requirements.txt .

# 3. Устанавливаем эти зависимости с помощью pip
RUN pip install -r requirements.txt

