# Официальный образ Prefect
FROM prefecthq/prefect:2-latest

# Файл зависимостями
COPY requirements.txt .
RUN pip install -r requirements.txt

