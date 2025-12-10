FROM prefecthq/prefect:2-latest
# Это костыль - Docker-to-Docker
RUN apt-get update && apt-get install -y docker.io

WORKDIR /opt/prefect
# Копируем и устанавливаем зависимости Python
COPY requirements.txt .
RUN pip install -r requirements.txt