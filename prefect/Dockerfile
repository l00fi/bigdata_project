# prefect/Dockerfile

FROM prefecthq/prefect:2-latest

# --------------------------------------------------------------------------------
# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y openjdk-21-jre-headless unzip && \
    pip install kaggle --no-cache-dir
# Устанавливаем переменную окружения JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 
# --------------------------------------------------------------------------------

RUN apt-get update && apt-get install -y docker.io

WORKDIR /opt/prefect

# Копируем и устанавливаем зависимости Python
COPY requirements.txt .
RUN pip install -r requirements.txt