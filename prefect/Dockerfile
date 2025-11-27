# 1. Берем официальный образ Prefect за основу
FROM prefecthq/prefect:2-latest

# 2. Копируем файл с нашими зависимостями внутрь образа
COPY requirements.txt .

# 1. Устанавливаем конкретную версию OpenJDK, совместимую со Spark 3.5
RUN apt-get update && apt-get install -y openjdk-17-jre-headless unzip

# 2. Явно устанавливаем переменную окружения JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 3. Устанавливаем эти зависимости с помощью pip
RUN pip install -r requirements.txt

