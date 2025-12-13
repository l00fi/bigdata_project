# ЭТАП 1: Берем готовый образ с Java 11 (Eclipse Temurin)
FROM eclipse-temurin:11-jre as java-base

# ЭТАП 2: Основной образ Prefect
FROM prefecthq/prefect:2-latest

# Переключаемся на root для установки
USER root

# --- Установка Java (копирование из этапа 1) ---
# Копируем папку с Java из первого образа
COPY --from=java-base /opt/java/openjdk /opt/java/openjdk

# Настраиваем переменные окружения, чтобы система видела Java
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Установка Spark ---
# Устанавливаем curl и procps (нужен для Spark)
RUN apt-get update && \
    apt-get install -y curl procps && \
    apt-get clean

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Скачиваем и распаковываем Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Настройка путей Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"

WORKDIR /opt/prefect

# --- Python зависимости ---
COPY requirements.txt .
RUN pip install -r requirements.txt