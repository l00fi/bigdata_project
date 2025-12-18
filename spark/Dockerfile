FROM apache/spark:3.5.1
# Копируем скрипт обраотки данных
COPY ./spark/apps/ /opt/spark/jobs/
# Кладём S3A драйверы
COPY ./spark/jars/*.jar /opt/spark/jars/
# Обновляем CLASSPATH для executors и drivers
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
