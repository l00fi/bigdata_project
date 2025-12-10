FROM apache/spark:3.5.1
# Копируем скрипт обраотки данных
COPY ./spark/apps/process_data.py /opt/spark/jobs/process_data.py
# Кладём S3A драйверы
COPY ./spark/jars/*.jar /opt/spark/jars/
# Обновляем CLASSPATH для executors и drivers
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
