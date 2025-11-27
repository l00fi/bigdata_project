# flows/etl_flow.py
import os
from prefect import task, flow
from minio import Minio
import subprocess

from pyspark.sql import SparkSession

# --- Настройки ---
MINIO_URL = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"

# --- ЗАДАЧА 1: EXTRACT ---
# Загружает локальные файлы в MinIO
@task(name="Upload data to MinIO")
def upload_to_minio():
    """Сканирует локальную папку /data/raw и загружает файлы в MinIO."""
    print("Connecting to MinIO...")
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Создаем бакет, если его нет
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
    else:
        print(f"Bucket '{BUCKET_NAME}' already exists.")

    # Загружаем файлы
    raw_data_path = "/data/raw" # Путь внутри контейнера
    for filename in os.listdir(raw_data_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(raw_data_path, filename)
            client.fput_object(BUCKET_NAME, filename, file_path)
            print(f"File '{filename}' uploaded to MinIO.")
    return True

# --- ЗАДАЧA 2: TRANSFORM ---
# Запускает Spark-джоб для обработки данных
@task(name="Run Spark Job")
def run_spark_job(upload_success: bool):
    """Подключается к Spark Master и выполняет скрипт обработки."""
    if not upload_success:
        raise Exception("Data upload failed, aborting Spark job.")
        
    print("Connecting to Spark and running the job...")
    
    # Создаем Spark-сессию, которая подключается к нашему кластеру
    # Имя 'spark-master' - это имя сервиса из docker-compose.yml
    spark = SparkSession.builder \
        .appName("ETL-from-Prefect") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "prefect-server") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark Session created. Running the processing logic...")
    
    # --- ВАЖНО: Копируем логику из process_data.py прямо сюда ---
    # или импортируем ее, если вынесли в отдельный модуль.
    # Для простоты, скопируем.

    # 1. Чтение данных из MinIO
    df = spark.read.csv("s3a://raw-data/olist_orders_dataset.csv", header=True, inferSchema=True) # Пример
    print("Data read from MinIO successfully.")
    df.show(5)

    # 2. Пример обработки данных (Трансформация)
    processed_df = df.limit(1000) # Ваша логика обработки
    print("Data processed successfully.")
    processed_df.show(5)

    # 3. Запись данных в PostgreSQL (Загрузка)
    processed_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/project_db") \
        .option("dbtable", "processed_data") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
        
    print("Data written to PostgreSQL successfully.")
    
    # Останавливаем сессию
    spark.stop()
    
    return True


# --- ЗАДАЧА 3: LOAD (в нашем случае выполняется внутри Spark-джоба) ---
# Spark сам загрузит данные в PostgreSQL. Эта задача просто для логической структуры.
@task(name="Confirm Load")
def confirm_load(spark_success: bool):
    if not spark_success:
        raise Exception("Spark job failed, so data was not loaded.")
    print("Data successfully processed and loaded into PostgreSQL.")


# --- Главный Flow ---
# Собирает все задачи в один пайплайн
@flow(name="Big Data ETL Flow")
def big_data_etl_flow():
    upload_success = upload_to_minio()
    spark_success = run_spark_job(upload_success)
    confirm_load(spark_success)

# Запуск flow для развертывания
if __name__ == "__main__":
    big_data_etl_flow.serve(name="my-first-deployment")