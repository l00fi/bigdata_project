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
SPARK_MASTER_REST = "http://spark-master:6066/v1/submissions/create"

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

# Запускает Spark-джоб для обработки данных
@task(name="Run Spark Job")
def run_spark_job():
    # Отправка запроса в spark
    cmd = [
        "docker", "exec", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar,/opt/spark/jars/postgresql-42.7.2.jar",
        "/opt/spark/jobs/process_data.py"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    # Подобие лоирования
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    return result.check_returncode() == 0

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
    print("Checking/Uploading data to MinIO...")
    upload_to_minio()
    print("Submitting Spark job...")
    r = run_spark_job()
    print("Submission complete.")
    return confirm_load(r)

if __name__ == "__main__":
    big_data_etl_flow()