# flows/etl_flow.py
import os
from prefect import task, flow
from minio import Minio
import subprocess
import json

# --- Настройки ---
MINIO_URL = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST", "spark-master")
SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT", "7077")

BUCKET_NAME = os.getenv("MINIO_BUCKET", "raw-data")
# SPARK_MASTER_REST = "http://spark-master:6066/v1/submissions/create"

# Загружает локальные файлы в MinIO
@task(name="Upload data to MinIO")
def upload_to_minio():
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

    # Список сырых данных
    raw_data_list = []
    
    # Загружаем файлы
    raw_data_path = "/data/raw" # Путь внутри контейнера
    for filename in os.listdir(raw_data_path):
        if filename.endswith(".csv"):
            raw_data_list.append(filename)
            file_path = os.path.join(raw_data_path, filename)
            client.fput_object(BUCKET_NAME, filename, file_path)
            print(f"File '{filename}' uploaded to MinIO.")

    return raw_data_list

@task(name="Run Spark Job")
def run_spark_job(data_list: list):
    raw_data_json = json.dumps(data_list)
    
    print("Preparing to submit Spark job via Client Mode...")

    # Путь к spark-submit внутри контейнера Prefect (мы его установили в Шаге 1)
    spark_submit_bin = "/opt/spark/bin/spark-submit"
    
    # Команда запуска
    cmd = [
        spark_submit_bin,
        "--master", f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}", # Подключение по сети к мастеру
        "--deploy-mode", "client", # Prefect - это драйвер, Worker - исполнитель
        "--name", "Prefect-ETL-Job",
        # Подключаем JARs, которые мы примонтировали в /opt/spark/jars
        "--jars", "/opt/extra-jars/hadoop-aws-3.3.4.jar,/opt/extra-jars/aws-java-sdk-bundle-1.12.517.jar,/opt/extra-jars/postgresql-42.7.2.jar",
        # Конфиги для S3 (Важно передать их драйверу здесь, если они не в spark-defaults.conf)
        "--conf", f"spark.hadoop.fs.s3a.endpoint=http://{MINIO_ENDPOINT}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        # Сам скрипт
        "/opt/spark/jobs/main.py", 
        # Аргументы скрипта
        "--files", raw_data_json     
    ]
    
    # Запуск
    env = os.environ.copy()
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    if result.returncode != 0:
        print("Spark Job Failed!")
        print("STDERR:", result.stderr)
        print("STDOUT:", result.stdout) # Иногда ошибки Spark пишет в stdout
        return False
    else:
        print("Spark Job Success!")
        # Spark в client mode пишет много логов в stdout/stderr, можно вывести часть
        print("Output snippet:", result.stderr[-500:]) 
        return True

# Spark сам загрузит данные в PostgreSQL. Эта задача просто для логической структуры.
@task(name="Confirm Load")
def confirm_load(spark_success: bool):
    if not spark_success:
        raise Exception("Spark job failed, so data was not loaded.")
    print("Data successfully processed and loaded into PostgreSQL.")

# Собирает все задачи в один пайплайн
@flow(name="Big Data ETL Flow")
def big_data_etl_flow():
    print("Checking/Uploading data to MinIO...")
    files = upload_to_minio()
    print("Submitting Spark job...")
    r = run_spark_job(files)
    print("Submission complete.")
    return confirm_load(r)

if __name__ == "__main__":
    big_data_etl_flow()