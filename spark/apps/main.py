import argparse
import json
import sys
import os 

from pyspark.sql.functions import col, avg
from pyspark.sql import SparkSession

from processor import DataProcessor

# --- Настройки ---
MINIO_URL = "http://" + os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Собираем JDBC URL динамически
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
POSTGRES_TABLE_POSTFIX = "_data"

DB_CONFIG = {
    "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:5432/{os.getenv('POSTGRES_DB')}",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

def main():
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", help="JSON list of files to process", required=True)
    args = parser.parse_args()

    # Десериализуем JSON обратно в список
    try:
        file_list = json.loads(args.files)
        print(f"Received files to process: {file_list}")
    except json.JSONDecodeError as e:
        print(f"Error parsing files JSON: {e}")
        sys.exit(1)

    # Инициалзизация Spark
    spark = SparkSession.builder \
        .appName("ETL-Project") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark Session created successfully.")

    processor = DataProcessor(spark, DB_CONFIG)

    bucket_path = f"s3a://{BUCKET_NAME}"

    for file in file_list:
        full_path = f"{bucket_path}/{file}"
        print(f"Processing: {full_path}")

        processor.process(full_path)
        # try:
        #     df = spark.read.csv(full_path, header=True, inferSchema=True)
        #     print("Data read from MinIO successfully.")

        #     # Пример обработки данных (Трансформация)
        #     # Заменить это на свою логику анализа!


        #     processed_df = df.limit(1000)

        #     # Запись данных в PostgreSQL (Загрузка)
        #     table_name = file.split('.')[0] + POSTGRES_TABLE_POSTFIX
        #     processed_df.write \
        #         .format("jdbc") \
        #         .option("url", POSTGRES_URL) \
        #         .option("dbtable", table_name) \
        #         .option("user", POSTGRES_USER) \
        #         .option("password", POSTGRES_PASSWORD) \
        #         .option("driver", "org.postgresql.Driver") \
        #         .mode("overwrite") \
        #         .save()

        #     print("Data written to PostgreSQL successfully.")
        # except:
        #     print(f"Failed to process {file}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()