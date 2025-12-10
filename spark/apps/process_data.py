import argparse
import json
import sys

from pyspark.sql.functions import col, avg
from pyspark.sql import SparkSession

# --- Настройки ---
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"
# FILE_NAME = "olist_orders_dataset.csv"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/project_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_TABLE_POSTFIX = "_data"

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

    bucket_path = f"s3a://{BUCKET_NAME}"

    for file in file_list:
        full_path = f"{bucket_path}/{file}"
        print(f"Processing: {full_path}")

        try:
            df = spark.read.csv(full_path, header=True, inferSchema=True)
            print("Data read from MinIO successfully.")
            df.show(5)

            # Пример обработки данных (Трансформация)
            # Заменить это на свою логику анализа!
            processed_df = df.limit(1000)

            # Запись данных в PostgreSQL (Загрузка)
            table_name = file.split('.')[0] + POSTGRES_TABLE_POSTFIX
            processed_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

            print("Data written to PostgreSQL successfully.")
        except:
            print(f"Failed to process {file}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()