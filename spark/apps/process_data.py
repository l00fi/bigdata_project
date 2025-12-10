# spark_jobs/process_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# --- Настройки ---
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"
FILE_NAME = "olist_orders_dataset.csv"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/project_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_TABLE = "processed_data"

def main():
    spark = SparkSession.builder \
        .appName("ETL-Project") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark Session created successfully.")

    # 1. Чтение данных из MinIO
    df = spark.read.csv(f"s3a://{BUCKET_NAME}/{FILE_NAME}", header=True, inferSchema=True)
    print("Data read from MinIO successfully.")
    df.show(5)

    # 2. Пример обработки данных (Трансформация)
    # Заменить это на свою логику анализа!
    processed_df = df.limit(1000)

    print("Data processed successfully.")

    # 3. Запись данных в PostgreSQL (Загрузка)
    processed_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Data written to PostgreSQL successfully.")

    spark.stop()

if __name__ == "__main__":
    main()