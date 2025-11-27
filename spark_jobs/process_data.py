# spark_jobs/process_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# --- Настройки ---
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"
FILE_NAME = "your_data_file.csv" # !!! ЗАМЕНИТЕ НА ИМЯ ВАШЕГО ФАЙЛА

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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    print("Spark Session created successfully.")

    # 1. Чтение данных из MinIO
    df = spark.read.csv(f"s3a://{BUCKET_NAME}/{FILE_NAME}", header=True, inferSchema=True)
    print("Data read from MinIO successfully.")
    df.show(5)

    # 2. Пример обработки данных (Трансформация)
    # Замените это на свою логику анализа!
    # Например, посчитаем среднее значение какого-то столбца
    # 'some_numeric_column' и 'grouping_column' - замените на реальные названия
    # processed_df = df.groupBy("grouping_column").agg(avg("some_numeric_column").alias("avg_value"))
    processed_df = df.limit(1000) # Простой пример: берем первые 1000 строк

    print("Data processed successfully.")
    processed_df.show(5)

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