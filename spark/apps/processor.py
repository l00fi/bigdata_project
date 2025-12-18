import os
import traceback
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

class DataProcessor:
    def __init__(self, spark, db_config):
        """
        :param spark: Активная SparkSession
        :param db_config: Словарь с настройками БД (url, user, password)
        """
        self.spark = spark
        self.db_config = db_config
        
        # Маршрутизация: Ключевое слово в имени файла -> Метод обработки
        self.mapping = {
            "orders": self._process_orders,
            "items": self._process_items,
            "customers": self._process_customers,
            "products": self._process_products,
            "payments": self._process_payments
            # Добавляй сюда новые файлы по мере необходимости
        }

    def process(self, file_path):
        """
        Главный метод. Определяет тип файла и запускает нужную логику.
        """
        filename = os.path.basename(file_path)
        print(f"--- [Processor] Received file: {filename} ---")

        try:
            # 1. Читаем файл (Raw Layer)
            # inferSchema=True дорогой, но для учебного проекта удобен.
            # В проде лучше задавать schema вручную.
            df_raw = self.spark.read.csv(file_path, header=True, inferSchema=True)

            # 2. Определяем, какой метод трансформации вызвать
            transform_method = self._get_transform_method(filename)
            
            if transform_method:
                # 3. Трансформация (Silver Layer)
                print(f"--- Applying specific logic for: {filename}")
                df_transformed = transform_method(df_raw)
                
                # 4. Запись (Gold Layer)
                # Генерируем имя таблицы из имени файла (убираем расширение и суффиксы)
                # Например: olist_orders_dataset.csv -> orders
                clean_name = self._get_clean_table_name(filename)
                self._write_to_postgres(df_transformed, clean_name)
                return True
            else:
                print(f"!!! No processing logic found for {filename}. Skipping.")
                return False

        except Exception as e:
            print(f"!!! Error processing {filename}: {e}")
            traceback.print_exc()
            return False

    def _get_transform_method(self, filename):
        """Ищет ключевое слово в имени файла и возвращает функцию"""
        for key, method in self.mapping.items():
            if key in filename:
                return method
        return None

    def _get_clean_table_name(self, filename):
        """Превращает 'olist_orders_dataset.csv' в 'orders_data'"""
        # Упрощенная логика: ищем ключевое слово из маппинга
        for key in self.mapping.keys():
            if key in filename:
                return key + "_data"
        return "unknown_data"

    # ==========================================
    # Специфичная логика для каждого файла
    # ==========================================

    def _process_orders(self, df):
        """Логика для заказов: работа с датами"""
        return df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")) \
                 .withColumn("order_approved_at", F.to_timestamp("order_approved_at")) \
                 .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date")) \
                 .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date")) \
                 .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))

    def _process_items(self, df):
        """Логика для товаров: работа с деньгами"""
        return df.withColumn("price", F.col("price").cast(DecimalType(10, 2))) \
                 .withColumn("freight_value", F.col("freight_value").cast(DecimalType(10, 2))) \
                 .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))

    def _process_customers(self, df):
        """Логика для клиентов: очистка строк, если нужно"""
        # Например, приводим город к нижнему регистру
        return df.withColumn("customer_city", F.lower(F.col("customer_city")))

    def _process_products(self, df):
        """Логика для продуктов"""
        # Заполняем пропуски в категориях
        return df.na.fill({"product_category_name": "unknown"})

    def _process_payments(self, df):
        """Логика для платежей"""
        return df.withColumn("payment_value", F.col("payment_value").cast(DecimalType(10, 2)))

    # ==========================================
    # Общие методы
    # ==========================================

    def _write_to_postgres(self, df, table_name):
        print(f"--- Writing to DB Table: {table_name} ---")
        df.write \
            .format("jdbc") \
            .option("url", self.db_config['url']) \
            .option("dbtable", table_name) \
            .option("user", self.db_config['user']) \
            .option("password", self.db_config['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"--- Written {df.count()} rows to {table_name} ---")