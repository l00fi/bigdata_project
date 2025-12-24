import os
from typing import Dict, Callable, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp

class DataProcessor:
    """
    Класс инкапсулирует логику трансформации данных (ETL) для Olist E-commerce dataset.
    
    Реализует паттерн 'Стратегия' (Strategy) для выбора метода обработки 
    в зависимости от входящего файла.
    """

    def __init__(self, spark: SparkSession, db_config: Dict[str, str]) -> None:
        """
        Инициализация процессора.

        :param spark: Активная сессия Spark (SparkSession).
        :param db_config: Словарь с конфигурацией подключения к БД (url, user, password, etc.).
        """
        self.spark = spark
        self.db_config = db_config

    def process(self, file_path: str) -> None:
        """
        Основной метод-оркестратор обработки одного файла.
        
        1. Читает файл из S3 (MinIO).
        2. Определяет стратегию трансформации.
        3. Применяет трансформации.
        4. Записывает результат в PostgreSQL.

        :param file_path: Полный S3-путь к файлу (например, 's3a://bucket/data.csv').
        :raises Exception: Если возникает ошибка чтения или записи.
        """
        filename: str = os.path.basename(file_path)
        print(f"Processing file: {filename}")

        try:
            # Чтение данных из Data Lake
            df_raw: DataFrame = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Выбор стратегии трансформации
            # Тип transform_method: Функция, принимающая DataFrame и возвращающая DataFrame
            transform_method: Callable[[DataFrame], DataFrame] = self._get_transform_method(filename)
            
            df_transformed: DataFrame = transform_method(df_raw)
            
            # Генерация имени таблицы назначения
            table_name: str = self._get_clean_table_name(filename)
            
            # Загрузка в DWH
            self._write_to_postgres(df_transformed, table_name)
            print(f"Successfully processed {filename} -> table: {table_name}")
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            # Пробрасываем ошибку наверх, чтобы Prefect пометил задачу как Failed
            raise e

    def _get_transform_method(self, filename: str) -> Callable[[DataFrame], DataFrame]:
        """
        Фабричный метод для выбора логики обработки на основе имени файла.

        :param filename: Имя файла (например, 'olist_orders_dataset.csv').
        :return: Функция-трансформер, принимающая DataFrame и возвращающая DataFrame.
        """
        if "olist_orders_dataset" in filename:
            return self._process_orders
        elif "olist_order_items_dataset" in filename:
            return self._process_items
        elif "olist_customers_dataset" in filename:
            return self._process_customers
        elif "olist_products_dataset" in filename:
            return self._process_products
        elif "olist_order_payments_dataset" in filename:
            return self._process_payments
        else:
            # Fallback: Возвращаем лямбда-функцию, которая ничего не меняет (Identity)
            print(f"Warning: No specific processor found for {filename}, using identity transformation.")
            return lambda df: df

    def _get_clean_table_name(self, filename: str) -> str:
        """
        Генерирует имя таблицы для базы данных.
        
        Пример: 'olist_orders_dataset.csv' -> 'orders_data'

        :param filename: Исходное имя файла.
        :return: Очищенное имя таблицы.
        """
        # Удаляем префикс 'olist_' и суффикс '_dataset.csv'
        base_name = filename.replace("olist_", "").replace("_dataset.csv", "")
        # Добавляем постфикс из конфига (обычно '_data')
        return f"{base_name}{self.db_config.get('table_postfix', '_data')}"

    def _process_orders(self, df: DataFrame) -> DataFrame:
        """
        Бизнес-логика для таблицы заказов (orders).
        
        Трансформации:
        - Приведение строковых полей даты к типу Timestamp.
        - Добавление технического поля 'processed_at'.

        :param df: Исходный DataFrame.
        :return: Обработанный DataFrame.
        """
        return df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                 .withColumn("processed_at", current_timestamp())

    def _process_items(self, df: DataFrame) -> DataFrame:
        """
        Бизнес-логика для товарных позиций (items).
        
        Трансформации:
        - Фильтрация ошибочных записей (цена <= 0).

        :param df: Исходный DataFrame.
        :return: Обработанный DataFrame.
        """
        return df.filter(col("price") > 0)

    def _process_customers(self, df: DataFrame) -> DataFrame:
        """
        Бизнес-логика для клиентов (customers).
        
        Трансформации:
        - Дедупликация по ID клиента.

        :param df: Исходный DataFrame.
        :return: Обработанный DataFrame.
        """
        return df.dropDuplicates(["customer_id"])

    def _process_products(self, df: DataFrame) -> DataFrame:
        """
        Бизнес-логика для продуктов (products).
        
        Трансформации:
        - Заполнение NULL значений в категории.

        :param df: Исходный DataFrame.
        :return: Обработанный DataFrame.
        """
        return df.na.fill({"product_category_name": "unknown"})

    def _process_payments(self, df: DataFrame) -> DataFrame:
        """
        Бизнес-логика для платежей (payments).
        
        :param df: Исходный DataFrame.
        :return: Обработанный DataFrame.
        """
        # Пока возвращаем как есть, но структура готова для расширения
        return df

    def _write_to_postgres(self, df: DataFrame, table_name: str) -> None:
        """
        Запись DataFrame в PostgreSQL через JDBC драйвер.

        :param df: DataFrame для записи.
        :param table_name: Имя целевой таблицы в БД.
        """
        df.write \
            .format("jdbc") \
            .option("url", self.db_config['url']) \
            .option("dbtable", table_name) \
            .option("user", self.db_config['user']) \
            .option("password", self.db_config['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()