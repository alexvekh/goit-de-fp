import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("Bronze to Silver") \
    .getOrCreate()

# Імена таблиць
tables = ["athlete_bio", "athlete_event_results"]

# Створення папки для silver-даних
os.makedirs("silver", exist_ok=True)

# Функція очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

# Spark UDF
clean_text_udf = udf(clean_text, StringType())

# Обробка кожної таблиці
for table_name in tables:
    print(f"\n    Processing table: {table_name}...")

    # Зчитування parquet з Bronze
    input_path = f"bronze/{table_name}"
    df = spark.read.parquet(input_path)

    # Обробка лише текстових колонок
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            print(f"        Cleaning column: {col_name}")
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Видалення дублікатів
    df = df.dropDuplicates()
    df.show()

    # Запис у Silver
    output_path = f"silver/{table_name}"
    print(f"    Writing cleaned data to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)

print("All tables successfully processed into Silver layer.")
spark.stop()
