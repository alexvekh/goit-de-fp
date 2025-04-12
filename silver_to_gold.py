import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, current_timestamp

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("Silver to Gold") \
    .getOrCreate()

# Створення gold-папки, якщо не існує
os.makedirs("gold", exist_ok=True)

# Зчитування Silver-даних
bio_df = spark.read.parquet("silver/athlete_bio")
results_df = spark.read.parquet("silver/athlete_event_results")
bio_df = bio_df.drop("country_noc")

# Джойн по athlete_id
print("  Join tables ...")
df = bio_df.join(results_df, on="athlete_id", how="inner")

# Приведення weight та height до типу Float (на випадок, якщо вони string)
print("  Changing column types to float ...")

df = df.withColumn("weight", col("weight").cast("float"))
df = df.withColumn("height", col("height").cast("float"))
df = df.filter(col("height").isNotNull() & col("weight").isNotNull())
# df.select("sport", "sex", "weight", "height").distinct().sort("weight", "height").show()
# df.printSchema()

# Групування та обчислення середніх значень
print("  Aggregation and averaging ...")
grouped_df = df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        round(avg("weight"), 2).alias("avg_weight"),
        round(avg("height"), 2).alias("avg_height")
    )

# Додавання timestamp
print("  Adding timestamp ...")
final_df = grouped_df.withColumn("timestamp", current_timestamp())
final_df.show()

# Запис у gold/avg_stats
output_path = "gold/avg_stats"
final_df.write.mode("overwrite").parquet(output_path)

print("Gold data successfully created at gold/avg_stats.")
spark.stop()