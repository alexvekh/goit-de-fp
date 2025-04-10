from pyspark.sql import SparkSession, Row
import config

SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1"
]
# Initialize Spark session
spark = SparkSession.builder \
    .appName("StreamingAthletePipeline") \
    .config("spark.jars", "jars/mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

jdbc_url = config.jdbc_url
jdbc_user = config.jdbc_user
jdbc_password = config.jdbc_password

# Прикладова схема, яка відповідає результату агрегації
sample_data = [
    Row(sport="Basketball", medal="Gold", sex="M", country_noc="USA",
        avg_height=200.0, avg_weight=95.0, timestamp="2025-04-08 00:00:00")
]

# Створення DataFrame з даними, тільки для схеми
df_schema = spark.createDataFrame(sample_data)

# Запис схеми до MySQL (без даних)
df_schema.limit(0).write.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="vekh__aggregated_results",
    user=jdbc_user,
    password=jdbc_password
).mode("overwrite").save()

print("Таблиця 'vekh__aggregated_results' створена успішно.")
