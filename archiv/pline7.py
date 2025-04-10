from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import to_json, struct, col
from kafka.admin import KafkaAdminClient, NewTopic
import config


# config.py under .gitignore
jdbc_url = config.jdbc_url
jdbc_user = config.jdbc_user
jdbc_password = config.jdbc_password
driver = 'com.mysql.cj.jdbc.Driver'
jdbc_bio = "athlete_bio"
jdbc_result = "athlete_event_results"
kafka_bootstrap = config.kafka_url
kafka_user = config.kafka_user
kafka_password = config.kafka_password
kafka_topic = "vekh__athlete_event_results"

#
SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5"
]
SPARK_JARS = [
    "jars/mysql-connector-j-8.0.32.jar"  # локальний шлях
]
spark = SparkSession.builder \
    .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
    .config("spark.jars", ",".join(SPARK_JARS)) \
    .appName("JDBCToKafka") \
    .getOrCreate()

print("Читання з даних з SQL")
# Читання даних з SQL бази даних
df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_bio,
    user=jdbc_user,
    password=jdbc_password) \
    .load()
# df.show()

print("Очищення")
# Конвертуємо height і weight в числовий тип
df_cleaned = df.withColumn("height", col("height").cast("double")) \
               .withColumn("weight", col("weight").cast("double"))

# Фільтруємо рядки, де height і weight не null
df_filtered = df_cleaned.filter(
    col("height").isNotNull() & col("weight").isNotNull()
)
# df_filtered.show()

# Готуємо дані для Kafka

df_to_kafka = df_filtered.withColumn("value", to_json(struct([col(c) for c in df_filtered.columns]))).select("value")

# df_to_kafka.show()
# df_to_kafka.selectExpr("CAST(value AS STRING) as json_str").limit(5).show(truncate=False)

# Запис у Kafka
print("Запис в кафка топік")
df_to_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", kafka_topic) \
    .option("kafka.security.protocol", config.security_protocol) \
    .option("kafka.sasl.mechanism", config.sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
    .save()

print("     Запис завершено")


# (WORKING - NOT NEED )# Назва топіку
# topic_name = kafka_topic
# df_to_kafka.selectExpr("CAST(uuid() AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .write \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", config.kafka_url) \
#     .option("kafka.security.protocol", config.security_protocol) \
#     .option("kafka.sasl.mechanism", config.sasl_mechanism) \
#     .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.kafka_user}" password="{config.kafka_password}";') \
#     .option("topic", kafka_topic) \
#     .save()



print("Читання з кафка топіку")
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.kafka_url) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", config.security_protocol) \
    .option("kafka.sasl.mechanism", config.sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.kafka_user}" password="{config.kafka_password}";') \
    .load()
print("      Читання з кафка топіку завершено")

# df_raw.selectExpr("CAST(value AS STRING) as json_str").limit(5).show(truncate=False)


# Схема, яка відповідає JSON
schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("born", StringType(), True),
    StructField("height", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("description", StringType(), True),
    StructField("special_notes", StringType(), True)
])



df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# from pyspark.sql.functions import to_date
# df_parsed = df_parsed.withColumn("born_clean", to_date(col("born"), "dMMMMuuuu"))

print("Розпарсено таблицю:")
df_parsed.limit(5).show(truncate=False)


df_results = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_result,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df_results.show()

print("join таблиць")
df_joined = df_results.join(df_parsed, on="athlete_id", how="inner")
df_joined.select("athlete_id", "name", "sex", "height", "weight", "born", "event", "medal").show(truncate=False)
df_joined.show()

print("Агрегацiя таблиць")
df_summary = df_joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("calculated_at", current_timestamp())

df_summary = df_summary.dropDuplicates()
df_summary.show(truncate=False)


spark.stop()
