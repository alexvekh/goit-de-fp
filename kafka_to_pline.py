from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, current_timestamp, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import config

# MySQL and Kafka configuration
jdbc_url = config.jdbc_url
jdbc_user = config.jdbc_user
jdbc_password = config.jdbc_password
kafka_bootstrap = config.kafka_url
kafka_user = config.kafka_user
kafka_password = config.kafka_password
security_protocol = config.security_protocol
sasl_mechanism = config.sasl_mechanism
kafka_topic_input = "vekh__athlete_event_results"
kafka_topic_output = "vekh__aggregated_results"
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

print("Етап 1. Читання з даних з SQL")
# Read athlete biographical data from MySQL
df_bio = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()

print("Етап 2. Очищення")
# Filter rows with valid height and weight values
df_bio_clean = df_bio.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
)
df_bio_clean = df_bio_clean.withColumn("athlete_id", col("athlete_id").cast("int"))
df_bio_clean = df_bio_clean.withColumn("height", col("height").cast("double"))
df_bio_clean = df_bio_clean.withColumn("weight", col("weight").cast("double"))

# Робиться в окремому файлi: jdbc_to_kafka.py
# # Read competition results from MySQL and send to Kafka (one-time batch operation)
# df_results = spark.read.format('jdbc').options(
#     url=jdbc_url,
#     driver='com.mysql.cj.jdbc.Driver',
#     dbtable="athlete_event_results",
#     user=jdbc_user,
#     password=jdbc_password
# ).load()

# print("Етап 3a. Запис в кафка топік")
# df_results.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
#     .write.format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap) \
#     .option("topic", kafka_topic_input) \
#     .option("kafka.security.protocol", security_protocol) \
#     .option("kafka.sasl.mechanism", sasl_mechanism) \
#     .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
#     .save()

print("Етап 3b. Читання з кафка топіку")
# Define schema for parsing JSON from Kafka
schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", StringType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

# Read Kafka topic as streaming source
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic_input) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", security_protocol) \
    .option("kafka.sasl.mechanism", sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
    .load()

print("Етап 3c. Парсiнг таблицi:")
# Deserialize JSON messages from Kafka
df_kafka_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast("int"))

df_kafka_json.printSchema()
df_bio_clean.printSchema()

print("Етап 4. join таблиць")
# Join streaming data with athlete bio data
df_bio_clean = df_bio_clean.drop("country_noc")
df_joined = df_kafka_json.join(df_bio_clean, on="athlete_id", how="inner")
print(df_joined.columns)


print("Етап 5. Агрегацiя")
# Aggregate average height/weight by sport, medal, sex, and country
df_aggregated = df_joined.groupBy("sport", "medal", "sex", "country_noc").agg(
    round(avg("height"), 3).alias("avg_height"),
    round(avg("weight"), 3).alias("avg_weight"),
    current_timestamp().alias("timestamp")
)
print(df_aggregated.columns)

print("Етап 6. Стрим результату")
# Function to write batch output to Kafka and MySQL
def write_to_kafka_and_mysql(batch_df, batch_id):
    print(f"\n--- Processing batch {batch_id} ---")
    if batch_df.count() > 0:
        try:
            # Write to Kafka
            batch_df.selectExpr("to_json(struct(*)) AS value") \
                .write.format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap) \
                .option("topic", kafka_topic_output) \
                .option("kafka.security.protocol", security_protocol) \
                .option("kafka.sasl.mechanism", sasl_mechanism) \
                .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
                .save()
            # Optionally write to MySQL (for persistent storage)
            batch_df.write.format("jdbc").options(
                url=jdbc_url,
                driver="com.mysql.cj.jdbc.Driver",
                dbtable="vekh__aggregated_results",
                user=jdbc_user,
                password=jdbc_password
            ).mode("append").save()
        except Exception as e:
            print(f"🔥 Помилка при записі: {e}")
    else:
        print("⚠️ Порожній batch")


# Start the stream and write output periodically
df_aggregated.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .foreachBatch(write_to_kafka_and_mysql) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoint/stream_join_aggregation") \
    .start() \
    .awaitTermination()

