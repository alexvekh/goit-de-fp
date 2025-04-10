from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json, to_json, struct
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
# Read competition results from MySQL and send to Kafka (one-time batch operation)
df_results = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password
).load()

print("Етап 3a. Запис в кафка топік")
df_results.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", kafka_topic_input) \
    .option("kafka.security.protocol", security_protocol) \
    .option("kafka.sasl.mechanism", sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
    .save()

spark.stop()