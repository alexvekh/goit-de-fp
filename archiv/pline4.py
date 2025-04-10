from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from kafka.admin import KafkaAdminClient, NewTopic
import config  # файл config.py, має бути в .gitignore

# --- Конфігурації ---
jdbc_url = config.jdbc_url
jdbc_user = config.jdbc_user
jdbc_password = config.jdbc_password
driver = 'com.mysql.cj.jdbc.Driver'

jdbc_bio = "athlete_bio"
jdbc_result = "athlete_event_results"

kafka_bootstrap = config.kafka_url
kafka_user = config.kafka_user
kafka_password = config.kafka_password
kafka_topic = "athlete_event_results"

kafka_security = {
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_user}' password='{kafka_password}';"
}
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

# --- Зчитування з MySQL ---
df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_bio,
    user=jdbc_user,
    password=jdbc_password
).load()

# --- Чистка даних ---
df_cleaned = df.withColumn("height", col("height").cast("double")) \
               .withColumn("weight", col("weight").cast("double"))
df_filtered = df_cleaned.filter(
    col("height").isNotNull() & col("weight").isNotNull()
)

df_filtered.show()






# --- Запис у Kafka ---
df_filtered.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", kafka_topic) \
    .options(**kafka_security) \
    .save()

# --- Читання з Kafka ---
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .options(**kafka_security) \
    .load()


# --- Десеріалізація JSON з Kafka ---
schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("name", StringType()),
    StructField("sex", StringType()),
    StructField("born", IntegerType()),
    StructField("height", DoubleType()),
    StructField("weight", DoubleType()),
    StructField("country", StringType()),
    StructField("country_noc", StringType()),
    StructField("description", StringType()),
    StructField("special_notes", StringType())
])

print("Кількість рядків:", df_json.count())

df_json = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_json.show(5)



# --- Читання результатів з іншої таблиці ---
df_results = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_result,
    user=jdbc_user,
    password=jdbc_password
).load()

df_results.show()

spark.stop()
