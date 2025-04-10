from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import config
# import settings as set
from pyspark.sql.functions import to_json, struct, col
from kafka.admin import KafkaAdminClient, NewTopic

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
kafka_topic = "athlete_event_results"

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

# Читання даних з SQL бази даних
df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_bio,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

# Конвертуємо height і weight в числовий тип
df_cleaned = df.withColumn("height", col("height").cast("double")) \
               .withColumn("weight", col("weight").cast("double"))

# Фільтруємо рядки, де height і weight не null
df_filtered = df_cleaned.filter(
    col("height").isNotNull() & col("weight").isNotNull()
)
df_filtered.show()

# ## ------------------------------------------------

# Функція для обробки кожної партії даних
def foreach_batch_function(batch_df, batch_id):

    # Відправка збагачених даних до Kafka
    kafka_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", kafka_topic) \
        .save()

    # Збереження збагачених даних до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{musql_server}:3306/{db}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# Налаштування потоку даних для обробки кожної партії за допомогою вказаної функції
#event_stream_enriched \
#    .writeStream \
#    .foreachBatch(foreach_batch_function) \
#    .outputMode("update") \
#    .start() \
#    .awaitTermination()













# admin_client = KafkaAdminClient(
#     bootstrap_servers=config.kafka_url,
#     security_protocol=config.security_protocol,
#     sasl_mechanism=config.sasl_mechanism,
#     sasl_plain_username=config.kafka_user,
#     sasl_plain_password=config.kafka_password
# )
#
# # Визначення нового топіку
# topic_name = kafka_topic
# num_partitions = 2
# replication_factor = 1
#
# new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
#
# # Створення нового топіку
# try:
#     admin_client.create_topics(new_topics=[new_topic], validate_only=False)
#     print(f"Topic '{topic_name}' created successfully.")
# except Exception as e:
#     print(f"An error occurred: {e}")
#
# # Перевіряємо список існуючих топіків
# print(admin_client.list_topics())
#
# # Закриття зв'язку з клієнтом
# admin_client.close()
## ------------------------------------------------------



producer.close()  # Закриття producer

# Запис у kafka
from kafka import KafkaProducer
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=config.kafka_url,
    security_protocol=config.security_protocol,
    sasl_mechanism=config.sasl_mechanism,
    sasl_plain_username=config.kafka_user,
    sasl_plain_password=config.kafka_password,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = kafka_topic

for i in range(30):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "value": random.randint(1, 100)  # Випадкове значення
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")


###  -------------------------"
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.5.1,"
            "org.apache.commons:commons-pool2:2.11.1") \
    .getOrCreate()

df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.kafka_url) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", config.security_protocol) \
    .option("kafka.sasl.mechanism", config.sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.kafka_user}" password="{config.kafka_password}";') \
    .load()

# Десеріалізація JSON-рядка
schema = StructType() \
    .add("timestamp", DoubleType()) \
    .add("value", IntegerType())

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_parsed.show()


##-------------------------------------------------------







# “.format("kafka") \\ .option("kafka.bootstrap.servers", "localhost:9092") \\ .option("subscribe", "athlete\_event\_results") \\ .load()”


#
# df_kafka_ready = df_filtered.select(to_json(struct("*")).alias("value"))
#
# df_kafka_ready.write \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "athlete_event_results") \
#     .save()
#
# # Зчитати дані з Kafka
# df_kafka = spark.read \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "athlete_event_results") \
#     .load()
#
#
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
#
# # Перетворити JSON
# schema = StructType([
#     StructField("ID", IntegerType()),
#     StructField("Name", StringType()),
#     StructField("Sex", StringType()),
#     StructField("Age", IntegerType()),
#     StructField("Height", DoubleType()),
#     StructField("Weight", DoubleType()),
#     StructField("Team", StringType()),
#     StructField("NOC", StringType()),
#     StructField("Games", StringType()),
#     StructField("Year", IntegerType()),
#     StructField("Season", StringType()),
#     StructField("City", StringType()),
#     StructField("Sport", StringType()),
#     StructField("Event", StringType()),
#     StructField("Medal", StringType())
# ])
#
# df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")
#
# df_json.show(5)


df_results = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=driver,
    dbtable=jdbc_result,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df_results.show()

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 🔧 Ініціалізація Spark
spark = SparkSession.builder \
    .appName("KafkaFanOutPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 📦 Схема JSON, який читаємо з Kafka (підлаштуй під свої дані)
json_schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("value", LongType())
])

# 🧾 Зчитування даних з Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "incoming_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 📥 Обробка даних
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), json_schema)) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())

# 💾 Функція FanOut-запису
def foreach_batch_function(batch_df, batch_id):
    # 1️⃣ Запис до Kafka (інший топік)
    batch_df.selectExpr("CAST(value AS STRING) as key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker1:9092") \
        .option("topic", "processed_topic") \
        .save()

    # 2️⃣ Запис до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/mydatabase") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "processed_data") \
        .option("user", "your_user") \
        .option("password", "your_password") \
        .mode("append") \
        .save()

# ⚙️ Запуск стріму з використанням forEachBatch
query = df_parsed.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

query.awaitTermination()
