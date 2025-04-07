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

# # Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "jars/mysql-connector-j-8.0.32.jar") \
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
df.show()

# Конвертуємо height і weight в числовий тип
df_cleaned = df.withColumn("height", col("height").cast("double")) \
               .withColumn("weight", col("weight").cast("double"))

# Фільтруємо рядки, де height і weight не null
df_filtered = df_cleaned.filter(
    col("height").isNotNull() & col("weight").isNotNull()
)
df_filtered.show()

# ## ------------------------------------------------
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

producer.close()  # Закриття producer



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
