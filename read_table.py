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


# Читання таблиці з MySQL
df_mysql = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "vekh__aggregated_results") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load()

# Перевірка даних
df_mysql.show()