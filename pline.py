from pyspark.sql import SparkSession

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "username"
jdbc_password = "password"

# # Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# Читання даних з SQL бази даних
df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

df.show()
