# goit-de-fp


## 1. Building an End-to-End Streaming Pipeline 
Ваша задача:
1. Зчитати дані фізичних показників атлетів за допомогою Spark із MySQL таблиці.
2. Відфільтрувати дані.
3. Зчитати дані з результатами змагань з Kafka-топіку.
4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці.
5. Зробити певні трансформації в даних.
6. Зробити стрим даних (за допомогою функції forEachBatch) у:
    а) вихідний кафка-топік,
    b) базу даних.

Ваша задача:
1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio (база даних і Credentials до неї вам будуть надані).
2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами. Можна це зробити на будь-якому етапі вашої програми.
3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
  - а) вихідний кафка-топік,
  - b) базу даних.

# Streaming with PySpark

This project demonstrates a real-time data processing pipeline using Apache Spark Structured Streaming. The goal is to process and aggregate Olympic athlete data streamed from a MySQL source and output real-time analytics based on sport, medal type, gender, and country.

### Project File Flow
- **config.py** – Stores shared configs: MySQL, Kafka, table and topic names.
- **create_topic.py** – Creates the Kafka topic for streaming data.
- **create_table.py** – Creates the MySQL table to store aggregated results.
- **jdbc_to_kafka.py** – Reads data from MySQL and sends it to Kafka as JSON.
- **kafka_to_pipeline.py** – Spark stream: reads Kafka, aggregates, writes to MySQL.
- **read_table.py** – Reads and displays the aggregated MySQL table for verification.

### Technologies Used
- Apache Spark (Structured Streaming)
- MySQL
- PySpark
- Pandas (for optional display and post-processing)
- JDBC (for database connection)

### Key Features
- Ingests real-time data from a MySQL table.
- Joins streaming data with static reference data (e.g., athlete info).
- Aggregates the data by sport, medal, sex, and country_noc.
- Calculates average height and weight with 3-decimal precision.
- Outputs processed data to the console or writes to MySQL.
- Uses watermarking to handle late-arriving data.

Example Aggregation 


    df_aggregated = df_joined \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            round(avg("height"), 3).alias("avg_height"),
            round(avg("weight"), 3).alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )

Streaming Output Example

        df_aggregated.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start() \
            .awaitTermination()


Sample Query from MySQL Table

        df_mysql = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "vekh__aggregated_results") \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .load()
        df_mysql.show(truncate=False)


### Requirements
- Python 3.9+ (recommended)
- Java 8/11
- Spark 3.x
- MySQL server running
- pandas, pyspark, mysql-connector-python



# 2. Building an End-to-End Batch Data Lake

### Project Overview
In this project, we work with athlete-related datasets:

- athlete_bio.csv
- athlete_event_results.csv

The goal is to build a three-layer data lake architecture using batch data processing with Apache Spark. The pipeline consists of:

- Landing Zone: raw CSV files are downloaded from the FTP server.
- Bronze/Silver Layers: data is cleaned, deduplicated, and transformed into Parquet format.
- Gold Layer: final analytical dataset is produced by merging and enriching the data.

This ETL process is orchestrated with Apache Airflow.