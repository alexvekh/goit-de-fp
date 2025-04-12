### goit-de-fp

# 1.  Streaming Pipeline with PySpark

This project demonstrates a real-time data processing pipeline using Apache Spark Structured Streaming. The goal is to process and aggregate Olympic athlete data streamed from a MySQL source and output real-time analytics based on sport, medal type, gender, and country.

### 🗂️Project File Flow
- *config.py* – Stores shared configs: MySQL, Kafka, table and topic names.
- *create_topic.py* – Creates the Kafka topic for streaming data.
- *create_table.py* – Creates the MySQL table to store aggregated results.
- *jdbc_to_kafka.py* – Reads data from MySQL and sends it to Kafka as JSON.
- *kafka_to_pipeline.py* – Spark stream: reads Kafka, aggregates, writes to MySQL.
- *read_table.py* – Reads and displays the aggregated MySQL table for verification.

### 🔧Technologies Used
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



# 2. Batch Datalake Pipeline with Apache Spark

### Project Overview

This project implements a multi-hop data pipeline using Apache Spark and Apache Airflow, designed to process large volumes of batch data in a scalable and modular way. The pipeline follows the classic Data Lake architecture using three layers: Landing (Raw) → Bronze → Silver → Gold.

In this project, we work with athlete-related datasets:

- athlete_bio.csv
- athlete_event_results.csv

This ETL process is orchestrated with Apache Airflow.

### 🔧 Technologies Used
- Apache Spark: Distributed data processing
- Apache Airflow: Workflow orchestration
- HDFS / S3 / Local FS: Data storage layers
- Git: Version control
- Python: DAG orchestration and transformations

### Pipeline Overview
- *Landing to Bronze*

    Raw data is ingested from source systems and stored in the Bronze layer with minimal transformation.

- *Bronze to Silver*

    Data is cleaned, validated, and enriched, preparing it for analytics or machine learning.

- *Silver to Gold*

    Business-level aggregations and optimized tables are created for dashboards and BI tools.

### 🛠️ How It Works
Each transformation step is written as a standalone Spark application.
Tasks are orchestrated with Airflow’s SparkSubmitOperator.
The pipeline can be triggered manually or scheduled to run daily (@daily).

### 🚀 Getting Started
Clone the repository
Set up Airflow and Spark
Configure the Airflow connection for Spark (spark-default)
Deploy DAGs via Git or upload them manually
Trigger the DAG in the Airflow UI or wait for the daily run

### 🗂️Project File Flow
- *landing_to_bronze.py* - Step 1: Load raw data to Bronze layer
- *bronze_to_silver.py* - Step 2: Clean & transform data to Silver layer
- *silver_to_gold.py* - Step 3: Aggregate & optimize data to Gold layer
- *batch_datalake_pipeline.py* - Airflow DAG to orchestrate the full pipeline