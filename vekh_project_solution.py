from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# DAG definition
default_args = {
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
}

with DAG(
    dag_id='vekh_project_solution',
    default_args=default_args,
    description='Multi-hop datalake pipeline with Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=["vekh"]
) as dag:

    # 1. Landing to Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/vekh/landing_to_bronze.py',
        conn_id='spark-default', 
        verbose=1,
    )

    # 2. Bronze to Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/vekh/bronze_to_silver.py',
        conn_id='spark_default',
        verbose=1,
    )

    # 3. Silver to Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/vekh/silver_to_gold.py',
        conn_id='spark_default',
        verbose=1,
    )

# Pipeline dependency
landing_to_bronze >> bronze_to_silver >> silver_to_gold
