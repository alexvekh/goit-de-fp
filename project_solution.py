# Спершу варто протестувати оператор SparkSubmitOperator на якомусь дуже простому spark скрипті.
# Щоб упевнитися, що він працює, необхідно перевірити логи.
# Spark jobs мають бути додані інкрементально, одна за одною, — для того, щоб упевнитись, що вони також правильно функціонують.

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# DAG definition
default_args = {
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='batch_datalake_pipeline',
    default_args=default_args,
    description='Multi-hop datalake pipeline with Spark',
    schedule_interval=None,  # Ти можеш задати розклад пізніше
    catchup=False,
)

# 1. Landing to Bronze
landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/alex/landing_to_bronze.py',  # Вкажи правильний шлях
    conn_id='spark_default',  # Spark connection ID в Airflow
    verbose=1,
    dag=dag,
)

# 2. Bronze to Silver
bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/alex/bronze_to_silver.py',
    conn_id='spark_default',
    verbose=1,
    dag=dag,
)

# 3. Silver to Gold
silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/alex/silver_to_gold.py',
    conn_id='spark_default',
    verbose=1,
    dag=dag,
)

# Pipeline dependency
landing_to_bronze >> bronze_to_silver >> silver_to_gold
